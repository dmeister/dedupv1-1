/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
 *
 * This file is part of dedupv1.
 *
 * dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
 */

#include "dedupv1_checker.h"

#include <iostream>
#ifndef NO_SYS_SYSINFO_H
#include <sys/sysinfo.h>
#include <sys/resource.h>
#endif
#include <stdint.h>
#include <tr1/unordered_set>
#include <map>
#include <limits>

#include <base/base.h>
#include <core/block_index.h>
#include <core/block_mapping.h>
#include <core/chunk_index.h>
#include <core/garbage_collector.h>
#include <core/container.h>
#include <core/container_storage.h>
#include <core/dedup_system.h>
#include <base/hashing_util.h>
#include <base/index.h>
#include <base/logging.h>
#include <base/strutil.h>
#include <base/startup.h>
#include <base/memory.h>
#include <core/fingerprinter.h>
#include <base/bitutil.h>
#include <core/chunk.h>
#include <base/bitutil.h>

#include <google/sparse_hash_map>
#include <google/sparse_hash_set>
#include <google/sparsetable>
#include <dedupv1.pb.h>

using std::list;
using std::set;
using std::pair;
using std::make_pair;
using std::vector;
using dedupv1::DedupSystem;
using dedupv1::StartContext;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::chunkstore::Storage;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::gc::GarbageCollector;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::blockindex::BlockIndex;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexIterator;
using dedupv1::base::ScopedPtr;
using dedupv1d::Dedupv1d;
using dedupv1::log::Log;
using dedupv1::base::Option;
using dedupv1::FileMode;
using dedupv1::Fingerprinter;
using dedupv1::base::bits;
using dedupv1::base::ScopedArray;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::raw_compare;
using dedupv1::Chunk;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using google::sparse_hash_map;
using google::sparse_hash_set;
using google::sparsetable;
using std::tr1::unordered_map;
using std::tr1::unordered_set;
using std::map;
using dedupv1::Session;
using dedupv1::Request;
using dedupv1::REQUEST_READ;

LOGGER("Dedupv1Checker");

namespace dedupv1 {
namespace contrib {
namespace check {

Dedupv1Checker::Dedupv1Checker(bool check_log_only, bool repair) {
    system_ = NULL;
    dedup_system_ = NULL;
    started_ = false;
    reported_errors_ = 0;
    fixed_errors_ = 0;
    check_log_only_ = check_log_only;
    repair_ = repair;
    run_passes_ = 0;
    actual_run_pass_ = 0;
    all_pass_processed_chunks_ = 0;
    all_pass_skipped_chunks_ = 0;
}

bool Dedupv1Checker::Initialize(const std::string& filename) {
    CHECK(!started_, "Dedupv1 check already started");
    system_ = new Dedupv1d();
    CHECK(system_->LoadOptions(filename), "Error loading options");
    CHECK(system_->OpenLockfile(), "Failed to acquire lock on lockfile");
    StartContext start_context(StartContext::NON_CREATE, StartContext::CLEAN, StartContext::FORCE);

    CHECK(system_->Start(start_context, true), // no log replay, we wait for the log check
        "Failed to start dedupv1 system");
    CHECK(system_->dedup_system()->idle_detector()->ForceBusy(true), "Could not force busy");
    dedup_system_ = system_->dedup_system();

    started_ = true;
    return true;
}

bool Dedupv1Checker::ReplayLog() {
    DCHECK(started_, "Dedupv1 check not started");
    DCHECK(dedup_system_ != NULL, "Dedup System is null");

    if (this->system_->start_context().dirty()) {
        // we need to perform a dirty replay as it is not done in Initialize
        dedupv1::log::Log* log = this->dedup_system_->log();
        CHECK(log, "Log not set");

        INFO("System is dirty: Full log replay");
        CHECK(log->PerformDirtyReplay(), "Crash replay failed");
    }

    // We run the block index and the chunk index to fasten up the importing process
    // we cannot run the complete system because the background processes, e.g. the gc change a lot of
    // state.
    CHECK(dedup_system_->block_index()->Run(), "Failed to run block index");
    CHECK(dedup_system_->chunk_index()->Run(), "Failed to run chunk index");

    // The idea behind doing both replays (on in Start if the system is dirty) directly after each other is that it is easier
    // to program the background replay if the state is already in memory as it is during a usual replay
    // instead of having to think about an additional special case.

    CHECK(
        system_->dedup_system()->log()->PerformFullReplayBackgroundMode(),
        "Failed to perform full replay");
    return true;

}

bool Dedupv1Checker::CalcPasses() {
    uint64_t ram = 0;
#ifdef NO_SYS_SYSINFO_H
    // always use one pass on mac, sysinfo calls are not available on Mac
    ram = std::numeric_limits<uint64_t>::max();
#else
    struct sysinfo info;
    struct rlimit64 limit;
    memset(&info, 0, sizeof(info));
    sysinfo(&info);
    ram = info.totalram;
    if (ram < (16 * 1024 * 1024)) {
        ERROR("This machine seems to have less then 16 MB of RAM.");
        return false;
    }
    memset(&limit, 0, sizeof(limit));
    getrlimit64(RLIMIT_AS, &limit);
    if ((limit.rlim_cur > 0) && (limit.rlim_cur < ram)) {
        ram = limit.rlim_cur;
    }
#endif
    // TODO(fermat): Perhaps we should also check other limits: RLIMIT_DATA, RLIMIT_STACK, RLIMIT_RSS
    // TODO(fermat): We can substract the amount of data needed RAM for the indices.

    // We try never to take more then half of the RAM, so divergations in the number of Chunks
    // in the different passes will not hurt us.
    ram <<= 1;
    uint64_t max_chunks_per_pass = ram / kChunkSize;
    uint64_t persistent_chunks = dedup_system_->chunk_index()->GetPersistentCount();
    this->run_passes_ = 1;
    this->pass_bitmask_ = 0;
    while ((max_chunks_per_pass * this->run_passes_) < persistent_chunks) {
        this->run_passes_ >>= 1;
        this->pass_bitmask_ >>= 1;
        this->pass_bitmask_++;
    }
    this->actual_run_pass_ = 0;
    return true;
}

bool Dedupv1Checker::Check() {
    DCHECK(started_, "Dedupv1 check not started");
    DCHECK(dedup_system_ != NULL, "Dedup System is null");

    all_pass_processed_chunks_ = 0;
    all_pass_skipped_chunks_ = 0;

    if (check_log_only_) {
        return true;
    }
    CHECK(ReplayLog(), "Failed to replay the log");

    if (this->run_passes_ == 0) {
        CHECK(CalcPasses(), "Failed to calculate the number of passes");
    }
    DEBUG("Will run in " << this->run_passes_ << " passes with bitmask " << this->pass_bitmask_);

    bool failed = false;

    // normal checks
    INFO("Step 1");
    if (!ReadContainerData()) {
        ERROR("Failed to check container storage");
        failed = true;
    }

    while (this->actual_run_pass_ < this->run_passes_) {
        uint32_t pass = this->actual_run_pass_ + 1; // just for output

        DEBUG("Starting pass " << pass << " of " << this->run_passes_);

        INFO("Step 2 (" << pass << "/" << run_passes_ << ")");
        if (!ReadBlockIndex()) {
            ERROR("Failed to check block index");
            failed = true;
        }

        INFO("Step 3 (" << pass << "/" << run_passes_ << ")");
        if (!ReadChunkIndex()) {
            ERROR("Failed to check chunk index");
            failed = true;
        }
        DEBUG("Finished pass " << pass << " of " << this->run_passes_);
        this->actual_run_pass_++;
    }
    return !failed;
}

bool Dedupv1Checker::ReadBlockIndex() {
    CHECK(started_, "Chunk index restorer not started");
    DCHECK(dedup_system_, "Dedup system not set");
    ChunkIndex* chunk_index = dedup_system_->chunk_index();
    DCHECK(chunk_index, "Chunk index not set");

    // Iterate over the block index to get the usage count.
    BlockIndex* block_index = dedup_system_->block_index();
    CHECK(block_index, "Dedup System block index NULL");
    PersistentIndex* persistent_block_index = block_index->persistent_block_index();
    CHECK(persistent_block_index, "Persistent Block Index NULL");

    IndexIterator* iter = persistent_block_index->CreateIterator();
    CHECK(iter, "Index iterator was NULL");
    ScopedPtr<IndexIterator> scoped_iter(iter);

    ScopedArray<byte> block_buffer(new byte[dedup_system_->block_size()]);

    uint64_t total_block_count = persistent_block_index->GetItemCount();
    uint64_t processed_block_count = 0;
    int last_full_percent_progress = 0;

    BlockMappingData block_mapping_data;
    uint64_t key;
    size_t key_size = sizeof(key);
    lookup_result lr = iter->Next(&key, &key_size, &block_mapping_data);
    for (; lr == LOOKUP_FOUND; lr = iter->Next(&key, &key_size, &block_mapping_data)) {
        BlockMapping block_mapping(key, dedup_system_->block_size());
        CHECK(block_mapping.CopyFrom(
                block_mapping_data), "Failed to create block mapping from data: " <<
            block_mapping_data.ShortDebugString());

        DEBUG("Process block: " << block_mapping.DebugString());
        processed_block_count++;

        // Iterate over the block mapping items.
        list<BlockMappingItem>::iterator j;
        for (j = block_mapping.items().begin(); j != block_mapping.items().end(); j++) {
            BlockMappingItem& item = *j;

            // here we have problems with fingerprints shorter than 64-bit
            // but then we would have much more problems
            uint64_t prefix = *reinterpret_cast<const uint64_t*>(item.fingerprint());

            // We only have a look at chunks fitting the actual pass
            if ((prefix & this->pass_bitmask_) == this->actual_run_pass_) {
                // Get the corresponding mapping from the chunk index.
                ChunkMapping mapping(item.fingerprint(), item.fingerprint_size());
                mapping.set_data_address(item.data_address());

                if (Fingerprinter::IsEmptyDataFingerprint(mapping.fingerprint(),
                        mapping.fingerprint_size())) {
                    // the empty fingerprint will not be in the chunk index and we do not
                    // count the usage of it.
                    continue;
                }

                Option<bool> b = chunk_index->sampling_strategy()->IsAnchor(mapping);
                CHECK(b.valid(), "Failed to check sampling state");
                if (!b.value()) {
                    continue;
                }

                lookup_result result = chunk_index->Lookup(&mapping, NO_EC);
                if (result != LOOKUP_FOUND) {
                    WARNING(
                        "Block mapping not found in chunk index: " << "chunk mapping " <<
                        mapping.DebugString()
                                                                   << ", block mapping " <<
                        block_mapping.DebugString() << " result: " << result);
                    reported_errors_++;
                }
            }
        }

        // Report progress
        double ratio = (100.0 * processed_block_count) / total_block_count;
        if (ratio < 0.0) {
            ratio = 0.;
        }
        if (ratio > 100.0) {
            ratio = 100.0;
        }
        if (ratio >= last_full_percent_progress + 1) {
            last_full_percent_progress = ratio; // implicit cast
            INFO("Step 2: " << last_full_percent_progress << "%");
        }
    }
    CHECK(lr != LOOKUP_ERROR, "Failed to iterator over block index");
    return true;
}

bool Dedupv1Checker::CheckContainerItem(ChunkIndex* chunk_index,
    Fingerprinter* fp_gen,
    Container* container,
    const ContainerItem* item) {
    DCHECK(item, "Item not set");
    DCHECK(fp_gen, "Fingerprinter not set");
    DCHECK(chunk_index, "Chunk index not set");

    ChunkMapping mapping(item->key(), item->key_size());
    Option<bool> b = chunk_index->sampling_strategy()->IsAnchor(mapping);
    CHECK(b.valid(), "Failed to check sampling state");
    if (b.value()) {
        // chunk is anchor
        lookup_result result = chunk_index->Lookup(&mapping, NO_EC);
        CHECK(result != LOOKUP_ERROR, "Failed to lookup chunk mapping: " << mapping.DebugString());
        if (result != LOOKUP_FOUND) {

            WARNING("Container item not found in chunk index: " <<
                "item " << item->DebugString() <<
                ", chunk " << mapping.DebugString() <<
                ", result: " << result);

            // We are able to repair this by adding the mapping to the index, but
            // until we have no way to fix the usage count, this would not help.

            reported_errors_++;
            return true;
        }
    }

    size_t chunk_data_buffer_size = item->raw_size();
    byte chunk_data_buffer[chunk_data_buffer_size];

    CHECK(container->CopyRawData(item, chunk_data_buffer, 0, chunk_data_buffer_size),
        "Failed to copy item data: " << item->DebugString());

    byte fp[fp_gen->GetFingerprintSize()];
    memset(fp, 0, fp_gen->GetFingerprintSize());
    size_t fp_size = fp_gen->GetFingerprintSize();
    CHECK(fp_gen->Fingerprint(chunk_data_buffer, item->raw_size(), fp, &fp_size),
        "Failed to calculate fingerprint: item " << item->DebugString());

    if (raw_compare(fp, fp_size, item->key(), item->key_size()) != 0) {
        WARNING("Fingerprint mismatch: " << item->DebugString() <<
            ", calculated data fingerprint: " << ToHexString(fp,
                fp_size));
        reported_errors_++;
    }

    return true;
}

bool Dedupv1Checker::ReadChunkIndex() {
    DCHECK(dedup_system_, "Dedup system not set");
    ChunkIndex* chunk_index = dedup_system_->chunk_index();
    DCHECK(chunk_index, "Chunk index not set");
    Storage* tmp_storage = dedup_system_->storage();
    DCHECK(tmp_storage, "Dedup System storage NULL");
    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(tmp_storage);
    DCHECK(storage, "Storage was not a container storage while restoring");

    IndexIterator* i = chunk_index->CreatePersistentIterator();
    DCHECK(i, "Failed to get iterator");
    ScopedPtr<IndexIterator> scoped_iterator(i);

    uint64_t total_chunk_count = chunk_index->GetPersistentCount();
    uint64_t processed_chunk_count = 0;
    int last_full_percent_progress = 0;

    // here we get the address without redirection to the primary container id
    byte fp[Fingerprinter::kMaxFingerprintSize];
    size_t fp_size = Fingerprinter::kMaxFingerprintSize;
    ChunkMappingData chunk_data;
    lookup_result lr = i->Next(fp, &fp_size, &chunk_data);
    while (lr == LOOKUP_FOUND) {
        uint64_t prefix = 0;
        memcpy(&prefix, fp, sizeof(prefix));
        // We only have a look at chunks fitting the actual pass
        processed_chunk_count++;

        if ((prefix & this->pass_bitmask_) == this->actual_run_pass_) {
            all_pass_processed_chunks_++;
            ChunkMapping chunk_mapping(fp, fp_size);
            CHECK(chunk_mapping.UnserializeFrom(chunk_data,
                    false), "Failed to process chunk mapping data: " <<
                chunk_data.ShortDebugString());

            TRACE("Process chunk: " << chunk_mapping.DebugString());

            Container container(chunk_mapping.data_address(),
                storage->container_size(), true);
            lookup_result read_result = storage->ReadContainer(&container, false);
            CHECK(read_result != LOOKUP_ERROR,
                "Failed to read container " << chunk_mapping.data_address());
            if (read_result == LOOKUP_NOT_FOUND) {
                WARNING("Failed to find container for chunk mapping: " << "chunk mapping "
                                                                       << chunk_mapping.DebugString(
                        ) << ", container " << container.DebugString());
                reported_errors_++;
            }
            // read_result == LOOKUP_FOUND

            ContainerItem* item = container.FindItem(
                chunk_mapping.fingerprint(), chunk_mapping.fingerprint_size(),
                true);
            if (item == NULL) {
                WARNING(
                    "Failed to find chunk in container for chunk mapping: " << "chunk mapping "
                                                                            << chunk_mapping.
                    DebugString() << ", container " << container.DebugString());
                reported_errors_++;
            }
        } else {
            all_pass_skipped_chunks_++;
        }

        // Report progress
        double ratio = (100.0 * processed_chunk_count) / total_chunk_count;
        if (ratio >= last_full_percent_progress + 1) {
            last_full_percent_progress = ratio; // implicit cast
            INFO("Step 3: " << last_full_percent_progress << "%");
        }

        lr = i->Next(fp, &fp_size, &chunk_data);
    }

    CHECK(lr != LOOKUP_ERROR, "Failed to get container id");
    return true;
}

bool Dedupv1Checker::ReadContainerData() {
    DCHECK(dedup_system_, "Dedup system not set");
    ChunkIndex* chunk_index = dedup_system_->chunk_index();
    DCHECK(chunk_index, "Chunk index not set");
    Storage* tmp_storage = dedup_system_->storage();
    DCHECK(tmp_storage, "Dedup System storage NULL");
    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(tmp_storage);
    DCHECK(storage, "Storage was not a container storage while restoring");
    Fingerprinter* fp_gen = Fingerprinter::Factory().Create(
        dedup_system_->content_storage()->fingerprinter_name());
    CHECK(fp_gen, "Failed to create fingerprinter");

    IndexIterator* i = storage->meta_data_index()->CreateIterator();
    DCHECK(i, "Failed to get iterator");
    ScopedPtr<IndexIterator> scoped_iterator(i);

    // Have a vector<bool> for all duplicate (i.e. secondary) ids so that we don't read
    // those containers also. Note that a vector<bool> only consumes 1 bit per field.
    vector<bool> duplicate_ids(storage->meta_data_index()->GetItemCount(), false);

    // key: primary id, vector all matching secondary ids
    // TODO (dmeister): Depending on the usage pattern (currently we
    // do not have enough experience with really large systems, this map
    // might not fit in memory. An alternative implementation would be to XOR every
    // secondary id and later compare with the XORed secondary ids
    sparse_hash_map<uint64_t, sparse_hash_set<uint64_t> > redirecting_map;

    // here we get the address without redirection to the primary container id
    uint64_t container_id;
    size_t key_size = sizeof(container_id);
    ContainerStorageAddressData container_address;
    lookup_result lr = i->Next(&container_id, &key_size, &container_address);
    while (lr == LOOKUP_FOUND) {
        DEBUG("Process container id " << container_id << ": " << container_address.ShortDebugString());

        if (container_address.has_primary_id()) {
            // the container id is a secondary one
            redirecting_map[container_address.primary_id()].insert(container_id);
        } else {
            // container id is a primary id
            redirecting_map[container_id].insert(container_id);
        }
        lr = i->Next(&container_id, &key_size, &container_address);
    }
    CHECK(lr != LOOKUP_ERROR, "Failed to get address for container: container id " << container_id);

    // variables to report the progress
    uint64_t container_count = redirecting_map.size();
    uint64_t processed_container_count = 0;
    int last_full_percent_progress = 0;

    sparse_hash_map<uint64_t, sparse_hash_set<uint64_t> >::iterator redirecting_iter;
    for (redirecting_iter = redirecting_map.begin();
         redirecting_iter != redirecting_map.end();
         ++redirecting_iter, processed_container_count++) {
        container_id = redirecting_iter->first;

        ContainerStorageAddressData container_address;
        lookup_result lr = storage->meta_data_index()->Lookup(&container_id,
            key_size,
            &container_address);
        CHECK(lr == LOOKUP_FOUND,
            "Failed to find address for container id " << container_id << ", result " << lr);

        if (container_address.has_primary_id()) {
            WARNING(
                "Container id expected to be primary: " << "address " <<
                container_address.ShortDebugString()
                                                        << ", container id " << container_id);
            reported_errors_++;
        }
        // here we have the address of the primary container id

        dedupv1::base::Option<bool> address_check = storage->allocator()->IsAddressFree(
            container_address);
        CHECK(
            address_check.valid(), "Address check failed: " << container_id << ", address " <<
            container_address.DebugString())
        if (address_check.value()) {
            WARNING(
                "Address of container is declared as free: " << "address " <<
                container_address.ShortDebugString()
                                                             << ", container id " <<
                container_id);
            reported_errors_++;
        }

        std::map<uint32_t, google::sparse_hash_map<uint64_t, uint64_t> >::iterator i =
            container_address_inverse_map_.find(container_address.file_index());
        if (i != container_address_inverse_map_.end()) {
            google::sparse_hash_map<uint64_t, uint64_t>::iterator j = i->second.find(
                container_address.file_offset());
            if (j != i->second.end()) {
                WARNING(
                    "Address " << container_address.ShortDebugString() << " already used: " <<
                    "container id "
                               << container_id << ", collision container id " << j->second);
                reported_errors_++;
            }
        }
        container_address_inverse_map_[container_address.file_index()][container_address.
                                                                       file_offset()] =
            container_id;

        DEBUG("Process container " << container_id);
        // non processed container

        // Read the container.
        Container container(container_id, storage->container_size(), false);
        lookup_result read_result = storage->ReadContainer(&container, false);
        if (read_result == LOOKUP_ERROR) {
            WARNING(
                "Failed to read container " << container_id << ", address " <<
                container_address.ShortDebugString());
            reported_errors_++;
        } else if (read_result == LOOKUP_NOT_FOUND) {
            WARNING(
                "Inconsistent container meta data: container " << container_id <<
                " not found, address "
                                                               << container_address.
                ShortDebugString());
            reported_errors_++;
        } else {
            DEBUG("Read container " << container.DebugString());

            if (container_id != container.primary_id()) {
                pair<lookup_result,
                    ContainerStorageAddressData> address1 = storage->LookupContainerAddress(
                    container.primary_id(), NULL, false);
                pair<lookup_result,
                    ContainerStorageAddressData> address2 = storage->LookupContainerAddress(
                    container_id, NULL, false);
                CHECK(address1.first != LOOKUP_ERROR, "Failed to lookup container address");
                CHECK(address2.first != LOOKUP_ERROR, "Failed to lookup container address");

                WARNING(
                    "Unexpected primary container id: container " << container.DebugString()
                                                                  << ", expected primary id "
                                                                  << container_id <<
                    ", address container " << container.primary_id()
                                                                  << ", "
                                                                  << (address1.first ==
                        LOOKUP_FOUND ? address1.second.ShortDebugString() : "<not found>")
                                                                  << ", address container " <<
                    container_id << ", "
                                                                  << (address2.first ==
                        LOOKUP_FOUND ? address2.second.ShortDebugString() : "<not found>"));
                reported_errors_++;
            }
            int matched_ids = 0;
            sparse_hash_set<uint64_t>::iterator j;
            for (j = redirecting_iter->second.begin(); j != redirecting_iter->second.end(); ++j) {
                if (*j != container_id) {
                    // for all secondary ids
                    set<uint64_t>::iterator k = container.secondary_ids().find(*j);
                    if (k == container.secondary_ids().end()) {
                        WARNING("Unmatched secondary id: container " << container.DebugString());
                        reported_errors_++;
                    }
                    matched_ids++;
                }
            }
            if (matched_ids != container.secondary_ids().size()) {
                WARNING(
                    "There are unmatched secondary ids: " << container.DebugString() <<
                    ", matched id count "
                                                          << matched_ids <<
                    ", expected secondary ids: [" << dedupv1::base::strutil::Join(
                        redirecting_iter->second.begin(),
                        redirecting_iter->second.end(), ", ") << "]");
                reported_errors_++;
            }
            // Get the container items.
            std::vector<ContainerItem*>& items = container.items();
            std::vector<ContainerItem*>::const_iterator item_iterator;
            for (item_iterator = items.begin(); item_iterator != items.end(); ++item_iterator) {
                ContainerItem* item = *item_iterator;

                if (!item->is_deleted()) {
                    CHECK(CheckContainerItem(chunk_index, fp_gen, &container, item),
                        "Failed to check container item: item " << item->DebugString());
                }
            }
        }

        double ratio = (100.0 * processed_container_count) / container_count;
        if (ratio >= last_full_percent_progress + 1) {
            last_full_percent_progress = ratio; // implicit cast
            INFO("Step 1: " << last_full_percent_progress << "%");
        }
    }
    container_address_inverse_map_.clear(); // we don't need the data anymore

    delete fp_gen;
    fp_gen = NULL;
    return true;
}

bool Dedupv1Checker::Stop() {
    DEBUG("Closing dedupv1 check");
    if (system_) {
        CHECK(system_->Shutdown(
                dedupv1::StopContext::FastStopContext()), "Failed to start dedupv1 shutdown");
        CHECK(system_->Stop(), "Failed to stop dedupv1 system");
    }
    return true;
}

Dedupv1Checker::~Dedupv1Checker() {
    if (system_) {
        delete system_;
        system_ = NULL;
        dedup_system_ = NULL;
    }
}

bool Dedupv1Checker::set_passes(const uint32_t passes) {
    CHECK(passes <= (2 ^ 15), "Maximum number of supported passes is 2^15");
    int length = bits(passes);
    uint32_t tmp_passes = 1 << length;
    if (passes > tmp_passes) {
        tmp_passes <<= 1;
        INFO(
            "Number of passes needs to be power of 2, expand from " << passes << " to " <<
            tmp_passes);
    }
    this->run_passes_ = tmp_passes;
    this->pass_bitmask_ = tmp_passes - 1;
    this->actual_run_pass_ = 0;
    INFO("Set to run in " << this->run_passes_ << " passes with bitmask " << this->pass_bitmask_);
    return true;
}

}
}
}
