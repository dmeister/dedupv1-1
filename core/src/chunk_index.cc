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

#include <fcntl.h>
#include <unistd.h>

#include <sstream>
#include <limits>

#include "dedupv1.pb.h"

#include <core/dedup.h>
#include <core/log_consumer.h>
#include <base/memory.h>
#include <base/index.h>
#include <core/storage.h>
#include <core/chunker.h>
#include <core/filter.h>
#include <base/logging.h>
#include <base/timer.h>
#include <base/strutil.h>
#include <core/container_storage.h>
#include <core/block_mapping.h>
#include <core/container.h>
#include <core/dedup_system.h>
#include <core/chunk_store.h>
#include <base/fileutil.h>
#include <core/log.h>
#include <base/timer.h>
#include <base/profile.h>
#include <core/chunk_index.h>
#include <base/disk_hash_index.h>
#include <base/fault_injection.h>
#include <dedupv1_stats.pb.h>
#include <base/runnable.h>

using std::list;
using std::string;
using std::stringstream;
using std::vector;
using std::pair;
using std::make_pair;
using dedupv1::base::MemoryIndex;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::ProfileTimer;
using dedupv1::base::SlidingAverageProfileTimer;
using dedupv1::base::File;
using dedupv1::chunkstore::Container;
using dedupv1::chunkstore::ContainerItem;
using dedupv1::chunkstore::storage_commit_state;
using dedupv1::chunkstore::STORAGE_ADDRESS_ERROR;
using dedupv1::chunkstore::STORAGE_ADDRESS_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_NOT_COMMITED;
using dedupv1::chunkstore::STORAGE_ADDRESS_WILL_NEVER_COMMITTED;
using dedupv1::chunkstore::Storage;
using dedupv1::blockindex::BlockMapping;
using dedupv1::blockindex::BlockMappingItem;
using dedupv1::base::ScopedArray;
using dedupv1::Fingerprinter;
using dedupv1::base::lookup_result;
using dedupv1::base::IndexIterator;
using dedupv1::base::Index;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::PUT_KEEP;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::DELETE_OK;
using dedupv1::base::delete_result;
using dedupv1::base::put_result;
using dedupv1::base::Option;
using dedupv1::base::make_option;
using dedupv1::base::make_bytestring;
using dedupv1::base::ErrorContext;
using dedupv1::base::Future;
using dedupv1::base::ThreadUtil;
using dedupv1::base::Runnable;
using dedupv1::base::Threadpool;
using dedupv1::base::make_option;
using dedupv1::base::cache_lookup_method;
using dedupv1::base::cache_dirty_mode;

LOGGER("ChunkIndex");

namespace dedupv1 {
namespace chunkindex {

MetaFactory<ChunkIndex> ChunkIndex::factory_("ChunkIndex", "chunk index");

MetaFactory<ChunkIndex>& ChunkIndex::Factory() {
    return factory_;
}

ChunkIndex* ChunkIndex::CreateIndex() {
    return new ChunkIndex();
}

void ChunkIndex::RegisterChunkIndex() {
    ChunkIndex::Factory().Register("disk", &ChunkIndex::CreateIndex);
}

ChunkIndex::Statistics::Statistics() : average_lookup_latency_(256) {
}

ChunkIndex::ChunkIndex() {
    this->chunk_index_ = NULL;
    this->log_ = NULL;
    this->storage_ = NULL;
    this->bg_committer_ = NULL;
    info_store_ = NULL;
    this->bg_thread_count_ = 1;
    this->state_ = CREATED;
    is_replaying_ = false;
    import_if_replaying_ = true;
    import_delay_ = 0;
    has_reported_importing_ = false;
    sampling_strategy_ = NULL;
    block_size_ = 0;
}



bool ChunkIndex::SetOption(const string& option_name, const string& option) {
    CHECK(this->state_ == CREATED, "Illegal state: state " << this->state_);
    CHECK(option_name.size() > 0, "Option name not set");
    CHECK(option.size() > 0, "Option not set");

    if (option_name == "replaying-import") {
        CHECK(To<bool>(option).valid(), "Illegal option " << option);
        this->import_if_replaying_ = To<bool>(option).value();
        return true;
    }
    if (option_name == "import-delay") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        import_delay_ = To<uint32_t>(option).value();
        return true;
    }
    if (option_name == "sampling-strategy") {
        CHECK(sampling_strategy_ == NULL, "Sampling strategy already set");
        sampling_strategy_ = ChunkIndexSamplingStrategy::Factory().Create(option);
        CHECK(sampling_strategy_, "Failed to create sampling strategy");
        return true;
    }
    if (StartsWith(option_name, "sampling-strategy.")) {
        CHECK(sampling_strategy_, "Sampling strategy not set");
        CHECK(sampling_strategy_->SetOption(
                option_name.substr(strlen("sampling-strategy.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "persistent") {
        Index* index = Index::Factory().Create(option);
        CHECK(index, "Persistent index creation failed");
        this->chunk_index_ = index->AsPersistentIndex();
        CHECK(this->chunk_index_, "Chunk index should persistent");
        CHECK(this->chunk_index_->HasCapability(dedupv1::base::PERSISTENT_ITEM_COUNT),
            "Index has no persistent item count");
        CHECK(this->chunk_index_->HasCapability(dedupv1::base::WRITE_BACK_CACHE),
            "Index has no write-cache support");

        // Set default options
        CHECK(this->chunk_index_->SetOption("max-key-size",
                ToString(Fingerprinter::kMaxFingerprintSize)),
            "Failed to set auto option");

        // currently the ChunkMappingData type contains three 64-bit values.
        CHECK(this->chunk_index_->SetOption("max-value-size", "32"),
            "Failed to set auto option");
        return true;
    }
    if (StartsWith(option_name, "persistent.")) {
        CHECK(this->chunk_index_, "Persistent data index not set");
        CHECK(this->chunk_index_->SetOption(option_name.substr(strlen("persistent.")),
                option), "Configuration failed");
        return true;
    }
    if (option_name == "bg-thread-count") {
        CHECK(To<uint32_t>(option).valid(), "Illegal option " << option);
        this->bg_thread_count_ = To<uint32_t>(option).value();
        return true;
    }
    ERROR("Illegal option: " << option_name);
    return false;
}

bool ChunkIndex::DumpMetaInfo() {
    DCHECK(info_store_, "Info store not set");

    ChunkIndexLogfileData logfile_data;
    CHECK(info_store_->PersistInfo("chunk-index", logfile_data),
        "Failed to store info: " << logfile_data.ShortDebugString());
    return true;
}

lookup_result ChunkIndex::ReadMetaInfo() {
    DCHECK_RETURN(info_store_, LOOKUP_ERROR, "Info store not set");

    ChunkIndexLogfileData logfile_data;
    lookup_result lr = info_store_->RestoreInfo("chunk-index", &logfile_data);
    CHECK_RETURN(lr != LOOKUP_ERROR, LOOKUP_ERROR, "Failed to restore chunk index info");
    if (lr == LOOKUP_NOT_FOUND) {
        return lr;
    }
    return LOOKUP_FOUND;
}

bool ChunkIndex::Start(const StartContext& start_context, DedupSystem* system) {
    DCHECK(system, "System not set");
    CHECK(this->state_ == CREATED, "Illegal state: state " << this->state_);
    CHECK(this->chunk_index_, "Persistent chunk index not set");

    INFO("Starting chunk index");

    block_size_ = system->block_size();
    this->log_ = system->log();
    DCHECK(this->log_, "Log not set");
    this->info_store_ = system->info_store();
    CHECK(this->info_store_, "Info store not set");

    if (sampling_strategy_ == NULL) {
        sampling_strategy_ = ChunkIndexSamplingStrategy::Factory().Create("full");
        CHECK(sampling_strategy_, "Failed to create sampling strategy");
    }
    CHECK(sampling_strategy_->Start(start_context, system),
        "Failed to start sampling strategy");

    if (!this->chunk_index_->IsWriteBackCacheEnabled()) {
        ERROR("Index has no write-back cache");
        return false;
    }
    if (dynamic_cast<dedupv1::base::DiskHashIndex*>(this->chunk_index_)) {
        // set better maximal key size if the static hash index is used
        // this is the only usage of the content storage in the chunk index
        dedupv1::ContentStorage* content_storage = system->content_storage();
#ifdef DEDUPV1_CORE_TEST
        // I need this for some tests
        if (content_storage == NULL) {
            CHECK(this->chunk_index_->SetOption("max-key-size", "20"),
                "Failed to set max key size");
        } else {
#endif
        CHECK(content_storage, "Content storage not set");
        Fingerprinter* fp = Fingerprinter::Factory().Create(content_storage->fingerprinter_name());
        CHECK(fp, "Failed to create fingerprinter");
        size_t fp_size = fp->GetFingerprintSize();
        delete fp;
        fp = NULL;
        CHECK(this->chunk_index_->SetOption("max-key-size", ToString(fp_size)),
            "Failed to set max key size");
#ifdef DEDUPV1_CORE_TEST
    }
#endif
    }

    CHECK(this->chunk_locks_.Start(start_context), "Failed to start chunk locks");
    CHECK(this->chunk_index_->Start(start_context), "Could not start index");

    this->storage_ = system->storage();
    CHECK(this->storage_, "Failed to set storage");

    lookup_result info_lookup = ReadMetaInfo();
    CHECK(info_lookup != LOOKUP_ERROR, "Failed to read meta info");
    CHECK(!(info_lookup == LOOKUP_NOT_FOUND && !start_context.create()),
        "Failed to lookup meta info in non-create startup mode");
    if (info_lookup == LOOKUP_NOT_FOUND && start_context.create()) {
        CHECK(DumpMetaInfo(), "Failed to dump info");
    }

    this->bg_committer_ = new ChunkIndexBackgroundCommitter(this, this->bg_thread_count_, 5000 /* check interval */,
        import_delay_ /* wait interval */, false);
    CHECK(this->bg_committer_, "Cannot create chunk index background committer");
    CHECK(this->bg_committer_->Start(), "Failed to start bg committer");

    CHECK(this->log_->RegisterConsumer("chunk-index", this), "Cannot register chunk index");
    this->state_ = STARTED;
    return true;
}

bool ChunkIndex::Run() {
    CHECK(this->state_ == STARTED, "Illegal state: " << this->state_);
    CHECK(this->bg_committer_->Run(), "Failed to run bg committer");
    return true;
}

bool ChunkIndex::ImportAllReadyContainer() {
    DCHECK(chunk_index_, "Persistent chunk index not set");

    ChunkIndexBackgroundCommitter stop_commit(this, 4, 0, 0, true);
    CHECK(stop_commit.Start(), "Failed to start stop background committer");
    CHECK(stop_commit.Run(), "Failed to start stop background committer");
    CHECK(stop_commit.Wait(), "Failed to wait for stop background committer");

    return true;
}

bool ChunkIndex::Stop(dedupv1::StopContext stop_context) {
    if (state_ == STOPPED) {
      return true;
    }
    INFO("Stopping chunk index");
    if (this->bg_committer_) {
        CHECK(this->bg_committer_->Stop(stop_context), "Failed to stop bg committer");
    }
    if (state_ == STARTED && stop_context.mode() == dedupv1::StopContext::WRITEBACK) {
        CHECK(ImportAllReadyContainer(), "Failed to import all ready container");
    }
    if (info_store_) {
        // if the chunk index is not started, there is nothing to dump
        if (!DumpMetaInfo()) {
            WARNING("Failed to dump meta info");
        }
    }
    DEBUG("Stopped chunk index");
    this->state_ = STOPPED;
    return true;
}

ChunkIndex::~ChunkIndex() {
    DEBUG("Closing chunk index");

    if(!this->Stop(dedupv1::StopContext::FastStopContext())) {
        WARNING("Failed to stop chunk index");
    }

    if (this->bg_committer_) {
        delete this->bg_committer_;
        this->bg_committer_ = NULL;
    }

    if (this->chunk_index_) {
        delete chunk_index_;
        this->chunk_index_ = NULL;
    }

    if (sampling_strategy_) {
        delete sampling_strategy_;
        sampling_strategy_ = NULL;
    }
    if (this->log_) {
        if (this->log_->IsRegistered("chunk-index").value()) {
            if (!this->log_->UnregisterConsumer("chunk-index")) {
                WARNING("Failed to unregister from log");
            }
        }
        this->log_ = NULL;
    }
}

#ifdef DEDUPV1_CORE_TEST
void ChunkIndex::ClearData() {
    if (this->bg_committer_) {
        this->bg_committer_->Stop(StopContext::WritebackStopContext());
    }
    if (this->log_) {
        log_->UnregisterConsumer("chunk-index");
        log_ = NULL;
    }
    if (this->chunk_index_) {
        delete chunk_index_;
        chunk_index_ = NULL;
    }
}

#endif

lookup_result ChunkIndex::LookupNextIterator(IndexIterator* it, ChunkMapping* mapping) {
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");
    DCHECK_RETURN(it, LOOKUP_ERROR, "Iterator not set");

    mapping->set_fingerprint_size(Fingerprinter::kMaxFingerprintSize);

    ChunkMappingData value_data;
    enum lookup_result result = it->Next(mapping->mutable_fingerprint(), mapping->mutable_fingerprint_size(),
        &value_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing index" <<
        " chunk mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        CHECK_RETURN(mapping->UnserializeFrom(value_data, true), LOOKUP_ERROR,
            "Cannot unserialize chunk mapping: " << value_data.ShortDebugString());
        TRACE("Found index entry: data " << mapping->DebugString());
    }
    return result;
}

dedupv1::base::lookup_result ChunkIndex::LookupPersistentIndex(ChunkMapping* mapping,
    enum cache_lookup_method cache_lookup_type,
    enum cache_dirty_mode dirty_mode,
    dedupv1::base::ErrorContext* ec) {
    DCHECK_RETURN(chunk_index_, LOOKUP_ERROR, "Chunk index not set");
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");

    ChunkMappingData value_data;
    lookup_result result = chunk_index_->LookupDirty(mapping->fingerprint(),
        mapping->fingerprint_size(),
        cache_lookup_type,
        dirty_mode,
        &value_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing index" <<
        " chunk mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        CHECK_RETURN(mapping->UnserializeFrom(value_data, true),
            LOOKUP_ERROR,
            "Cannot unserialize chunk mapping: " << value_data.ShortDebugString());

        TRACE("Found index entry: "
            "chunk " << mapping->DebugString() <<
            ", data " << value_data.ShortDebugString() <<
            ", cache lookup method " << ToString(cache_lookup_type) <<
            ", dirty mode " << ToString(dirty_mode) <<
            ", source persistent");
    }
    return result;
}

lookup_result ChunkIndex::LookupIndex(Index* index, ChunkMapping* mapping, dedupv1::base::ErrorContext* ec) {
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");
    DCHECK_RETURN(index, LOOKUP_ERROR, "Index not set");

    ChunkMappingData value_data;
    lookup_result result = index->Lookup(mapping->fingerprint(), mapping->fingerprint_size(), &value_data);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing index" <<
        " chunk mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        CHECK_RETURN(mapping->UnserializeFrom(value_data, true),
            LOOKUP_ERROR,
            "Cannot unserialize chunk mapping: " << value_data.ShortDebugString());
        TRACE("Found index entry: "
            "chunk " << mapping->DebugString() << ", data " << value_data.ShortDebugString());
    }
    return result;
}

lookup_result ChunkIndex::Lookup(ChunkMapping* mapping,
    ErrorContext* ec) {
    CHECK_RETURN(this->state_ == STARTED, LOOKUP_ERROR, "Illegal state: state " << this->state_);
    DCHECK_RETURN(mapping, LOOKUP_ERROR, "Mapping not set");

    SlidingAverageProfileTimer average_lookup_timer(this->stats_.average_lookup_latency_);

    ProfileTimer total_timer(this->stats_.profiling_);
    ProfileTimer lookup_timer(this->stats_.lookup_time_);

    enum lookup_result result = this->LookupPersistentIndex(mapping,
        dedupv1::base::CACHE_LOOKUP_DEFAULT,
        dedupv1::base::CACHE_ALLOW_DIRTY,
        ec);
    CHECK_RETURN(result != LOOKUP_ERROR, LOOKUP_ERROR, "Error while accessing main index: " <<
        "mapping " << mapping->DebugString());
    if (result == LOOKUP_FOUND) {
        TRACE("Lookup chunk mapping " << mapping->DebugString() << ", result found");
    } else {
        TRACE("Lookup chunk mapping " << Fingerprinter::DebugString(mapping->fingerprint(),
                mapping->fingerprint_size()) << ", result not found");
    }
    return result;
}

bool ChunkIndex::PutIndex(Index* index,
    const ChunkMapping& mapping,
    dedupv1::base::ErrorContext* ec) {
    ChunkMappingData data;
    CHECK(mapping.SerializeTo(&data),
        "Failed to serialize chunk mapping: " << mapping.DebugString());

    put_result result = index->Put(mapping.fingerprint(), mapping.fingerprint_size(), data);
    CHECK(result != PUT_ERROR,
        "Cannot put chunk mapping data: " << mapping.DebugString());

    return true;
}

bool ChunkIndex::PutPersistentIndex(const ChunkMapping& mapping,
    bool ensure_persistence,
    dedupv1::base::ErrorContext* ec) {
    ChunkMappingData data;
    CHECK(mapping.SerializeTo(&data),
        "Failed to serialize chunk mapping: " << mapping.DebugString());

    TRACE("Put index entry: "
        "chunk " << mapping.DebugString() <<
        ", ensure persistence " << ToString(ensure_persistence));

    put_result result;
    if (ensure_persistence) {
        result = chunk_index_->Put(mapping.fingerprint(),
            mapping.fingerprint_size(), data);
    } else {
        result = chunk_index_->PutDirty(mapping.fingerprint(),
            mapping.fingerprint_size(),
            data);
    }
    CHECK(result != PUT_ERROR,
        "Cannot put chunk mapping data: " << mapping.DebugString());
    return true;
}

bool ChunkIndex::Delete(const ChunkMapping& mapping) {
    CHECK_RETURN(this->state_ == STARTED, DELETE_ERROR,
        "Illegal state: state " << this->state_);

    ProfileTimer timer(this->stats_.profiling_);

    TRACE("Delete from persistent chunk index: " << mapping.DebugString());
    enum delete_result result_persistent = this->chunk_index_->Delete(mapping.fingerprint(), mapping.fingerprint_size());
    CHECK(result_persistent != DELETE_ERROR, "Failed to delete mapping from persistent chunk index: " << mapping.DebugString());
    return true;
}

bool ChunkIndex::Put(const ChunkMapping& mapping, ErrorContext* ec) {
    DCHECK_RETURN(this->state_ == STARTED, PUT_ERROR, "Illegal state: state " << this->state_);

    ProfileTimer total_timer(this->stats_.profiling_);
    ProfileTimer update_timer(this->stats_.update_time_);

    TRACE("Updating chunk index: " << mapping.DebugString());

    // we pin the item into main memory
    return this->PutPersistentIndex(mapping, false, ec);
}

bool ChunkIndex::PutOverwrite(ChunkMapping& mapping, ErrorContext* ec) {
    DCHECK_RETURN(this->state_ == STARTED, PUT_ERROR,
        "Illegal state: state " << this->state_);

    ProfileTimer total_timer(this->stats_.profiling_);
    ProfileTimer update_timer(this->stats_.update_time_);

    TRACE("Updating persistent chunk index: " << mapping.DebugString());
    bool result = this->PutPersistentIndex(mapping, false, ec);
    CHECK(result, "Failed to put overwrite a chunk mapping: " << mapping.DebugString());

    return true;
}

bool ChunkIndex::PersistStatistics(std::string prefix,
    dedupv1::PersistStatistics* ps) {
    ChunkIndexStatsData data;
    CHECK(ps->Persist(prefix, data), "Failed to persist chunk index stats");
    return true;
}

bool ChunkIndex::RestoreStatistics(std::string prefix,
    dedupv1::PersistStatistics* ps) {
    ChunkIndexStatsData data;
    CHECK(ps->Restore(prefix, &data), "Failed to restore chunk index stats");
    return true;
}

string ChunkIndex::PrintLockStatistics() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"chunk locks\": " << chunk_locks_.PrintLockStatistics() << "," << std::endl;
    sstr << "\"index\": " << (chunk_index_ ? this->chunk_index_->PrintLockStatistics() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndex::PrintTrace() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"index\": " << (chunk_index_ ? this->chunk_index_->PrintTrace() : "null") << "," << std::endl;
    sstr << "\"replaying state\": " << ToString(static_cast<bool>(is_replaying_)) << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndex::PrintStatistics() {
    stringstream sstr;
    sstr.setf(std::ios::fixed, std::ios::floatfield);
    sstr.setf(std::ios::showpoint);
    sstr.precision(3);

    sstr << "{";
    sstr << "\"index item count\": " << (chunk_index_ ? ToString(this->chunk_index_->GetItemCount()) : "null") << ","
         << std::endl;
    sstr << "\"total index item count\": " << (chunk_index_ ? ToString(this->chunk_index_->GetTotalItemCount()) : "null") << ","
         << std::endl;
    sstr << "\"dirty index item count\": " << (chunk_index_ ? ToString(this->chunk_index_->GetDirtyItemCount()) : "null") << ","
         << std::endl;
    sstr << "\"index size\": " << (chunk_index_ ? ToString(this->chunk_index_->GetPersistentSize()) : "null")
         << std::endl;
    sstr << "}";
    return sstr.str();
}

string ChunkIndex::PrintProfile() {
    stringstream sstr;
    sstr << "{";
    sstr << "\"average lookup latency\": " << this->stats_.average_lookup_latency_.GetAverage() << "," << std::endl;
    sstr << "\"chunk index\": " << this->stats_.profiling_.GetSum() << "," << std::endl;
    sstr << "\"replay time\": " << this->stats_.replay_time_.GetSum() << "," << std::endl;
    sstr << "\"lookup time\": " << this->stats_.lookup_time_.GetSum() << "," << std::endl;
    sstr << "\"import time\": " << this->stats_.import_time_.GetSum() << "," << std::endl;
    sstr << "\"update time\": " << this->stats_.update_time_.GetSum() << "," << std::endl;
    sstr << "\"chunk locks\": " << chunk_locks_.PrintProfile() << "," << std::endl;

    sstr << "\"index\": " << (chunk_index_ ? this->chunk_index_->PrintProfile() : "null") << std::endl;
    sstr << "}";
    return sstr.str();
}

bool ChunkIndex::LoadContainerIntoCache(uint64_t container_id,
    dedupv1::base::ErrorContext* ec) {

    DEBUG("Load container " << container_id << " from log into cache (loading)");
    Container container(container_id, storage_->container_size(), true);

    enum lookup_result read_result = storage_->ReadContainer(&container, false);
    CHECK(read_result != LOOKUP_ERROR,
        "Could not read container for import: " <<
        "container id " << container_id <<
        ", container " << container.DebugString());
    if (read_result == LOOKUP_NOT_FOUND) {
        WARNING("Could find container for import: " << 
            "container " << container.DebugString());
        return true;
    }

    // found
    DEBUG("Load container from log into cache: " << container.DebugString() <<
        ", current container id " << container_id);

    vector<ContainerItem*>::const_iterator i;
    for (i = container.items().begin(); i != container.items().end(); i++) {
        ContainerItem* item = *i;
        DCHECK(item, "Item not set");
        if (item->is_deleted()) {
            TRACE("Container item is deleted: " << item->DebugString());
            continue;
        }
        if (item->original_id() != container_id) {
            TRACE("Skip item: " << item->DebugString() << ", import container id " << container_id);
            continue;
        }
        if (!item->is_indexed()) {
            TRACE("Skip item: " << item->DebugString() << ", not indexed");
            continue;
        }
        TRACE("Load container item: " << item->DebugString());

        // the lookup is not essential, but it should not take a long time.
        ChunkMapping mapping(item->key(), item->key_size());
        lookup_result result = LookupPersistentIndex(&mapping,
            dedupv1::base::CACHE_LOOKUP_ONLY,
            dedupv1::base::CACHE_ALLOW_DIRTY, ec);
        CHECK(result != LOOKUP_ERROR, "Failed to search for chunk mapping in cache: " << mapping.DebugString())
        if (result == LOOKUP_FOUND) {
            // The usage count is absolutely wrong here.
            // It must be corrected using a background garbage collecting process. Therefore it is safe to ignore the usage count here.
            // It has to be zero.

            // When this container has not been imported before, there can be no block write event that is replayed
            // before. Therefore the id has to be zero.

            // every container is imported at least once, but if container has been merged before
            // they are imported a given item might be imported multiple times. This is avoided
            // here.
            if (container_id != mapping.data_address()) {
                TRACE("Container item was imported before: item " << item->DebugString() << ", imported container id "
                                                                  << container_id << ", mapping " << mapping.DebugString());
            }
        } else {
            DEBUG("We have a item from a non-imported container that is unused: " <<
                item->DebugString());

            result = LookupPersistentIndex(&mapping,
                dedupv1::base::CACHE_LOOKUP_BYPASS,
                dedupv1::base::CACHE_ALLOW_DIRTY, ec);
            CHECK(result != LOOKUP_ERROR, "Failed to search for chunk mapping in chunk index: " << mapping.DebugString())
            if (result == LOOKUP_FOUND) {
                DEBUG("Item was imported before: " << item->DebugString());
            } else {
                DEBUG("Add empty item into chunk index: " << item->DebugString());
                // When this container has not been imported before, there can be no block write event that is replayed
                // before. Therefore the id has to be zero.

                // Here we have to use the original container id as otherwise
                // already written block mapping entries and the chunk index entry
                // get ouf of sync of the container id: Container id mismatch
                mapping.set_data_address(item->original_id());

                CHECK(PutPersistentIndex(mapping, false, ec),
                    "Cannot put logged mapping into persistent index (dirty): " << mapping.DebugString() <<
                    ", container item " << item->DebugString());
            }
        }
    }

    DEBUG("Finished loading container " << container_id << " from log into cache: "
                                        << container.DebugString());

    return true;
}

bool ChunkIndex::ImportContainer(uint64_t container_id, dedupv1::base::ErrorContext* ec) {

    storage_commit_state commit_state = storage_->IsCommitted(container_id);

    CHECK(commit_state != STORAGE_ADDRESS_ERROR,
        "Failed to check commit state: " << container_id);
    CHECK(commit_state != STORAGE_ADDRESS_NOT_COMMITED, "Missing container for import: " <<
        "container id " << container_id <<
        ", commit state: not committed");
    if (commit_state == STORAGE_ADDRESS_WILL_NEVER_COMMITTED) {
        WARNING("Missing container for import: " << "container id " << container_id
                                                 << ", commit state: will never be committed")
        // This can happen if the system crashed before the container was committed.
        // In this case we can no more restore the chunk and throw it away. During
        // the lock replay the block mappings will also be rewinded, so that the chunk
        // is not referenced.
        return true; // leave method
    }
    // is committed

    FAULT_POINT("chunk-index.import.pre");

    if (chunk_index_->GetDirtyItemCount() > 0) {
    TRACE("Import container " << container_id << " from log (loading)");
    Container container(container_id, storage_->container_size(), true);

        enum lookup_result read_result = storage_->ReadContainer(&container, false);
        CHECK(read_result != LOOKUP_ERROR,
            "Could not read container for import: " <<
            "container id " << container_id <<
            ", container " << container.DebugString());
        if (read_result == LOOKUP_NOT_FOUND) {
            // This should not happen, as we checked before that it is committed.
            WARNING("Could find container for import: " << "container " << container.DebugString());
            return true;
        }

        // found
        INFO("Import container: " << container.DebugString());

        vector<ContainerItem*>::const_iterator i;
        for (i = container.items().begin(); i != container.items().end(); i++) {
            ContainerItem* item = *i;
            if (item) {
                if (item->original_id() != container_id) {
                    // This happens after a merge.
                    TRACE("Skip item: " << item->DebugString() << ", import container id " << container_id);
                    continue;
                }
                if (item->is_deleted()) {
                    TRACE("Container item is deleted: " << item->DebugString());
                    continue;
                }
                if (!item->is_indexed()) {
                    TRACE("Container item should not be indexed: " << item->DebugString());
                    continue;
                }
                ChunkMapping mapping(item->key(), item->key_size());
                mapping.set_data_address(item->original_id());

                DEBUG("Import container item: " << item->DebugString());
                put_result pr = EnsurePersistent(mapping);
                CHECK(pr != PUT_ERROR, "Failed to ensure that container item is persisted: " <<
                    item->DebugString());
                if (pr == PUT_KEEP) {
                    DEBUG("Item was not dirty in chunk index: " << item->DebugString());
                }
            }
        }
        DEBUG("Finished importing container " << container_id << " from log (count " << container.item_count() << ")");
    } else {
        TRACE("Import container " << container_id << " from log (skipping, all clean)");
    }
    FAULT_POINT("chunk-index.import.post");
    return true;
}

enum put_result ChunkIndex::EnsurePersistent(const ChunkMapping &mapping) {
    DCHECK_RETURN(chunk_index_, PUT_ERROR, "Chunk index not set");

    DEBUG("Ensure persistence: "
        "chunk " << mapping.DebugString());

    put_result pr;
    pr = chunk_index_->EnsurePersistent(mapping.fingerprint(),
        mapping.fingerprint_size());
    CHECK_RETURN(pr != PUT_ERROR, PUT_ERROR, "Failed to persist chunk mapping: " << mapping.DebugString());

    if (pr == PUT_KEEP) {
        DEBUG("Mapping wasn't dirty in cache: " << mapping.DebugString());
    } else {
        // it is persisted, we are fine here
    }
    return pr;
}
bool ChunkIndex::LogReplayStateChange(const dedupv1::log::LogReplayStateChange& change) {
    if (!change.is_replaying()) {
        is_replaying_ = false;
        CHECK(DumpMetaInfo(), "Failed to dump chunk index meta data");
    } else {
        is_replaying_ = true;
    }
    return true;
}

bool ChunkIndex::LogReplay(dedupv1::log::event_type event_type,
    const LogEventData& event_value,
    const dedupv1::log::LogReplayContext& context) {
    ProfileTimer timer(this->stats_.replay_time_);
    FAULT_POINT("chunk-index.log.pre");
    if (event_type == dedupv1::log::EVENT_TYPE_CONTAINER_COMMITED) {
        CHECK(state_ != STOPPED, "Failed to replay log event: chunk index already stopped");
        ContainerCommittedEventData event_data = event_value.container_committed_event();
        uint64_t container_id = event_data.container_id();

        if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START) {
            TRACE("Handle opened container (dirty start): " << "" << event_data.ShortDebugString() << ", event log id "
                                                            << context.log_id());

            CHECK(LoadContainerIntoCache(container_id, NO_EC),
                "Failed to load container data into cache: container id " << container_id);
        } else if (context.replay_mode() == dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG) {
            CHECK(state_ != STOPPED, "Failed to replay log event: chunk index already stopped");

            DEBUG("Replay container open (bg replay): " <<
                "container id " << container_id <<
                ", event data " << event_data.ShortDebugString() <<
                ", event log id " << context.log_id());

            CHECK(ImportContainer(container_id, NO_EC),
                "Failed to import container: container id " << container_id);
        }
    }
    return true;
}

ChunkIndex::import_result ChunkIndex::TryImportDirtyChunks(uint64_t * resume_handle) {
    ProfileTimer timer(this->stats_.import_time_);

    bool should_import = (import_if_replaying_ && is_replaying_ && this->chunk_index_->GetDirtyItemCount() > 0);
    if (!should_import) {
        if (unlikely(has_reported_importing_)) {
            has_reported_importing_ = false;
            INFO("Chunk index importing stopped: " <<
                "replaying state " << ToString(is_replaying_) <<
                ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
                ", total item count " << chunk_index_->GetTotalItemCount() <<
                ", persistent item count " << chunk_index_->GetItemCount());
        }
        return IMPORT_NO_MORE;
    }
    if (!has_reported_importing_.compare_and_swap(true, false)) {
        INFO("Chunk index importing started: " <<
            "replaying state " << ToString(is_replaying_) <<
            ", dirty item count " << chunk_index_->GetDirtyItemCount() <<
            ", total item count " << chunk_index_->GetTotalItemCount() <<
            ", persistent item count " << chunk_index_->GetItemCount());
    }

    bool persisted_page = false;
    CHECK_RETURN(chunk_index_->TryPersistDirtyItem(128, resume_handle, &persisted_page),
        IMPORT_ERROR, "Failed to persist dirty items");

    if (persisted_page) {
        return IMPORT_BATCH_FINISHED;
    } else {
        return IMPORT_NO_MORE;
    }
}

void ChunkIndex::set_state(chunk_index_state new_state) {
    this->state_ = new_state;
}

}
}
