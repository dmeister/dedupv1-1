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
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see
 ***http://www.gnu.org/licenses/.
 */

#include <string>
#include <list>

#include <gtest/gtest.h>
#include <tbb/atomic.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <base/index.h>
#include <core/container.h>
#include <core/log.h>
#include <core/fixed_log.h>
#include <core/storage.h>
#include <base/strutil.h>
#include <base/logging.h>
#include <base/crc32.h>
#include <core/container_storage.h>
#include <core/container_storage_gc.h>
#include <core/fingerprinter.h>
#include <base/thread.h>
#include <base/runnable.h>

#include "storage_test.h"
#include "container_test_helper.h"
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <test/chunk_index_mock.h>

using std::string;
using std::pair;
using dedupv1::base::crc;
using dedupv1::base::strutil::ToString;
using dedupv1::Fingerprinter;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::PUT_OK;
using dedupv1::base::lookup_result;
using dedupv1::base::ThreadUtil;
using dedupv1::log::Log;
using dedupv1::log::EVENT_REPLAY_MODE_DIRTY_START;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::base::Thread;
using dedupv1::base::NewRunnable;
using testing::Return;
using testing::_;
using ::std::tr1::tuple;
using dedupv1::base::Option;

LOGGER("ContainerStorageTest");

namespace dedupv1 {
namespace chunkstore {

/**
 * Test cases about the container storage system
 */
class ContainerStorageTest : public testing::TestWithParam<tuple<const char*, int> > {
public:
    static const size_t TEST_DATA_SIZE = 128 * 1024;
    static const size_t TEST_DATA_COUNT = 64;
protected:
    USE_LOGGING_EXPECTATION();

    ContainerStorage* storage;
    ContainerStorage* crashed_storage;
    Log* log;
    IdleDetector* idle_detector;
    dedupv1::MemoryInfoStore info_store;
    MockDedupSystem system;
    MockChunkIndex chunk_index;

    ContainerTestHelper* container_helper;

    virtual void SetUp() {
        storage = NULL;
        idle_detector = NULL;
        log = NULL;
        crashed_storage = NULL;

        container_helper = new ContainerTestHelper(ContainerStorageTest::TEST_DATA_SIZE,
            ContainerStorageTest::TEST_DATA_COUNT);
        ASSERT_TRUE(container_helper->SetUp());

        idle_detector = new IdleDetector();
        EXPECT_CALL(system, idle_detector()).WillRepeatedly(Return(idle_detector));
        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));
        EXPECT_CALL(system, chunk_index()).WillRepeatedly(Return(&chunk_index));

        log = new dedupv1::log::FixedLog();
        ASSERT_TRUE(log->SetOption("filename", "work/log"));
        ASSERT_TRUE(log->SetOption("max-log-size", "1M"));
        ASSERT_TRUE(log->SetOption("info.type", "sqlite-disk-btree"));
        ASSERT_TRUE(log->SetOption("info.filename", "work/log-info"));
        ASSERT_TRUE(log->Start(StartContext(), &system));
        EXPECT_CALL(system, log()).WillRepeatedly(Return(log));

        storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(storage);
        SetDefaultStorageOptions(storage);

    }

    void SetDefaultStorageOptions(Storage* storage) {

        string use_compression(std::tr1::get<0>(GetParam()));
        int explicit_file_size = std::tr1::get<1>(GetParam());

        ASSERT_TRUE(storage->SetOption("filename", "work/container-data-1"));
        if (explicit_file_size >= 1) {
            ASSERT_TRUE(storage->SetOption("filesize", "512M"));
        }
        ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
        if (explicit_file_size >= 2) {
            ASSERT_TRUE(storage->SetOption("filesize", "512M"));
        }
        ASSERT_TRUE(storage->SetOption("meta-data", "leveldb-disk-lsm"));
        ASSERT_TRUE(storage->SetOption("meta-data.filename", "work/container-metadata"));
        ASSERT_TRUE(storage->SetOption("container-size", "512K"));
        ASSERT_TRUE(storage->SetOption("size", "1G"));
        ASSERT_TRUE(storage->SetOption("gc", "greedy"));
        ASSERT_TRUE(storage->SetOption("gc.type","leveldb-disk-lsm"));
        ASSERT_TRUE(storage->SetOption("gc.filename", "work/merge-candidates"));
        ASSERT_TRUE(storage->SetOption("alloc", "memory-bitmap"));
        ASSERT_TRUE(storage->SetOption("alloc.type","leveldb-disk-lsm"));
        ASSERT_TRUE(storage->SetOption("alloc.filename", "work/container-bitmap"));

        if (!use_compression.empty()) {
            ASSERT_TRUE(storage->SetOption("compression", use_compression));
        }
    }

    void WriteTestData(Storage* storage) {
        ASSERT_TRUE(storage);
        ASSERT_TRUE(container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT));
    }

    void DeleteTestData(Storage* storage) {
        ASSERT_TRUE(storage);
        for (size_t i = 0; i < TEST_DATA_COUNT; i++) {
            ASSERT_TRUE(storage->DeleteChunk(container_helper->data_address(i),
                    container_helper->fingerprint(i).data()
                    , container_helper->fingerprint(i).size(), NO_EC))
            << "Delete " << i << " failed";
        }
    }

    void ReadDeletedTestData(Storage* storage) {
        ASSERT_TRUE(storage);
        size_t i = 0;

        byte* result = new byte[TEST_DATA_SIZE];
        memset(result, 0, TEST_DATA_SIZE);

        for (i = 0; i < TEST_DATA_COUNT; i++) {
            size_t result_size = TEST_DATA_SIZE;
            Option<uint32_t> r = storage->ReadChunk(container_helper->data_address(i),
                container_helper->fingerprint(i).data(),
                container_helper->fingerprint(i).size(),
                result, 0, result_size, NO_EC);
            ASSERT_FALSE(r.valid()) << "Found data that should be deleted: key " <<
            Fingerprinter::DebugString(container_helper->fingerprint(i));
        }

        delete[] result;
    }

    void CrashAndRestart() {
        storage->ClearData();
        crashed_storage = storage;

        storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(storage);
        SetDefaultStorageOptions(storage);

        StartContext start_context;
        start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
        ASSERT_TRUE(storage->Start(start_context, &system));
        ASSERT_TRUE(log->PerformDirtyReplay());
        ASSERT_TRUE(storage->Run());
    }

    void Restart() {
        delete storage;
        storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
        ASSERT_TRUE(storage);
        SetDefaultStorageOptions(storage);

        StartContext start_context;
        start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
        ASSERT_TRUE(storage->Start(start_context, &system));
        ASSERT_TRUE(log->PerformDirtyReplay());
    }

    void ReadTestData(Storage* storage) {
        ASSERT_TRUE(storage);
        size_t i = 0;

        byte result[TEST_DATA_SIZE];
        memset(result, 0, TEST_DATA_SIZE);

        size_t result_size;
        result_size = TEST_DATA_SIZE;

        for (i = 0; i < TEST_DATA_COUNT; i++) {
            memset(result, 0, TEST_DATA_SIZE);
            result_size = TEST_DATA_SIZE;

            Option<uint32_t> r = storage->ReadChunk(container_helper->data_address(i),
                container_helper->fingerprint(i).data(),
                container_helper->fingerprint(i).size(),
                result,
                0,
                result_size, NO_EC);
            ASSERT_TRUE(r.valid()) << "Read " << i << " failed";
            ASSERT_TRUE(r.value() == TEST_DATA_SIZE) << "Read " << i << " error";
            ASSERT_TRUE(memcmp(container_helper->data(i), result, result_size) == 0) << "Compare " << i << " error";
        }
    }

    virtual void TearDown() {
        if (storage) {
            delete storage;
            storage = NULL;
        }
        if (crashed_storage) {
            delete crashed_storage;
            crashed_storage = NULL;
        }
        if (log) {
            delete log;
            log = NULL;
        }

        if (container_helper) {
            delete container_helper;
            container_helper = NULL;
        }

        if (idle_detector) {
            delete idle_detector;
            idle_detector = NULL;
        }
    }

};

TEST_P(ContainerStorageTest, Create) {
    // do nothing
}

TEST_P(ContainerStorageTest, Start) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, Run) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
}

TEST_P(ContainerStorageTest, SimpleReopen) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);

    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());
}

TEST_P(ContainerStorageTest, SimpleReadWrite) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);
    ReadTestData(storage);
}

TEST_P(ContainerStorageTest, SimpleCrash) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Writing data");

    WriteTestData(storage);
    storage->Flush(NO_EC);

    DEBUG("Crashing");
    CrashAndRestart();

    DEBUG("Reading data");
    ReadTestData(storage);

    DEBUG("Closing data");
}

TEST_P(ContainerStorageTest, CrashedDuringBGLogReplay) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Writing data");

    WriteTestData(storage);
    storage->Flush(NO_EC);

    ASSERT_TRUE(log->ReplayStart(EVENT_REPLAY_MODE_REPLAY_BG, true));
    dedupv1::log::log_replay_result result = dedupv1::log::LOG_REPLAY_OK;
    uint64_t replay_log_id = 0;
    while (result == dedupv1::log::LOG_REPLAY_OK) {
        replay_log_id = 0;
        result = log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, &replay_log_id, NULL);
    }
    DEBUG("Crashing");
    storage->ClearData();
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Reading data");
    ReadTestData(storage);

    DEBUG("Closing data");
}

TEST_P(ContainerStorageTest, CrashedDuringCrashLogReplay) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Writing data");

    WriteTestData(storage);
    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG("Crashing");
    storage->ClearData();
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // We simulate a log replay where the last container commit event
    // is replayed, but the system crashes before the replay stopped.
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());

    DEBUG("Crashing");
    storage->ClearData();
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Reading data");
    ReadTestData(storage);

    DEBUG("Closing data");
}

TEST_P(ContainerStorageTest, Delete) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage); // data is in write cache

    // here we check the COW system
    pair<lookup_result, ContainerStorageAddressData> address_result =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result.first, LOOKUP_FOUND);

    DeleteTestData(storage);
    ReadDeletedTestData(storage);

    pair<lookup_result, ContainerStorageAddressData> address_result2 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result2.first, LOOKUP_FOUND);

    ASSERT_FALSE(address_result.second.file_index() == address_result2.second.file_index() &&
        address_result.second.file_offset() && address_result.second.file_offset())
    << "Container hasn't changed position after deletion";
}

TEST_P(ContainerStorageTest, DeleteBeforeRun) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage); // data is in write cache

    // here we check the COW system
    pair<lookup_result, ContainerStorageAddressData> address_result =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result.first, LOOKUP_FOUND);

    Restart();

    DeleteTestData(storage);

    ASSERT_TRUE(storage->Run());
    ReadDeletedTestData(storage);

    pair<lookup_result, ContainerStorageAddressData> address_result2 =
        storage->LookupContainerAddress(container_helper->data_address(0), NULL, false);
    ASSERT_EQ(address_result2.first, LOOKUP_FOUND);

    ASSERT_FALSE(address_result.second.file_index() == address_result2.second.file_index() &&
        address_result.second.file_offset() && address_result.second.file_offset())
    << "Container hasn't changed position after deletion";
}

TEST_P(ContainerStorageTest, DeleteAfterClose) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());
    DeleteTestData(storage); // data should not in read or write cache
    ReadDeletedTestData(storage);
}

TEST_P(ContainerStorageTest, DeleteAfterFlush) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Key not found").Repeatedly();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);

    ASSERT_TRUE(storage->Flush(NO_EC)); // data is in read cache

    DeleteTestData(storage);
    ReadDeletedTestData(storage);
}

TEST_P(ContainerStorageTest, Extend) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    DEBUG(storage->PrintStatistics());

    container_helper->WriteDefaultData(storage, NULL, TEST_DATA_COUNT / 2, TEST_DATA_COUNT / 2);

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());
}

TEST_P(ContainerStorageTest, RestartMissingFile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    DEBUG(storage->PrintStatistics());

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    ASSERT_TRUE(storage->SetOption("filename.clear", "true"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
    // container-data-1 is missing

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, RestartWrongFileOrder) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    DEBUG(storage->PrintStatistics());

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    ASSERT_TRUE(storage->SetOption("filename.clear", "true"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-2"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-1"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, RestartChangeContainerSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    ASSERT_TRUE(storage->SetOption("container-size", "2M"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, ExtendWithoutForce) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    DEBUG(storage->PrintStatistics());

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, DoubleExtend) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    delete storage;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // 1. extend
    ASSERT_TRUE(storage->SetOption("size", "4G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));
    // 2. extend
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-5"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-6"));

    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    DEBUG(storage->PrintStatistics());

    container_helper->WriteDefaultData(storage, NULL, TEST_DATA_COUNT / 2, TEST_DATA_COUNT / 2);

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());
}

TEST_P(ContainerStorageTest, ExtendWithExplicitSize) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    DEBUG(storage->PrintStatistics());

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2560M"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(storage->Run());

    DEBUG(storage->PrintStatistics());

    container_helper->WriteDefaultData(storage, NULL, TEST_DATA_COUNT / 2, TEST_DATA_COUNT / 2);

    ASSERT_TRUE(storage->Flush(NO_EC));

    DEBUG(storage->PrintStatistics());
}

TEST_P(ContainerStorageTest, ExtendWithIllegalExplicitSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("size", "2560M"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_FALSE(storage->Start(start_context,
            &system)) << "Should fail because we didn't change the total size";
}

TEST_P(ContainerStorageTest, ExtendWithoutChangingSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // extend
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-3"));
    ASSERT_TRUE(storage->SetOption("filename", "work/container-data-4"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_FALSE(storage->Start(start_context,
            &system)) << "Should fail because we didn't change the total size";
}

TEST_P(ContainerStorageTest, IllegalExplicitSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->SetOption("filesize", "1G"));
    ASSERT_FALSE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, IllegalExplicitSize2) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->SetOption("filesize", "1023M"));
    ASSERT_FALSE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, IllegalSize) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->SetOption("size", "1023M"));
    ASSERT_FALSE(storage->Start(StartContext(), &system));
}

TEST_P(ContainerStorageTest, ChangeExplcitSizeOfExistingFile) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // change explicit size of existing file
    ASSERT_TRUE(storage->SetOption("size", "1536M"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, ChangeExplcitSizeOfExistingFileWithForce) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    container_helper->WriteDefaultData(storage, NULL, 0, TEST_DATA_COUNT / 2);
    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    SetDefaultStorageOptions(storage);

    // change explicit size of existing file
    ASSERT_TRUE(storage->SetOption("size", "1536M"));
    ASSERT_TRUE(storage->SetOption("filesize", "1G"));

    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE).set_dirty(StartContext::DIRTY).set_force(
        StartContext::FORCE);
    ASSERT_FALSE(storage->Start(start_context, &system));
}

TEST_P(ContainerStorageTest, NextContainerIDAfterClose) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);
    ReadTestData(storage);

    uint64_t container_id = storage->GetLastGivenContainerId();
    ASSERT_GT(container_id, static_cast<uint64_t>(2));

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create("container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));

    uint64_t new_container_id = storage->GetLastGivenContainerId();
    ASSERT_EQ(container_id, new_container_id) << "last given container id not restored after close";
}

TEST_P(ContainerStorageTest, NextContainerIDAfterCrash) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);
    ReadTestData(storage);

    uint64_t container_id = storage->GetLastGivenContainerId();

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));

    storage->SetLastGivenContainerId(0);

    ASSERT_TRUE(log->PerformDirtyReplay());

    uint64_t new_container_id = storage->GetLastGivenContainerId();
    ASSERT_EQ(container_id, new_container_id) << "last given container id not restored after close";
    ASSERT_TRUE(container_id > 0);
}

TEST_P(ContainerStorageTest, CommitOnFlush) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    WriteTestData(storage);
    ReadTestData(storage);
    ASSERT_TRUE(storage->Flush(NO_EC));
    ReadTestData(storage);
}

TEST_P(ContainerStorageTest, CommitOnStorageClose) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);
    ReadTestData(storage);

    delete storage;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    ASSERT_TRUE(storage);
    ReadTestData(storage);
}

TEST_P(ContainerStorageTest, MergeWithSameContainerId) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Matches("merge").Once();

    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);

    ASSERT_TRUE(storage->DeleteChunk(container_helper->data_address(0),
            container_helper->fingerprint(0).data(),
            container_helper->fingerprint(0).size(), NO_EC));

    ASSERT_TRUE(storage->Flush(NO_EC));

    bool aborted = false;
    ASSERT_FALSE(storage->TryMergeContainer(container_helper->data_address(0),
            container_helper->data_address(0), &aborted));
    ASSERT_FALSE(aborted);
}

TEST_P(ContainerStorageTest, MergeWithSameContainerLock) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    DEBUG("Search lock pair");

    uint64_t container_id1 = 1;
    uint64_t container_id2 = 2;
    dedupv1::base::ReadWriteLock* lock1 = storage->GetContainerLock(container_id1);
    dedupv1::base::ReadWriteLock* lock2 = NULL;
    while (lock1 != lock2) {
        container_id2++;
        lock2 = storage->GetContainerLock(container_id2);
    }
    // Here we have found a lock pair

    byte buffer[1024];
    memset(buffer, 0, 1024);
    uint64_t key1 = 1;
    uint64_t key2 = 2;
    Container container1(container_id1, storage->container_size(), false);
    ASSERT_TRUE(container1.AddItem(reinterpret_cast<const byte*>(&key1), sizeof(key1), buffer, 1024, true, NULL));
    Container container2(container_id2, storage->container_size(), false);
    ASSERT_TRUE(container2.AddItem(reinterpret_cast<const byte*>(&key2), sizeof(key2), buffer, 1024, true, NULL));

    ContainerStorageAddressData address1;
    ContainerStorageAddressData address2;
    ASSERT_TRUE(storage->allocator()->OnNewContainer(container1, true, &address1));
    ASSERT_TRUE(storage->allocator()->OnNewContainer(container2, true, &address2));

    DEBUG("Write container");
    ASSERT_TRUE(storage->CommitContainer(&container1, address1));
    ASSERT_TRUE(storage->CommitContainer(&container2, address2));

    GreedyContainerGCStrategy* gc =
        static_cast<GreedyContainerGCStrategy*>(storage->GetGarbageCollection());
    ASSERT_TRUE(gc);

    uint64_t bucket = 0;
    gc->merge_candidates()->Delete(&bucket, sizeof(bucket));

    DEBUG("Merge container");
    bool aborted = false;
    ASSERT_TRUE(storage->TryMergeContainer(container_id1, container_id2, &aborted));
    ASSERT_FALSE(aborted);
}

TEST_P(ContainerStorageTest, WriteReadRead) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());

    WriteTestData(storage);
    ReadTestData(storage);

    delete storage;
    storage = NULL;

    storage = dynamic_cast<ContainerStorage*>(Storage::Factory().Create(
                                                  "container-storage"));
    ASSERT_TRUE(storage);
    StartContext start_context(StartContext::NON_CREATE);
    SetDefaultStorageOptions(storage);
    ASSERT_TRUE(storage->Start(start_context, &system));
    ASSERT_TRUE(storage->Run());

    ReadTestData(storage);
}

TEST_P(ContainerStorageTest, ReadContainer) {
    ASSERT_TRUE(storage->Start(StartContext(), &system));
    ASSERT_TRUE(storage->Run());
    WriteTestData(storage);

    ASSERT_TRUE(storage->Flush(NO_EC)); // data is no committed

    for (int i = 0; i < TEST_DATA_COUNT; i++) {
        Container container(container_helper->data_address(i),
                            storage->container_size(), false);

        enum lookup_result r = storage->ReadContainer(&container, true);
        ASSERT_EQ(r, LOOKUP_FOUND);

        ContainerItem* item = container.FindItem(container_helper->fingerprint(
                i).data(), container_helper->fingerprint(i).size());
        ASSERT_TRUE(item);
    }
}

INSTANTIATE_TEST_CASE_P(ContainerStorage,
    StorageTest,
    ::testing::Values(
        "container-storage;filename=work/container-data;meta-data=static-disk-hash;meta-data.page-size=2K;meta-data.size=4M;meta-data.filename=work/container-meta"));

INSTANTIATE_TEST_CASE_P(ContainerStorage,
    ContainerStorageTest,
    ::testing::Combine(
        ::testing::Values("", "deflate", "bz2", "lz4", "snappy"),
        ::testing::Values(0, 1, 2)));

}
}
