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

#include <string>
#include <list>

#include <gtest/gtest.h>

#include <core/dedup.h>
#include <base/locks.h>

#include <core/log_consumer.h>
#include <core/log.h>
#include <base/logging.h>
#include <core/dedup_system.h>
#include <core/chunk_index.h>
#include <core/storage.h>
#include <core/chunk_store.h>
#include <core/container_storage.h>
#include <core/container_storage_gc.h>
#include <test_util/log_assert.h>
#include <base/index.h>
#include <base/disk_hash_index.h>
#include <base/strutil.h>
#include <iostream>
#include "dedup_system_test.h"
#include <base/option.h>
#include <base/fileutil.h>
#include <base/strutil.h>
#include <base/protobuf_util.h>
#include <base/disk_hash_index.h>
#include <dedupv1.pb.h>
#include <dedupv1_base.pb.h>
#include <base/index.h>
#include <signal.h>
#include "container_test_helper.h"

using std::string;
using dedupv1::base::strutil::ToHexString;
using dedupv1::chunkstore::ContainerStorage;
using dedupv1::chunkstore::ContainerGCStrategy;
using dedupv1::chunkstore::StorageSession;
using dedupv1::chunkstore::Storage;
using dedupv1::chunkstore::StorageRequest;
using dedupv1::chunkstore::ChunkStore;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::DedupSystem;
using dedupv1::log::EVENT_REPLAY_MODE_REPLAY_BG;
using dedupv1::base::Index;
using dedupv1::base::DiskHashIndex;
using dedupv1::base::strutil::FromHexString;
using dedupv1::base::strutil::ToHexString;
using dedupv1::base::SerializeSizedMessage;
using dedupv1::base::Option;
using testing::TestWithParam;
using dedupv1::base::File;

LOGGER("ChunkIndexTest");

namespace dedupv1 {
namespace chunkindex {

class ChunkIndexTest : public TestWithParam<const char*> {
protected:
    static const uint32_t kTestDataSize = 256 * 1024;
    static const uint32_t kTestDataCount = 128;

    USE_LOGGING_EXPECTATION();

    dedupv1::MemoryInfoStore info_store;
    dedupv1::base::Threadpool tp;
    DedupSystem* system;

    ContainerTestHelper* container_helper;

    virtual void SetUp() {
        container_helper = new ContainerTestHelper(kTestDataSize, kTestDataCount);
        container_helper->SetUp();

        ASSERT_TRUE(tp.SetOption("size", "8"));
        ASSERT_TRUE(tp.Start());

        system = NULL;
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            ASSERT_TRUE(system->Close());
            system = NULL;
        }

        delete container_helper;
    }

    void ValidateTestData(ChunkIndex* chunk_index) {
        for (int i = 0; i < kTestDataCount; i++) {
            ChunkMapping mapping(container_helper->fingerprint(i));
            ASSERT_EQ(chunk_index->Lookup(&mapping, false, NO_EC), LOOKUP_FOUND)
            << "Validate " << i << " failed";
            ASSERT_EQ(container_helper->data_address(i), mapping.data_address());
        }
    }
};

INSTANTIATE_TEST_CASE_P(ChunkIndex,
    ChunkIndexTest,
    ::testing::Values("data/dedupv1_test.conf"));

TEST_P(ChunkIndexTest, Start) {
    system =  DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->chunk_index());
}

TEST_P(ChunkIndexTest, Update) {
    system =  DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);

    container_helper->WriteDefaultData(system, 0, kTestDataCount);
    ValidateTestData(system->chunk_index());

}

TEST_P(ChunkIndexTest, ContainerFailed) {
    EXPECT_LOGGING(dedupv1::test::WARN).Matches("Failed to commit container").Times(0, 1);

    string config = GetParam();
    config += ";storage.container-size=4M";
    system =  DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);

    ContainerStorage* storage = dynamic_cast<ContainerStorage*>(system->storage());
    ASSERT_TRUE(storage);

    container_helper->WriteDefaultData(system, 0, kTestDataCount);

    ASSERT_TRUE(storage->FailWriteCacheContainer(
          container_helper->data_address(kTestDataCount - 1)));
}

TEST_P(ChunkIndexTest, UsageCountUpdate) {
    system =  DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);
    ChunkIndex* chunk_index = system->chunk_index();
    ASSERT_TRUE(chunk_index);

    container_helper->WriteDefaultData(system, 0, 1);

    ChunkMapping mapping2(container_helper->fingerprint(0));
    ASSERT_EQ(chunk_index->Lookup(&mapping2, false, NO_EC), LOOKUP_FOUND);
    ASSERT_EQ(mapping2.usage_count(), 10U);

    mapping2.set_usage_count(11);
    ASSERT_TRUE(chunk_index->PutOverwrite(mapping2, NO_EC));

    ChunkMapping mapping3(container_helper->fingerprint(0).data());
    ASSERT_EQ(chunk_index->Lookup(&mapping3, false, NO_EC), LOOKUP_FOUND);
    ASSERT_EQ(mapping3.usage_count(), 11U);
}

TEST_P(ChunkIndexTest, UpdateAfterClose) {
    system =  DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);

    container_helper->WriteDefaultData(system, 0, kTestDataCount);
    ValidateTestData(system->chunk_index());

    // Close and Restart
    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    ASSERT_TRUE(system->Close());

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    ValidateTestData(system->chunk_index());
}

TEST_P(ChunkIndexTest, UpdateAfterSlowShutdown) {
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 2).Matches("Still .* chunks in auxiliary chunk index");

    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);

    container_helper->WriteDefaultData(system, 0, kTestDataCount);
    ASSERT_TRUE(system->chunk_store()->Flush(NO_EC));
    ValidateTestData(system->chunk_index());

    // Close and Restart
    ASSERT_TRUE(system->Stop(dedupv1::StopContext::WritebackStopContext()));
    ASSERT_TRUE(system->Close());

    system =  DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, true, false /* dirty */);
    ASSERT_TRUE(system);
    // no replay happened

    ValidateTestData(system->chunk_index());
}

TEST_P(ChunkIndexTest, LogReplayAfterMerge) {
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 2).Matches("Still .* chunks in auxiliary chunk index");

    string config = GetParam();
    config += ";storage.gc.eviction-timeout=0";
    system =  DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp, true, false);
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->chunk_index());
    ASSERT_TRUE(system->chunk_store());

    INFO("Write data");
    container_helper->WriteDefaultData(system, 0, kTestDataCount);

    INFO("Delete data");
    StorageSession* session = system->storage()->CreateSession();

    for (int i = 0; i < kTestDataCount; i += 3) {
        ChunkMapping mapping(container_helper->fingerprint(i));
        ASSERT_EQ(system->chunk_index()->Lookup(&mapping, false, NO_EC), LOOKUP_FOUND);
        ASSERT_TRUE(session->Delete(mapping.data_address(),
            container_helper->fingerprint(i).data(),
            container_helper->fingerprint(i).size(),
            NO_EC));

        ChunkMapping mapping2(container_helper->fingerprint(i+1).data());
        ASSERT_EQ(system->chunk_index()->Lookup(&mapping2, false, NO_EC), LOOKUP_FOUND);
        ASSERT_TRUE(session->Delete(mapping2.data_address(),
              container_helper->fingerprint(i+1).data(),
              container_helper->fingerprint(i+1).size(),
              NO_EC));
    }
    ASSERT_TRUE(session->Close());

    INFO("Force gc");
    for (int i = 0; i < 16; i++) {
        ContainerStorage* container_storage = dynamic_cast<ContainerStorage*>(system->storage());
        ASSERT_TRUE(container_storage);
        ContainerGCStrategy* gc = container_storage->GetGarbageCollection();
        ASSERT_TRUE(gc);
        ASSERT_TRUE(gc->OnStoragePressure());
    }

    INFO("Stop");
    // Close and Restart
    ASSERT_TRUE(system->Stop(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(system->Close());
    system = NULL;

    INFO("Start");
    system =  DedupSystemTest::CreateDefaultSystem(config, &info_store, &tp, true, true);
    ASSERT_TRUE(system);

    // we can only validate half of the entries
    INFO("Validate");
    for (int i = 2; i < kTestDataCount; i += 3) {
        ChunkMapping mapping(container_helper->fingerprint(i));
        ASSERT_EQ(system->chunk_index()->Lookup(&mapping, false, NO_EC), LOOKUP_FOUND)
        << "Validate " << i << " failed";
        ASSERT_EQ(container_helper->data_address(i), mapping.data_address());
    }
}

/**
 * This unit test verify that the correct (and minimal) maximal key size is used for the persistent
 * chunk index
 */
TEST_P(ChunkIndexTest, CorrectMaxKeySize) {
    system =  DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);

    ChunkIndex* chunk_index = system->chunk_index();
    if (chunk_index->TestPersistentIndexIsDiskHashIndex()) {
        size_t max_key_size = chunk_index->TestPersistentIndexAsDiskHashIndexMaxKeySize();

        if (system->content_storage()->fingerprinter_name() == "sha1") {
            ASSERT_EQ(max_key_size, 20);
        } else {
            // ignore
        }
    } else {
        // ignore
    }
}

TEST_F(ChunkIndexTest, LoadBrokenChunkMapping) {
    bytestring value;
    ASSERT_TRUE(FromHexString("08fb8101100318fad08f01", &value));
    ASSERT_EQ(value.size(), 0x0b);

    ChunkMappingData message;
    ASSERT_TRUE(message.ParseFromArray(value.data(), value.size()));
}

TEST_P(ChunkIndexTest, WriteBack) {
    system = DedupSystemTest::CreateDefaultSystem(GetParam(), &info_store, &tp, true, false, false);
    ASSERT_TRUE(system);
    StorageSession* session = system->chunk_store()->CreateSession();

    container_helper->WriteDefaultData(system, 0, kTestDataCount);

    ASSERT_TRUE(system->storage()->Flush(NO_EC));
    ASSERT_TRUE(system->log()->WaitUntilDirectReplayQueueEmpty(10));

    ASSERT_TRUE(system->idle_detector()->ForceIdle(true));
    LogEventData event_value;
    dedupv1::log::LogReplayContext context(dedupv1::log::EVENT_REPLAY_MODE_DIRECT, 1);
    system->chunk_index()->LogReplay(dedupv1::log::EVENT_TYPE_REPLAY_STARTED, event_value, context);

    // This is kind of a timeout for the test.
    for (int i = 0; i < 120 && system->chunk_index()->GetDirtyCount() > 0; i++) {
        sleep(1);
    }
    // After 2 minutes, all data should be written back
    ASSERT_EQ(0, system->chunk_index()->GetDirtyCount());
}

}
}
