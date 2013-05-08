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
    DedupSystem* system;

    ContainerTestHelper* container_helper;

    virtual void SetUp() {
        container_helper = new ContainerTestHelper(kTestDataSize, kTestDataCount);
        container_helper->SetUp();

        system = NULL;
    }

    virtual void TearDown() {
        if (system) {
            ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
            delete system;
            system = NULL;
        }

        delete container_helper;
    }

    void ValidateTestData(ChunkIndex* chunk_index) {
        for (int i = 0; i < kTestDataCount; i++) {
            ChunkMapping mapping(container_helper->fingerprint(i));
            ASSERT_EQ(chunk_index->Lookup(&mapping, NO_EC), LOOKUP_FOUND)
            << "Validate " << i << " failed";
            ASSERT_EQ(container_helper->data_address(i), mapping.data_address());
        }
    }
};

INSTANTIATE_TEST_CASE_P(ChunkIndex,
    ChunkIndexTest,
    ::testing::Values("data/dedupv1_test.conf"));

TEST_P(ChunkIndexTest, Start) {
    system =  DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, false, false);
    ASSERT_TRUE(system);
    ASSERT_TRUE(system->chunk_index());
}

TEST_P(ChunkIndexTest, Update) {
    system =  DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, false, false);
    ASSERT_TRUE(system);

    container_helper->WriteDefaultData(system, 0, kTestDataCount);
    ValidateTestData(system->chunk_index());

}

TEST_P(ChunkIndexTest, UpdateAfterClose) {
    system =  DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, false, false);
    ASSERT_TRUE(system);

    container_helper->WriteDefaultData(system, 0, kTestDataCount);
    ValidateTestData(system->chunk_index());

    // Close and Restart
    ASSERT_TRUE(system->Stop(StopContext::FastStopContext()));
    delete system;

    system = DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, true);
    ASSERT_TRUE(system);

    ValidateTestData(system->chunk_index());
}

TEST_P(ChunkIndexTest, UpdateAfterSlowShutdown) {
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 2).Matches("Still .* chunks in auxiliary chunk index");

    system = DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, false, false);
    ASSERT_TRUE(system);
    container_helper->WriteDefaultData(system, 0, kTestDataCount);
    ASSERT_TRUE(system->chunk_store()->Flush(NO_EC));
    ValidateTestData(system->chunk_index());

    // Close and Restart
    ASSERT_TRUE(system->Stop(dedupv1::StopContext::WritebackStopContext()));
    delete system;

    system =  DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, true, false /* dirty */);
    ASSERT_TRUE(system);
    // no replay happened

    ValidateTestData(system->chunk_index());
}

/**
 * This unit test verify that the correct (and minimal) maximal key size is used for the persistent
 * chunk index
 */
TEST_P(ChunkIndexTest, CorrectMaxKeySize) {
    system =  DedupSystemTest::CreateDefaultSystemWithOptions(GetParam(), &info_store, true, false, false);
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

}
}
