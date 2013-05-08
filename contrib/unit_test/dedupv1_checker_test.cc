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

#include <map>
#include <vector>
#include <string>
#include <iostream>

#include <dedupv1.pb.h>
#include <gtest/gtest.h>

#include <dedupv1_checker.h>
#include <dedupv1_replayer.h>

#include <core/chunk_index.h>
#include <core/chunk_mapping.h>
#include <core/dedup_system.h>
#include <core/dedupv1_scsi.h>
#include <base/logging.h>
#include <base/memory.h>
#include <core/storage.h>
#include <base/strutil.h>
#include <base/index.h>
#include "dedupv1d.h"
#include <test_util/log_assert.h>

using std::map;
using std::string;
using std::vector;
using dedupv1::scsi::SCSI_OK;
using dedupv1::chunkindex::ChunkIndex;
using dedupv1::gc::GarbageCollector;
using dedupv1::base::Index;
using dedupv1::base::PersistentIndex;
using dedupv1::base::IndexIterator;
using dedupv1::chunkindex::ChunkMapping;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::strutil::ToHexString;
using dedupv1d::Dedupv1d;
using dedupv1::contrib::check::Dedupv1Checker;

using namespace dedupv1;
LOGGER("Dedupv1CheckerTest");

class Dedupv1CheckerTest : public testing::TestWithParam<uint32_t> {
protected:
    USE_LOGGING_EXPECTATION();

    Dedupv1d* system;
    uint32_t passes;

    virtual void SetUp() {
        system = NULL;
    }

    virtual void TearDown() {
        if (system) {
            delete system;
        }
    }
};

TEST_P(Dedupv1CheckerTest, Init)
{
    passes = GetParam();
    Dedupv1Checker checker(false, false);
    ASSERT_TRUE(checker.set_passes(passes));
}

TEST_P(Dedupv1CheckerTest, CheckWithUnreplayedLog)
{
    passes = GetParam();
    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(system->Stop());
    delete system;
    system = NULL;

    Dedupv1Checker checker(false, true);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    EXPECT_TRUE(checker.Stop());
}

TEST_P(Dedupv1CheckerTest, Check)
{
    passes = GetParam();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext::FastStopContext()));
    ASSERT_TRUE(system->Stop());
    delete system;
    system = NULL;

    // replay
    {
        dedupv1::contrib::replay::Dedupv1Replayer replayer;
        ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
        ASSERT_TRUE(replayer.Replay());
        ASSERT_TRUE(replayer.Stop());
    } // let replayer go out of scope here

    Dedupv1Checker checker(false, false);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_EQ(0, checker.reported_errors());
    EXPECT_TRUE(checker.Stop());
}

TEST_P(Dedupv1CheckerTest, CheckWithChunkDataAddressError)
{
    passes = GetParam();
    EXPECT_LOGGING(dedupv1::test::WARN).Repeatedly();

    // write some data to the system
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    ASSERT_TRUE(system->Start(dedupv1::StartContext()));
    ASSERT_TRUE(system->Run());
    FILE* file = fopen("data/random","r");
    ASSERT_TRUE(file);

    byte buffer[64 * 1024];
    ASSERT_EQ(65536, fread(buffer, sizeof(byte), 65536, file));

    DedupVolume* volume = system->dedup_system()->GetVolume(0);
    ASSERT_TRUE(volume);

    ASSERT_TRUE(volume->MakeRequest(REQUEST_WRITE, 0, 64 * 1024, buffer, NO_EC));
    fclose(file);
    ASSERT_TRUE(system->Shutdown(dedupv1::StopContext()));
    ASSERT_TRUE(system->Stop());
    delete system;
    system = NULL;

    // replay
    {
        dedupv1::contrib::replay::Dedupv1Replayer replayer;
        ASSERT_TRUE(replayer.Initialize("data/dedupv1_test.conf"));
        ASSERT_TRUE(replayer.Replay());
        ASSERT_TRUE(replayer.Stop());
    } // let replayer go out of scope here

    // open to introduce an error
    system = new Dedupv1d();
    ASSERT_TRUE(system->LoadOptions("data/dedupv1_test.conf"));
    StartContext start_context;
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(system->Start(start_context));

    dedupv1::chunkindex::ChunkIndex* chunk_index = system->dedup_system()->chunk_index();

    // get a fingerprint
    dedupv1::base::IndexIterator* i = chunk_index->CreatePersistentIterator();
    ASSERT_TRUE(i);
    size_t fp_size = 20;
    byte fp[fp_size];
    ChunkMappingData chunk_data;
    ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, i->Next(fp, &fp_size, &chunk_data));
    delete i;

    chunk_data.set_data_address(0); // a wrong data address
    dedupv1::base::PersistentIndex* persitent_chunk_index =
        chunk_index->persistent_index();
    ASSERT_EQ(persitent_chunk_index->Put(fp, fp_size, chunk_data), dedupv1::base::PUT_OK);

    delete system;
    system = NULL;

    // replay 2
    {
        dedupv1::contrib::replay::Dedupv1Replayer replayer2;
        ASSERT_TRUE(replayer2.Initialize("data/dedupv1_test.conf"));
        ASSERT_TRUE(replayer2.Replay());
        ASSERT_TRUE(replayer2.Stop());
    } // let replayer go out of scope here

    // check
    Dedupv1Checker checker(false, false);
    checker.set_passes(passes);
    EXPECT_TRUE(checker.Initialize("data/dedupv1_test.conf"));
    EXPECT_TRUE(checker.Check());
    uint64_t number_of_chunks = checker.dedupv1d()->dedup_system()->chunk_index()->GetPersistentCount();
    ASSERT_EQ(checker.get_all_pass_processed_chunks(), number_of_chunks);
    ASSERT_EQ(checker.get_all_pass_skipped_chunks(), number_of_chunks * (checker.passes() - 1));
    ASSERT_GT(checker.reported_errors(), 0);
    EXPECT_TRUE(checker.Stop());
}

INSTANTIATE_TEST_CASE_P(Dedupv1Checker,
    Dedupv1CheckerTest,
    ::testing::Values(0U, 1U, 2U, 3U, 4U))
// passes
;
