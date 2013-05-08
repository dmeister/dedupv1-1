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

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include <map>
#include <string>
#include <list>

#include <gtest/gtest.h>

#include <core/dedup_system.h>
#include <base/strutil.h>
#include <base/memory.h>
#include <core/storage.h>
#include <core/dedup.h>
#include <base/locks.h>
#include <base/option.h>
#include <base/crc32.h>
#include <base/thread.h>
#include <base/runnable.h>

#include <core/log_consumer.h>
#include <core/log.h>
#include <base/index.h>
#include <base/logging.h>
#include <core/fixed_log.h>
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <test/base_matcher.h>
#include <cryptopp/cryptlib.h>
#include <cryptopp/rng.h>

#include "log_test_util.h"
#include "log_test.h"

using std::map;
using std::list;
using std::vector;
using std::string;
using dedupv1::base::crc;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::Split;
using dedupv1::base::PersistentIndex;
using dedupv1::base::Index;
using dedupv1::base::DELETE_OK;
using dedupv1::StartContext;
using testing::Return;
using dedupv1::base::Option;
using dedupv1::base::NewRunnable;
using dedupv1::base::Thread;
using dedupv1::testutil::OpenFixedLogIndex;
using namespace CryptoPP;

LOGGER("LogTest");

namespace dedupv1 {
namespace log {

/**
 * Testing the operations log
 */
class FixedLogTest : public testing::TestWithParam<std::tr1::tuple<std::string, int> > {
protected:
    USE_LOGGING_EXPECTATION();

    dedupv1::MemoryInfoStore info_store;
    MockDedupSystem system;

    FixedLog* log;
    bool use_size_limit;

    static const enum event_type kEventTypeTestLarge;
    static const enum event_type kEventTypeTestLarge2;

    const string& config_file() {
        return std::tr1::get<0>(GetParam());
    }

    int message_size() {
        return std::tr1::get<1>(GetParam());
    }

    virtual void SetUp() {
        use_size_limit = false;
        log = NULL;

        dedupv1::log::EventTypeInfo::RegisterEventTypeInfo(kEventTypeTestLarge,
            dedupv1::log::EventTypeInfo(
                LogEventData::kMessageDataFieldNumber));
        dedupv1::log::EventTypeInfo::RegisterEventTypeInfo(kEventTypeTestLarge2,
            dedupv1::log::EventTypeInfo(
                LogEventData::kMessageDataFieldNumber));

        EXPECT_CALL(system, info_store()).WillRepeatedly(Return(&info_store));
    }

    void FillMessage(MessageData* message) {
        char* data = new char[message_size()];
        memset(data, 1, message_size());
        message->set_message(data, message_size());
        delete[] data;
    }

    virtual void TearDown() {
        if (log) {
            bool started = false;
            if (log->state() == FixedLog::LOG_STATE_RUNNING ||
                log->state() == FixedLog::LOG_STATE_STARTED) {
                Option<bool> b = log->CheckLogId();
                ASSERT_TRUE(b.valid());
                ASSERT_TRUE(b.value());
                started = true;
            }
            if (log->wasStarted_) {
                started = true;
            }
            delete log;

            // Here we check if it is possible to reopen the log
            log = CreateLog(config_file());
            if (use_size_limit) {
                ASSERT_TRUE(log->SetOption( "max-log-size", "64K"));
            }
            ASSERT_TRUE(log);
            if (started) {
                EXPECT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
            } else {
                EXPECT_TRUE(log->Start(StartContext(), &system));
            }
            delete log;
        }
    }

    void StartSizeLimitedLog(Log* log,
        bool start = true,
        bool crashed = false,
        bool restart = false) {
        use_size_limit = true;
        ASSERT_TRUE(log->SetOption( "max-log-size", "64K"));
        if (start) {
            StartContext start_context;
            if (crashed) {
                start_context.set_crashed(true);
            }
            if (restart) {
                start_context.set_create(StartContext::NON_CREATE);
            }
            ASSERT_TRUE(log->Start(start_context, &system));
        }
    }

    FixedLog* CreateLog(const string& config_option) {
        vector<string> options;
        CHECK_RETURN(Split(config_option,
                ";",
                &options), NULL, "Failed to split: " << config_option);

        FixedLog* log = new FixedLog();
        for (size_t i = 0; i < options.size(); i++) {
            string option_name;
            string option;
            CHECK_RETURN(Split(options[i],
                    "=",
                    &option_name,
                    &option), NULL, "Failed to split " << options[i]);
            CHECK_RETURN(log->SetOption(option_name,
                    option), NULL, "Failed set option: " << options[i]);
        }
        return log;
    }

};

const enum event_type FixedLogTest::kEventTypeTestLarge = EVENT_TYPE_NEXT_ID;
const enum event_type FixedLogTest::kEventTypeTestLarge2 =
    static_cast<enum event_type>(EVENT_TYPE_NEXT_ID + 1);

INSTANTIATE_TEST_CASE_P(FixedLog,
    FixedLogTest,
    ::testing::Combine(
        ::testing::Values(
            "max-log-size=1M;filename=work/test-log;info.type=sqlite-disk-btree;info.filename=work/test-log-info",
            "max-log-size=1M;filename=work/test-log1;filename=work/test-log2;info.type=sqlite-disk-btree;info.filename=work/test-log-info")
        ,
        ::testing::Values(10, 2 * 1024)));

INSTANTIATE_TEST_CASE_P(FixedLog,
    LogTest,
    ::testing::Combine(
        ::testing::Values(
            "fixed;max-log-size=1M;filename=work/test-log;info.type=sqlite-disk-btree;info.filename=work/test-log-info",
            "fixed;max-log-size=1M;filename=work/test-log1;filename=work/test-log2;info.type=sqlite-disk-btree;info.filename=work/test-log-info")
        ,
        ::testing::Values(10, 2 * 1024)));

TEST_P(FixedLogTest, FullReplayWithDifferentSizedEventsBackgroundReplayWithoutBoundaris) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions

    uint32_t commited_events = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();
}

TEST_P(FixedLogTest, FullReplayWithDifferentSizedEventsBackgroundReplayWitBoundaris) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions

    uint32_t commited_events = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(true));

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();
}

TEST_P(FixedLogTest, FullReplayWithDifferentSizedEventsBackgroundReplayRandomNumber) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions
    const uint32_t max_replay_events = 50;
    const uint32_t min_replay_events = 0;

    uint32_t commited_events = 0;
    uint32_t zero_replayed = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    log_replay_result replay_result = LOG_REPLAY_OK;
    while (replay_result == LOG_REPLAY_OK) {
        uint32_t size = rng.GenerateWord32(min_replay_events, max_replay_events);
        uint32_t replayed = 0;
        uint64_t last_replayed_id = 0;
        TRACE("Will try to replay " << size << " Events.");
        replay_result = log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, size, &last_replayed_id, &replayed);
        if (size == 0) {
            zero_replayed++;
        }
        ASSERT_GE(size,
            replayed) << "Replayed " << replayed << " events, but should not be more then " <<
        size << ". Last Replayed Log ID " << last_replayed_id;
        EXPECT_LOGGING(dedupv1::test::FATAL).Never();
        EXPECT_LOGGING(dedupv1::test::ERROR).Never();
        if (zero_replayed > 0) {
            EXPECT_LOGGING(dedupv1::test::WARN).Times(zero_replayed);
        } else {
            EXPECT_LOGGING(dedupv1::test::WARN).Never();
        }
    }
    ASSERT_EQ(LOG_REPLAY_NO_MORE_EVENTS, replay_result);
}

TEST_P(FixedLogTest, FullReplayWithDifferentSizedEventsDirtyReplayRandomNumber) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log->Start(StartContext(), &system));

    const uint32_t max_events = 100; // Will place no more then this number of messages
    const uint32_t max_event_size = 10; // In Partitions
    const uint32_t min_event_size = 1; // In Partitions
    const uint32_t max_replay_events = 50;
    const uint32_t min_replay_events = 0;

    uint32_t commited_events = 0;
    uint32_t zero_replayed = 0;
    LC_RNG rng(1024);
    char* data = new char[1024 * max_event_size];
    memset(data, 1, 1024 * max_event_size);

    while (commited_events < max_events) {
        uint32_t size = rng.GenerateWord32(min_event_size, max_event_size);
        MessageData message;
        message.set_message(data, (1024 * size) - 512);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        commited_events++;
        TRACE("Placed events with size " << size);
    }
    delete[] data;
    INFO("Placed " << commited_events << " events");

    EXPECT_LOGGING(dedupv1::test::FATAL).Never();
    EXPECT_LOGGING(dedupv1::test::ERROR).Never();
    EXPECT_LOGGING(dedupv1::test::WARN).Never();

    log_replay_result replay_result = LOG_REPLAY_OK;
    while (replay_result == LOG_REPLAY_OK) {
        uint32_t size = rng.GenerateWord32(min_replay_events, max_replay_events);
        uint32_t replayed = 0;
        uint64_t last_replayed_id = 0;
        TRACE("Will try to replay " << size << " Events.");
        replay_result = log->Replay(EVENT_REPLAY_MODE_DIRTY_START,
            size,
            &last_replayed_id,
            &replayed);
        if (size == 0) {
            zero_replayed++;
        }
        ASSERT_GE(size,
            replayed) << "Replayed " << replayed << " events, but should not be more then " <<
        size << ". Last Replayed Log ID " << last_replayed_id;
        EXPECT_LOGGING(dedupv1::test::FATAL).Never();
        EXPECT_LOGGING(dedupv1::test::ERROR).Never();
        if (zero_replayed > 0) {
            EXPECT_LOGGING(dedupv1::test::WARN).Times(zero_replayed);
        } else {
            EXPECT_LOGGING(dedupv1::test::WARN).Never();
        }
    }
    ASSERT_EQ(LOG_REPLAY_NO_MORE_EVENTS, replay_result);
}

/**
 * Tests the behavior of the log when more events are committed that it can store. The log
 * overflows.
 */
TEST_P(FixedLogTest, Overflow) {
    if (message_size() > 1024) {
        return; // skip large message sizes
    }
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 8; i++) {
        VolumeAttachedEventData message;
        message.set_volume_id(1);
        ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NO_EC), IsValid());
        VolumeDetachedEventData message2;
        message2.set_volume_id(1);
        ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
    ASSERT_GT(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 8U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 8U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(FixedLogTest, LargeValues) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->Start(StartContext(), &system));

    char buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);
    MessageData message;
    message.set_message(buffer, 16 * 1024);

    context.value_list.clear();
    ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(FixedLogTest, RestartWithLargeValuesTailDestroyedNearHead) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 1);

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    char buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);
    MessageData message;
    message.set_message(buffer, 16 * 1024);
    ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    INFO("Replayed " << replayed_ids[0] << ", " << replayed_ids[1]);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenFixedLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));
    int64_t key = (replayed_ids[1] * 2) - 1;
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = (replayed_ids[1] * 2);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    delete log;
    log_index = NULL;

    INFO("Destroy " << (replayed_ids[1] * 2) - 1 << ", " << ((replayed_ids[1] * 2)));

    INFO("Restart");
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 6);
    ASSERT_GE(context.type_map[kEventTypeTestLarge2], 5);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

/**
 * Restart the log with large values (multiple buckets) when the tail id destroyed. This
 * means that the first elements have been removed, but the last elements of a multi-bucket entry
 * not
 */
TEST_P(FixedLogTest, FailedRestartWithDestroyedReplayEvent) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Once();
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(2);

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC).valid());
        ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC).valid());
    }

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    INFO("Replayed " << replayed_ids[0] << ", " << replayed_ids[1]);

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 2U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenFixedLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));
    int64_t key = (replayed_ids[1] * 2);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = (replayed_ids[1] * 2) + 1;
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    delete log;
    log_index = NULL;

    INFO("Destroy " << (replayed_ids[1] * 2) - 1 << ", " << ((replayed_ids[1] * 2)));

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_FALSE(log->Start(start_context, &system));
    log->ClearData();
    delete log;
    log = NULL;
}

TEST_P(FixedLogTest, RestartWithHeadDestroyedNearTail) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    char buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    // two items
    MessageData message;
    message.set_message(buffer, 16 * 1024);
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC).valid());
    ASSERT_TRUE(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC).valid());
    Option<int64_t> commit_result = log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC);
    ASSERT_THAT(commit_result, IsValid());
    int64_t commit_log_id = commit_result.value();

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenFixedLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));

    int64_t event_size = replayed_ids[1] - replayed_ids[0];

    int64_t key = commit_log_id + event_size - 2;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = commit_log_id + event_size - 1;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    delete log_index;
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 6U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 5U); // one entry is destroyed

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(FixedLogTest, RestartWithLargeValuesNearHeadDestroyed) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::WARN).Once();
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 4).Logger("Log");

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    int64_t commit_log_id = 0;
    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        Option<int64_t> commit_result = log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC);
        ASSERT_THAT(commit_result, IsValid());
        commit_log_id = commit_result.value();
    }
    ASSERT_GT(commit_log_id, 0) << "Commit log id should be set";
    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // new log event
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenFixedLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext(StartContext::NON_CREATE)));

    int64_t event_size = replayed_ids[1] - replayed_ids[0];

    int64_t key = commit_log_id + event_size - 1;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);

    INFO("Manipulate last written id: " << commit_log_id - (event_size * 2));
    for (key = commit_log_id; key < commit_log_id + event_size - 2; key++) {
        DEBUG("Read log id: " << key);
        LogEntryData log_data;
        ASSERT_EQ(dedupv1::base::LOOKUP_FOUND, log_index->Lookup(&key, sizeof(key), &log_data));
        log_data.set_last_fully_written_log_id(commit_log_id - (event_size * 2));
        log_index->Put(&key, sizeof(key), log_data);
    }

    delete log_index;
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));
    ASSERT_TRUE(log->PerformDirtyReplay());

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->PerformDirtyReplay());
    delete log;
    log = NULL;
}

TEST_P(FixedLogTest, NoRestartWithDestroyedLog) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }
    EXPECT_LOGGING(dedupv1::test::ERROR).Times(0, 4).Logger("Log");
    EXPECT_LOGGING(dedupv1::test::WARN).Times(0, 4).Logger("Log");

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    for (int i = 0; i < 10; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed));
    ASSERT_EQ(3, number_replayed); // New Log, then skip

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenFixedLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));

    int64_t key = 19;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);

    delete log_index;
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_FALSE(log->Start(start_context, &system));
    log->ClearData();
    delete log;
    log = NULL;
}

/**
 * Restart the log with large values (multiple buckets) when the head id destroyed. This
 * means that the last elements of an log event have not been written
 */
TEST_P(FixedLogTest, RestartWithLargeValuesHeadDestroyed) {
    if (message_size() < 1024) {
        return; // skip small message sizes
    }

    EXPECT_LOGGING(dedupv1::test::WARN).Once();
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    byte buffer[16 * 1024];
    memset(buffer, 1, 16 * 1024);

    int64_t commit_log_id = 0;
    for (int i = 0; i < 4; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        Option<int64_t> commit_result = log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC);
        ASSERT_THAT(commit_result, IsValid());
        commit_log_id = commit_result.value();
    }

    uint64_t replayed_ids[2];
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 0, NULL));
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, replayed_ids + 1, NULL));

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 1U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    // Remove the two bucket from the next entry to replay
    // This simulates that the next entry has been replayed, but to deletion has not been
    // completed
    PersistentIndex* log_index = OpenFixedLogIndex(config_file());
    ASSERT_TRUE(log_index);
    ASSERT_TRUE(log_index->Start(StartContext()));

    int64_t event_size = replayed_ids[1] - replayed_ids[0];

    int64_t key = commit_log_id + event_size - 2;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    key = commit_log_id + event_size - 1;
    INFO("Delete " << key);
    ASSERT_TRUE(log_index->Delete(&key, sizeof(key)) == DELETE_OK);
    delete log_index;
    log_index = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        char buffer[16 * 1024];
        memset(buffer, 1, 16 * 1024);
        MessageData message;
        message.set_message(buffer, 16 * 1024);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 8U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 7U); // one entry is detroyed

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(FixedLogTest, RestartWithLogEntries) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }

    uint32_t number_replayed = 0;
    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed));
    ASSERT_EQ(1, number_replayed); // kEventTypeTestLarge

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed));
    ASSERT_EQ(1, number_replayed); // kEventTypeTestLarge2

    ASSERT_EQ(context.type_map[kEventTypeTestLarge], 1U);
    ASSERT_EQ(context.type_map[kEventTypeTestLarge2], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(FixedLogTest, PickCorrectReplayIdAfterCrash) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }

    ASSERT_TRUE(log->PerformDirtyReplay());
    ASSERT_TRUE(log->UnregisterConsumer("context"));

    int64_t replay_id = log->replay_id();
    log->ClearData();
    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));

    int64_t replay_id_2 = log->replay_id();
    INFO("replay id " << replay_id << ", replay id after restart " << replay_id_2);

    ASSERT_TRUE(abs(replay_id_2 - replay_id) < 2) << "Difference should be small";
}

TEST_P(FixedLogTest, RestartAfterCrash) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }

    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // Large1
    ASSERT_EQ(1, number_replayed);

    number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 3, NULL, &number_replayed)); // Large2
    ASSERT_EQ(1, number_replayed);

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    log->SetLogPosition(0); // introduce a corrupt state
    // log->SetReplayPosition(0); // introduce a corrupt state

    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartContext start_context;
    start_context.set_crashed(true);
    start_context.set_create(StartContext::NON_CREATE);
    ASSERT_TRUE(log->Start(start_context, &system));

    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 4; i < 8; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge2, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(FixedLogTest, RestartLogWithOverflow) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    StartSizeLimitedLog(log);
    int limit_count = log->log_data_->GetMaxItemCount();
    int overflow_count = limit_count + 10;

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < overflow_count - 20; i++) {
        MessageData message;
        message.set_message("Hello World");
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
    for (int i = overflow_count - 20; i < overflow_count; i++) {
        MessageData message;
        message.set_message("Hello World");
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_count, log->log_data_->GetMaxItemCount());
    EXPECT_LE(overflow_count, log->log_id_);
    EXPECT_GT(log->replay_id_, 10);
}

TEST_P(FixedLogTest, RestartRandom) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data()->GetMaxItemCount();
    DEBUG("Limit id " << limit_id);
    LC_RNG rng(1024);

    int64_t current_log_id = 0;
    int64_t current_replay_id = 0;
    for (int i = 0; i < 256; i++) {
        INFO("Round " << i);
        int commit_count = rng.GenerateWord32(1, limit_id / 2);
        int replay_count = rng.GenerateWord32(limit_id / 4, 0.75 * limit_id);
        if (message_size() > 1024) {
            commit_count = rng.GenerateWord32(1, limit_id / 4);
            replay_count = rng.GenerateWord32(limit_id / 5, 0.5 * limit_id);
        }

        DEBUG(
            "Round " << i << ", commit " << commit_count << ", replay " << replay_count <<
            ", limit id " << limit_id);

        for (int i = 0; i < commit_count; i++) {
            MessageData message;
            FillMessage(&message);
            ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        }

        uint32_t sum_number_replayed = 0;
        dedupv1::log::log_replay_result result = dedupv1::log::LOG_REPLAY_OK;
        while ((sum_number_replayed < replay_count) && (result == dedupv1::log::LOG_REPLAY_OK)) {
            uint32_t number_replayed = 0;
            result = log->Replay(EVENT_REPLAY_MODE_REPLAY_BG,
                replay_count - sum_number_replayed,
                NULL,
                &number_replayed);
            ASSERT_GE(replay_count, number_replayed);
            sum_number_replayed += number_replayed;
        }
        ASSERT_FALSE(result == dedupv1::log::LOG_REPLAY_ERROR);
        ASSERT_GE(replay_count, sum_number_replayed);
        ASSERT_FALSE((result == dedupv1::log::LOG_REPLAY_OK) &&
            (sum_number_replayed != replay_count));

        current_log_id = log->log_id();
        current_replay_id = log->replay_id();

        Option<bool> b = log->CheckLogId();
        ASSERT_TRUE(b.valid());
        ASSERT_TRUE(b.value());

        delete log;
        log = NULL;

        log = CreateLog(config_file());
        ASSERT_TRUE(log);
        StartSizeLimitedLog(log, true, false, true);
        ASSERT_EQ(limit_id, log->log_data()->GetMaxItemCount());
        EXPECT_EQ(current_log_id, log->log_id());
        EXPECT_EQ(current_replay_id, log->replay_id());
    }
}

TEST_P(FixedLogTest, RestartAll) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data()->GetMaxItemCount();
    DEBUG("Limit id " << limit_id);

    int64_t current_log_id = 0;
    int64_t current_replay_id = 0;
    int rounds = 16;
    if (message_size() > 1024) {
        rounds = 6;
    }

    for (int i = 0; i < rounds; i++) {
        int commit_count = 6;
        int replay_count = 5;

        DEBUG(
            "Round " << i << ", commit " << commit_count << ", replay " << replay_count <<
            ", limit id " << limit_id);

        for (int j = 0; j < commit_count; j++) {
            MessageData message;
            FillMessage(&message);
            ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
        }

        uint32_t number_replayed = 0;
        ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, replay_count, NULL, &number_replayed));
        ASSERT_EQ(replay_count, number_replayed);

        current_log_id = log->log_id();
        current_replay_id = log->replay_id();

        Option<bool> b = log->CheckLogId();
        ASSERT_TRUE(b.valid());
        ASSERT_TRUE(b.value());

        delete log;
        log = NULL;

        log = CreateLog(config_file());
        ASSERT_TRUE(log);
        StartSizeLimitedLog(log, true, false, true);
        ASSERT_EQ(limit_id, log->log_data()->GetMaxItemCount());
        EXPECT_EQ(current_log_id, log->log_id());
        EXPECT_EQ(current_replay_id, log->replay_id());
    }
}

TEST_P(FixedLogTest, RestartLogWithLogIdOnPositionZero) {
    if (message_size() > 1024) {
        return; // skip for large values
    }
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data()->GetMaxItemCount();
    DEBUG("Limit id " << limit_id);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < limit_id - 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));

    for (int i = 0; i < 9; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }
    // Replay all elements to delete them.
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));
    MessageData message;
    message.set_message("Hello World");

    ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode(false));

    int64_t current_log_id = log->log_id();
    int64_t current_replay_id = log->replay_id();

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data()->GetMaxItemCount());
    EXPECT_EQ(current_log_id, log->log_id());
    EXPECT_EQ(current_replay_id, log->replay_id());
}

TEST_P(FixedLogTest, RestartLogWithOverflowAndDeletedLastHalfAfterCrash) {
    if (message_size() > 1024) {
        return; // skip for large values
    }

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data_->GetMaxItemCount();
    DEBUG("Limit id " << limit_id);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < limit_id - 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    // Replay all elements to delete them.
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    // Insert some more elements
    for (int i = 0; i < 20; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));

    log->SetLogPosition(log->replay_id_); // introduce a corrupt state

    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, true, true); // start crashed
    ASSERT_EQ(limit_id, log->log_data_->GetMaxItemCount());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

TEST_P(FixedLogTest, RestartLogWithOverflowAndDeletedLastHalf) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);

    int64_t limit_id = log->log_data_->GetMaxItemCount();

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    // Replay all elements to delete them.
    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 10, NULL, &number_replayed));
    ASSERT_EQ(10, number_replayed);

    // Insert some more elements
    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data_->GetMaxItemCount());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

TEST_P(FixedLogTest, RestartLogWithOverflowAndDeletedMiddle) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);
    // Lower the limit so that the log doesn't replay everything.
    log->nearly_full_limit_ = 2;
    int64_t limit_id = log->log_data_->GetMaxItemCount();

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 10; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    // Replay 8 elements to delete them. There are still 2 valid elements at the start of the array
    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 8, NULL, &number_replayed));
    ASSERT_EQ(8, number_replayed);

    // Insert some more elements. Situation is now (v: valid, d: deleted): vvvvddddvv
    for (int i = 0; i < 4; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data_->GetMaxItemCount());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

TEST_P(FixedLogTest, RestartLogWithOverflowAndDeletedStartAndEnd) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log);
    // Lower the limit so that the log doesn't replay everything.
    log->nearly_full_limit_ = 2;

    int64_t limit_id = log->log_data_->GetMaxItemCount();

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    for (int i = 0; i < 9; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message,NO_EC), IsValid());
    }

    // Delete all elements
    uint32_t number_replayed = 0;
    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 9, NULL, &number_replayed));
    ASSERT_EQ(9, number_replayed);

    // Insert some more elements. Situation is now (v: valid, d: deleted): vvvvvvvvdd
    for (int i = 0; i < 8; i++) {
        MessageData message;
        FillMessage(&message);
        ASSERT_THAT(log->CommitEvent(kEventTypeTestLarge, &message, NO_EC), IsValid());
    }

    ASSERT_TRUE(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, NULL, NULL)); // Next one

    int64_t current_log_id = log->log_id_;
    int64_t current_replay_id = log->replay_id_;

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    StartSizeLimitedLog(log, true, false, true);
    ASSERT_EQ(limit_id, log->log_data_->GetMaxItemCount());
    EXPECT_EQ(current_log_id, log->log_id_);
    EXPECT_EQ(current_replay_id, log->replay_id_);
}

}
}
