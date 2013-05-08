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

LogTestLogConsumer::LogTestLogConsumer() {
    waiting_time_ = 0;
}

bool LogTestLogConsumer::LogReplay(enum event_type event_type,
    const LogEventData& event_value,
    const LogReplayContext& context) {
    replay_mode_map[context.replay_mode()]++;
    type_map[event_type]++;

    value_list.push_back(event_value);
    type_list_.push_back(event_type);

    if (waiting_time_ > 0) {
        sleep(waiting_time_);
    }

    DEBUG("Replay event " <<
        Log::GetReplayModeName(context.replay_mode()) << " - " <<
        Log::GetEventTypeName(
            event_type));
    return true;
}

void LogTestLogConsumer::set_waiting_time(uint32_t s) {
    waiting_time_ = s;
}

void LogTestLogConsumer::Clear() {
    type_map.clear();
    replay_mode_map.clear();
}

const std::list<enum event_type>& LogTestLogConsumer::type_list() const {
    return type_list_;
}

const string& LogTest::config_file() {
    return std::tr1::get<0>(GetParam());
}

int LogTest::message_size() {
    return std::tr1::get<1>(GetParam());
}

void LogTest::SetUp() {
    log = NULL;

    dedupv1::log::EventTypeInfo::RegisterEventTypeInfo(kEventTypeTestLarge,
        dedupv1::log::EventTypeInfo(
            LogEventData::kMessageDataFieldNumber));
    dedupv1::log::EventTypeInfo::RegisterEventTypeInfo(kEventTypeTestLarge2,
        dedupv1::log::EventTypeInfo(
            LogEventData::kMessageDataFieldNumber));

    EXPECT_CALL(system_, info_store()).WillRepeatedly(Return(&info_store_));
}

void LogTest::FillMessage(MessageData* message) {
    char* data = new char[message_size()];
    memset(data, 1, message_size());
    message->set_message(data, message_size());
    delete[] data;
}

void LogTest::TearDown() {
    if (log) {
        delete log;
        log = NULL;
    }
}

Log* LogTest::CreateLog(const string& config_option) {
    vector<string> options;
    CHECK_RETURN(Split(config_option, ";", &options), NULL, "Failed to split: " << config_option);

    Log* log = Log::Factory().Create(options[0]);
    CHECK_RETURN(log, NULL, "Failed to create log");

    for (size_t i = 1; i < options.size(); i++) {
        string option_name;
        string option;
        CHECK_RETURN(Split(options[i],
                "=",
                &option_name,
                &option), NULL, "Failed to split " << options[i]);
        CHECK_RETURN(log->SetOption(option_name, option), NULL, "Failed set option: " << options[i]);
    }
    return log;
}

const enum event_type LogTest::kEventTypeTestLarge = EVENT_TYPE_NEXT_ID;
const enum event_type LogTest::kEventTypeTestLarge2 =
    static_cast<enum event_type>(EVENT_TYPE_NEXT_ID + 1);

TEST_P(LogTest, Init) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
}

TEST_P(LogTest, Start) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    ASSERT_EQ(log->consumer_count(), 0U);
}

TEST_P(LogTest, DoubleStart) {
    EXPECT_LOGGING(dedupv1::test::ERROR).Once();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    ASSERT_FALSE(log->Start(StartContext(), &system_));
}

TEST_P(LogTest, Restart) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));
    SystemStartEventData event_data;
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_SYSTEM_START, &event_data, NO_EC), IsValid());
    delete log;

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system_));
}

TEST_P(LogTest, SimpleCommit) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    VolumeDetachedEventData event_data;
    event_data.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &event_data, NO_EC), IsValid());
}

TEST_P(LogTest, EmptyCommit) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_NONE, NULL, NO_EC), IsValid());
}

TEST_P(LogTest, RegisterAndUnregister) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    LogTestLogConsumer c1;
    LogTestLogConsumer c2;

    ASSERT_TRUE(log->RegisterConsumer("c1", &c1));
    ASSERT_TRUE(log->RegisterConsumer("c2", &c2));

    ASSERT_EQ(log->consumer_count(), 2U);

    ASSERT_TRUE(log->UnregisterConsumer("c1"));
    ASSERT_TRUE(log->UnregisterConsumer("c2"));

    ASSERT_EQ(log->consumer_count(), 0U);
}

/**
 * Tests if the IsRegistered method works correctly
 */
TEST_P(LogTest, IsRegistered) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    LogTestLogConsumer c1;

    ASSERT_FALSE(log->IsRegistered("c1").value());
    ASSERT_TRUE(log->RegisterConsumer("c1", &c1));
    ASSERT_TRUE(log->IsRegistered("c1").value());

    ASSERT_TRUE(log->UnregisterConsumer("c1"));
    ASSERT_FALSE(log->IsRegistered("c1").value());
}

/**
 * Tests if it is able to register a consumer before the log is started
 */
TEST_P(LogTest, RegisterBeforeStart) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer c1;
    LogTestLogConsumer c2;

    ASSERT_TRUE(log->RegisterConsumer("c1", &c1));
    ASSERT_TRUE(log->RegisterConsumer("c2", &c2));

    ASSERT_EQ(log->consumer_count(), 2U);

    ASSERT_TRUE(log->UnregisterConsumer("c1"));
    ASSERT_TRUE(log->UnregisterConsumer("c2"));

    ASSERT_EQ(log->consumer_count(), 0U);
}

TEST_P(LogTest, ReplayWithoutConsumer) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    VolumeAttachedEventData message;
    message.set_volume_id(19);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NO_EC), IsValid());
    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
}

TEST_P(LogTest, MovingReplayID) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NO_EC), IsValid());
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NO_EC), IsValid());

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;
    context.Clear();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system_));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    uint64_t replay_log_id = 0;
    ASSERT_EQ(log->Replay(EVENT_REPLAY_MODE_REPLAY_BG, 1, &replay_log_id, NULL), LOG_REPLAY_OK);

    ASSERT_GT(log->replay_id(), replay_log_id);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, ReplayCrash) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(), &system_));

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NO_EC), IsValid());
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NO_EC), IsValid());

    ASSERT_TRUE(log->UnregisterConsumer("context"));
    delete log;
    log = NULL;
    context.Clear();

    log = CreateLog(config_file());
    ASSERT_TRUE(log);
    ASSERT_TRUE(log->Start(StartContext(StartContext::NON_CREATE), &system_));
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());

    ASSERT_GT(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 1U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 1U);

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

TEST_P(LogTest, ReplayWithConsumer) {
    log = CreateLog(config_file());
    ASSERT_TRUE(log);

    LogTestLogConsumer context;
    ASSERT_TRUE(log->RegisterConsumer("context", &context));

    ASSERT_TRUE(log->Start(StartContext(), &system_));

    VolumeAttachedEventData message;
    message.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_ATTACH, &message, NO_EC), IsValid());
    VolumeDetachedEventData message2;
    message2.set_volume_id(1);
    ASSERT_THAT(log->CommitEvent(EVENT_TYPE_VOLUME_DETACH, &message2, NO_EC), IsValid());

    ASSERT_TRUE(log->PerformFullReplayBackgroundMode());
    ASSERT_GT(context.replay_mode_map[EVENT_REPLAY_MODE_REPLAY_BG], 0U);
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_ATTACH], 1U); // 1 BG
    ASSERT_EQ(context.type_map[EVENT_TYPE_VOLUME_DETACH], 1U); // 1 BG

    ASSERT_TRUE(log->UnregisterConsumer("context"));
}

}
}
