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

#ifndef LOG_MOCK_H_
#define LOG_MOCK_H_

#include <gmock/gmock.h>
#include <core/log.h>
#include <core/log_consumer.h>

class MockLog : public dedupv1::log::Log {
    public:
        MOCK_METHOD3(CommitEvent, dedupv1::base::Option<int64_t>(dedupv1::log::event_type,
                const google::protobuf::Message*,
                dedupv1::base::ErrorContext*));
        MOCK_METHOD1(PerformFullReplayBackgroundMode, bool(bool write_boundary_events));
        MOCK_METHOD0(PerformDirtyReplay, bool());

        MOCK_METHOD3(ReplayStart, bool(dedupv1::log::replay_mode replay_mode,
              bool is_full_replay,
              bool commit_replay_event));
        MOCK_METHOD4(Replay, dedupv1::log::log_replay_result(dedupv1::log::replay_mode replay_mode,
              uint32_t number_to_replay,
              uint64_t* replayed_log_id,
              uint32_t* number_replayed));
        MOCK_METHOD4(ReplayStop, bool(dedupv1::log::replay_mode replay_mode,
              bool success,
              bool is_full_replay,
              bool commit_replay_event));
        MOCK_METHOD2(Throttle, dedupv1::base::Option<bool>(int, int));
        MOCK_METHOD0(IsReplaying, bool());

        MOCK_METHOD2(RegisterConsumer, bool(const std::string& consumer_name, dedupv1::log::LogConsumer* consumer_));
        MOCK_METHOD1(UnregisterConsumer, bool(const std::string& consumer_name));

        MOCK_CONST_METHOD0(IsStarted, bool());
        MOCK_METHOD0(IsFull, bool());

        MOCK_METHOD2(ReadEvent,Log::log_read(int64_t id,
            LogEventData* log_entry));
        MOCK_CONST_METHOD0(log_id, int64_t());
        MOCK_CONST_METHOD0(replay_id, int64_t());
};

#endif /* LOG_MOCK_H_ */
