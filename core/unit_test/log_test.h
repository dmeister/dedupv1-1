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

#ifndef LOG_TEST_H_
#define LOG_TEST_H_

#include <gtest/gtest.h>

#include <string>
#include <map>
#include <list>

#include <core/log.h>
#include <test_util/log_assert.h>
#include <test/dedup_system_mock.h>
#include <test/block_index_mock.h>
#include <test/chunk_index_mock.h>

namespace dedupv1 {
namespace log {

class LogTestLogConsumer : public LogConsumer {
public:
    std::map<enum event_type, uint32_t> type_map;
    std::map<enum replay_mode, uint32_t> replay_mode_map;
    std::list<LogEventData> value_list;
    std::list<enum event_type> type_list_;

    uint32_t waiting_time_;

    LogTestLogConsumer();

    virtual bool LogReplay(enum event_type event_type,
                           const LogEventData& event_value,
                           const LogReplayContext& context);

    void set_waiting_time(uint32_t s);

    void Clear();

    const std::list<enum event_type>& type_list() const;
};

/**
 * Test for log classes
 */
class LogTest : public testing::TestWithParam<std::tr1::tuple<std::string, int> > {
protected:
    USE_LOGGING_EXPECTATION();

    Log* log;

    static const enum event_type kEventTypeTestLarge;
    static const enum event_type kEventTypeTestLarge2;

    dedupv1::MemoryInfoStore info_store_;
    MockDedupSystem system_;

    const std::string& config_file();
    int message_size();

    void FillMessage(MessageData* message);

    virtual void SetUp();
    virtual void TearDown();
public:

    /**
     * Creates a log with the options given.
     * @param options
     * @return
     */
    static Log* CreateLog(const std::string& options);
};

}
}

#endif /* FILTER_TEST_H_ */
