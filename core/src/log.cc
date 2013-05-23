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

#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <errno.h>

#include <string>
#include <list>
#include <sstream>

#include <tbb/tick_count.h>

#include <dedupv1.pb.h>
#include <dedupv1_stats.pb.h>
#include <google/protobuf/descriptor.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <core/log.h>
#include <base/strutil.h>
#include <base/fileutil.h>
#include <base/protobuf_util.h>
#include <base/index.h>
#include <base/logging.h>
#include <base/memory.h>
#include <base/timer.h>
#include <base/fault_injection.h>
#include <core/dedup_system.h>

using std::list;
using std::string;
using std::stringstream;
using std::string;
using std::vector;
using dedupv1::base::strutil::ToString;
using dedupv1::base::strutil::Join;
using dedupv1::base::strutil::To;
using dedupv1::base::strutil::ToStorageUnit;
using dedupv1::base::strutil::StartsWith;
using dedupv1::base::strutil::FriendlySubstr;
using dedupv1::base::ProfileTimer;
using dedupv1::base::MutexLock;
using dedupv1::base::timed_bool;
using dedupv1::base::ScopedLock;
using dedupv1::base::ScopedReadWriteLock;
using dedupv1::base::TIMED_FALSE;
using dedupv1::base::ScopedArray;
using dedupv1::base::NewRunnable;
using dedupv1::base::IDBasedIndex;
using dedupv1::base::ThreadUtil;
using dedupv1::base::Option;
using dedupv1::base::lookup_result;
using dedupv1::base::LOOKUP_ERROR;
using dedupv1::base::LOOKUP_FOUND;
using dedupv1::base::LOOKUP_NOT_FOUND;
using dedupv1::base::DELETE_ERROR;
using dedupv1::base::Index;
using dedupv1::base::PUT_ERROR;
using dedupv1::base::put_result;
using dedupv1::base::SerializeMessageToString;
using tbb::tick_count;
using tbb::spin_mutex;
using tbb::spin_rw_mutex;
using dedupv1::base::Thread;
using google::protobuf::Message;
using google::protobuf::FieldDescriptor;
using dedupv1::base::make_option;
using dedupv1::base::ThreadUtil;

LOGGER("Log");

namespace dedupv1 {
namespace log {

MetaFactory<Log> Log::factory_("Log", "log");

MetaFactory<Log>& Log::Factory() {
    return factory_;
}

Log::Log() {
}

bool Log::SetOption(const string& option_name, const string& option) {
    ERROR("Illegal option name: " << option_name);
    return false;
}

bool Log::Start(const StartContext& start_context, dedupv1::DedupSystem* system) {
    return true;
}

bool Log::Run() {
    return true;
}

bool Log::Stop(const dedupv1::StopContext& stop_context) {
    return true;
}

Log::~Log() {
    if (this->consumer_list_.size() != 0) {
        vector<string> consumer_names;
        list<LogConsumerListEntry>::iterator i;
        for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
            consumer_names.push_back(i->name());
        }
        WARNING("Log should not close with consumers: [" << Join(consumer_names.begin(), consumer_names.end(), ", ")
                                                         << "]");
    }
}

bool Log::PublishLogStateChange(const LogReplayStateChangeEvent& change) {
    bool success = true;

    spin_rw_mutex::scoped_lock l(this->consumer_list_lock_, false);
    list<LogConsumerListEntry>::iterator i;
    for (i = consumer_list_.begin(); i != consumer_list_.end(); i++) {
        LogConsumer* consumer = i->consumer();
        CHECK(consumer, "Consumer not set");

        const string& consumer_name(i->name());
        if (!consumer->LogReplayStateChange(change)) {
            WARNING("Replay state change publishing failed: " <<
                ", log consumer " << consumer_name);
            success = false;
        }
    }
    return success;

}

bool Log::PublishEvent(const LogReplayContext& replay_context,
                       enum event_type event_type,
                       const LogEventData& event_data) {
    bool success = true;

    TRACE("Publish event: " << Log::GetEventTypeName(event_type) <<
        ", replay mode " << Log::GetReplayModeName(replay_context.replay_mode()) <<
        ", event log id " << replay_context.log_id());

    spin_rw_mutex::scoped_lock l(this->consumer_list_lock_, false);
    list<LogConsumerListEntry>::iterator i;
    for (i = consumer_list_.begin(); i != consumer_list_.end(); i++) {
        LogConsumer* consumer = i->consumer();
        CHECK(consumer, "Consumer not set");

        const string& consumer_name(i->name());
        if (!consumer->LogReplay(event_type, event_data, replay_context)) {
            WARNING("Replay failed: " << Log::GetEventTypeName(event_type) <<
                ", log consumer " << consumer_name <<
                ", log id " << replay_context.log_id() <<
                ", replay mode " << Log::GetReplayModeName(replay_context.replay_mode()));
            success = false;
        }
    }
    // RACE CONDITION
    // if a "publish" is active, a delete is pushed to a list
    return success;
}

Option<bool> Log::IsRegistered(const std::string& consumer_name) {
    spin_rw_mutex::scoped_lock l(this->consumer_list_lock_);
    list<LogConsumerListEntry>::iterator i;
    for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
        if (i->name() == consumer_name) {
            return make_option(true);
        }
    }
    return make_option(false);
}

bool Log::RegisterConsumer(const string& consumer_name, LogConsumer* consumer) {
    CHECK(consumer, "Consumer replay function not set");
    CHECK(consumer_name.size() < 128, "Consumer name too long");
    CHECK(consumer_name.size() > 0, "Consumer name too long");

    DEBUG("Register consumer: name " << consumer_name);

    // acquire the lock for writing without setting of lock into the
    // write lock waiting state.
    // If we would go into the write lock waiting state, further requests to acquire
    // the lock for reading would block and this might lead to a deadlock since
    // the log may acquire read locks during it holds read locks.
    spin_rw_mutex::scoped_lock l;
    while (!l.try_acquire(this->consumer_list_lock_)) {
        sleep(1);
    }

    list<LogConsumerListEntry>::iterator i;
    for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
        if (i->name() == consumer_name) {
            ERROR("Found already registered consumer: " << consumer_name);
            return false;
        }
    }
    LogConsumerListEntry entry(consumer_name, consumer);
    this->consumer_list_.push_back(entry);
    return true;
}

bool Log::UnregisterConsumer(const string& consumer_name) {
    DEBUG("Remove log consumer " << consumer_name);

    // acquire the lock for writing without setting of lock into the
    // write lock waiting state.
    // If we would go into the write lock waiting state, further requests to acquire
    // the lock for reading would block and this might lead to a deadlock since
    // the log may acquire read locks during it holds read locks.
    spin_rw_mutex::scoped_lock l;
    while (!l.try_acquire(this->consumer_list_lock_)) {
        sleep(1);
    }
    list<LogConsumerListEntry>::iterator i;
    for (i = this->consumer_list_.begin(); i != this->consumer_list_.end(); i++) {
        if (i->name() == consumer_name) {
            this->consumer_list_.erase(i);
            return true;
        }
    }
    ERROR("Cannot find log consumer: " << consumer_name);
    return false;
}

string Log::GetEventTypeName(enum event_type event_type) {
    switch (event_type) {
    case EVENT_TYPE_CONTAINER_OPEN:
        return "Container Open";
    case EVENT_TYPE_CONTAINER_COMMIT_FAILED:
        return "Container Commit Failed";
    case EVENT_TYPE_CONTAINER_COMMITED:
        return "Container Committed";
    case EVENT_TYPE_CONTAINER_MERGED:
        return "Container Merged";
    case EVENT_TYPE_CONTAINER_MOVED:
        return "Container Moved";
    case EVENT_TYPE_CONTAINER_DELETED:
        return "Container Deleted";
    case EVENT_TYPE_BLOCK_MAPPING_DELETED:
        return "Block Mapping Deleted";
    case EVENT_TYPE_BLOCK_MAPPING_WRITTEN:
        return "Block Mapping Written";
    case EVENT_TYPE_VOLUME_ATTACH:
        return "Volume Attached";
    case EVENT_TYPE_VOLUME_DETACH:
        return "Volume Detached";
    case EVENT_TYPE_SYSTEM_START:
        return "System Start";
    default:
        return "Unknown event type (" + ToString(event_type) + ")";
    }
}

string Log::GetReplayModeName(enum replay_mode replay_mode) {
    if (replay_mode == EVENT_REPLAY_MODE_REPLAY_BG) {
        return "Replay Background";
    } else if (replay_mode == EVENT_REPLAY_MODE_DIRTY_START) {
        return "Replay Dirty";
    }
    return "Unknown Replay mode";
}

bool Log::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

bool Log::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

string Log::PrintLockStatistics() {
    return "null";
}

string Log::PrintStatistics() {
    return "null";
}

string Log::PrintTrace() {
    return "null";
}

string Log::PrintProfile() {
    return "null";
}

#ifdef DEDUPV1_CORE_TEST

void Log::ClearData() {
}
#endif

size_t Log::consumer_count() {
    return this->consumer_list_.size();
}

LogConsumerListEntry::LogConsumerListEntry() {
    consumer_ = NULL;
}

LogConsumerListEntry::LogConsumerListEntry(const std::string& name, LogConsumer* consumer) {
    this->name_ = name;
    this->consumer_ = consumer;
}

const std::string& LogConsumerListEntry::name() {
    return name_;
}

LogConsumer* LogConsumerListEntry::consumer() {
    return consumer_;
}

FullReplayLogConsumer::FullReplayLogConsumer(int64_t replay_id_at_start, int64_t log_id_at_start) {
    this->replay_id_at_start = replay_id_at_start;
    this->log_id_at_start = log_id_at_start;
    this->last_full_percent_progress = 0;
    this->start_time = tick_count::now();
}

bool FullReplayLogConsumer::LogReplay(dedupv1::log::event_type event_type, const LogEventData& data,
                                      const dedupv1::log::LogReplayContext& context) {
    int64_t total_replay_count = log_id_at_start - replay_id_at_start - 1;
    int64_t replayed_count = context.log_id() - replay_id_at_start;
    double ratio = (100.0 * replayed_count) / total_replay_count;

    TRACE("Replayed event: " << Log::GetEventTypeName(event_type) << ", log id " << context.log_id());

    if (ratio >= this->last_full_percent_progress + 1) {
        tick_count::interval_t run_time = tick_count::now() - this->start_time;

        this->last_full_percent_progress = ratio; // implicit cast
        if (last_full_percent_progress >= 0.0 && last_full_percent_progress <= 100.0) {
            INFO("Replayed " << this->last_full_percent_progress << "% of log, running time " << run_time.seconds()
                             << "s, mode " << Log::GetReplayModeName(context.replay_mode()));
        }
    }
    return true;
}

}
}

