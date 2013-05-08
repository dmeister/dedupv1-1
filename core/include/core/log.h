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

/**
 * \page log_overview Description of log usage
 *
 * This page describes the idea behind the operations log in dedupv1.
 *
 * The operations Log is used to speed up write access. Therefore some requests are not committed when they
 * happen. This is done later, when the system is idle or if the system has no other chance. To be able
 * to recover after a crash, all necessary informations are stored in the operations log, which is kept
 * persistent on SSDs.
 *
 * At the moment there are exactly three reasons, why the log is replayed:
 * - During a dirty start. Here the log is only passed to regenerate aux-indices, but not realy replayed.
 *   This is initiated by Log::PerformDirtyReplay()
 * - If the Log is told to do a Full Replay. This is initiated by Log::PerformFullReplay()
 * - In the background if the system is idle or if the Log is going full. This is initiated by LogReplayer.
 */

#ifndef LOG_H__
#define LOG_H__

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/spin_mutex.h>
#include <tbb/spin_rw_mutex.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/log_consumer.h>
#include <base/index.h>
#include <base/profile.h>
#include <base/fileutil.h>
#include <base/thread.h>
#include <base/sliding_average.h>
#include <base/future.h>
#include <base/barrier.h>
#include <core/statistics.h>
#include <core/info_store.h>
#include <base/error.h>
#include <base/factory.h>
#include <core/throttle_helper.h>

#include <gtest/gtest_prod.h>

#include <dedupv1.pb.h>

#include <time.h>
#include <tbb/tick_count.h>

#include <set>
#include <list>
#include <string>

namespace dedupv1 {

class DedupSystem;

namespace log {

/**
 * A log consumer that prints out progress reports during the replay
 */
class FullReplayLogConsumer : public LogConsumer {
private:
    int64_t replay_id_at_start;
    int64_t log_id_at_start;
    int last_full_percent_progress;
    tbb::tick_count start_time;
public:
    /**
     * Constructor
     */
    FullReplayLogConsumer(int64_t replay_id_at_start, int64_t log_id_at_start);

    bool LogReplay(dedupv1::log::event_type event_type, const LogEventData& data,
                   const dedupv1::log::LogReplayContext& context);
};

/**
 * Type for the different results of the replay
 * of a log entry.
 */
enum log_replay_result {
    /**
     * result when a log replay had errors.
     */
    LOG_REPLAY_ERROR = 0,

    /**
     * result when a log replay completed normally.
     */
    LOG_REPLAY_OK = 1,

    /**
     * result when there are no more log entries to replay.
     */
    LOG_REPLAY_NO_MORE_EVENTS = 2

};

/**
 * A log consumer list entry is the internal representation
 * of a log consumer inside the log system.
 * Note: Default copy constructor and assignment is fine
 */
class LogConsumerListEntry {
    /**
     * Name of the log consumer
     */
    std::string name_;

    /**
     * Pointer to the log consumer
     */
    LogConsumer* consumer_;
public:
    /**
     * Default constructor to use the class in STL containers
     * @return
     */
    LogConsumerListEntry();

    /**
     * Constructor for normal use
     * @param name
     * @param consumer
     * @return
     */
    LogConsumerListEntry(const std::string& name, LogConsumer* consumer);

    /**
     * returns the name of the log consumer.
     * @return
     */
    const std::string& name();

    /**
     * returns the pointer to the log consumer.
     * @return
     */
    LogConsumer* consumer();
};

/**
 * The operations log is central for the consistency of the system in cases of crashes.
 * In addition to that it is used to move expensive (IO, network) operations out of the
 * critical data path.
 *
 *  We here apply a modified binary search method.
 */
class Log: public dedupv1::StatisticProvider {
        DISALLOW_COPY_AND_ASSIGN(Log);
    public:
        /**
         * Enumerations
         */
        enum log_read {
            LOG_READ_ERROR = 0, //!< LOG_READ_ERROR
            LOG_READ_OK = 1, //!< LOG_READ_OK
            LOG_READ_NOENT = 2, //!< LOG_READ_NOENT
            //            LOG_READ_REPLAYED = 3,//!< LOG_READ_REPLAYED
            LOG_READ_PARTIAL = 4
        //!< LOG_READ_PARTIAL
        };

        /**
         * Constructor.
         * @return
         */
        Log();

        /**
         * Destructor.
         * @return
         */
        virtual ~Log();

        /**
         * Starts the log.
         *
         * @param start_context Start context
         * @param system
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Start(const dedupv1::StartContext& start_context, dedupv1::DedupSystem* system);

        /**
         *
         * Available options:
         * - filename: String with file where the transaction data is stored
         *
         * Configures the log
         * @param option_name
         * @param option
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * Runs thread inside the log system (e.g. the background delayed direct
         * replay thread)
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Run();

        /**
         * Stops all threads in the log system.
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Stop(const dedupv1::StopContext& stop_context);

        virtual bool IsStarted() const = 0;

        /**
         * Commits the given event to the operations log.
         *
         * @param event_type
         * @param message message used as event value or NULL
         * @param commit_log_id out parameter that stored the log id of the event. If the log id is set,
         * in case of an error, the event has been committed, but some part of the postprocessing failed.
         * @param ack ack consumer that is called after the commit to disk and before the direct publishing
         * @return true iff ok, otherwise an error has occurred
         */
        virtual dedupv1::base::Option<int64_t> CommitEvent(enum event_type event_type,
            const google::protobuf::Message* message,
                dedupv1::base::ErrorContext* ec) = 0;

        /**
         * replays all the log entries in Background Mode.
         * Often it is preferred to have more control about the execution, e.g. to stop
         * the log replay so most clients of the log prefer calling ReplayStart, replayStop
         * and Replay.
         * @param write_boundary_events iff true, events for Replay start and stop will be send.
         * @return
         */
        virtual bool PerformFullReplayBackgroundMode(bool write_boundary_events = true) = 0;

        /**
         * performs a full replay.
         * The difference between PerformFullReplay and this method is that PerformFullReplay should only
         * be considered for testing replays while this method is used for the real forced
         * full replay of the system.
         * @return
         */
        virtual bool PerformDirtyReplay() = 0;

        /**
         * Denotes that a series of log replays started.
         *
         * @param replay_mode replay mode of following replayed events
         * @param is_full_replay true iff usually all open events are replayed in the series, e.g. in
         * dedupv1_check or dedupv1_replay.
         */
        virtual bool ReplayStart(enum replay_mode replay_mode, bool is_full_replay, bool commit_replay_event = true) = 0;

        /**
         * Replays the next events logged.
         *
         * If the replay fails, the log replay id may or may not be changed. Elemtnes are removes iff replay mode
         * is EVENT_REPLAY_MODE_REPLAY_BG.
         *
         * @param replay_mode replay mode to use (can be EVENT_REPLAY_MODE_DIRTY_START or EVENT_REPLAY_MODE_REPLAY_BG)
         * @param number_to_replay maximum number of elements to be replayed
         * @param replayed_log_id the last replayed log id. The method tries to fill in the last replayed log id even in
         * cases the replayed failed, but the client of this method should not rely on that. If the client is not
         * interested in the log id of the replayed event, NULL should be passed.
         * @param number_replayed the number of replayed events
         *
         * @return
         */
        virtual enum log_replay_result Replay(enum replay_mode replay_mode,
            uint32_t number_to_replay,
                uint64_t* replayed_log_id,
                uint32_t* number_replayed) = 0;

        /**
         * Denotes that a seried of log replayes ended.
         * @param success false if the replay stopped because of a replay error
         */
        virtual bool ReplayStop(enum replay_mode replay_mode,
            bool success,
            bool is_full_log_replay,
            bool commit_replay_event) = 0;

        /**
         * Throttled down the calling thread if the log is filling up or if the direct replay queue gets too large.
         *
         * Warning: Should never be called on a thread replaying log events as it might deadlock.
         *
         * @param max_direct_wait_time maximal time to wait for the direct replay queue
         * @param consider_direct_replay_queue if set to false, the method call is not sleeping because the direct replay
         * queue is too large.
         * @return
         */
        virtual dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count) = 0;

        /**
         *
         * @param id
         * @param log_entry
         * @param event_value
         * @param partial_count May be null
         * @return
         */
        virtual enum log_read ReadEvent(int64_t id,
            LogEventData* log_entry) = 0;

        /**
         * registers a log consumer.
         * The log consumer and the dedup system are responsible that the
         * pointer is valid as long as the log holds a pointer to the consumer.
         * The log releases the pointer at the close time and after a call
         * of UnregisterConsumer with the same consumer name.
         *
         * The call might deadlock if called inside the call stack of a log event.
         *
         * @param consumer_name
         * @param consumer
         * @return
         */
        virtual bool RegisterConsumer(const std::string& consumer_name, LogConsumer* consumer);

        /**
         * Removes the consumer with the given name from the log.
         *
         * The call might deadlock if called inside the call stack of a log event.
         *
         * @param consumer_name
         * @return
         */
        virtual bool UnregisterConsumer(const std::string& consumer_name);

        /**
         * Checks if a consumer with the given name is registered at the log.
         *
         * @param consumer_name
         * @return
         */
        virtual dedupv1::base::Option<bool> IsRegistered(const std::string& consumer_name);

        /**
         * returns a developer-readable name of the log event type
         * @param event_type
         * @return
         */
        static std::string GetEventTypeName(enum event_type event_type);

        /**
         * returns a developer-readable name for the replay mode
         * @param replay_mode
         * @return
         */
        static std::string GetReplayModeName(enum replay_mode replay_mode);

        /**
         * returns the number of registers log consumers.
         * @return
         */
        size_t consumer_count();

        virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Prints statistics about the log
         */
        virtual std::string PrintStatistics();

        /**
         * prints lock usage and contention statistics about the log
         * @return
         */
        virtual std::string PrintLockStatistics();

        /**
         * print trace statistics about the log
         * @return
         */
        virtual std::string PrintTrace();

        /**
         * prints profile statistics about the log
         * @return
         */
        virtual std::string PrintProfile();

        virtual bool IsReplaying() = 0;

        virtual bool IsFull() = 0;

        virtual int64_t log_id() const = 0;

        virtual int64_t replay_id() const = 0;

        static MetaFactory<Log>& Factory();

#ifdef DEDUPV1_CORE_TEST

    virtual void ClearData();
#endif
protected:
    bool PublishLogStateChange(const LogReplayStateChange& change);

    /**
     * Publish the event to the log consumers.
     *
     * @param replay_context
     * @param event_type
     * @param event_value
     * @return true iff ok, otherwise an error has occurred
     */
    bool PublishEvent(const LogReplayContext& replay_context,
                      enum event_type event_type,
                      const LogEventData& event_data);
    /**
     * list of consumes of log events during a replay
     *
     * Protected by the consumer_list_lock
     */
    std::list<LogConsumerListEntry> consumer_list_;

    /**
     * Spin lock to protect the consumer list
     */
    tbb::spin_rw_mutex consumer_list_lock_;
private:
    static MetaFactory<Log> factory_;

};

}
}

#endif  // LOG_H__
