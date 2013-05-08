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

#ifndef FIXED_LOG_H__
#define FIXED_LOG_H__

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
#include <core/throttle_helper.h>
#include <core/log.h>
#include <gtest/gtest_prod.h>

#include <dedupv1.pb.h>

#include <time.h>

#include <set>
#include <list>
#include <string>

namespace dedupv1 {

class DedupSystem;

namespace log {

/**
 * The operations log is central for the consistency of the system in cases of crashes.
 * In addition to that it is used to move expensive (IO, network) operations out of the
 * critical data path.
 *
 * The log is implemented using a fixed-size id-based index (aka a kind of persistent
 * array) that is used as cyclic buffer as presented in every basic data structure course.
 *
 * Example:
 * [--|--|--|--|--|--|--|--|--|--]
 *  x  H               T  x  x  x
 *  x = valid log entry
 *  H - head pointer (denoting the place for the next log entry
 *  T - tail pointer
 *
 *  At the start of the log, it is crucial to recover the head and the tail pointer.
 */
class FixedLog : public dedupv1::log::Log {
    friend class FixedLogTest;
    FRIEND_TEST(FixedLogTest, RestartAfterCrash);
    FRIEND_TEST(FixedLogTest, RestartLogWithOverflow);
    FRIEND_TEST(FixedLogTest, RestartLogWithOverflowAndDeletedLastHalf);
    FRIEND_TEST(FixedLogTest, RestartLogWithOverflowAndDeletedMiddle);
    FRIEND_TEST(FixedLogTest, RestartLogWithOverflowAndDeletedStartAndEnd);
    FRIEND_TEST(FixedLogTest, RestartLogWithOverflowAndDeletedLastHalfAfterCrash);
    FRIEND_TEST(FixedLogTest, RestartLogWithTotallyEmptyLog);
    DISALLOW_COPY_AND_ASSIGN(FixedLog);
public:

    /**
     * The default index type for the log system
     */
    static const std::string kDefaultLogIndexType;

    /**
     * Default soft limit factor
     */
    static const double kDefaultSoftLimitFactor = 0.5;

    /**
     * Default hard limit factor
     */
    static const double kDefaultHardLimitFactor = 0.75;

    /**
     * Default number of elements replayed at one during dirty replay
     */
    static const uint32_t kDefaultMaxAreaSizeDirtyReplay_ = 4096;

    /**
     * Default number of elements replayed at one during full replay
     */
    static const uint32_t kDefaultMaxAreaSizeFullReplay_ = 4096;

    /**
     * Default update intervall of Log ID
     */
    static const uint32_t kDefaultLogIDUpdateIntervall_ = 4096;

    /**
     * Type for statistics about the log system
     */
    class Statistics {
public:
        Statistics();

        /**
         * Time spend to commit log entries
         */
        dedupv1::base::Profile commit_time_;

        dedupv1::base::Profile write_time_;

        /**
         * Time spend to replay log entries.
         */
        dedupv1::base::Profile replay_time_;

        dedupv1::base::Profile replay_read_time_;

        dedupv1::base::Profile replay_publish_time_;

        dedupv1::base::Profile replay_update_id_time_;

        dedupv1::base::Profile publish_time_;

        /**
         * Number of handled events
         */
        tbb::atomic<uint64_t> event_count_;

        /**
         * number of events that have been replayed. Replayed here means
         * that the events have been replayed in the background mode.
         */
        tbb::atomic<uint64_t> replayed_events_;

        tbb::atomic<uint64_t> replayed_events_by_type_[EVENT_TYPE_MAX_ID];

        dedupv1::base::SimpleSlidingAverage average_commit_latency_;

        dedupv1::base::SimpleSlidingAverage average_read_event_latency_;

        dedupv1::base::SimpleSlidingAverage average_replay_events_latency_;

        dedupv1::base::SimpleSlidingAverage average_replayed_events_per_step_;

        dedupv1::base::TemplateSimpleSlidingAverage<256>
        average_replay_events_latency_by_type_[EVENT_TYPE_MAX_ID];

        dedupv1::base::TemplateSimpleSlidingAverage<256>
        average_replayed_events_per_step_by_type_[EVENT_TYPE_MAX_ID];

        tbb::atomic<uint64_t> throttle_count_;

        dedupv1::base::Profile throttle_time_;

        /**
         * number of events that take more than one log entry.
         */
        tbb::atomic<uint64_t> multi_entry_event_count_;
    };

    /**
     * Type for the state of the log system
     */
    enum log_state {
        LOG_STATE_CREATED, // !< LOG_STATE_CREATED
        LOG_STATE_STARTED, // !< LOG_STATE_STARTED
        LOG_STATE_RUNNING, // !< LOG_STATE_RUNNING
        LOG_STATE_STOPPED
        // !< LOG_STATE_STOPPED
    };

    static const uint32_t kDefaultLogEntryWidth;
private:

    /**
     * State of the log system.
     */
    tbb::atomic<enum log_state> state_;

    /**
     * Indicates if this Log Object was started before (needed in Close())
     */
    bool wasStarted_;

    /**
     * Lock to protect the members of the log
     */
    tbb::spin_mutex lock_;

    /**
     * Index holding the log data.
     * Currently the tc-disk-fixed and the disk-fixed indexes are supported.
     */
    dedupv1::base::IDBasedIndex* log_data_;

    /**
     * Info store the log uses.
     * NULL before start, must be set to a valid info store after start.
     */
    dedupv1::IndexInfoStore log_info_store_;

    /**
     * Maximal aggregates size of the log files
     * (in bytes).
     */
    uint64_t max_log_size_;
    uint32_t max_log_entry_width_;
    uint32_t max_log_value_size_per_bucket_;
    uint32_t nearly_full_limit_;

    /**
     * Number of events to be replayed at once during dirty replay.
     */
    uint32_t max_area_size_dirty_replay_;

    /**
     * Number of events to be replayed at once during full replay.
     */
    uint32_t max_area_size_full_replay_;

    /**
     * Default update intervall of Log ID
     */
    uint32_t log_id_update_intervall_;

    /**
     * The next used log id.
     *
     * Protected by lock
     *
     * TODO(fermat): Why is volatile?
     */
    volatile int64_t log_id_;

    /**
     * Current replay offset.
     *
     * Is not protected by lock because only a single thread (log bg) is allowed to change this value.
     */
    tbb::atomic<int64_t> replay_id_;

    /**
     * The last fully written log id denotes the log id from which we
     * know that is has been fully written. A replay error before is extremely serious, an crash replay
     * error after it is also serious, but it can happen.
     *
     * Before we introduces this value, we assumes that only the last log id (replay id = log id)
     * is allowed to fail, but this was simply wrong, because multiple log commit operations can
     * be performed at the same time.
     *
     * Protected by lock_
     */
    tbb::atomic<int64_t> last_fully_written_log_id_;

    /**
     * Protected by lock_
     */
    std::set<int64_t> in_progress_log_id_set_;

    /**
     * Statistics about the operations log.
     */
    Statistics stats_;

    /**
     * True if the log is currently replaying.
     */
    bool is_replaying_;

    /**
     * Lock to protect is_replaying_.
     * Lock ordering: If the main log lock is acquired, the replaying lock should be
     * acquired before the main lock.
     */
    tbb::spin_mutex is_replaying_lock_;

    /**
     * During Replay we mostly read one element more then really replayed. This Element is stored here.
     *
     * May only be used if is_last_read_event_data_valid_ is true.
     */
    LogEventData last_read_event_data_;

    /**
     * During Replay we mostly read one element more then really replayed. If we have such an element,
     * the number if partitions it had is stored here.
     *
     * May only be used if is_last_read_event_data_valid_ is true.
     */
    uint32_t last_read_partial_count_;

    /**
     * if true, Replay may use last_read_event_data_ and last_read_partial_count_, if not, it has to read first
     */
    bool is_last_read_event_data_valid_;

    bool readonly_;

    ThrottleHelper throttling_;

    /**
     * Persist the given logID
     *
     * This method does not use log_id_ directly as we want to be able to
     * guarantee, that the persistent value is updated before log_id_ is
     * updated.
     *
     * @param logID The new log_id
     * @return true on success
     */
    bool PersistLogID(int64_t logID);

    /**
     * Persist the given replayID
     *
     * This method does not use replay_id_ directly as we want to be able
     * to guarantee, that the persistent value is updated before
     * replay_id_ is updated.
     *
     * @param replayID The new log_id
     * @return true on success
     */
    bool PersistReplayID(int64_t replayID);

    /**
     * returns the log position (in the index) given a log
     * id. The log index is used as a cyclic buffer.
     * @param id
     * @return
     */
    int64_t GetLogPositionFromId(int64_t id);

    /**
     * removes the log entry with the given id.
     * @param id
     * @return true iff ok, otherwise an error has occurred
     */
    bool RemoveEntry(int64_t id);

    /**
     *
     * the id is still in the in progress log id set. It is the responsibility of the caller to remove
     * the id if its own processing has finished.
     *
     * @param log_entry
     * @param log_value
     * @param log_id_given optional parameter that holds the log id given for the written entry.
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteNextEntry(const LogEventData& log_value,
        int64_t* log_id_given,
        uint32_t* log_id_count,
        dedupv1::base::ErrorContext* ec);

    /**
     * Private function that writes a given log entry to the log file.
     * It writes the log entry to the given position. It must be assured that a) no existing valid
     * log entry is written there and b) no other log entry is written there concurrently.
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteEntry(int64_t id,
        int64_t position_count,
        const LogEventData& log_value);

    /**
     * Dump the log metadata to the metadata file.
     * @return true iff ok, otherwise an error has occurred
     */
    bool DumpMetaInfo();

    /**
     * Read the metadata from the metadata file and verify its contents
     * against the current configuration.
     * @param logID_data Store here the message with the log ID
     * @param replayID_data Store here the message with the replay ID
     * @param state_data Store here the message with the state
     * @return
     */
    dedupv1::base::lookup_result ReadMetaInfo(LogLogIDData* logID_data, LogReplayIDData* replayID_data,
                                              LogStateData* state_data);

    /**
     * Call only with log lock held
     * @param reserve
     * @return
     */
    bool IsNearlyFull(int reserve);

    /**
     *
     * @param position Note: Postion != id
     * @param data Object to store the read entry
     * @return
     */
    dedupv1::base::lookup_result ReadEntryRaw(int64_t position, LogEntryData* data);

    /**
     * Calling this method should be avoided for anything else than testing purposes (e.g. to introduce
     * a corrupt state)
     * @param log_id
     */
    void SetLogPosition(int64_t log_id);

    /**
     * Calling this method should be avoided for anything else than testing purposes (e.g. to introduce
     * a corrupt state)
     * @param replay_id
     */
    void SetReplayPosition(int64_t replay_id);

    /**
     * Recovers the position of log_id and replay_id.
     * The method used a binary search variant to search the least and the maximal ids.
     * The complexity of O(n log n) where n is the size of the log file.
     * @return true iff ok, otherwise an error has occurred
     */
    bool RecoverPosition();

    /**
     * Create a new fixed-size, id-based log index using a default configuration.
     * @return
     */
    dedupv1::base::IDBasedIndex* CreateDefaultLogData();

    /**
     * returns the ratio the log is filled.
     * 0.0 if the log is empty, 1.0 if the log is totally filled with non-replayed entries.
     */
    double GetFillRatio();

    /**
     * Replays the next event logged. The replaying is strictly sequential.
     *
     * If the replay fails, the log replay id may or may not be changed. Elemtnes are removes iff replay mode
     * is EVENT_REPLAY_MODE_REPLAY_BG.
     *
     * @param replay_mode replay mode to use (can be EVENT_REPLAY_MODE_DIRTY_START or EVENT_REPLAY_MODE_REPLAY_BG)
     * @param replayed_log_id the replayed log id. The method tries to fill in the replayed log id even in cases the replayed failed, but the
     * client of this method should not rely on that. If the client is not interested in the log id of the replayed event, NULL should
     * be passed.
     *
     * @return
     */
    virtual enum FixedLog::log_read ReadEvent(int64_t replay_log_id, uint32_t* partial_count, LogEventData* event_data);

#ifdef DEDUPV1_CORE_TEST
public:
#endif

    /**
     * Internaly used method to overwrite an log entry
     * with a valid, but meaning less event (EVENT_TYPE_NONE).
     * It is used to overwrite holes in the log to overcome certain
     * kinds of split-log situations.
     * @return true iff ok, otherwise an error has occured. Usually these
     * errors are fatal
     */
    bool MakeValidEntry(int64_t id);
public:
    /**
     * Constructor.
     * @return
     */
    FixedLog();

    /**
     * Destructor.
     * @return
     */
    virtual ~FixedLog();

    static Log* CreateLog();

    static void RegisterLog();

    /**
     * Starts the log.
     *
     * @param start_context Start context
     * @param system
     * @return true iff ok, otherwise an error has occurred
     */
    bool Start(const dedupv1::StartContext& start_context,
        dedupv1::DedupSystem* system);

    /**
     *
     * Available options:
     * - filename: String with file where the transaction data is stored
     * - delayed-replay-thread-prio: int
     * - max-log-size: StorageUnit
     * - max-entry-width: StorageUnit (0..512)
     * - area-size-dirty-replay: uint32
     * - area-size-full-replay: uint32
     * - max-consistency-area-size: uint32
     * - type: String
     * - index.*
     * - throttle.*
     *
     * Configures the log
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Runs thread inside the log system (e.g. the background delayed direct
     * replay thread)
     * @return true iff ok, otherwise an error has occurred
     */
    bool Run();

    /**
     * Stops all threads in the log system.
     * @return true iff ok, otherwise an error has occurred
     */
    bool Stop(const dedupv1::StopContext& stop_context);

    virtual enum log_read ReadEvent(int64_t log_id,
        LogEventData* event_data);

    /**
     * Commits the given event to the operations log.
     *
     * @param event_type
     * @param message message used as event value or NULL
     * @return true iff ok, otherwise an error has occurred
     */
    virtual dedupv1::base::Option<int64_t> CommitEvent(enum event_type event_type,
        const google::protobuf::Message* message,
                             dedupv1::base::ErrorContext* ec);

    /**
     * replays all the log entries in Background Mode.
     * Often it is preferred to have more control about the execution, e.g. to stop
     * the log replay so most clients of the log prefer calling ReplayStart, replayStop
     * and Replay.
     * @param write_boundary_events iff true, events for Replay start and stop will be send.
     * @return
     */
    virtual bool PerformFullReplayBackgroundMode(bool write_boundary_events = true);

    /**
     * performs a full replay.
     * The difference between PerformFullReplay and this method is that PerformFullReplay should only
     * be considered for testing replays while this method is used for the real forced
     * full replay of the system.
     * @return
     */
    bool PerformDirtyReplay();

    /**
     * Denotes that a series of log replays started.
     *
     * @param replay_mode replay mode of following replayed events
     * @param is_full_replay true iff usually all open events are replayed in the series, e.g. in
     * dedupv1_check or dedupv1_replay.
     */
    virtual bool ReplayStart(enum replay_mode replay_mode,
        bool is_full_replay,
        bool commit_replay_event = true);

    /**
     * Replays the next events logged.
     *
     * If there are less events in the log as number_to_replay, then the available events will be replayed.
     *
     * At the moment the replaying is strictly sequential, but this will change in future.
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
                                          uint32_t* number_replayed);

    /**
     * Denotes that a seried of log replayes ended.
     * @param success false if the replay stopped because of a replay error
     */
    virtual bool ReplayStop(enum replay_mode replay_mode,
                            bool success,
                            bool is_full_log_replay,
                            bool commit_replay_event);

    /**
     * Throttled down the calling thread if the log is filling up or if the direct replay queue gets too large.
     *
     * Warning: Should never be called on a thread replaying log events as it might deadlock.
     *
     * @return
     */
    virtual dedupv1::base::Option<bool> Throttle(int thread_id, int thread_count);

    /**
     * Returns true if the log has been started.
     * @return
     */
    bool IsStarted() const;

    /**
     * Returns the current log id.
     * @return
     */
    int64_t log_id() const;

    /**
     * Current replay offset
     */
    int64_t replay_id() const;

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

#ifdef DEDUPV1_CORE_TEST

    bool data_cleared;

    void ClearData();
#endif

    uint64_t log_size();

    bool IsFull();

    /**
     *
     * @param id
     * @param log_entry
     * @param event_value
     * @param partial_count May be null
     * @return
     */
    virtual enum log_read ReadEntry(int64_t id, LogEntryData* log_entry, bytestring* event_value, uint32_t* partial_count);

    /**
     * returns the number of remaining free log places
     */
    bool GetRemainingFreeLogPlaces(int64_t* remaining_log_places);

    /**
     * Search actual Log Id after crash and check the whole Log.
     */
    dedupv1::base::Option<bool> CheckLogId();

    static const int32_t kDefaultNearlyFullLimit = 4;

    /**
     * returns the log data index.
     * @return
     */
    inline dedupv1::base::IDBasedIndex* log_data();

    /**
     * Returns the current state of the log
     */
    inline log_state state() const;

    /**
     * is the log currently replaying?
     *
     * @return true if replaying, false otherwise
     */
    virtual bool IsReplaying();
};

FixedLog::log_state FixedLog::state() const {
    return state_;
}

dedupv1::base::IDBasedIndex* FixedLog::log_data() {
    return log_data_;
}

}
}

#endif  // LOG_H__
