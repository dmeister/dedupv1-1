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
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see
 * http://www.gnu.org/licenses/.
 */

#ifndef CONTAINER_STORAGE_H__
#define CONTAINER_STORAGE_H__

#include <base/barrier.h>
#include <base/cache_strategy.h>
#include <base/compress.h>
#include <base/fileutil.h>
#include <base/handover_store.h>
#include <base/index.h>
#include <base/locks.h>
#include <base/profile.h>
#include <base/thread.h>
#include <base/uuid.h>
#include <core/chunk_index.h>
#include <core/container.h>
#include <core/container_storage_alloc.h>
#include <core/container_storage_bg.h>
#include <core/container_storage_cache.h>
#include <core/container_storage_write_cache.h>
#include <core/dedup.h>
#include <core/idle_detector.h>
#include <core/info_store.h>
#include <core/log_consumer.h>
#include <core/storage.h>

#include <tbb/atomic.h>

#include <ctime>
#include <vector>

#include <gtest/gtest_prod.h>
#include <tbb/concurrent_unordered_map.h>

#include "dedupv1.pb.h"

namespace dedupv1
{
class DedupSystem;

namespace chunkstore
{
// declare classes (inside the namespace)
class ContainerItem;
class ContainerStorage;
class ContainerGCStrategy;
class ContainerStorageBackgroundCommitter;
class ContainerStorageAllocator;
class ContainerStorageReadCache;
class ContainerStorageWriteCache;

/**
 * The container storage is a storage implementation that
 * collects a lot of chunk data in memory and only writes them
 * to disk if the container data structure is full. This avoids
 * a lot of disk io on the storage backend.
 *
 * A container consists of a metadata section and a data section. In the metadata section, we store
 * all fingerprints collected in the container and pointers to the area of the container where the
 * data
 * is stored.
 * Additionally we store there container related metadata, e.g. if the chunk data is compressed or
 * not.
 *
 * Each container has a unique id. This id can be used during reads to lookup the position of the
 * container on disk. Reads accesses to container that are not yet committed to disk are answered
 * from a read cache.
 *
 * The clients of the container storage cannot rely on the fact that if a container with id x
 * is committed, every container with id y with y < x - w is also committed. Crashes and
 * thread scheduling issues might prevent this. However, clients can assume that
 * if a system is started and the last committed container had the id x, the system
 * will not commit any new container with id y, y < x:
 *
 * An important implementation criteria is that a container should never be overwritten as the
 * container
 * data would be lost in cases of crashes. The container storage is not "in-place transactional" and
 * it would be very performance costly to implement it that way. Always choose a Copy-On-Write
 * system.
 *
 * Thread safety: The container storage can be used from multiple threads in parallel.
 *
 * Lock ordering:
 * - Aquire a cache lock before the container lock
 * - Do not acquire a meta data lock while holding a container lock
 * - Do not acquire a cache lock while holding the meta data lock
 * - Acquire a container lock (with the intention to use the container) only when holding the meta
 * data lock.
 */
class ContainerStorage : public Storage, public dedupv1::log::LogConsumer,
    public IdleTickConsumer
{
    DISALLOW_COPY_AND_ASSIGN(ContainerStorage);
public:

    static const uint64_t kSuperBlockSize = 4096;

    /**
     * Runtime states of the container storage
     */
    enum container_storage_state {
        CREATED, // !< CREATED
        STARTING, // !< STARTING
        STARTED, // !< STARTED
        RUNNING, // !< RUNNING
        STOPPING,
        STOPPED, // !< STOPPED
    };

    class ContainerFile
    {
public:
        ContainerFile();
        ~ContainerFile();

        void Init(const std::string& f);

        bool Start(dedupv1::base::File* file, bool is_new);

        const std::string filename() const
        {
            return filename_;
        }

        void set_uuid(const dedupv1::base::UUID& uuid)
        {
            uuid_ = uuid;
        }

        uint64_t file_size() const
        {
            return file_size_;
        }

        dedupv1::base::File* file()
        {
            return file_;
        }

        bool new_file() const
        {
            return new_;
        }

        const dedupv1::base::UUID& uuid() const
        {
            return uuid_;
        }

        void set_file_size(uint64_t fs)
        {
            file_size_ = fs;
        }

        dedupv1::base::MutexLock* lock()
        {
            return lock_;
        }

        void clear_file() {
          file_ = NULL;
        }

private:
        std::string filename_;
        dedupv1::base::File* file_;

        dedupv1::base::MutexLock* lock_;

        uint64_t file_size_;

        bool new_;

        dedupv1::base::UUID uuid_;
    };
private:
    /**
     * Type for statistics about the container storage.
     */
    class Statistics
    {
public:
        Statistics();
        /**
         * Number of container read requests
         */
        tbb::atomic<uint64_t> reads_;
        tbb::atomic<uint32_t> file_lock_busy_;
        tbb::atomic<uint32_t> file_lock_free_;
        tbb::atomic<uint32_t> handover_lock_busy_;
        tbb::atomic<uint32_t> handover_lock_free_;
        tbb::atomic<uint64_t> read_cache_hit_;
        tbb::atomic<uint64_t> write_cache_hit_;
        tbb::atomic<uint32_t> container_lock_free_;
        tbb::atomic<uint32_t> container_lock_busy_;

        dedupv1::base::Profile pre_commit_time_;
        dedupv1::base::Profile total_write_time_;
        dedupv1::base::Profile total_read_time_;
        dedupv1::base::Profile total_delete_time_;
        dedupv1::base::Profile add_time_;
        dedupv1::base::Profile total_read_container_time_;
        dedupv1::base::Profile is_committed_time_;
        dedupv1::base::Profile container_write_time_;
        dedupv1::base::Profile total_file_lock_time_;
        dedupv1::base::Profile total_file_load_time_;
        dedupv1::base::Profile partial_log_time_;

        tbb::atomic<uint64_t> committed_container_;
        tbb::atomic<uint64_t> readed_container_;
        tbb::atomic<uint64_t> moved_container_;
        tbb::atomic<uint64_t> merged_container_;
        tbb::atomic<uint64_t> failed_container_;
        tbb::atomic<uint64_t> deleted_container_;

        tbb::atomic<uint64_t> partial_container_file_truncate_count_;

        /**
         * Time spent with log replay
         */
        dedupv1::base::Profile replay_time_;

        dedupv1::base::SimpleSlidingAverage average_container_load_latency_;
    };

    int64_t partial_file_size_threshold_;

    /**
     * Container files.
     */
    std::vector<ContainerFile> file_;

    std::vector<ContainerFile> partial_file_;

    std::vector<int64_t> partial_file_offset_;

    tbb::atomic<int64_t> partial_write_count_;

    /**
     * Iff all files should be preallocated at the first startup.
     */
    bool preallocate_;

    /**
     * Size of the container storage in byte
     */
    uint64_t size_;

    /**
     * map that stores the mapping from all containers currently in the write cache to the
     * position to that the container should be written later.
     */
    std::map<uint64_t, ContainerStorageAddressData> address_map_;

    dedupv1::base::Condition address_map_condition_;

    dedupv1::base::MutexLock address_map_lock_;

    /**
     * meta data index storing a map from an container id to
     * the file and file offset (merged into a single 64-bit value).
     */
    dedupv1::base::PersistentIndex* meta_data_index_;

    /**
     * Protects the meta data index when there are multiple operations that should be
     * done atomically. Simply put operations are allowed to use the read lock mode.
     *
     * The overlapping of the container locks and the meta data lock is complex. You should not
     * hold a container lock when acquiring the lock. As it is often necessary to holds the meta
     * data lock
     * to acquire the correct container lock.
     */
    dedupv1::base::ReadWriteLock meta_data_lock_;

    /**
     * Size of each container.
     */
    size_t container_size_;

    /**
     * The initial given container id at the time when the container storage is started.
     * Any container id less or equal than this value that is not committed yet, will
     * never be committed.
     */
    uint32_t initial_given_container_id_;

    /**
     * Container id for the last given container to open.
     * The next container id is last_given_container_id + 1.
     */
    tbb::atomic<uint64_t> last_given_container_id_;

    tbb::atomic<uint64_t> highest_committed_container_id_;

    /**
     * Statistical data
     */
    Statistics stats_;

    /**
     * The container lock is used to prevent to thread to concurrently modify a container.
     * Everyone that is modifying a container after it has been written (merging, deleting) must
     * acquire the lock.
     *
     * The container lock to use is determined by the GetContainerLock method. It should always the
     * primary id be used
     */
    dedupv1::base::ReadWriteLockVector container_lock_;

    /**
     * Current container state
     */
    enum container_storage_state state_;

    /**
     * Reference to the log system.
     */
    dedupv1::log::Log* log_;

    /**
     * Reference to the idle detector
     */
    IdleDetector* idle_detector_;

    /**
     * Pointer to a compressor used for compressing the container data.
     * If the pointer is NULL, no compression is used.
     */
    dedupv1::base::Compression* compression_;

    /**
     */
    ContainerStorageBackgroundCommitter background_committer_;

    /**
     * Thread to commit open container after a certain time.
     */
    dedupv1::base::Thread<bool> partial_commit_cleanup_thread_;

    /**
     * flag that is set, when the timeout commit thread should stop
     */
    // volatile bool timeout_committer_should_stop_;

    /**
     * Garbage collecting strategy.
     * Might be (and per default is) set to NULL.
     */
    ContainerGCStrategy* gc_;

    /**
     * Container allocation strategy.
     */
    ContainerStorageAllocator* allocator_;

    ContainerStorageReadCache cache_;

    ContainerStorageWriteCache write_cache_;

    dedupv1::InfoStore* info_store_;

    dedupv1::StartContext start_context_;

    bool had_been_started_;

    /**
     *
     * protected by in_move_set_lock_
     */
    std::set<uint64_t> in_move_set_;

    /**
     * protects in_move_set_
     */
    tbb::spin_mutex in_move_set_lock_;

    dedupv1::chunkindex::ChunkIndex* chunk_index_;

#ifdef DEDUPV1_CORE_TEST
    bool clear_data_called_;
#endif

    bool OpenContainerFiles(const ContainerLogfileData& log_data,
                            const dedupv1::StartContext& start_context);

    bool OpenPartialContainerFiles(const ContainerLogfileData& log_data,
        const dedupv1::StartContext& start_context);

    inline dedupv1::base::ReadWriteLock* GetContainerLock(uint64_t container_id);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool FillMergedContainerEventData(ContainerMergedEventData* event_data,
                                      const Container& leader_container,
                                      const ContainerStorageAddressData& leader_address,
                                      const Container& slave_container,
                                      const ContainerStorageAddressData& slave_address,
                                      const ContainerStorageAddressData& new_container_address);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    bool MarkContainerCommitAsFailed(Container* container);

    /**
     * Read a container from disk.
     *
     * Assumes that the container lock is already held.
     */
    dedupv1::base::lookup_result ReadContainerFromDiskLocked(Container* container,
                                                     const ContainerStorageAddressData&
                                                     container_address);

    /**
     * Writes the given container (directly) to disk. No locks must be hold at the call.
     *
     * The difference between commit container and write container is e.g. that write container does
     * not writes a CONTAINER_COMMIT log entry.
     * The caller of this method must log the now address.
     *
     * @param container
     * @param container_address
     * @return true iff ok, otherwise an error has occurred
     */
    bool WriteContainer(Container* container,
                        const ContainerStorageAddressData& container_address);

    /**
     * Writes the given container (directly) to disk. No locks must be hold at the call.
     *
     * Either a CONTAINER_COMMIT event is logged or a CONTAINER_COMMIT_FAILED event.
     *
     * THe container is handled nearly as constant, but we set the commit time
     *
     * @param container container to write to disk
     * @param address
     * @return true iff ok, otherwise an error has occurred
     */
    bool CommitContainer(Container* container,
                         const ContainerStorageAddressData& address);

    bool CheckForPartialContainerFileCleanup();

    /**
     * Runner for the partial commit cleanup thread.
     * @return true iff ok, otherwise an error has occurred
     */
    bool PartialCommitCleanupRunner();

    alloc_result GetNewContainerId(Container* container);

    /**
     * Dumps meta information about the container storage into a info store.
     * The meta information is dumped during the shutdown and additional points
     * after the start. The container storage can rely that the a read will return
     * the last dumped information, it cannot rely that the meta info is uptodate.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    bool DumpMetaInfo();

    /**
     *
     * Reads meta information, e.g. parts of the state about the chunk index from a info store.
     * This includes the next container id.
     * Additionally, the method performs a basic
     * verification if the stored info is compatible
     * to the configuration.
     *
     * @return
     */
    dedupv1::base::lookup_result ReadMetaInfo(ContainerLogfileData* log_data);

    /**
     * Prepares the commitment of the usually full container.
     * The container is handed over to the background committer.
     * @param container
     * @return true iff ok, otherwise an error has occurred
     */
    bool PrepareCommit(Container* container);

    bool WriteContainerPartial(const ContainerPartialData& partial_data,
                               dedupv1::base::ErrorContext* ec);

    bool RecoverContainerFromPartials();

    /**
     * Formats the given storage file.
     *
     * @param format_file
     * @return true iff ok, otherwise an error has occurred
     */
    bool Format(const ContainerFile& file, dedupv1::base::File* format_file);

    bool DoDeleteContainer(Container* container,
                           const ContainerStorageAddressData& container_address,
                           dedupv1::base::ReadWriteLock* container_lock);

    /**
     * Reads the given fp in the given container.
     *
     * We use this utility method as the storage session has to do a bit more than
     * the normal FindItem/CopyRawData pair, e.g. notifying the gc and other components.
     *
     * @param container
     * @param key
     * @param key_size
     * @param data
     * @param data_size
     * @return
     */
    dedupv1::base::Option<uint32_t> ReadInContainer(const Container& container,
                                                    const void* key,
                                                    size_t key_size,
                                                    void* data,
                                                    uint32_t offset,
                                                    uint32_t size,
                                                    bool* found);

    /**
     * Performs the deletion of items from the given container
     * The method assumes that
     * - the container is in the in_move_set_
     * - the cache entry lock may be held for writing.
     *
     * The method gets a lock on the container and will release it before returning.
     * The method will release the lock on the cache entry in all cases.
     *
     * It is used by the Delete method of the session mainly to make the handling
     * of the in-move set easier.
     */
    bool DoDelete(uint64_t container_id,
                  uint64_t primary_id,
                  const ContainerStorageAddressData& address,
                  const std::list<bytestring>& key_list,
                  CacheEntry* cache_entry,
                  dedupv1::base::ErrorContext* ec);

    bool PrepareWriteContainer(Container* write_container,
    const StorageRequest& request,
    dedupv1::base::ErrorContext* ec);

    bool FinishDirtyLogReplay();

    dedupv1::base::Option<bool> TryReadContainerFromWriteCache(
        Container* container);

    /**
     * Helper method for LookupContainerAddress.
     * Should usually not be called directly.
     */
    std::pair<dedupv1::base::lookup_result,
    ContainerStorageAddressData> LookupContainerAddressNoWait(
      uint64_t container_id,
      dedupv1::base::ReadWriteLock** primary_container_lock,
      bool acquire_write_lock);

    /**
     * Reads a container and fills the data into the given container.
     * The container must be initiated and the id must be set.
     *
     * Do not call this method when you hold a container or a meta data lock.
     *
     * @param container
     * @return
     */
    dedupv1::base::lookup_result ReadContainerFromDisk(Container* container);
public:
    /**
     * Constructor
     * @return
     */
    ContainerStorage();

    /**
     * Destructor
     * @return
     */
    virtual ~ContainerStorage();

    /**
     * Sets an option of an storage implementation. set_option should only be
     * called before calling start
     *
     * Available options:
     * - container-size: StorageUnit
     * - size: StorageUnit
     * - checksum: Boolean
     * - preallocate: Boolean
     * - read-cache-size
     * - write-container-count
     * - background-commit.*
     * - compression (deflate, bz2, snappy, none)
     * - filename: String
     * - filename.clear: Boolean
     * - filesize: StorageUnit
     * - meta-data: String
     * - meta-data.*
     * - write-cache.*
     * - read-cache.*
     * - gc: String
     * - gc.*: String
     * - alloc: String
     * - alloc.*: String
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name,
                           const std::string& option);

    /**
     * Starts a storage system. After a successful start the write, and read calls should work.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context,
                       DedupSystem* system);

    /**
     * Runs the storage system.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Stops the storage system.
     * @param stop_context
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

        /**
         *
         * @param key
         * @param key_size
         * @param data
         * @param data_size
         * @param address
         * @param ec error context (can be NULL)
         * @return
         */
        virtual bool WriteNew(
                std::list<StorageRequest>* requests,
            dedupv1::base::ErrorContext* ec);

    /**
     *
     * Note: In contrast to ReadInContainer and other methods, the Read method
     * should report an error, if the
     * key has not been found in the container.
     *
     * Also the read method is slightly more efficient than using ReadContainer
     *
     * @param address
     * @param key
     * @param key_size
     * @param data
     * @param data_size
     * @param ec error context (can be NULL)
     * @return
     */
    virtual dedupv1::base::Option<uint32_t> ReadChunk(uint64_t address,
                                                 const void* key, size_t key_size,
                                                 void* data,
                                                 uint32_t offset,
                                                 uint32_t size,
                                                 dedupv1::base::ErrorContext* ec);

    /**
     *
     * @param address
     * @param key_list
     * @param ec error context (can be NULL)
     * @return
     */
    virtual bool DeleteChunks(uint64_t address,
                              const std::list<bytestring>& key_list,
                              dedupv1::base::ErrorContext* ec);

    /**
     * Checks if a given address is already committed. This is checked by
     * comparing the address with the own metadata.
     *
     * If a container is currently in the write cache
     * this method will return NOT_COMMITTED.
     * If a container is currently in the bg committer, the method will
     * block until the container is committed.
     */
    virtual enum storage_commit_state IsCommitted(uint64_t address);

    /**
     * Persists the statistics of the container storage.
     * @param prefix
     * @param ps
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool PersistStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    /**
     * Restores the statistics of the container storage.
     * @param prefix
     * @param ps
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool RestoreStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    virtual std::string PrintLockStatistics();

    virtual std::string PrintStatistics();

    virtual std::string PrintTrace();

    virtual std::string PrintProfile();

    /**
     * Creates a new instance of the container storage
     * @return
     */
    static Storage* CreateStorage();

    /**
     * Registers the container-storage type.
     */
    static void RegisterStorage();



    /**
     * Do not call this method when you hold a container or a meta data lock.
     *
     * @param container
     * @return
     */
    virtual dedupv1::base::lookup_result ReadContainer(Container* container,
        bool use_write_cache);

    uint64_t GetLastGivenContainerId();

    /**
     * Sets the last given container id.
     * This method should usually only called in unit tests. In general
     * the container storage should manage the ids by itself.
     *
     * @param id
     */
    void SetLastGivenContainerId(uint64_t id);

    /**
     * Sets the last committed container id.
     * This method should usually only called in unit tests. In general
     * the container storage should manage the ids by itself.
     *
     * @param id
     */
    void SetLastCommittedContainerId(uint64_t id);

    /**
     * Returns the size of a container in bytes
     * @return
     */
    uint32_t container_size() const;

    /**
     * Returns the number of files used by the container storage.
     *
     * @return
     */
    inline uint32_t GetFileCount() const;

    inline ContainerStorageBackgroundCommitter* background_committer();

    virtual bool LogReplayStateChange(const dedupv1::log::LogReplayStateChange& change);

    /**
     *
     * @param event_type type of the event
     * @param event_value value of the event.
     * @param context context information about the event, e.g. the event log id or the replay mode
     * @return true iff ok, otherwise an error has occurred
     */
    bool LogReplay(dedupv1::log::event_type event_type,
                   const LogEventData& event_value,
                   const dedupv1::log::LogReplayContext& context);

    virtual bool PostContainerEvent(
        const Container& container,
        dedupv1::log::event_type event_type,
                        const google::protobuf::Message* log_message,
                        uint64_t log_id);

    /**
     * Flushes all data to disk, even if the containers are not yet full.
     *
     * If the container storage is not started or already stopped, Flush should also return
     * successfully as
     * by definition every data that is in flux is committed to disk. If the container storage is
     * stopped
     * or not yet started, there is no data ready to be written to disk. Flush call causes the
     * container to be written in that thread. A background committer is not used as we could than
     * not assume that the data is really written to disk after the end of this call.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Flush(dedupv1::base::ErrorContext* ec);

    /**
     * Note: The container of both addresses have to be committed, however, the can be
     * stored in the read cache.
     *
     * Both container ids must be the primary id of the container. Merging
     * using secondary ids is not allowed.
     *
     * TODO (dmeister): Currently the container gc is the only component that is allowed to call
     * merge because otherwise the internal bookkeeping of the
     * gc fails. This should be changed someday.
     *
     * @param container_id_1
     * @param container_id_2
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool TryMergeContainer(uint64_t container_id_1,
                                   uint64_t container_id_2,
                                   bool* aborted);

    /**
     * Note: The container has to be committed, however, the can be
     * stored in the read cache.
     *
     * The container id must be the primary id of the container. Deleting
     * using secondary ids is not allowed.
     *
     * The container is not allowed to have any entries
     *
     * @param container_id
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool TryDeleteContainer(uint64_t container_id, bool* aborted);

    /**
     * Called in unknown (for the container storage) intervals when the system
     * (more specific: the current IdleDetector) is idle. The container storage
     * can do cleanup tasks during the idle times.
     */
    virtual void IdleTick();

    /**
     * Looking up the container address.
     *
     * @param container_id container id of the container whose address is looked up. If the
     * container id a secondary address,
     * the address of the matching primary container id is lookup up.
     *
     * If the container is in the write cache, the method will return
     * LOOKUP_NOT_FOUND.
     * If the container is currently in the bg committer, the lookup will block.
     * @param primary_container_lock Lock of a container lock used. The client
     * of the function is responsible to release the lock
     * @param acquire_write_lock iff true, the lock should be acquired for writing.
     * @return
     */
    virtual std::pair<dedupv1::base::lookup_result,
        ContainerStorageAddressData> LookupContainerAddress(
        uint64_t container_id,
        dedupv1::base::ReadWriteLock** primary_container_lock,
        bool acquire_write_lock);

    /**
     * Returns the primary id of the given container id.
     * If the container id is the primary id for itself, the container id is returns.
     * If the container id has not been found, the LOOKUP_NOT_FOUND is returned.
     *
     * The method only considers already committed containers.
     *
     * @param container_id
     * @param primary_id
     * @param primary_container_lock out parameter containing the lock of the primary id. The
     * client is responsible to release the lock.
     * @param acquire_write_lock
     * @return
     */
    dedupv1::base::lookup_result GetPrimaryId(uint64_t container_id,
                                              uint64_t* primary_id,
                                              dedupv1::base::ReadWriteLock**
                                              primary_container_lock,
                                              bool acquire_write_lock);

    /**
     * Returns the write cache.
     */
    inline ContainerStorageWriteCache* GetWriteCache();

    /**
     * Returns the read cache
     */
    inline ContainerStorageReadCache* GetReadCache();

    /**
     * returns the container gc.
     * @return
     */
    inline ContainerGCStrategy* GetGarbageCollection();

    /**
     * returns the storage allocator.
     * @return
     */
    inline ContainerStorageAllocator* allocator();

    inline bool is_preallocated() const;

    inline uint64_t size() const;

    inline const ContainerFile& file(int i) const;

    inline container_storage_state state() const
    {
        return state_;
    }

    /**
     * returns a pointer to the meta data index of the container storage
     * @return
     */
    inline dedupv1::base::PersistentIndex* meta_data_index();

    /**
     * returns the log
     * @return
     */
    virtual dedupv1::log::Log* log();

    inline ContainerGCStrategy* container_gc()
    {
        return this->gc_;
    }

    /**
     * returns the maximal number of items per container.
     * @return
     */
    uint32_t GetMaxItemsPerContainer() const;

    /**
     * returns a developer-readable representation of a container address.
     * @param address_data
     * @return
     */
    static std::string DebugString(const ContainerStorageAddressData& address_data);

    virtual bool CheckIfFull();

    virtual uint64_t GetActiveStorageDataSize();

    friend class ContainerStorageBackgroundCommitter;

#ifdef DEDUPV1_CORE_TEST
    virtual void ClearData();

#endif
    FRIEND_TEST(ContainerStorageTest, MergeWithSameContainerLock);
};

const ContainerStorage::ContainerFile& ContainerStorage::file(int i) const {
    return this->file_[i];
}

bool ContainerStorage::is_preallocated() const
{
    return preallocate_;
}

uint64_t ContainerStorage::size() const
{
    return size_;
}

ContainerGCStrategy* ContainerStorage::GetGarbageCollection()
{
    return this->gc_;
}



uint32_t ContainerStorage::GetFileCount() const
{
    return this->file_.size();
}

dedupv1::base::PersistentIndex* ContainerStorage::meta_data_index()
{
    return meta_data_index_;
}

ContainerStorageWriteCache* ContainerStorage::GetWriteCache()
{
    return &this->write_cache_;
}

ContainerStorageReadCache*  ContainerStorage::GetReadCache()
{
    return &this->cache_;
}

ContainerStorageAllocator* ContainerStorage::allocator()
{
    return this->allocator_;
}

ContainerStorageBackgroundCommitter* ContainerStorage::background_committer()
{
    return &this->background_committer_;
}

dedupv1::base::ReadWriteLock* ContainerStorage::GetContainerLock(uint64_t container_id)
{
    if (this->container_lock_.empty()) {
        return NULL;
    }
    return this->container_lock_.Get(container_id % this->container_lock_.size());
}

}
}

#endif  // CONTAINER_STORAGE_H__

