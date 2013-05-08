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

#ifndef STORAGE_H__
#define STORAGE_H__

#include <core/dedup.h>
#include <base/factory.h>
#include <base/error.h>

#include <stdint.h>
#include <map>
#include <string>
#include <list>

#include <core/idle_detector.h>
#include <core/log.h>
#include <core/statistics.h>
#include <core/container.h>

namespace dedupv1 {

class DedupSystem;

/**
 * \namespace dedupv1::chunkstore
 * Namespace for classes related to the chunk store
 */
namespace chunkstore {

  class StorageRequest {
  private:
    const void* key_;
    uint32_t key_size_;

    const void* data_;
    uint32_t data_size_;

    bool is_indexed_;

    uint64_t address_;
  public:
    StorageRequest(const void* key, uint32_t key_size,
        const void* data, uint32_t data_size,
        bool is_indexed);

    inline const void* key() const;

    inline uint32_t key_size() const;

    inline const void* data() const;

    inline uint32_t data_size() const;

    inline bool is_indexed() const ;

    inline uint64_t address() const;

    inline void set_address(uint64_t a);

    std::string DebugString() const;
};

 /*
 * Type for the commit state of a address
 */
enum storage_commit_state {
    STORAGE_ADDRESS_ERROR = 0,              // !< STORAGE_ADDRESS_ERROR
    STORAGE_ADDRESS_COMMITED = 1,           // !< STORAGE_ADDRESS_COMMITED
    STORAGE_ADDRESS_NOT_COMMITED = 2,       // !< STORAGE_ADDRESS_NOT_COMMITED
    STORAGE_ADDRESS_WILL_NEVER_COMMITTED = 3 // !< STORAGE_ADDRESS_WILL_NEVER_COMMITTED
};

/**
 * The Storage system is used to store and read chunks of data.
 *
 * While it was a nice idea to have polymorphism for the storage system,
 * it was impossible to develop a crash-safe fast system with it.
 * Currently, there is only one implementation (container-storage) and
 * nearly all other components depend on the fact that the container storage
 * is used.
 */
class Storage : public dedupv1::StatisticProvider {
private:
    DISALLOW_COPY_AND_ASSIGN(Storage);

    static MetaFactory<Storage> factory_;
public:

    static MetaFactory<Storage>& Factory();

    /**
     * storage address used only for the empty chunk (-2).
     * This storage address is not valid to be ever saved persistently.
     */
    static const uint64_t EMPTY_DATA_STORAGE_ADDRESS;

    /**
     * storage address used when no legal storage address is known (-1).
     * This storage address is not valid to be ever saved persistently.
     */
    static const uint64_t ILLEGAL_STORAGE_ADDRESS;

    /**
     * Constructor
     * @return
     */
    Storage();

    /**
     * Destructor.
     * @return
     */
    virtual ~Storage();

    /**
     * Sets an option of an storage implementation. set_option should only be called before calling start
     *
     * No available options
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts a storage system. After a successful start the write, and read calls should work.
     *
     * @param start_context
     * @param system
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context, DedupSystem* system);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Stops the storage
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Checks if a given address is committed or not
     * @param address
     * @return
     */
    virtual enum storage_commit_state IsCommitted(uint64_t address) = 0;

    /**
     * @return true iff ok, otherwise an error has occurred
         */
        virtual bool WriteNew(
            std::list<StorageRequest>* requests,
                dedupv1::base::ErrorContext* ec) = 0;

    /**
     * Short reads are possible to the chunk is less offset+size.
     * In this case, read returns the number of read bytes.
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual dedupv1::base::Option<uint32_t> ReadChunk(uint64_t address,
                                                 const void* key, size_t key_size,
                                                 void* data,
                                                 uint32_t offset,
                                                 uint32_t size,
                                                 dedupv1::base::ErrorContext* ec) = 0;

    /**
     * Do not call this method when you hold a container or a meta data lock.
     *
     * @param container
     * @return
     */
    virtual dedupv1::base::lookup_result ReadContainer(Container* container,
        bool use_write_cache) = 0;
    
    /**
     * Deletes the record from the storage system.
     *
     * @param address
     * @param key_list
     * @param ec
     * @return
     */
    virtual bool DeleteChunks(uint64_t address, const std::list<bytestring>& key_list,
                              dedupv1::base::ErrorContext* ec) = 0;

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool DeleteChunk(uint64_t address, const byte* key, size_t key_size, dedupv1::base::ErrorContext* ec);

    /**
     * Flushes all open data to disk.
     *
     * Might block for a longer time (seconds) and should
     * therefore not used in the critical data path.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Flush(dedupv1::base::ErrorContext* ec);

    /**
     * Checks if the given address might represent a valid (but
     * not necessary committed) address or if it contains
     * a special magic number
     *
     * @param address
     * @param allow_empty
     * @return
     */
    static bool IsValidAddress(uint64_t address, bool allow_empty = false);

    virtual uint64_t GetActiveStorageDataSize() = 0;

    virtual bool CheckIfFull();

    virtual uint32_t container_size() const = 0;

#ifdef DEDUPV1_CORE_TEST
    virtual void ClearData();
#endif
};

    const void* StorageRequest::key() const {
      return key_;
    }

    uint32_t StorageRequest::key_size() const {
      return key_size_;
    }

    const void* StorageRequest::data() const {
      return data_;
    }

    uint32_t StorageRequest::data_size() const {
      return data_size_;
    }

    bool StorageRequest::is_indexed() const {
      return is_indexed_;
    }

    uint64_t StorageRequest::address() const {
      return address_;
    }

    void StorageRequest::set_address(uint64_t a) {
      address_ = a;
    }

}
}

#endif  // STORAGE_H__
