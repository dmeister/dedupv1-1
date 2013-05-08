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

#ifndef GARBAGE_COLLECTOR_H__
#define GARBAGE_COLLECTOR_H__

#include <core/container_storage.h>
#include <core/statistics.h>
#include <core/block_index.h>
#include <base/thread.h>
#include <base/locks.h>
#include <base/profile.h>
#include <base/threadpool.h>

#include <gtest/gtest_prod.h>

#include <set>
#include <string>
#include <tbb/atomic.h>
#include <tbb/task_scheduler_init.h>

using dedupv1::base::Profile;

namespace dedupv1 {
namespace gc {

/**
 * Abstract class for all garbage collection implementations
 */
class GarbageCollector : public dedupv1::StatisticProvider {
public:
    DISALLOW_COPY_AND_ASSIGN(GarbageCollector);

    static MetaFactory<GarbageCollector>& Factory();

    /**
     * Constructor
     */
    GarbageCollector();

    /**
     * Destructor
     */
    virtual ~GarbageCollector();

    /**
     * Starts the gc.
     *
     * @param system
     * @param start_context
     *
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(const dedupv1::StartContext& start_context,
                       DedupSystem* system);

    /**
     * Runs the gc background thread(s)
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Run();

    /**
     * Stops the gc background thread(s)
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Stop(const dedupv1::StopContext& stop_context);

    /**
     * Configures the gc
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);


    virtual bool PersistStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    virtual bool RestoreStatistics(std::string prefix,
                                   dedupv1::PersistStatistics* ps);

    /**
     * prints statistics about the gc
     * @return
     */
    virtual std::string PrintStatistics();

    /**
     * prints trace statistics about the gc
     * @return
     */
    virtual std::string PrintTrace();

    /**
     * prints profile statistics
     * @return
     */
    virtual std::string PrintProfile();

    /**
     * prints lock statistics
     * @return
     */
    virtual std::string PrintLockStatistics();

#ifdef DEDUPV1_CORE_TEST
    /**
     * Closes all indexes to allow crash-like tests.
     */
    virtual void ClearData();
#endif

private:
    static MetaFactory<GarbageCollector> factory_;
};

}
}

#endif  // GARBAGE_COLLECTOR_H__
