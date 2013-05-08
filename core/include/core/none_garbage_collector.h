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

#ifndef NONE_GARBAGE_COLLECTOR_H__
#define NONE_GARBAGE_COLLECTOR_H__

#include <core/garbage_collector.h>
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
 * None Garbage collection of the dedup system.
 *
 */
class NoneGarbageCollector : public GarbageCollector {
public:
    /**
     * Constructor
     * @return
     */
    NoneGarbageCollector();

    /**
     * Destructor
     * @return
     */
    virtual ~NoneGarbageCollector();

    static GarbageCollector* CreateGC();

    static void RegisterGC();

};

}
}

#endif  // NONE_GARBAGE_COLLECTOR_H__
