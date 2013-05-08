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

#include <string>
#include <list>
#include <sstream>

#include "dedupv1.pb.h"
#include "dedupv1_stats.pb.h"

#include <core/dedup.h>

#include <base/logging.h>

#include <core/dedup_system.h>
#include <core/garbage_collector.h>

using std::string;

LOGGER("GarbageCollector");

namespace dedupv1 {
namespace gc {

MetaFactory<GarbageCollector> GarbageCollector::factory_("GarbageCollector", "gc");

MetaFactory<GarbageCollector>& GarbageCollector::Factory() {
  return factory_;
}

GarbageCollector::GarbageCollector() {
}

GarbageCollector::~GarbageCollector() {
}

bool GarbageCollector::SetOption(const std::string& option_name, const std::string& option) {
    ERROR("Invalid option: " << option_name << "=" << option);
    return false;
}

bool GarbageCollector::Start(const dedupv1::StartContext& start_context,
                             dedupv1::DedupSystem* system) {
    return true;
}

bool GarbageCollector::Stop(const dedupv1::StopContext& stop_context) {
    return true;
}

bool GarbageCollector::Run() {
    return true;
}

bool GarbageCollector::PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

bool GarbageCollector::RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps) {
    return true;
}

string GarbageCollector::PrintStatistics() {
    return "null";
}

string GarbageCollector::PrintProfile() {
    return "null";
}

string GarbageCollector::PrintTrace() {
    return "null";
}

string GarbageCollector::PrintLockStatistics() {
    return "null";
}

#ifdef DEDUPV1_CORE_TEST
void GarbageCollector::ClearData() {
}
#endif

}
}
