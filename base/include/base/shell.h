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

#ifndef SHELL_H_
#define SHELL_H_

#include <base/base.h>
#include <base/option.h>

#include <string>

namespace dedupv1 {
namespace base {

/**
 * Runs a given command as a subprocess and returns the process result and the output data.
 */
dedupv1::base::Option<std::pair<int, bytestring> > RunUntilCompletion(const std::string& cmd);

}
}

#endif /* SHELL_H_ */
