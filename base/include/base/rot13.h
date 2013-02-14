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

#ifndef ROT13_H_
#define ROT13_H_

#include <string>

#include <base/base.h>

namespace dedupv1 {
namespace base {

/**
 * Transforms a bytestring to a ROT-13 transformed bytestring
 */
bytestring ToRot13(const bytestring& data);

/**
 * Transforms a ROT-13 transformed bytestring into a normal bytestring
 */
bytestring FromRot13(const bytestring& data);

}
}

#endif /* ROT13_H_ */
