/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#pragma once

#include <vector>

#include "mongo/bson/optime.h"
#include "mongo/db/jsobj.h"

/**
   local.slaves  - current location for all slaves

 */
namespace mongo {

    class CurOp;

    bool updateSlaveLocations(BSONArray optimes);

    void updateSlaveLocation( CurOp& curop, const char * oplog_ns , OpTime lastOp );

    /** @return true if op has made it to w servers */
    bool opReplicatedEnough( OpTime op , int w );
    bool opReplicatedEnough( OpTime op , BSONElement w );

    bool waitForReplication( OpTime op , int w , int maxSecondsToWait );

    std::vector<BSONObj> getHostsWrittenTo(OpTime& op);

    void resetSlaveCache();
    unsigned getSlaveCount();
}