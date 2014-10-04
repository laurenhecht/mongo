// wiredtiger_metadata.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *    Copyright (C) 2014 WiredTiger Inc.
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

#include <list>
#include <map>
#include <string>

#include <boost/thread/mutex.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_engine.h"


namespace mongo {
    class WiredTigerDatabase;

    // A class that wraps all the metadata information for a database including:
    //  * Translating from table names to identifiers
    //  * Generating new identifiers
    //  * Storing, retrieving and updating metadata
    //
    // This class holds an entry for each Collection and for all indexes
    class WiredTigerMetaData {
    public:
        WiredTigerMetaData( );

        virtual ~WiredTigerMetaData();
        void initialize( WiredTigerDatabase &db );

        std::string getTableName(uint64_t identifier);
        std::string getURI(uint64_t identifier);
        std::string getURI(std::string name);   // Inefficient, prefer identifier version.
        BSONObj &getConfig(uint64_t identifer);
        uint64_t getIdentifier(std::string tableName);
        uint64_t generateIdentifier(std::string tableName, BSONObj config);

        Status remove(uint64_t identifier, bool failedDrop = false);
        Status rename(uint64_t identifier, std::string newName);
        Status updateConfig(uint64_t identifier, BSONObj newConfig);
        std::vector<uint64_t> getDeleted();
        std::vector<uint64_t> getAllTables();
        std::vector<uint64_t> getAllIndexes(uint64_t identifier);

        static const char * WT_METADATA_URI;
        static const char * WT_METADATA_CONFIG;
        static const uint64_t INVALID_METADATA_IDENTIFIER;
    private:

        struct MetaDataEntry {
            MetaDataEntry(
                    std::string name_,
                    std::string uri_,
                    BSONObj config_,
                    bool isIndex_,
                    bool isDeleted_ ) :
                name(name_),
                uri(uri_),
                config(config_),
                isIndex(isIndex_),
                isDeleted(isDeleted_) {}

            std::string name;
            std::string uri;        // Not saved to disk, kept for efficiency
            BSONObj     config;
            bool        isIndex;
            bool        isDeleted;  // Not saved to disk, any deleted entry should not have a
                                    // matching entry in the persistent table.
        };

        // Called from constructor to read metadata table and populate in-memory map
        Status _populate();
        uint64_t _generateIdentifier(std::string tableName);
        uint64_t _getIdentifierLocked(std::string tableName);
        std::string _generateURI(std::string tableName, uint64_t id);
        bool _isIndexName(std::string tableName);
        void _persistEntry(uint64_t id, MetaDataEntry &entry);

        //AtomicUInt64 _nextId; TODO: I'm getting an error relating to this not being copyable
        uint64_t _nextId;

        typedef std::map<uint64_t, MetaDataEntry> WiredTigerMetaDataMap;
        WiredTigerMetaDataMap _tables;
        boost::scoped_ptr<WiredTigerSession> _session;
        WT_CURSOR *_metaDataCursor;
        bool _isInitialized;
        mutable boost::mutex _metaDataLock;
    };
}
