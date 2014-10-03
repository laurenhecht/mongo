#pragma once

#include <wiredtiger.h>

#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"

namespace mongo {

    class WiredTigerMetaData;

    class WiredTigerDatabase {
    public:
        WiredTigerDatabase(WT_CONNECTION *conn);
        virtual ~WiredTigerDatabase();

        WT_CONNECTION* Get() const { return _conn; }

        WiredTigerSessionCache* getSessionCache() { return &_sessionCache; }
        void ClearCache();

        void InitMetaData();
        WiredTigerMetaData& GetMetaData();
        void DropDeletedTables();

    private:
        WT_CONNECTION *_conn;
        WiredTigerSessionCache _sessionCache;
        WiredTigerMetaData *_metaData;
    };


}
