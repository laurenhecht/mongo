// wiredtiger_kv_engine.h

#pragma once

#include <wiredtiger.h>

#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"

namespace mongo {

    class WiredTigerSessionCache;

    class WiredTigerKVEngine : public KVEngine {
    public:
        WiredTigerKVEngine( const std::string& path );
        virtual ~WiredTigerKVEngine();

        virtual RecoveryUnit* newRecoveryUnit();

        virtual Status createRecordStore( OperationContext* opCtx,
                                          const StringData& ident,
                                          const CollectionOptions& options );

        virtual RecordStore* getRecordStore( OperationContext* opCtx,
                                             const StringData& ns,
                                             const StringData& ident );

        virtual Status dropRecordStore( const StringData& ident ) ;
    private:

        string _uri( const StringData& ident ) const;

        WT_CONNECTION* _conn;
        boost::scoped_ptr<WiredTigerSessionCache> _sessionCache;
    };

}
