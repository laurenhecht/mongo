// wiredtiger_kv_engine.cpp

#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"

#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_recovery_unit.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/util/log.h"

namespace mongo {

    WiredTigerKVEngine::WiredTigerKVEngine( const std::string& path ) {
        string config = "create,cache_size=1G,extensions=[local=(entry=index_collator_extension)],";

        invariantWTOK(wiredtiger_open(path.c_str(), NULL, config.c_str(), &_conn));

        _sessionCache.reset( new WiredTigerSessionCache( _conn ) );
    }


    WiredTigerKVEngine::~WiredTigerKVEngine() {

    }

    RecoveryUnit* WiredTigerKVEngine::newRecoveryUnit() {
        return new WiredTigerRecoveryUnit( _sessionCache.get(), false );
    }

    Status WiredTigerKVEngine::createRecordStore( OperationContext* opCtx,
                                                  const StringData& ident,
                                                  const CollectionOptions& options ) {
        scoped_ptr<WiredTigerSession> session( _sessionCache->getSession() );

        string extra = ""; // todo: wiredTigerGlobalOptions.collectionConfig );
        std::string config = WiredTigerRecordStore::generateCreateString( options, extra );

        string uri = _uri( ident );
        WT_SESSION* s = session->getSession();
        LOG(1) << "WiredTigerKVEngine::createRecordStore uri: " << uri;
        return wtRCToStatus( s->create( s, uri.c_str(), config.c_str() ) );
    }

    RecordStore* WiredTigerKVEngine::getRecordStore( OperationContext* opCtx,
                                                     const StringData& ns,
                                                     const StringData& ident ) {

        return new WiredTigerRecordStore( opCtx, ns, _uri( ident ) );
    }

    Status WiredTigerKVEngine::dropRecordStore( const StringData& ident )  {
        invariant( false );
    }

    string WiredTigerKVEngine::_uri( const StringData& ident ) const {
        return string("table:") + ident.toString();
    }

}
