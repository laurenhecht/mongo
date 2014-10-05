// kv_storage_engine.cpp

#include "mongo/db/storage/kv/kv_storage_engine.h"

#include "mongo/db/storage/kv/kv_database_catalog_entry.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/log.h"

namespace mongo {
    KVStorageEngine::KVStorageEngine( KVEngine* engine )
        : _engine( engine ) {
    }

    KVStorageEngine::~KVStorageEngine() {
    }

    void KVStorageEngine::finishInit() {
        // todo
    }

    RecoveryUnit* KVStorageEngine::newRecoveryUnit( OperationContext* opCtx ) {
        return _engine->newRecoveryUnit();
    }

    void KVStorageEngine::listDatabases( std::vector<std::string>* out ) const {
        boost::mutex::scoped_lock lk( _dbsLock );
        for ( DBMap::const_iterator it = _dbs.begin(); it != _dbs.end(); ++it ) {
            if ( it->second->isEmpty() )
                continue;
            out->push_back( it->first );
        }
    }

    DatabaseCatalogEntry* KVStorageEngine::getDatabaseCatalogEntry( OperationContext* opCtx,
                                                                    const StringData& dbName ) {
        boost::mutex::scoped_lock lk( _dbsLock );
        KVDatabaseCatalogEntry*& db = _dbs[dbName.toString()];
        if ( !db ) {
            db = new KVDatabaseCatalogEntry( dbName, _engine.get() );
        }
        return db;
    }

    Status KVStorageEngine::closeDatabase( OperationContext* txn, const StringData& db ) {
        // todo: do I have to suppor this?
        return Status::OK();
    }

    Status KVStorageEngine::dropDatabase( OperationContext* txn, const StringData& db ) {
        invariant( 0 );
    }

    int KVStorageEngine::flushAllFiles( bool sync ) {
        // todo: do I have to support this?
        return 0;
    }

    Status KVStorageEngine::repairDatabase( OperationContext* txn,
                                            const std::string& dbName,
                                            bool preserveClonedFilesOnFailure,
                                            bool backupOriginalFiles ) {
        // todo: do I have to support this?
        return Status::OK();
    }

}
