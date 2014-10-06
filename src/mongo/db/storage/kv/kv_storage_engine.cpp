// kv_storage_engine.cpp

#include "mongo/db/storage/kv/kv_storage_engine.h"

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/kv/kv_database_catalog_entry.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/log.h"

namespace mongo {
    KVStorageEngine::KVStorageEngine( KVEngine* engine )
        : _engine( engine ), _initialized( false ) {
    }

    KVStorageEngine::~KVStorageEngine() {
    }

    void KVStorageEngine::finishInit() {
        if ( _initialized )
            return;

        OperationContextNoop opCtx( _engine->newRecoveryUnit() );
        WriteUnitOfWork uow( &opCtx );

        Status status = _engine->createRecordStore( &opCtx, "catalog", CollectionOptions() );
        fassert( 28520, status );

        _catalogRecordStore.reset( _engine->getRecordStore( &opCtx,
                                                            "catalog", "catalog",
                                                            CollectionOptions() ) );
        _catalog.reset( new KVCatalog( _catalogRecordStore.get() ) );
        _catalog->init( &opCtx );
        uow.commit();

        _initialized = true;
    }

    RecoveryUnit* KVStorageEngine::newRecoveryUnit( OperationContext* opCtx ) {
        invariant( _initialized );
        return _engine->newRecoveryUnit();
    }

    void KVStorageEngine::listDatabases( std::vector<std::string>* out ) const {
        invariant( _initialized );
        boost::mutex::scoped_lock lk( _dbsLock );
        for ( DBMap::const_iterator it = _dbs.begin(); it != _dbs.end(); ++it ) {
            if ( it->second->isEmpty() )
                continue;
            out->push_back( it->first );
        }
    }

    DatabaseCatalogEntry* KVStorageEngine::getDatabaseCatalogEntry( OperationContext* opCtx,
                                                                    const StringData& dbName ) {
        invariant( _initialized );
        boost::mutex::scoped_lock lk( _dbsLock );
        KVDatabaseCatalogEntry*& db = _dbs[dbName.toString()];
        if ( !db ) {
            db = new KVDatabaseCatalogEntry( dbName, this );
        }
        return db;
    }

    Status KVStorageEngine::closeDatabase( OperationContext* txn, const StringData& db ) {
        invariant( _initialized );
        // todo: do I have to suppor this?
        return Status::OK();
    }

    Status KVStorageEngine::dropDatabase( OperationContext* txn, const StringData& db ) {
        invariant( _initialized );

        KVDatabaseCatalogEntry* entry;
        {
            boost::mutex::scoped_lock lk( _dbsLock );
            DBMap::const_iterator it = _dbs.find( db.toString() );
            if ( it == _dbs.end() )
                return Status( ErrorCodes::NamespaceNotFound, "db not found to drop" );
            entry = it->second;
        }

        std::list<std::string> toDrop;
        entry->getCollectionNamespaces( &toDrop );

        for ( std::list<std::string>::iterator it = toDrop.begin(); it != toDrop.end(); ++it ) {
            string coll = *it;
            entry->dropCollection( txn, coll );
        }
        toDrop.clear();
        entry->getCollectionNamespaces( &toDrop );
        invariant( toDrop.empty() );

        {
            boost::mutex::scoped_lock lk( _dbsLock );
            _dbs.erase( db.toString() );
        }
        return Status::OK();
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
