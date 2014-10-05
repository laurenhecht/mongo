// kv_storage_engine.h

#pragma once

#include <map>
#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "mongo/db/storage/kv/kv_catalog.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/storage_engine.h"

namespace mongo {

    class KVCatalog;
    class KVEngine;
    class KVDatabaseCatalogEntry;

    class KVStorageEngine : public StorageEngine {
    public:
        /**
         * @param engine - owneership passes to me
         */
        KVStorageEngine( KVEngine* engine );
        virtual ~KVStorageEngine();

        virtual void finishInit();

        virtual RecoveryUnit* newRecoveryUnit( OperationContext* opCtx );

        virtual void listDatabases( std::vector<std::string>* out ) const;

        virtual DatabaseCatalogEntry* getDatabaseCatalogEntry( OperationContext* opCtx,
                                                               const StringData& db );

        virtual bool supportsDocLocking() const { return true; }

        virtual Status closeDatabase( OperationContext* txn, const StringData& db );

        virtual Status dropDatabase( OperationContext* txn, const StringData& db );

        virtual int flushAllFiles( bool sync );

        virtual Status repairDatabase( OperationContext* txn,
                                       const std::string& dbName,
                                       bool preserveClonedFilesOnFailure = false,
                                       bool backupOriginalFiles = false );

        // ------ kv ------

        KVEngine* getEngine() { return _engine.get(); }
        const KVEngine* getEngine() const { return _engine.get(); }

        KVCatalog* getCatalog() { return _catalog.get(); }
        const KVCatalog* getCatalog() const { return _catalog.get(); }

    private:
        boost::scoped_ptr<KVEngine> _engine;
        bool _initialized;

        boost::scoped_ptr<RecordStore> _catalogRecordStore;
        boost::scoped_ptr<KVCatalog> _catalog;

        typedef std::map<std::string,KVDatabaseCatalogEntry*> DBMap;
        DBMap _dbs;
        mutable boost::mutex _dbsLock;
    };

}
