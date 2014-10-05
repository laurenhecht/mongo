// kv_database_catalog_entry.h

#pragma once

#include <map>
#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "mongo/db/catalog/database_catalog_entry.h"

namespace mongo {

    class KVCollectionCatalogEntry;
    class KVStorageEngine;

    class KVDatabaseCatalogEntry : public DatabaseCatalogEntry {
    public:
        KVDatabaseCatalogEntry( const StringData& db, KVStorageEngine* engine );
        virtual ~KVDatabaseCatalogEntry();

        virtual bool exists() const;
        virtual bool isEmpty() const;

        virtual void appendExtraStats( OperationContext* opCtx,
                                       BSONObjBuilder* out,
                                       double scale ) const;

        virtual bool isOlderThan24( OperationContext* opCtx ) const { return false; }
        virtual void markIndexSafe24AndUp( OperationContext* opCtx ) {}

        virtual bool currentFilesCompatible( OperationContext* opCtx ) const;

        virtual void getCollectionNamespaces( std::list<std::string>* out ) const;

        virtual CollectionCatalogEntry* getCollectionCatalogEntry( OperationContext* txn,
                                                                   const StringData& ns ) const;

        virtual RecordStore* getRecordStore( OperationContext* txn,
                                             const StringData& ns );

        virtual IndexAccessMethod* getIndex( OperationContext* txn,
                                             const CollectionCatalogEntry* collection,
                                             IndexCatalogEntry* index );

        virtual Status createCollection( OperationContext* txn,
                                         const StringData& ns,
                                         const CollectionOptions& options,
                                         bool allocateDefaultSpace );

        virtual Status renameCollection( OperationContext* txn,
                                         const StringData& fromNS,
                                         const StringData& toNS,
                                         bool stayTemp );

        virtual Status dropCollection( OperationContext* opCtx,
                                       const StringData& ns );

    private:
        KVStorageEngine* _engine; // not owned here
        bool _used;

        typedef std::map<std::string,KVCollectionCatalogEntry*> CollectionMap;
        CollectionMap _collections;
        mutable boost::mutex _collectionsLock;
    };
}
