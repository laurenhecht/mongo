// kv_collection_catalog_entry.h

#pragma once

#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/storage/bson_collection_catalog_entry.h"
#include "mongo/db/storage/record_store.h"

namespace mongo {

    class KVCatalog;
    class KVEngine;

    class KVCollectionCatalogEntry : public BSONCollectionCatalogEntry {
    public:
        KVCollectionCatalogEntry( KVEngine* engine,
                                  KVCatalog* catalog,
                                  const StringData& ns,
                                  const StringData& ident,
                                  RecordStore* rs );

        virtual ~KVCollectionCatalogEntry();

        virtual int getMaxAllowedIndexes() const { return 64; };

        virtual bool setIndexIsMultikey(OperationContext* txn,
                                        const StringData& indexName,
                                        bool multikey = true);

        virtual void setIndexHead( OperationContext* txn,
                                   const StringData& indexName,
                                   const DiskLoc& newHead );

        virtual Status removeIndex( OperationContext* txn,
                                    const StringData& indexName );

        virtual Status prepareForIndexBuild( OperationContext* txn,
                                             const IndexDescriptor* spec );

        virtual void indexBuildSuccess( OperationContext* txn,
                                        const StringData& indexName );

        /* Updates the expireAfterSeconds field of the given index to the value in newExpireSecs.
         * The specified index must already contain an expireAfterSeconds field, and the value in
         * that field and newExpireSecs must both be numeric.
         */
        virtual void updateTTLSetting( OperationContext* txn,
                                       const StringData& idxName,
                                       long long newExpireSeconds );

        RecordStore* getRecordStore() { return _recrodStore.get(); }

    protected:
        virtual MetaData _getMetaData( OperationContext* txn ) const;

    private:
        KVEngine* _engine; // not owned
        KVCatalog* _catalog; // not owned
        std::string _ident;
        boost::scoped_ptr<RecordStore> _recrodStore; // owned
    };

}
