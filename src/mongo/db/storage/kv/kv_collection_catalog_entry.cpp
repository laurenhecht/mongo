// kv_collection_catalog_entry.cpp

#include "mongo/db/storage/kv/kv_collection_catalog_entry.h"

#include "mongo/db/storage/kv/kv_catalog.h"

namespace mongo {

    KVCollectionCatalogEntry::KVCollectionCatalogEntry( KVCatalog* catalog,
                                                        const StringData& ns,
                                                        const StringData& ident,
                                                        RecordStore* rs)
        : BSONCollectionCatalogEntry( ns ),
          _catalog( catalog ),
          _ident( ident.toString() ),
          _recrodStore( rs ) {
    }

    KVCollectionCatalogEntry::~KVCollectionCatalogEntry() {
    }

    bool KVCollectionCatalogEntry::setIndexIsMultikey(OperationContext* txn,
                                                      const StringData& indexName,
                                                      bool multikey ) {
        invariant( false );
    }

    void KVCollectionCatalogEntry::setIndexHead( OperationContext* txn,
                                                 const StringData& indexName,
                                                 const DiskLoc& newHead ) {
        invariant( false );
    }

    Status KVCollectionCatalogEntry::removeIndex( OperationContext* txn,
                                                  const StringData& indexName ) {
        invariant( false );
    }

    Status KVCollectionCatalogEntry::prepareForIndexBuild( OperationContext* txn,
                                                           const IndexDescriptor* spec ) {
        invariant( false );
    }

    void KVCollectionCatalogEntry::indexBuildSuccess( OperationContext* txn,
                                                      const StringData& indexName ) {
        invariant( false );
    }

    void KVCollectionCatalogEntry::updateTTLSetting( OperationContext* txn,
                                                     const StringData& idxName,
                                                     long long newExpireSeconds ) {
        invariant( false );
    }

    BSONCollectionCatalogEntry::MetaData KVCollectionCatalogEntry::_getMetaData( OperationContext* txn ) const {
        return _catalog->getMetaData( txn, ns().toString() );
    }

}
