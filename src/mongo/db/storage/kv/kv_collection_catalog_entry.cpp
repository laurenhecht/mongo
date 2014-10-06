// kv_collection_catalog_entry.cpp

#include "mongo/db/storage/kv/kv_collection_catalog_entry.h"

#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/kv/kv_catalog.h"
#include "mongo/db/storage/kv/kv_engine.h"

namespace mongo {

    KVCollectionCatalogEntry::KVCollectionCatalogEntry( KVEngine* engine,
                                                        KVCatalog* catalog,
                                                        const StringData& ns,
                                                        const StringData& ident,
                                                        RecordStore* rs)
        : BSONCollectionCatalogEntry( ns ),
          _engine( engine ),
          _catalog( catalog ),
          _ident( ident.toString() ),
          _recrodStore( rs ) {
    }

    KVCollectionCatalogEntry::~KVCollectionCatalogEntry() {
    }

    bool KVCollectionCatalogEntry::setIndexIsMultikey(OperationContext* txn,
                                                      const StringData& indexName,
                                                      bool multikey ) {
        MetaData md = _getMetaData(txn);

        int offset = md.findIndexOffset( indexName );
        invariant( offset >= 0 );
        if ( md.indexes[offset].multikey == multikey )
            return false;
        md.indexes[offset].multikey = multikey;
        _catalog->putMetaData( txn, ns().toString(), md );
        return true;
    }

    void KVCollectionCatalogEntry::setIndexHead( OperationContext* txn,
                                                 const StringData& indexName,
                                                 const DiskLoc& newHead ) {
        MetaData md = _getMetaData( txn );
        int offset = md.findIndexOffset( indexName );
        invariant( offset >= 0 );
        md.indexes[offset].head = newHead;
        _catalog->putMetaData( txn,
                               ns().toString(),
                               md );
    }

    Status KVCollectionCatalogEntry::removeIndex( OperationContext* txn,
                                                  const StringData& indexName ) {
        invariant( false );
    }

    Status KVCollectionCatalogEntry::prepareForIndexBuild( OperationContext* txn,
                                                           const IndexDescriptor* spec ) {
        MetaData md = _getMetaData( txn );
        md.indexes.push_back( IndexMetaData( spec->infoObj(), false, DiskLoc(), false ) );
        _catalog->putMetaData( txn, ns().toString(), md );

        string ident = _catalog->getIndexIdent( ns().ns(), spec->indexName() );

        return _engine->createSortedDataInterface( txn, ident, spec );
    }

    void KVCollectionCatalogEntry::indexBuildSuccess( OperationContext* txn,
                                                      const StringData& indexName ) {
        MetaData md = _getMetaData( txn );
        int offset = md.findIndexOffset( indexName );
        invariant( offset >= 0 );
        md.indexes[offset].ready = true;
        _catalog->putMetaData( txn,
                               ns().toString(),
                               md );
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
