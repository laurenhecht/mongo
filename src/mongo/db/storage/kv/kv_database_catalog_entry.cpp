// kv_database_catalog_entry.cpp

#include "mongo/db/storage/kv/kv_database_catalog_entry.h"

#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/2d_access_method.h"
#include "mongo/db/index/btree_access_method.h"
#include "mongo/db/index/fts_access_method.h"
#include "mongo/db/index/hash_access_method.h"
#include "mongo/db/index/haystack_access_method.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/s2_access_method.h"
#include "mongo/db/storage/kv/kv_collection_catalog_entry.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {
    KVDatabaseCatalogEntry::KVDatabaseCatalogEntry( const StringData& db, KVStorageEngine* engine )
        : DatabaseCatalogEntry( db ), _engine( engine ) {

    }

    KVDatabaseCatalogEntry::~KVDatabaseCatalogEntry() {
        for ( CollectionMap::const_iterator it = _collections.begin(); it != _collections.end(); ++it ) {
            delete it->second;
        }
        _collections.clear();
    }

    bool KVDatabaseCatalogEntry::exists() const {
        return !isEmpty();
    }

    bool KVDatabaseCatalogEntry::isEmpty() const {
        boost::mutex::scoped_lock lk( _collectionsLock );
        return _collections.empty();
    }

    void KVDatabaseCatalogEntry::appendExtraStats( OperationContext* opCtx,
                                                   BSONObjBuilder* out,
                                                   double scale ) const {
        // todo
    }

    bool KVDatabaseCatalogEntry::currentFilesCompatible( OperationContext* opCtx ) const {
        // todo
        return true;
    }

    void KVDatabaseCatalogEntry::getCollectionNamespaces( std::list<std::string>* out ) const {
        boost::mutex::scoped_lock lk( _collectionsLock );
        for ( CollectionMap::const_iterator it = _collections.begin(); it != _collections.end(); ++it ) {
            out->push_back( it->first );
        }
    }

    CollectionCatalogEntry* KVDatabaseCatalogEntry::getCollectionCatalogEntry( OperationContext* txn,
                                                                               const StringData& ns ) const {
        boost::mutex::scoped_lock lk( _collectionsLock );
        CollectionMap::const_iterator it = _collections.find( ns.toString() );
        if ( it == _collections.end() )
            return NULL;
        return it->second;
    }

    RecordStore* KVDatabaseCatalogEntry::getRecordStore( OperationContext* txn,
                                                         const StringData& ns ) {
        boost::mutex::scoped_lock lk( _collectionsLock );
        CollectionMap::const_iterator it = _collections.find( ns.toString() );
        if ( it == _collections.end() )
            return NULL;
        return it->second->getRecordStore();
    }

    IndexAccessMethod* KVDatabaseCatalogEntry::getIndex( OperationContext* txn,
                                                         const CollectionCatalogEntry* collection,
                                                         IndexCatalogEntry* index ) {
        IndexDescriptor* desc = index->descriptor();

        const string& type = desc->getAccessMethodName();

        string ident = _engine->getCatalog()->getIndexIdent( txn,
                                                             collection->ns().ns(),
                                                             desc->indexName() );

        SortedDataInterface* sdi =
            _engine->getEngine()->getSortedDataInterface( txn, ident, desc );

        if ("" == type)
            return new BtreeAccessMethod( index, sdi );

        if (IndexNames::HASHED == type)
            return new HashAccessMethod( index, sdi );

        if (IndexNames::GEO_2DSPHERE == type)
            return new S2AccessMethod( index, sdi );

        if (IndexNames::TEXT == type)
            return new FTSAccessMethod( index, sdi );

        if (IndexNames::GEO_HAYSTACK == type)
            return new HaystackAccessMethod( index, sdi );

        if (IndexNames::GEO_2D == type)
            return new TwoDAccessMethod( index, sdi );

        log() << "Can't find index for keyPattern " << desc->keyPattern();
        invariant( false );
    }

    Status KVDatabaseCatalogEntry::createCollection( OperationContext* txn,
                                                     const StringData& ns,
                                                     const CollectionOptions& options,
                                                     bool allocateDefaultSpace ) {
        // we assume there is a logical lock on the collection name above

        {
            boost::mutex::scoped_lock lk( _collectionsLock );
            if ( _collections[ns.toString()] ) {
                return Status( ErrorCodes::NamespaceExists,
                               "collection already exists" );
            }
        }

        // need to create it
        Status status = _engine->getCatalog()->newCollection( txn, ns, options );
        if ( !status.isOK() )
            return status;

        string ident = _engine->getCatalog()->getCollectionIdent( ns );

        status = _engine->getEngine()->createRecordStore( txn, ident, options );
        if ( !status.isOK() )
            return status;

        RecordStore* rs = _engine->getEngine()->getRecordStore( txn, ns, ident, options );
        invariant( rs );
        boost::mutex::scoped_lock lk( _collectionsLock );
        _collections[ns.toString()] =
            new KVCollectionCatalogEntry( _engine->getEngine(), _engine->getCatalog(),
                                          ns, ident, rs );

        return Status::OK();
    }

    void KVDatabaseCatalogEntry::initCollection( OperationContext* opCtx,
                                                 const std::string& ns ) {
        string ident = _engine->getCatalog()->getCollectionIdent( ns );
        BSONCollectionCatalogEntry::MetaData md = _engine->getCatalog()->getMetaData( opCtx, ns );

        RecordStore* rs = _engine->getEngine()->getRecordStore( opCtx, ns, ident, md.options );
        invariant( rs );

        boost::mutex::scoped_lock lk( _collectionsLock );
        invariant( !_collections[ns] );
        _collections[ns] = new KVCollectionCatalogEntry( _engine->getEngine(),
                                                         _engine->getCatalog(),
                                                         ns,
                                                         ident,
                                                         rs );
    }

    Status KVDatabaseCatalogEntry::renameCollection( OperationContext* txn,
                                                     const StringData& fromNS,
                                                     const StringData& toNS,
                                                     bool stayTemp ) {
        {
            boost::mutex::scoped_lock lk( _collectionsLock );
            CollectionMap::const_iterator it = _collections.find( fromNS.toString() );
            if ( it == _collections.end() )
                return Status( ErrorCodes::NamespaceNotFound, "rename cannot find collection" );
            KVCollectionCatalogEntry* fromEntry = it->second;

            it = _collections.find( toNS.toString() );
            if ( it != _collections.end() )
                return Status( ErrorCodes::NamespaceExists, "for rename to already exists" );

            _collections.erase( fromNS.toString() );
            delete fromEntry;
        }

        Status status = _engine->getCatalog()->renameCollection( txn, fromNS, toNS, stayTemp );
        if ( !status.isOK() )
            return status;

        string ident = _engine->getCatalog()->getCollectionIdent( toNS );
        BSONCollectionCatalogEntry::MetaData md = _engine->getCatalog()->getMetaData( txn, toNS );
        RecordStore* rs = _engine->getEngine()->getRecordStore( txn, toNS, ident, md.options );

        boost::mutex::scoped_lock lk( _collectionsLock );
        _collections[toNS.toString()] =
            new KVCollectionCatalogEntry( _engine->getEngine(), _engine->getCatalog(),
                                          toNS, ident, rs );

        return Status::OK();
    }

    Status KVDatabaseCatalogEntry::dropCollection( OperationContext* opCtx,
                                                   const StringData& ns ) {
        KVCollectionCatalogEntry* entry;
        {
            boost::mutex::scoped_lock lk( _collectionsLock );
            CollectionMap::const_iterator it = _collections.find( ns.toString() );
            if ( it == _collections.end() )
                return Status( ErrorCodes::NamespaceNotFound, "cannnot find collection to drop" );
            entry = it->second;
        }

        invariant( entry->getTotalIndexCount( opCtx ) == entry->getCompletedIndexCount( opCtx ) );
        {
            std::vector<std::string> indexNames;
            entry->getAllIndexes( opCtx, &indexNames );
            for ( size_t i = 0; i < indexNames.size(); i++ ) {
                entry->removeIndex( opCtx, indexNames[i] );
            }
        }
        invariant( entry->getTotalIndexCount( opCtx ) == 0 );

        string ident = _engine->getCatalog()->getCollectionIdent( ns );

        Status status = _engine->getEngine()->dropRecordStore( opCtx, ident );
        if ( !status.isOK() )
            return status;

        status = _engine->getCatalog()->dropCollection( opCtx, ns );
        if ( !status.isOK() )
            return status;

        boost::mutex::scoped_lock lk( _collectionsLock );
        _collections.erase( ns.toString() );

        return Status::OK();
    }

}
