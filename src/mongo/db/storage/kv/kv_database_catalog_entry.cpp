// kv_database_catalog_entry.cpp

#include "mongo/db/storage/kv/kv_database_catalog_entry.h"

#include "mongo/util/assert_util.h"

namespace mongo {
    KVDatabaseCatalogEntry::KVDatabaseCatalogEntry( const StringData& db, KVEngine* engine )
        : DatabaseCatalogEntry( db ), _engine( engine ) {

    }

    KVDatabaseCatalogEntry::~KVDatabaseCatalogEntry() {

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
        invariant( false );
    }

    IndexAccessMethod* KVDatabaseCatalogEntry::getIndex( OperationContext* txn,
                                                         const CollectionCatalogEntry* collection,
                                                         IndexCatalogEntry* index ) {
        invariant( false );
    }

    Status KVDatabaseCatalogEntry::createCollection( OperationContext* txn,
                                                     const StringData& ns,
                                                     const CollectionOptions& options,
                                                     bool allocateDefaultSpace ) {
        invariant( false );
    }

    Status KVDatabaseCatalogEntry::renameCollection( OperationContext* txn,
                                                     const StringData& fromNS,
                                                     const StringData& toNS,
                                                     bool stayTemp ) {
        invariant( false );
    }

    Status KVDatabaseCatalogEntry::dropCollection( OperationContext* opCtx,
                                                   const StringData& ns ) {
        invariant( false );
    }

}
