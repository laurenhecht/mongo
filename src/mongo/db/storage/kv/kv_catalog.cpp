// kv_catalog.cpp

#include "mongo/db/storage/kv/kv_catalog.h"

#include <stdlib.h>

#include "mongo/db/storage/record_store.h"
#include "mongo/platform/random.h"
#include "mongo/util/log.h"

namespace mongo {

    KVCatalog::KVCatalog( RecordStore* rs )
        : _rs( rs ) {
        boost::scoped_ptr<SecureRandom> r( SecureRandom::create() );
        _rand = r->nextInt64();
    }

    KVCatalog::~KVCatalog() {
        _rs = NULL;
    }

    void KVCatalog::init( OperationContext* opCtx ) {
        scoped_ptr<RecordIterator> it( _rs->getIterator( opCtx ) );
        while ( !it->isEOF()  ) {
            DiskLoc loc = it->getNext();
            RecordData data = it->dataFor( loc );
            BSONObj obj( data.data() );

            // no locking needed since can only be one
            string ns = obj["ns"].String();
            string ident = obj["ident"].String();
            _idents[ns] = Entry( ident, loc );
        }
    }



    Status KVCatalog::newCollection( OperationContext* opCtx,
                                     const StringData& ns,
                                     const CollectionOptions& options ) {
        std::stringstream ss;
        ss << ns << "-" << _rand << "-" << _next.fetchAndAdd( 1 );
        string ident = ss.str();

        boost::mutex::scoped_lock lk( _identsLock );
        Entry& old = _idents[ns.toString()];
        if ( !old.ident.empty() ) {
            return Status( ErrorCodes::NamespaceExists, "collection already exists" );
        }

        BSONObj obj;
        {
            BSONObjBuilder b;
            b.append( "ns", ns );
            b.append( "ident", ident );
            obj = b.obj();
        }
        StatusWith<DiskLoc> res = _rs->insertRecord( opCtx, obj.objdata(), obj.objsize(), false );
        if ( !res.isOK() )
            return res.getStatus();

        old = Entry( ident, res.getValue() );
        return Status::OK();
    }

    std::string KVCatalog::getCollectionIdent( const StringData& ns ) const {
        boost::mutex::scoped_lock lk( _identsLock );
        NSToIdentMap::const_iterator it = _idents.find( ns.toString() );
        invariant( it != _idents.end() );
        return it->second.ident;
    }

    Status KVCatalog::dropCollection( OperationContext* opCtx,
                                      const StringData& ns ) {
        boost::mutex::scoped_lock lk( _identsLock );
        Entry old = _idents[ns.toString()];
        if ( old.ident.empty() ) {
            return Status( ErrorCodes::NamespaceNotFound, "collection not found" );
        }

        _rs->deleteRecord( opCtx, old.storedLoc );
        _idents.erase( ns.toString() );

        return Status::OK();
    }


}
