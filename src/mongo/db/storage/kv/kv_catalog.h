// kv_catalog.h

#pragma once

#include <map>
#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "mongo/base/string_data.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/diskloc.h"
#include "mongo/db/storage/bson_collection_catalog_entry.h"

namespace mongo {

    class OperationContext;
    class RecordStore;

    class KVCatalog {
    public:
        /**
         * @param rs - does NOT take ownership
         */
        KVCatalog( RecordStore* rs );
        ~KVCatalog();

        void init( OperationContext* opCtx );

        /**
         * @return error or ident for instance
         */
        Status newCollection( OperationContext* opCtx,
                              const StringData& ns,
                              const CollectionOptions& options );

        std::string getCollectionIdent( const StringData& ns ) const;

        const BSONCollectionCatalogEntry::MetaData getMetaData( OperationContext* opCtx,
                                                                const StringData& ns );
        void putMetaData( OperationContext* opCtx,
                          const StringData& ns,
                          BSONCollectionCatalogEntry::MetaData& md );

        Status dropCollection( OperationContext* opCtx,
                               const StringData& ns );
    private:

        RecordStore* _rs; // not owned
        int64_t _rand;
        AtomicUInt64 _next;

        struct Entry {
            Entry(){}
            Entry( std::string i, DiskLoc l )
                : ident(i), storedLoc( l ) {}
            std::string ident;
            DiskLoc storedLoc;
        };
        typedef std::map<std::string,Entry> NSToIdentMap;
        NSToIdentMap _idents;
        mutable boost::mutex _identsLock;
    };

}
