// kv_engine_test_harness.cpp

#include "mongo/db/storage/kv/kv_engine_test_harness.h"

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/kv/kv_catalog.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

    namespace {
        class MyOperationContext : public OperationContextNoop {
        public:
            MyOperationContext( KVEngine* engine )
                : OperationContextNoop( engine->newRecoveryUnit() ) {
            }
        };
    }

    TEST( KVEngineTestHarness, SimpleRS1 ) {
        scoped_ptr<KVHarnessHelper> helper( KVHarnessHelper::create() );
        KVEngine* engine = helper->getEngine();
        ASSERT( engine );

        string ns = "a.b";
        scoped_ptr<RecordStore> rs;
        {
            MyOperationContext opCtx( engine );
            ASSERT_OK( engine->createRecordStore( &opCtx, ns, CollectionOptions() ) );
            rs.reset( engine->getRecordStore( &opCtx, ns, ns, CollectionOptions() ) );
            ASSERT( rs );
        }


        DiskLoc loc;
        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            StatusWith<DiskLoc> res = rs->insertRecord( &opCtx, "abc", 4, false );
            ASSERT_OK( res.getStatus() );
            loc = res.getValue();
            uow.commit();
        }

        {
            MyOperationContext opCtx( engine );
            ASSERT_EQUALS( string("abc"), rs->dataFor( &opCtx, loc ).data() );
        }

    }

    TEST( KVEngineTestHarness, SimpleSorted1 ) {
        scoped_ptr<KVHarnessHelper> helper( KVHarnessHelper::create() );
        KVEngine* engine = helper->getEngine();
        ASSERT( engine );

        string ident = "abc";
        IndexDescriptor desc( NULL, "", BSON( "key" << BSON( "a" << 1 ) ) );
        scoped_ptr<SortedDataInterface> sorted;
        {
            MyOperationContext opCtx( engine );
            ASSERT_OK( engine->createSortedDataInterface( &opCtx, ident, &desc ) );
            sorted.reset( engine->getSortedDataInterface( &opCtx, ident, &desc ) );
            ASSERT( sorted );
        }

        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            ASSERT_OK( sorted->insert( &opCtx, BSON( "" << 5 ), DiskLoc( 6, 4 ), true ) );
            uow.commit();
        }

        {
            MyOperationContext opCtx( engine );
            ASSERT_EQUALS( 1, sorted->numEntries( &opCtx ) );
        }

    }

    TEST( KVCatalogTest, Coll1 ) {
        scoped_ptr<KVHarnessHelper> helper( KVHarnessHelper::create() );
        KVEngine* engine = helper->getEngine();

        scoped_ptr<RecordStore> rs;
        scoped_ptr<KVCatalog> catalog;
        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            ASSERT_OK( engine->createRecordStore( &opCtx, "catalog", CollectionOptions() ) );
            rs.reset( engine->getRecordStore( &opCtx, "catalog", "catalog", CollectionOptions() ) );
            catalog.reset( new KVCatalog( rs.get() ) );
            uow.commit();
        }

        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            ASSERT_OK( catalog->newCollection( &opCtx, "a.b", CollectionOptions() ) );
            ASSERT_NOT_EQUALS( "a.b", catalog->getCollectionIdent( "a.b" ) );
            uow.commit();
        }

        string ident = catalog->getCollectionIdent( "a.b" );
        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            catalog.reset( new KVCatalog( rs.get() ) );
            catalog->init( &opCtx );
            uow.commit();
        }
        ASSERT_EQUALS( ident, catalog->getCollectionIdent( "a.b" ) );

        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            catalog->dropCollection( &opCtx, "a.b" );
            catalog->newCollection( &opCtx, "a.b", CollectionOptions() );
            uow.commit();
        }
        ASSERT_NOT_EQUALS( ident, catalog->getCollectionIdent( "a.b" ) );
    }


    TEST( KVCatalogTest, Idx1 ) {
        scoped_ptr<KVHarnessHelper> helper( KVHarnessHelper::create() );
        KVEngine* engine = helper->getEngine();

        scoped_ptr<RecordStore> rs;
        scoped_ptr<KVCatalog> catalog;
        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            ASSERT_OK( engine->createRecordStore( &opCtx, "catalog", CollectionOptions() ) );
            rs.reset( engine->getRecordStore( &opCtx, "catalog", "catalog", CollectionOptions() ) );
            catalog.reset( new KVCatalog( rs.get() ) );
            uow.commit();
        }

        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );
            ASSERT_OK( catalog->newCollection( &opCtx, "a.b", CollectionOptions() ) );
            ASSERT_NOT_EQUALS( "a.b", catalog->getCollectionIdent( "a.b" ) );
            uow.commit();
        }

        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );

            BSONCollectionCatalogEntry::MetaData md;
            md.ns ="a.b";
            md.indexes.push_back( BSONCollectionCatalogEntry::IndexMetaData( BSON( "name" << "foo" ),
                                                                             false,
                                                                             DiskLoc(),
                                                                             false ) );
            catalog->putMetaData( &opCtx, "a.b", md );
            uow.commit();
        }

        string idxIndent;
        {
            MyOperationContext opCtx( engine );
            idxIndent = catalog->getIndexIdent( &opCtx, "a.b", "foo" );
        }

        {
            MyOperationContext opCtx( engine );
            ASSERT_EQUALS( idxIndent, catalog->getIndexIdent( &opCtx, "a.b", "foo" ) );
        }

        {
            MyOperationContext opCtx( engine );
            WriteUnitOfWork uow( &opCtx );

            BSONCollectionCatalogEntry::MetaData md;
            md.ns ="a.b";
            catalog->putMetaData( &opCtx, "a.b", md ); // remove index
            md.indexes.push_back( BSONCollectionCatalogEntry::IndexMetaData( BSON( "name" << "foo" ),
                                                                             false,
                                                                             DiskLoc(),
                                                                             false ) );
            catalog->putMetaData( &opCtx, "a.b", md );
            uow.commit();
        }

        {
            MyOperationContext opCtx( engine );
            ASSERT_NOT_EQUALS( idxIndent, catalog->getIndexIdent( &opCtx, "a.b", "foo" ) );
        }

    }

}
