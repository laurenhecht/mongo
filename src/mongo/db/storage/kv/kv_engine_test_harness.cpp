// kv_engine_test_harness.cpp

#include "mongo/db/storage/kv/kv_engine_test_harness.h"

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/record_store.h"
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

    TEST( KVEngineTestHarness, Simple1 ) {
        scoped_ptr<KVHarnessHelper> helper( KVHarnessHelper::create() );
        KVEngine* engine = helper->getEngine();
        ASSERT( engine );

        string ns = "a.b";
        scoped_ptr<RecordStore> rs;
        {
            MyOperationContext opCtx( engine );
            ASSERT_OK( engine->createRecordStore( &opCtx, ns, CollectionOptions() ) );
            rs.reset( engine->getRecordStore( &opCtx, ns, ns ) );
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

}
