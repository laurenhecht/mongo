// kv_engine.h

#pragma once

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/catalog/collection_options.h"

namespace mongo {

    class IndexDescriptor;
    class OperationContext;
    class RecordStore;
    class RecoveryUnit;
    class SortedDataInterface;

    class KVEngine {
    public:

        virtual ~KVEngine() {}

        virtual RecoveryUnit* newRecoveryUnit() = 0;

        // ---------

        /**
         * @param ident Ident is a one time use string. It is used for this instance
         *              and never again.
         */
        virtual Status createRecordStore( OperationContext* opCtx,
                                          const StringData& ident,
                                          const CollectionOptions& options ) = 0;

        /**
         * Caller takes ownership
         * Having multiple out for the same ns is a rules violation;
         * Calling on a non-created ident is invalid and may crash.
         */
        virtual RecordStore* getRecordStore( OperationContext* opCtx,
                                             const StringData& ns,
                                             const StringData& ident,
                                             const CollectionOptions& options ) = 0;

        virtual Status dropRecordStore( OperationContext* opCtx,
                                        const StringData& ident ) = 0;

        // --------

        virtual Status createSortedDataInterface( OperationContext* opCtx,
                                                  const StringData& ident,
                                                  const IndexDescriptor* desc ) = 0;

        virtual SortedDataInterface* getSortedDataInterface( OperationContext* opCtx,
                                                             const StringData& ident,
                                                             const IndexDescriptor* desc ) = 0;

        virtual Status dropSortedDataInterface( OperationContext* opCtx,
                                                const StringData& ident ) = 0;
    };

}
