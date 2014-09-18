// wiredtiger_record_store.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *    Copyright (C) 2014 WiredTiger Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
 
#pragma once

#include <string>

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/capped_callback.h"
#include "mongo/platform/atomic_word.h"

#include "mongo/db/storage/wiredtiger/wiredtiger_engine.h"

namespace mongo {

    class WiredTigerRecoveryUnit;

    class WiredTigerRecordStore : public RecordStore {
        public:
        static int Create(WiredTigerDatabase &db,
            const StringData &ns, const CollectionOptions &options, bool allocateDefaultSpace);

        static std::string _getURI(const StringData &ns) {
            return "table:" + ns.toString();
        }

        static std::string _fromURI(const std::string &uri) {
            return uri.substr(strlen("table:"));
        }

        static uint64_t _makeKey(const DiskLoc &loc);
        static DiskLoc _fromKey(uint64_t k);

        WiredTigerRecordStore(const StringData& ns,
                          WiredTigerDatabase &db,
                          bool isCapped = false,
                          int64_t cappedMaxSize = -1,
                          int64_t cappedMaxDocs = -1,
                          CappedDocumentDeleteCallback* cappedDeleteCallback = NULL );

        virtual ~WiredTigerRecordStore();

        // name of the RecordStore implementation
        virtual const char* name() const { return "wiredtiger"; }

        virtual long long dataSize( OperationContext *txn ) const;

        virtual long long numRecords( OperationContext* txn ) const;

        virtual bool isCapped() const;

        virtual int64_t storageSize( OperationContext* txn,
                                     BSONObjBuilder* extraInfo = NULL,
                                     int infoLevel = 0 ) const;

        // CRUD related

        virtual RecordData dataFor( OperationContext* txn, const DiskLoc& loc ) const;

        virtual void deleteRecord( OperationContext* txn, const DiskLoc& dl );

        virtual StatusWith<DiskLoc> insertRecord( OperationContext* txn,
                                                  const char* data,
                                                  int len,
                                                  bool enforceQuota );

        virtual StatusWith<DiskLoc> insertRecord( OperationContext* txn,
                                                  const DocWriter* doc,
                                                  bool enforceQuota );

        virtual StatusWith<DiskLoc> updateRecord( OperationContext* txn,
                                                  const DiskLoc& oldLocation,
                                                  const char* data,
                                                  int len,
                                                  bool enforceQuota,
                                                  UpdateMoveNotifier* notifier );

        virtual Status updateWithDamages( OperationContext* txn,
                                          const DiskLoc& loc,
                                          const char* damangeSource,
                                          const mutablebson::DamageVector& damages );

        virtual RecordIterator* getIterator( OperationContext* txn,
                                             const DiskLoc& start = DiskLoc(),
                                             bool tailable = false,
                                             const CollectionScanParams::Direction& dir =
                                             CollectionScanParams::FORWARD ) const;

        virtual RecordIterator* getIteratorForRepair( OperationContext* txn ) const;

        virtual std::vector<RecordIterator*> getManyIterators( OperationContext* txn ) const;

        virtual Status truncate( OperationContext* txn );

        virtual bool compactSupported() const { return true; }

        virtual Status compact( OperationContext* txn,
                                RecordStoreCompactAdaptor* adaptor,
                                const CompactOptions* options,
                                CompactStats* stats );

        virtual Status validate( OperationContext* txn,
                                 bool full, bool scanData,
                                 ValidateAdaptor* adaptor,
                                 ValidateResults* results, BSONObjBuilder* output ) const;

        virtual void appendCustomStats( OperationContext* txn,
                                        BSONObjBuilder* result,
                                        double scale ) const;

        virtual Status touch( OperationContext* txn, BSONObjBuilder* output ) const;

        virtual Status setCustomOption( OperationContext* txn,
                                        const BSONElement& option,
                                        BSONObjBuilder* info = NULL );

        virtual void temp_cappedTruncateAfter(OperationContext* txn,
                                              DiskLoc end,
                                              bool inclusive);

        void setCappedDeleteCallback(CappedDocumentDeleteCallback* cb) {
            _cappedDeleteCallback = cb;
        }
        void setCapped(int64_t cappedMaxDocs, int64_t cappedMaxSize);
        int64_t cappedMaxDocs() const;
        int64_t cappedMaxSize() const;

        const std::string &GetURI() const { return _uri; }

    private:

        class Iterator : public RecordIterator {
        public:
            Iterator( const WiredTigerRecordStore& rs,
                      OperationContext* txn,
                      const DiskLoc& start,
                      bool tailable,
                      const CollectionScanParams::Direction& dir );

            virtual ~Iterator();

            virtual bool isEOF();
            virtual DiskLoc curr();
            virtual DiskLoc getNext();
            virtual void invalidate(const DiskLoc& dl);
            virtual void saveState();
            virtual bool restoreState(OperationContext *txn);
            virtual RecordData dataFor( const DiskLoc& loc ) const;

        private:
            bool _forward() const;
            void _getNext();
            void _locate( const DiskLoc &loc, bool exact );
            void _checkStatus();
            DiskLoc _curr() const; // const version of public curr method

            const WiredTigerRecordStore& _rs;
            OperationContext* _txn;
            bool _tailable;
            CollectionScanParams::Direction _dir;
            WiredTigerCursor *_cursor;
            bool _eof;

            // Position for save/restore
            DiskLoc _lastLoc;
            DiskLoc _savedLoc;
            bool _savedAtEnd, _savedInvalidated;
        };

        static WiredTigerRecoveryUnit* _getRecoveryUnit( OperationContext* txn );

        DiskLoc _nextId();
        void _setId(DiskLoc loc);
        bool cappedAndNeedDelete(OperationContext* txn) const;
        void cappedDeleteAsNeeded(OperationContext* txn);
        void _changeNumRecords(OperationContext* txn, bool insert);
        void _increaseDataSize(OperationContext* txn, int amount);
        RecordData _getData( const WiredTigerCursor &cursor) const;

        WiredTigerDatabase &_db;
        const std::string _uri;
        // The capped settings should not be updated once operations have started
        bool _isCapped;
        int64_t _cappedMaxSize;
        int64_t _cappedMaxDocs;
        CappedDocumentDeleteCallback* _cappedDeleteCallback;

        AtomicUInt64 _nextIdNum;
        long long _dataSize;
        long long _numRecords;
        mutable boost::mutex _idLock;
    };
}
