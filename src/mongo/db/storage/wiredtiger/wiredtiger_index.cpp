// wiredtiger_index.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *    Copyright (C) 2014 WiredTiger Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <set>

#include "mongo/db/json.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage_options.h"
#include "mongo/util/log.h"

#include "mongo/db/storage/wiredtiger/wiredtiger_index.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"

namespace mongo {
namespace {
    static const int TempKeyMaxSize = 1024; // this goes away with SERVER-3372

    static const WiredTigerItem emptyItem(NULL, 0);

    bool hasFieldNames(const BSONObj& obj) {
        BSONForEach(e, obj) {
            if (e.fieldName()[0])
                return true;
        }
        return false;
    }

    BSONObj stripFieldNames(const BSONObj& query) {
        if (!hasFieldNames(query))
            return query;

        BSONObjBuilder bb;
        BSONForEach(e, query) {
            bb.appendAs(e, StringData());
        }
        return bb.obj();
    }

    /**
     * Constructs an IndexKeyEntry from a slice containing the bytes of a BSONObject followed
     * by the bytes of a DiskLoc
     */
    static IndexKeyEntry makeIndexKeyEntry(const WT_ITEM *keyCols) {
        const char* data = reinterpret_cast<const char*>( keyCols->data );
        BSONObj key( data );
        if ( keyCols->size == static_cast<size_t>( key.objsize() ) ) {
            // in unique mode
            return IndexKeyEntry( key, DiskLoc() );
        }
        invariant( keyCols->size == key.objsize() + sizeof(DiskLoc) );
        DiskLoc loc = reinterpret_cast<const DiskLoc*>( data + key.objsize() )[0];
        return IndexKeyEntry( key, loc );
    }

    WiredTigerItem _toItem( const BSONObj& key, const DiskLoc& loc,
                            boost::scoped_array<char>*out ) {
        size_t keyLen = key.objsize() + sizeof(DiskLoc);
        out->reset( new char[keyLen] );
        memcpy( out->get(), key.objdata(), key.objsize() );
        memcpy( out->get() + key.objsize(), reinterpret_cast<const char*>(&loc), sizeof(DiskLoc) );

        return WiredTigerItem( out->get(), keyLen );
    }

    DiskLoc _toDiskLoc( const WT_ITEM& item ) {
        DiskLoc l;
        memcpy( &l, item.data, sizeof(DiskLoc) );
        return l;
    }

    /**
     * Custom comparator used to compare Index Entries by BSONObj and DiskLoc
     */
    struct WiredTigerIndexCollator : public WT_COLLATOR {
        public:
            WiredTigerIndexCollator(const Ordering& order)
                    :WT_COLLATOR(), _indexComparator(order) {
                compare = _compare;
                terminate = _terminate;
            }

            int Compare(WT_SESSION *s, const WT_ITEM *a, const WT_ITEM *b) const {
                const IndexKeyEntry lhs = makeIndexKeyEntry(a);
                const IndexKeyEntry rhs = makeIndexKeyEntry(b);
                int cmp = _indexComparator.compare( lhs, rhs );
                if (cmp < 0)
                    cmp = -1;
                else if (cmp > 0)
                    cmp = 1;
                return cmp;
            }

            static int _compare(WT_COLLATOR *coll, WT_SESSION *s, const WT_ITEM *a, const WT_ITEM *b, int *cmp) {
                    WiredTigerIndexCollator *c = static_cast<WiredTigerIndexCollator *>(coll);
                    *cmp = c->Compare(s, a, b);
                    return 0;
            }

            static int _terminate(WT_COLLATOR *coll, WT_SESSION *s) {
                    WiredTigerIndexCollator *c = static_cast<WiredTigerIndexCollator *>(coll);
                    delete c;
                    return 0;
            }

        private:
            const IndexEntryComparison _indexComparator;
    };

    extern "C" int index_collator_customize(WT_COLLATOR *coll, WT_SESSION *s, const char *uri, WT_CONFIG_ITEM *metadata, WT_COLLATOR **collp) {
            IndexDescriptor desc(0, "unknown", fromjson(std::string(metadata->str, metadata->len)));
            *collp = new WiredTigerIndexCollator(Ordering::make(desc.keyPattern()));
            return 0;
    }

    extern "C" MONGO_COMPILER_API_EXPORT int index_collator_extension(WT_CONNECTION *conn, WT_CONFIG_ARG *cfg) {
            static WT_COLLATOR idx_static;

            idx_static.customize = index_collator_customize;
            return conn->add_collator(conn, "mongo_index", &idx_static, NULL);
    }

    // taken from btree_logic.cpp
    Status dupKeyError(const BSONObj& key) {
        StringBuilder sb;
        sb << "E11000 duplicate key error ";
        // sb << "index: " << _indexName << " "; // TODO
        sb << "dup key: " << key;
        return Status(ErrorCodes::DuplicateKey, sb.str());
    }
} // namespace

    int WiredTigerIndex::Create(OperationContext* txn,
                                const std::string& uri,
                                const std::string& extraConfig,
                                const IndexDescriptor* desc ) {
        WT_SESSION* s = WiredTigerRecoveryUnit::get( txn )->getSession()->getSession();

        // Separate out a prefix and suffix in the default string. User configuration will
        // override values in the prefix, but not values in the suffix.
        string default_config_pfx = "type=file,leaf_page_max=16k,";
        // Indexes need to store the metadata for collation to work as expected.
        string default_config_sfx = ",key_format=u,value_format=u,collator=mongo_index,app_metadata=";

        std::string config = default_config_pfx + extraConfig + default_config_sfx + desc->infoObj().jsonString();
        LOG(1) << "create uri: " << uri << " config: " << config;
        return s->create(s, uri.c_str(), config.c_str());
    }

    WiredTigerIndex::WiredTigerIndex(const std::string &uri )
        : _uri( uri ),
          _instanceId( WiredTigerSession::genCursorId() ) {
    }

    Status WiredTigerIndex::insert(OperationContext* txn,
              const BSONObj& key,
              const DiskLoc& loc,
              bool dupsAllowed) {
        invariant(!loc.isNull());
        invariant(loc.isValid());
        invariant(!hasFieldNames(key));

        if ( key.objsize() >= TempKeyMaxSize ) {
            string msg = mongoutils::str::stream()
                << "WiredTigerIndex::insert: key too large to index, failing "
                << ' ' << key.objsize() << ' ' << key;
            return Status(ErrorCodes::KeyTooLong, msg);
        }

        WiredTigerCursor curwrap(_uri, _instanceId, txn);
        WT_CURSOR *c = curwrap.get();

        return _insert( c, key, loc, dupsAllowed );
    }

    bool WiredTigerIndex::unindex(OperationContext* txn,
                                  const BSONObj& key,
                                  const DiskLoc& loc,
                                  bool dupsAllowed ) {
        invariant(!loc.isNull());
        invariant(loc.isValid());
        invariant(!hasFieldNames(key));

        WiredTigerCursor curwrap(_uri, _instanceId, txn);
        WT_CURSOR *c = curwrap.get();
        invariant( c );

        return _unindex( c, key, loc, dupsAllowed );
    }

    void WiredTigerIndex::fullValidate(OperationContext* txn, long long *numKeysOut) const {
        // TODO check invariants?
        WiredTigerCursor curwrap(_uri, _instanceId, txn);
        WT_CURSOR *c = curwrap.get();
        if (!c)
            return;
        int ret;
        long long count = 0;
        while ((ret = c->next(c)) == 0)
            ++count;
        *numKeysOut = count;
    }

    Status WiredTigerIndex::dupKeyCheck( OperationContext* txn,
                                         const BSONObj& key,
                                         const DiskLoc& loc) {
        invariant(!hasFieldNames(key));
        invariant(unique());

        WiredTigerCursor curwrap(_uri, _instanceId, txn);
        WT_CURSOR *c = curwrap.get();

        if ( isDup(c, key, loc) )
            return dupKeyError(key);
        return Status::OK();
    }

    bool WiredTigerIndex::isEmpty(OperationContext* txn) {
        WiredTigerCursor curwrap(_uri, _instanceId, txn);
        WT_CURSOR *c = curwrap.get();
        if (!c)
            return true;
        int ret = c->next(c);
        if (ret == WT_NOTFOUND)
            return true;
        invariantWTOK(ret);
        return false;
    }

    Status WiredTigerIndex::touch(OperationContext* txn) const {
        // already in memory...
        return Status::OK();
    }

    long long WiredTigerIndex::getSpaceUsedBytes( OperationContext* txn ) const { return 1; }

    bool WiredTigerIndex::isDup(WT_CURSOR *c, const BSONObj& key, const DiskLoc& loc ) {
        invariant( unique() );
        // First check whether the key exists.
        WiredTigerItem item( key.objdata(), key.objsize() );
        c->set_key( c, item.Get() );
        int ret = c->search(c);
        if ( ret == WT_NOTFOUND )
            return false;
        invariantWTOK( ret );

        WT_ITEM value;
        invariantWTOK( c->get_value(c,&value) );
        DiskLoc found = _toDiskLoc( value );
        return found != loc;
    }

    /* Cursor implementation */
    WiredTigerIndex::IndexCursor::IndexCursor(const WiredTigerIndex &idx,
            OperationContext *txn,
            bool forward)
       : _txn(txn),
         _cursor(idx.GetURI(), idx.instanceId(), txn ),
         _idx(idx),
         _forward(forward),
         _eof(true) {
    }

    WiredTigerIndex::IndexCursor::~IndexCursor() {
    }

    int WiredTigerIndex::IndexCursor::getDirection() const { return _forward ? 1 : -1; }

    bool WiredTigerIndex::IndexCursor::isEOF() const { return _eof; }

    bool WiredTigerIndex::IndexCursor::pointsToSamePlaceAs(
            const SortedDataInterface::Cursor &genother) const {
        const WiredTigerIndex::IndexCursor &other =
            dynamic_cast<const WiredTigerIndex::IndexCursor &>(genother);

        if ( _eof && other._eof )
            return true;
        else if ( _eof || other._eof )
            return false;
        WT_CURSOR *c = _cursor.get(), *otherc = other._cursor.get();
        int cmp, ret = c->compare(c, otherc, &cmp);
        invariantWTOK(ret);
        return cmp == 0;
    }

    void WiredTigerIndex::IndexCursor::aboutToDeleteBucket(const DiskLoc& bucket) {
        invariant(!"aboutToDeleteBucket should not be called");
    }

    bool WiredTigerIndex::_search(WT_CURSOR *c, const BSONObj &key, const DiskLoc& loc, bool forward) {
        DiskLoc searchLoc = loc;
        // Null cursors should start at the zero key to maintain search ordering in the
        // collator.
        // Reverse cursors should start on the last matching key.
        if (loc.isNull())
            searchLoc = forward ? DiskLoc(0, 0) : DiskLoc(INT_MAX, INT_MAX);
        boost::scoped_array<char> data;
        WiredTigerItem myKey = _toItem( key, searchLoc, &data );
        return ( _search( c, myKey, forward ) );
    }

    bool WiredTigerIndex::_search(WT_CURSOR *c, const WiredTigerItem& myKey, bool forward) {
        int cmp = -1, ret;
        c->set_key(c, myKey.Get() );
        ret = c->search_near(c, &cmp);

        // Make sure we land on a matching key
        if (ret == 0 && (forward ? cmp < 0 : cmp > 0))
            ret = forward ? c->next(c) : c->prev(c);
        if (ret != WT_NOTFOUND) invariantWTOK(ret);
        return (ret == 0);
    }


    bool WiredTigerIndex::IndexCursor::_locate(const BSONObj &key, const DiskLoc& loc) {
        WT_CURSOR *c = _cursor.get();
        _eof = !WiredTigerIndex::_search(c, key, loc, _forward);
        if ( _eof )
            return false;
        return key == getKey();
    }

    bool WiredTigerIndex::IndexCursor::locate(const BSONObj &key, const DiskLoc& loc) {
        const BSONObj finalKey = stripFieldNames(key);
        bool result = _locate(finalKey, loc);

        // An explicit search at the start of the range should always return false
        if (loc == minDiskLoc || loc == maxDiskLoc )
            return false;
        return result;
   }

    void WiredTigerIndex::IndexCursor::advanceTo(const BSONObj &keyBegin,
           int keyBeginLen,
           bool afterKey,
           const vector<const BSONElement*>& keyEnd,
           const vector<bool>& keyEndInclusive) {

        BSONObj key = IndexEntryComparison::makeQueryObject(
                         keyBegin, keyBeginLen,
                         afterKey, keyEnd, keyEndInclusive, getDirection() );

        _locate(key, DiskLoc());
    }

    void WiredTigerIndex::IndexCursor::customLocate(const BSONObj& keyBegin,
                  int keyBeginLen,
                  bool afterKey,
                  const vector<const BSONElement*>& keyEnd,
                  const vector<bool>& keyEndInclusive) {
        advanceTo(keyBegin, keyBeginLen, afterKey, keyEnd, keyEndInclusive);
    }

    BSONObj WiredTigerIndex::IndexCursor::getKey() const {
        WT_CURSOR *c = _cursor.get();
        WT_ITEM keyItem;
        int ret = c->get_key(c, &keyItem);
        invariantWTOK(ret);
        return makeIndexKeyEntry(&keyItem).key;
    }

    DiskLoc WiredTigerIndex::IndexCursor::getDiskLoc() const {
        WT_CURSOR *c = _cursor.get();
        WT_ITEM item;
        if ( _idx.unique() ) {
            invariantWTOK( c->get_value(c, &item ) );
            return _toDiskLoc( item );
        }
        invariantWTOK( c->get_key(c, &item) );
        return makeIndexKeyEntry( &item ).loc;
    }

    void WiredTigerIndex::IndexCursor::advance() {
        // Advance on a cursor at the end is a no-op
        if ( _eof )
            return;
        WT_CURSOR *c = _cursor.get();
        int ret = _forward ? c->next(c) : c->prev(c);
        if (ret == WT_NOTFOUND)
            _eof = true;
        else {
            invariantWTOK(ret);
            _eof = false;
        }
    }

    void WiredTigerIndex::IndexCursor::savePosition() {
        _savedForCheck = _txn->recoveryUnit();
        _txn = NULL;
    }

    void WiredTigerIndex::IndexCursor::restorePosition( OperationContext *txn ) {
        // Update the session handle with our new operation context.
        _txn = txn;
        invariant( _savedForCheck == txn->recoveryUnit() );
    }

    SortedDataInterface::Cursor* WiredTigerIndex::newCursor(OperationContext* txn,
                                                            int direction) const {
        invariant((direction == 1) || (direction == -1));
        return new IndexCursor(*this, txn, direction == 1);
    }

    Status WiredTigerIndex::initAsEmpty(OperationContext* txn) {
        // No-op
        return Status::OK();
    }

    const std::string &WiredTigerIndex::GetURI() const { return _uri; }

    class WiredTigerBuilderImpl : public SortedDataBuilderInterface {
    public:
        WiredTigerBuilderImpl(WiredTigerIndex* idx,
                              OperationContext *txn,
                              bool dupsAllowed)
            : _idx(idx), _txn(txn), _dupsAllowed(dupsAllowed), _count(0) {
        }

        ~WiredTigerBuilderImpl() {
        }

        Status addKey(const BSONObj& key, const DiskLoc& loc) {
            Status s = _idx->insert(_txn, key, loc, _dupsAllowed);
            if (s.isOK())
                _count++;
            return s;
        }

        void commit(bool mayInterrupt) {
            // this is bizarre, but required as part of the contract
            WriteUnitOfWork uow( _txn );
            uow.commit();
        }

    private:
        WiredTigerIndex* _idx;
        OperationContext* _txn;
        bool _dupsAllowed;
        unsigned long long _count;
    };

    SortedDataBuilderInterface* WiredTigerIndex::getBulkBuilder( OperationContext* txn,
                                                                 bool dupsAllowed ) {
        if ( !dupsAllowed ) {
            // if we don't allow dups, we better be unique
            invariant( unique() );
        }
        return new WiredTigerBuilderImpl(this, txn, dupsAllowed);
    }

    // ------------------------------

    WiredTigerIndexUnique::WiredTigerIndexUnique( const std::string& uri )
        : WiredTigerIndex( uri ) {
    }

    Status WiredTigerIndexUnique::_insert( WT_CURSOR* c,
                                           const BSONObj& key,
                                           const DiskLoc& loc,
                                           bool dupsAllowed ) {

        WiredTigerItem keyItem( key.objdata(), key.objsize() );
        WiredTigerItem valueItem( &loc, sizeof(loc) );
        c->set_key( c, keyItem.Get() );
        c->set_value( c, valueItem.Get() );
        int ret = c->insert( c );

        if ( ret != WT_DUPLICATE_KEY )
            return wtRCToStatus( ret );

        if ( !dupsAllowed ) {
            return dupKeyError(key);
        }

        // we're in weird mode where there might be multiple values
        // we put them all in the "list"
        ret = c->search(c);
        invariantWTOK( ret );

        WT_ITEM old;
        invariantWTOK( c->get_value(c, &old ) );

        // see if its already in the array
        for ( int i = 0; i < (old.size/sizeof(DiskLoc)); i++ ) {
            DiskLoc temp;
            memcpy( &temp,
                    reinterpret_cast<const char*>( old.data ) + ( i * sizeof(DiskLoc) ),
                    sizeof(DiskLoc) );
            if ( loc == temp )
                return Status::OK();
        }

        // not in the array, add it to the back
        size_t newSize = old.size + sizeof(DiskLoc);
        boost::scoped_array<char> bigger( new char[newSize] );
        memcpy( bigger.get(), old.data, old.size );
        memcpy( bigger.get() + old.size, &loc, sizeof(DiskLoc) );
        valueItem = WiredTigerItem( bigger.get(), newSize );
        c->set_value( c, valueItem.Get() );
        return wtRCToStatus( c->update( c ) );
    }

    bool WiredTigerIndexUnique::_unindex( WT_CURSOR* c,
                                          const BSONObj& key,
                                          const DiskLoc& loc,
                                          bool dupsAllowed ) {
        if ( !dupsAllowed ) {
            // nice and clear
            WiredTigerItem keyItem( key.objdata(), key.objsize() );
            c->set_key( c, keyItem.Get() );
            int ret = c->remove(c);
            if (ret == WT_NOTFOUND) {
                return false;
            }
            invariantWTOK(ret);
            return true;
        }

        // ewww
        invariant( false );
    }


    // ------------------------------

    WiredTigerIndexStandard::WiredTigerIndexStandard( const std::string& uri )
        : WiredTigerIndex( uri ) {
    }

    Status WiredTigerIndexStandard::_insert( WT_CURSOR* c,
                                             const BSONObj& key,
                                             const DiskLoc& loc,
                                             bool dupsAllowed ) {
        invariant( dupsAllowed );

        boost::scoped_array<char> data;
        WiredTigerItem item = _toItem( key, loc, &data );
        c->set_key(c, item.Get() );
        c->set_value(c, &emptyItem);
        return wtRCToStatus( c->insert(c) );
    }

    bool WiredTigerIndexStandard::_unindex( WT_CURSOR* c,
                                            const BSONObj& key,
                                            const DiskLoc& loc,
                                            bool dupsAllowed ) {
        invariant( dupsAllowed );
        boost::scoped_array<char> data;
        WiredTigerItem item = _toItem( key, loc, &data);
        c->set_key(c, item.Get() );
        int ret = c->remove(c);
        if (ret == WT_NOTFOUND) {
            return false;
        }
        invariantWTOK(ret);
        return true;
    }


}  // namespace mongo
