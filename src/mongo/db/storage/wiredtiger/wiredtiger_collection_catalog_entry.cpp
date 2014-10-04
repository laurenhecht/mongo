// wiredtiger_collection_catalog_entry.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include <map>
#include <string>

#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/json.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_collection_catalog_entry.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_database.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_index.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_metadata.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_recovery_unit.h"
#include "mongo/util/log.h"

namespace mongo {

    WiredTigerCollectionCatalogEntry::WiredTigerCollectionCatalogEntry( WiredTigerDatabase& db,
                                                                        const StringData& ns,
                                                                        const CollectionOptions& o )
        : CollectionCatalogEntry( ns ), options( o ), _db( db ) {
    }

    // Constructor for a catalog entry that loads information from
    // an existing collection.
    WiredTigerCollectionCatalogEntry::WiredTigerCollectionCatalogEntry(OperationContext* ctx,
                                                                       WiredTigerDatabase& db,
                                                                       const StringData& ns,
                                                                       bool stayTemp)
        : CollectionCatalogEntry ( ns ), options(), _db( db ) {

        WiredTigerMetaData &md = db.GetMetaData();
        uint64_t tblIdentifier = md.getIdentifier( ns.toString() );
        std::string tbl_uri = md.getURI( tblIdentifier );
        BSONObj b = md.getConfig( tblIdentifier );

        // Open the WiredTiger metadata so we can retrieve saved options.

        // Create the collection
        options.parse(b);
        if (!stayTemp)
            options.temp = false;
        WiredTigerRecordStore *wtRecordStore = new WiredTigerRecordStore( ctx, ns, tbl_uri );
        if ( options.capped )
            wtRecordStore->setCapped(options.cappedSize ? options.cappedSize : 4096,
                options.cappedMaxDocs ? options.cappedMaxDocs : -1);
        rs.reset(wtRecordStore);

        // Open any existing indexes
        std::vector<uint64_t> indexIds = md.getAllIndexes( tblIdentifier );
        for ( std::vector<uint64_t>::iterator it = indexIds.begin();
                it != indexIds.end(); ++it ) {
            b = md.getConfig( *it );
            std::string name(b.getStringField("name"));
            IndexDescriptor desc(0, "unknown", b);
            auto_ptr<IndexEntry> newEntry( new IndexEntry() );
            newEntry->name = name;
            newEntry->spec = desc.infoObj();
            // TODO: We need to stash the options field on create and decode them
            // here.
            newEntry->ready = true;
            newEntry->isMultikey = false;

            indexes[name] = newEntry.release();
        }
    }

    WiredTigerCollectionCatalogEntry::~WiredTigerCollectionCatalogEntry() {
        for ( Indexes::const_iterator i = indexes.begin(); i != indexes.end(); ++i )
            delete i->second;
        indexes.clear();
    }

    int WiredTigerCollectionCatalogEntry::getTotalIndexCount( OperationContext* txn ) const {
        return static_cast<int>( indexes.size() );
    }

    int WiredTigerCollectionCatalogEntry::getCompletedIndexCount( OperationContext* txn ) const {
        int ready = 0;
        for ( Indexes::const_iterator i = indexes.begin(); i != indexes.end(); ++i )
            if ( i->second->ready )
                ready++;
        return ready;
    }

    void WiredTigerCollectionCatalogEntry::getAllIndexes( OperationContext* txn, std::vector<std::string>* names ) const {
        for ( Indexes::const_iterator i = indexes.begin(); i != indexes.end(); ++i )
            names->push_back( i->second->name );
    }

    BSONObj WiredTigerCollectionCatalogEntry::getIndexSpec( OperationContext *txn, const StringData& idxName ) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->spec; 
    }

    bool WiredTigerCollectionCatalogEntry::isIndexMultikey( OperationContext* txn, const StringData& idxName) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->isMultikey;
    }

    bool WiredTigerCollectionCatalogEntry::setIndexIsMultikey(OperationContext* txn,
                                                              const StringData& idxName,
                                                              bool multikey ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        if (i->second->isMultikey == multikey)
            return false;

        i->second->isMultikey = multikey;
        return true;
    }

    DiskLoc WiredTigerCollectionCatalogEntry::getIndexHead( OperationContext* txn, const StringData& idxName ) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->head;
    }

    void WiredTigerCollectionCatalogEntry::setIndexHead( OperationContext* txn,
                                                         const StringData& idxName,
                                                         const DiskLoc& newHead ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        i->second->head = newHead;
    }

    bool WiredTigerCollectionCatalogEntry::isIndexReady( OperationContext* txn, const StringData& idxName ) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->ready;
    }

    Status WiredTigerCollectionCatalogEntry::removeIndex( OperationContext* txn,
                                                          const StringData& idxName ) {
        // XXX use a temporary session for creates: WiredTiger doesn't allow transactional creates
        // and can't deal with rolling back a creates.
        WiredTigerRecoveryUnit::Get(txn).getSession()->closeAllCursors();

        WiredTigerRecoveryUnit session_temp( _db.getSessionCache(), false );
        session_temp.getSession()->closeAllCursors();

        WiredTigerMetaData &md = _db.GetMetaData();
        uint64_t identifier = md.getIdentifier(WiredTigerIndex::toTableName( ns().toString(),
                                                                             idxName.toString() ) );
        std::string uri = md.getURI( identifier );
        WT_SESSION *session = session_temp.getSession()->getSession();
        int ret = session->drop(session, uri.c_str(), "force");
        indexes.erase( idxName.toString() );
        md.remove( identifier, ret != 0 );
        log() << "removeIndex " << ns() << " " << idxName << " " << uri << " " << ret;
        return Status::OK();
    }

    Status WiredTigerCollectionCatalogEntry::prepareForIndexBuild( OperationContext* txn,
                                                                   const IndexDescriptor* spec ) {
        auto_ptr<IndexEntry> newEntry( new IndexEntry() );
        newEntry->name = spec->indexName();
        newEntry->spec = spec->infoObj();
        newEntry->ready = false;
        newEntry->isMultikey = false;

        indexes[spec->indexName()] = newEntry.release();
        return Status::OK();
    }

    void WiredTigerCollectionCatalogEntry::indexBuildSuccess( OperationContext* txn,
                                                              const StringData& idxName ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        i->second->ready = true;
    }

    void WiredTigerCollectionCatalogEntry::updateTTLSetting( OperationContext* txn,
                                                             const StringData& idxName,
                                                             long long newExpireSeconds ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );

        BSONObjBuilder b;
        for ( BSONObjIterator bi( i->second->spec ); bi.more(); ) {
            BSONElement e = bi.next();
            if ( e.fieldNameStringData() == "expireAfterSeconds" ) {
                continue;
            }
            b.append( e );
        }

        b.append( "expireAfterSeconds", newExpireSeconds );

        i->second->spec = b.obj();
    }

    bool WiredTigerCollectionCatalogEntry::indexExists( const StringData &idxName ) const {
        WiredTigerCollectionCatalogEntry::Indexes::const_iterator i = indexes.find( idxName.toString() );
        return ( i != indexes.end() );
    }

}
