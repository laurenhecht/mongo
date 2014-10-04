// wiredtiger_session_cache.h

#pragma once

#include <map>
#include <string>
#include <vector>

#include <boost/thread/mutex.hpp>

#include <wiredtiger.h>

namespace mongo {

    /**
     * This is a structure that caches 1 cursor for each uri.
     * The idea is that there is a pool of these somewhere.
     * NOT THREADSAFE
     */
    class WiredTigerSession {
    public:
        WiredTigerSession( WT_CONNECTION* conn );
        ~WiredTigerSession();

        WT_SESSION* getSession() const { return _session; }

        WT_CURSOR* getCursor(const std::string &uri);
        void releaseCursor(WT_CURSOR *cursor);

        void closeAllCursors();

    private:
        WT_SESSION* _session; // owned
        typedef std::map<const std::string, WT_CURSOR *> CursorMap;
        CursorMap _curmap; // owned
    };

    class WiredTigerSessionCache {
    public:

        WiredTigerSessionCache( WT_CONNECTION* conn );
        ~WiredTigerSessionCache();

        WiredTigerSession* getSession();
        void releaseSession( WiredTigerSession* session );

        void closeAll();

    private:

        void _closeAll(); // does not lock

        WT_CONNECTION* _conn; // now owned
        typedef std::vector<WiredTigerSession*> SessionPool;
        SessionPool _sessionPool; // owned
        mutable boost::mutex _sessionLock;

    };

}
