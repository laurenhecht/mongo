// kv_engine_test_harness.h

#pragma once

#include "mongo/db/storage/kv/kv_engine.h"

namespace mongo {
    class KVHarnessHelper {
    public:
        virtual ~KVHarnessHelper(){}

        // returns same thing for entire life
        virtual KVEngine* getEngine() = 0;

        virtual KVEngine* restartEngine() = 0;

        static KVHarnessHelper* create();
    };
}
