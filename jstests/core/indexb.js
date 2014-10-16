// unique index test for a case where the object grows
// and must move

// see indexa.js for the test case for an update with dup id check 
// when it doesn't move


t = db.indexb;
t.drop();
t.ensureIndex({a:1},true);

t.insert({a:1});
assert.eq( 1,t.find({a:1}).count() );

x = { a : 2 };
t.save(x);

assert( t.count() == 2, "count wrong B");

x.a = 1;
x.filler = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
t.save(x); // should fail, not unique.

assert.eq( 2, t.count(),"count wrong" );
assert.eq( 1,t.find({a:1}).count(),"bfail1" );
assert( 1, t.find({a:2}).count(),"bfail2" );
