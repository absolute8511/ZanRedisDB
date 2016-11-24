// Usage
//
// First create a rockredis instance before use:
//
//  db := rockredis.Open(cfg)
//
// cfg is a Config instance which contains configuration for rockredis use
//
// After you create a rockredis instance, you can store you data:
//
// KV
//
// KV is the most basic type like any other key-value database.
//
//  err := db.KVSet(key, value)
//  value, err := db.KVGet(key)
//
// List
//
// List is simply lists of values, sorted by insertion order.
// You can push or pop value on the list head (left) or tail (right).
//
//  err := db.LPush(key, value1)
//  err := db.RPush(key, value2)
//  value1, err := db.LPop(key)
//  value2, err := db.RPop(key)
//
// Hash
//
// Hash is a map between fields and values.
//
//  n, err := db.HSet(key, field1, value1)
//  n, err := db.HSet(key, field2, value2)
//  value1, err := db.HGet(key, field1)
//  value2, err := db.HGet(key, field2)
//
// ZSet
//
// ZSet is a sorted collections of values.
// Every member of zset is associated with score, a int64 value which used to sort, from smallest to greatest score.
// Members are unique, but score may be same.
//
//  n, err := db.ZAdd(key, ScorePair{score1, member1}, ScorePair{score2, member2})
//  ay, err := db.ZRangeByScore(key, minScore, maxScore, 0, -1)
//
//
package rockredis
