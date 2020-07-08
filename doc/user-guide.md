# 用户使用指南

## 注意事项

- Key的格式必须为namespace:table:real-key，否则将返回错误。
- key本身的长度尽可能短, 最长不要超过1KB.
- 子key里面的单个value大小不能大于8MB, 子key总数不限制。
- 默认使用“非一致性本地删除”策略进行数据过期，数据过期功能仅保证数据不会被提前删除，不保证删除的实时性。不支持使用TTL指令获取数据生存时间，和persist指令持久化数据, 只支持固定的过期时间, 不支持动态调整过期, 过期时间一旦确定, 不能变更. 即使key发生了后继更新操作(set, setex), 过期时间依然保持第一次的不变, 如果需要动态调整TTL, 需要在创建namespace时指定使用一致性过期删除策略.
- del, expire 操作仅用于kv类型数据, 对其他类型数据无效, 其他数据类型因为是集合类型, 针对key的删除, 需要删除整个集合的所有数据, 因此需要使用对应类型的扩展命令(hclear, hexpire, sclear, zclear, zexpire等)
- 为了防止一次获取太多数据, 对HASH (hgetall, hmget, hkeys, hvals等), list(lrange等), set (smembers等), zset (zrange等) 集合类型的数据批量获取命令做了最大限制(服务端目前配置最大一次性获取5000, 超过会直接返回错误信息), 如果需要超过此限制, 建议使用分页hscan, sscan, zscan等操作, 限制每一次获取的数据数量.

## 协议兼容性

支持的命令列表如下:

### KV String类型

|Command|说明|
| ---- | ---- |
|set|√|
|setex|√|
|get|√|
|getset|√|
|expire|√|
|del|√|
|ttl|√|
|persist|√|
|incr|√|
|incrby|√|
|exists|√|
|mget|√|
|decr|√|
|decrby|√|
|setnx|√|

#### Hash数据类型

|Command|说明|
| ---- | ---- |
|hget|√|
|hset|√|
|hdel|√|
|hgetall|√|
|hmget|√|
|hmset|√|
|hexists|√|
|hincrby|√|
|hkeys|	√|
|hlen	|√|
|hclear	|扩展命令|
|hexpire|扩展命令|
|httl	|扩展命令|
|hpersist|扩展命令|

#### List数据类型

|Command|说明|
| ---- | ---- |
|lindex|√|
|llen|√|
|lrange|√|
|lset|√|
|lpush|	√|
|lpop|√|
|ltrim|	√|
|rpop|√|
|rpush|	√|
|lclear|扩展命令|
|lexpire|扩展命令|
|lttl|扩展命令|
|lpersist|扩展命令|

#### Set 数据类型

|Command|说明|
| ---- | ---- |
|scard|√|
|sismember|√|
|smembers|√|
|srandmember|√, 按顺序返回|
|sadd|√|
|srem|√|
|spop|√|
|sclear|扩展命令|
|smclear|扩展命令|
|sexpire|扩展命令|
|sttl|扩展命令|
|spersist|扩展命令|

#### Zset 数据类型

|Command|说明|
| ---- | ---- |
|zscore|√|
|zcount|√|
|zcard|√|
|zlexcount|√|
|zrange	|√|
|zrevrange|√|
|zrangebylex|√|
|zrangebyscore|√|
|zrevrangebyscore|√|
|zrank|√|
|zrevrank|√|
|zadd|√|
|zincrby|√|
|zrem|√|
|zremrangebyrank|√|
|zremrangebyscore|√|
|zremrangebylex|√|
|zclear	|扩展命令|
|zexpire|扩展命令|
|zttl|扩展命令|
|zpersist|扩展命令|

#### HyperLogLog数据类型

|Command|说明|
| ---- | ---- |
|pfadd|√|
|pfcount|√|

#### GeoHash数据类型

|Command|说明|
| ---- | ---- |
|geoadd	|√|
|geohash|√|
|geopos	|√|
|geodist|√|
|georadius|√|
|georadiusbymember|√|

#### scan命令

|Command|说明|
| ---- | ---- |
|scan|仅支持扫描kv数据的key|
|advscan|扩展命令|
|hscan|√|
|sscan|√|
|zscan|√|

说明:

扫表命令系列的用法和官方文档基本一致, 除了cursor的使用有所不同, cursor是字符串标示, 不一定是整型字符串, 扫表终结标示是空字符串. (如果需要只扫描其中的一个表, 那么判断终止的条件应该是看cursor的table前缀是否一致.)

scan的扫表仅支持kv类型数据, 并且cursor必须包含 namespace和table前缀.  可以使用 advscan 来扫描其他类型的key列表.

注意扫表结果的顺序, 跨分区的情况, 只保证分区内的数据顺序.

advscan命令语法
```
ADVSCAN cursor datatype [MATCH pattern] [COUNT count]
其中datatype为字符串, 可以为: KV, HASH, LIST, ZSET, SET
```

scan示例
```
127.0.0.1:12381> scan yz_cp_simple:test5:0000000100 match *1180* count 10
1) "test5:0000011807"
2)  1) "0000001180"
    2) "0000011180"
    3) "0000011800"
    4) "0000011801"
    5) "0000011802"
    6) "0000011803"
    7) "0000011804"
    8) "0000011805"
    9) "0000011806"
   10) "0000011807"
  
继续scan, 使用返回的cursor继续, 直到cursor返回空串:
  
127.0.0.1:12381> scan yz_cp_simple:test5:0000011807 count 1
1) "test5:0000011808"
2)  1) "0000011808"
```

advscan示例
```
> advscan yz_cp_simple:test5 kv count 10
1) "test5:0000000010"
2)  1) "0000000001"
    2) "0000000002"
    3) "0000000003"
    4) "0000000004"
    5) "0000000005"
    6) "0000000006"
    7) "0000000007"
    8) "0000000008"
    9) "0000000009"
   10) "0000000010"
  
> advscan yz_cp_simple:tt zset count 10
1) "tt:myzsetkey16"
2)  1) "myzsetkey0"
    2) "myzsetkey1"
    3) "myzsetkey10"
    4) "myzsetkey100"
    5) "myzsetkey11"
    6) "myzsetkey12"
    7) "myzsetkey13"
    8) "myzsetkey14"
    9) "myzsetkey15"
   10) "myzsetkey16"
```

hscan示例(SSCAN, ZSCAN类似):
```
> hmset yz_cp_simple:test5:hash name Jack age 33
OK
 
> hscan yz_cp_simple:test5:hash
1) "name"
2) 1) "age"
   2) "33"
   3) "name"
   4) "Jack"
 
> hscan yz_cp_simple:test5:hash  name
1) ""
2) (empty list or set)
> hscan yz_cp_simple:test5:hash  age
1) "name"
2) 1) "name"
   2) "Jack"
```

#### JSON扩展命令

支持大部分官方json扩展里面的命令: http://rejson.io/ , 其中几个命令说明如下:

|Command|说明|
| ---- | ---- |
| json.del|√|
| json.get |√(不支持格式化等参数)|
| json.mkget|√(等价于json.mget)|
| json.set|√|
| json.type|√|
| json.arrappend|√|
| json.arrlen|√|
| json.arrpop|√|
| json.objkeys|√|
| json.objlen|√|

注意json命令的path语法仅支持'.', 不支持'[ ]'形式

示例:
```
{
  "name": {"first": "Tom", "last": "Anderson"},
  "age":37,
  "children": ["Sara","Alex","Jack"],
  "fav.movie": "Deer Hunter",
  "friends": [
    {"first": "Dale", "last": "Murphy", "age": 44},
    {"first": "Roger", "last": "Craig", "age": 68},
    {"first": "Jane", "last": "Murphy", "age": 47}
  ]
}
 
path使用示例:
"name.last"          >> "Anderson"
"age"                >> 37
"children"           >> ["Sara","Alex","Jack"]
"children.1"         >> "Alex"
"fav\.movie"         >> "Deer Hunter"
"friends.1.last"     >> "Craig"
```

## 其他语言支持

使用go-sdk, 可以构建一个proxy支持redis协议, 其他语言使用redis协议客户端直接访问proxy即可

## FAQ

## Examples