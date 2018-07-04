# 运维指南

## 操作和接口说明

rsync进程:用于异常恢复时传输zankv的备份数据

集群的一些统计数据, 可以分别从placedriver和zankv的接口获取,统计数据接口如下:

placedriver API

```
/cluster/stats
获取集群总体状态, 标识集群是否稳定, 此请求需要发送到leader节点

/datanodes
获取存活的数据节点(zankv)信息, 存活节点数量有变更需要warning级别报警
```

zankv API

```
/kv/optimize
为了避免太多删除数据影响性能, 可以定期执行此API清理优化性能, 建议每几个月执行一次 (每台zankv机子错峰执行).

/stats
获取统计数据,其中db_write_stats, cluster_write_stats中两个长度为16的数据对应的数据, 标识对应区间统计的计数器. 其中db_write_stats代表存储层的统计数据, cluster_write_stats表示服务端协议层的统计数据(从收到网络请求开始, 到回复网络请求结束), 具体的统计区间含义可以参考代码WriteStats结构的定义.

/raft/stats
获取raft集群状态, 用于判断异常信息
```

## 备份恢复

### 备份文件格式
备份工具备份的文件名格式:
datatype:yyyy-mm-dd:namespace:table.db, 每个不同类型的数据备份文件有一个相同的备份文件头, 数据内容和具体的数据格式有关.

#### 公共文件头格式

在所有的文件头会写入如下几个信息：

|字段|字节数|说明|
|----|----|----|
|MAGIC|5|魔数，目前为"ZANKV"|
|VERSION|4|	版本，目前为"0001"|
|NameSpace Len|4|namespace的长度|
|NameSpace|	变长,由namespace len指定| namespace名称|
|TableName Len|	4|	table name的长度|
|TableName|变长|table name|
|Type|1|类型。0:kv;1:hash;2:set;3:zset;4:list|

#### kv文件格式

kv文件中除去公共文件格式之外，就是key-value的内容。格式如下:

|字段|字节数|说明|
|----|----|----|
|Key Len| 4 |key的长度|
|Key|变长| key的内容|
|Value Len| 4 |value的长度|
|Value| 变长 |value的内容|


#### hash文件格式

除去公共文件格式之外，就是key-field-value的内容。格式如下:

|字段|字节数|说明|
|----|----|----|
|Key Len| 4 |key的长度|
|Key|变长| key的内容|
|Field number|	4	|所有field个数|
|field Len	|4|	field长度|
|field	| 变长 |field内容|
|Value Len| 4 |value的长度|
|Value| 变长 |value的内容|

#### set文件格式

#### list文件格式

#### zset文件格式

### 备份工具backup

```
backup -lookup lookuplist -ns namespace -table table_name [-data_dir backup -type all|kv[,hash,set,zset,list] -qps 100]
 
参数说明:
-lookup            zankv lookup服务器,可以是用","分割的一组服务器
-ns                  要备份的namespace
-table              要备份的table
-data_dir         备份目录，默认当前目录下打data目录
-type              要备份的数据结构类型，支持kv,hash,set,zset,list，all表示备份所有类型，输入all了，就不能输入其他的类型了，默认为all
-qps               速度控制，默认1000 qps
```
 
### 恢复工具restore

```
restore -lookup lookuplist -data restore  [-ns namespace -table table_name -qps 1000]
参数说明:
-lookup            zankv lookup服务器,可以是用","分割的一组服务器
-data               需要恢复的备份文件
-ns                  要备份的namespace，如果不输入，则恢复的时候，按照备份的ns进行恢复
-table              要备份的table，如果不输入，则恢复的时候，按照备份的table进行恢复
-qps               速度控制，默认1000 qps
```


## 跨机房同步运维

同城3机房的情况, 使用默认的跨机房大集群模式部署即可, 使用raft自动同步和做故障切换.

异地机房部署, 为了减少同步延迟, 一般部署成跨机房异步同步模式, 运维流程如下:
假设部署了A机房和B机房集群, 开始使用A机房做主

### 初始化

- B机房集群用于同步, 使用相同配置, 更改其中的cluster_id和A机房不同, 以及etcd地址使用B机房, B机房zankv配置增加一项: "syncer_write_only": true, 逐一启动
- B机房调用API创建初始化namespace, 保持和A机房一样
- B机房选择2台机子做同步程序部署. 用于A到B机房同步
- 反向同步部署到A机房, 用于切换后从B到A同步. (反向同步准备好配置, 正常不启动)
- B机房部署的同步管理程序placedriver和数据同步程序zankv都和A机房配置一样, 都需要增加 learner_role="role_log_syncer" 配置, 表明该程序只做数据同步, 同步程序placedriver只需部署一台, 同步程序zankv部署2台做主备, 同步程序zankv, 配置还增加需要同步的目标集群(B机房集群)地址 "remote_sync_cluster": "remoteip:port"
- 启动同步placedriver, 再启动2台同步程序zankv.
- 等待数据同步初始化
- 反向同步, 配置对应修改好, 不启动

部署好之后, 架构如下:
![zankv-cluster-sync](resource/zankv-sync-cluster.png)

### 机房正常切换

- A机房集群禁止客户端写入, 所有节点调用API设置只允许同步程序写入 POST /synceronly?enable=true
- 观察数据同步完成,  停止用于同步A集群到B机房的数据同步程序和同步管理程序. (停同步程序, 向管理程序发送删除raft learner请求, 清理磁盘数据)
- 启动事先准备好的反向同步, 用于同步B集群到A机房, 注意配置修改, 增加 syncer_normal_init=true用于正常初始化
- 数据同步程序会自动初始化同步数据(获取B集群各个分区最新的term-index数据, 设置A集群的同步起始term-index为B集群的最新term-index), 观察同步启动初始化完成后, 去掉syncer_normal_init配置重启数据同步程序.
- B集群API设置允许非同步程序写入. B机房集群开始接收新的客户端写入 POST /synceronly?enable=false, 并去掉配置文件中的 "syncer_write_only": true

切换完成

### 机房异常切换

- 机房异常因此A机房集群无法访问.
- 记录此时的正常B机房从A已经完成的同步位移点term-index, 以及当前B集群自己的term-index数据.
- B机房调用API允许非同步程序写入, 恢复客户端读写 POST /synceronly?enable=false, 去掉配置文件中的 "syncer_write_only": true

故障时切换完成, 后继等待A机房故障恢复.

- 等待异常A机房恢复后,(最好先确保网络不能跨机房) , 先调用API只允许同步程序写入POST /synceronly?enable=true , 禁止客户端写入. 记录此时A集群自己的term-index, 并导出之前同步位移点之后的数据用于校验补偿.(对于同步程序如果是slave, 可以开启同步日志, 记录原始同步数据, 便于补偿数据)
- 等待同步程序将故障时的数据同步到B完成. B机房集群此时会根据时间戳信息决定同步的KV类型数据是覆盖还是忽略(记录冲突log). 其他类型数据, 如果根据时间戳判断期间没有写入过, 则同步成功, 否则报错记录冲突log, 需要人工修订.
- 同步完故障期间数据后, 停掉A集群到B机房的数据同步程序
- 启动反向同步程序, 用于将B机房数据同步到A机房, 此时A机房转换为备集群 (故障会需要传输全量数据, 注意网络情况)
- 修复数据, A机房集群故障时记录的同步位移点之后的数据, B集群故障切换之后写入的新数据在A机房未同步的数据集合做对比.