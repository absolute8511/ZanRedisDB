## FullScan 

### FullScan诞生背景
	
fullscan命令是zankv独有的命令，其主要作用是用于备份的时候可以一次性scan出key以及对应的value，避免使用先使用scan命令然后再使用对应的get命令获取数据而产生的多次传输造成的性能浪费。

### FullScan 设计思路

fullscan 在设计之初对于不同的命令采用了不同的实现,即kv,hash,list,set,zset使用了不同的函数来实现，在开发的过程中，发现有很多重用的部分，于是，经过梳理，将所有的共用代码抽取出一个单独的函数来处理，但是由于kv的处理流程相比其他的，又要简单，所以在大的流程上还是有所不同，这是最初的设计。后期经过不断的研磨，将各种类型的处理流程进行标准化之后发现，最终的不同只有在解析迭代器的时候是不同的，所以就将对迭代器解析，以及元素的提取采用一个func作为参数传进公共处理函数里面，这样，就大大简化了fullscan的设计逻辑以及代码量。

基于以上的思考与改进，则fullscan的主要逻辑集中在RockDB::fullScanCommon中，其函数原型如下:
	
	func (db *RockDB) fullScanCommon(tp byte, key []byte, count int, match string, f itemFunc) *common.FullScanResult

其中tp 表示的是类型，f 是迭代器处理以及元素提取函数，原型如下：

	type itemFunc func(*RangeLimitedIterator, glob.Glob)(*ItemContainer, error)
	
该函数通过传入一个迭代器，在不同的类型处理函数中来解析迭代器获取key，value等。
ItemContainer的定义如下:
	
	type ItemContainer struct {
		table []byte
		key []byte
		item interface{}
		cursor []byte
	}

fullScanCommon函数主要的逻辑是构建一个迭代器，然后调用各个类型的迭代器处理以及元素提取函数来获取一个结果，并把结果append到一个slice中。

### Cursor组成

cursor由基础的cursor加上各个分区信息组成.

	base64_encode(partionId1:basecursor1;pationId2:basecursor2)
	
其中basecursor是由key+cursor组成，对于不同的类型，格式不一样

#### KV
	
	base64_encode(base64_encode(key):)

#### Hash
	
	base64_encode(base64_encode(key):base64_encode(field))
	
#### List
	
	base64_encode(base64_encode(key):base64_encode(sequence))
	
#### Set
	
	base64_encode(base64_encode(key):base64_encode(member))
	
#### ZSet
	
	base64_encode(base64_encode(key):base64_encode(member))


### FullScan 传输格式

经过对fullscan流程的梳理，我们将fullscan对各种类型的处理逻辑进行了统一，这也涉及到对数据返回格式的统一，在这里简单介绍下fullscan对各种类型返回的格式。

所有返回的格式，都以数组的形式返回，数组的第一个元素即为key，下面分别介绍下各种类型:

#### KV

对于KV类型，因为一个Key对应一个Value，所以返回格式也是k,v的集合

	[[key1,val1],[key2,val2],[key3,val3],...,[keyn, valn]]
	
#### Hash

Hash类型与KV类型不同的地方，是一个Key可以对应多个Filed & Value。

	[[key1, [filed11, value11], [field12, value12]], [key2, [field21, value21], [field22, value22], [field23, value23]],...,[keyn, [fieldn1, valuen1], [fieldn2, valuen2],...,[fieldnn, valuenn]]]

#### List

List类型是一个Key后面跟n个元素，

	[[key1, item11, item12, item13], [key2, item21, item22], ... ,[keyn, itemn1, itemn2,...,itemnn]]
	
#### Set

Set类型格式与List类型比较像，

	[[key1, item11, item12, item13], [key2, item21, item22], ... ,[keyn, itemn1, itemn2,...,itemnn]]
	
#### ZSet
ZSet类型格式与Hash格式比较像

	[[key1, [item11, score11], [item12, score12]], [key2, [item21, score21], [item22, score22], [item23, score23]], ... , [keyn, [itemn1, scoren1], [itemn2, scoren2], ..., [itemnn, scorenn]]] 
	
### FullScan 格式解析

在清楚fullscan传输格式之后，我们就很容易对其进行解析，下面以[goredis](http://github.com/siddontang/goredis)为例，对各个类型进行解析,

	其中c类型为*goredis.PoolConn

#### KV

KV类型比较简单:
	
	ay, err := goredis.Value(c.Do("FULLSCAN", "default:test:", "KV", "count", 100))
	if err != nil {
		fmt.Println(err)
		return
	}
	cursor := ay[0].([]byte);
	fmt.Println("Cursor:", string(cursor))
	a, err := goredis.MultiBulk(ay[1], nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	for idx, _ := range a {
		item := a[idx].([]interface{})
		//length must be 2
		if len(item) != 2 {
			fmt.Println("length is not 2")
			return
		}
		key := item[0].([]byte)
		value := item[1].([]byte)
		fmt.Println("Key:", string(key), "; Value:", string(value))
	}
		

#### Hash

	ay, err := goredis.Value(c.Do("FULLSCAN", "default:test:", "HASH", "count", 100))
	if err != nil {
		fmt.Println(err)
		return
	}
	cursor := ay[0].([]byte);
	fmt.Println("Cursor:", string(cursor))
	a, err := goredis.MultiBulk(ay[1], nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	for idx, _ := range a {
		item := a[idx].([]interface{})
		length := len(item)
		key := item[0].([]byte)
		fmt.Println("Key:", string(key))
		for i := 1; i < length; i++ {
			fv := item[i].([]interface{})
			if len(fv) != 2 {
				fmt.Println("length is not 2")
				return
			}
			field := fv[0].([]byte)
			value := fv[1].([]byte)
			fmt.Println("		Field:", string(field), "; Value:", string(value))
		}
	}
	
#### List
	
	ay, err := goredis.Value(c.Do("FULLSCAN", "default:test:", "HASH", "count", 100))
	if err != nil {
		fmt.Println(err)
		return
	}
	cursor := ay[0].([]byte);
	fmt.Println("Cursor:", string(cursor))
	a, err := goredis.MultiBulk(ay[1], nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	for idx, _ := range a {
		item := a[idx].([]interface{})
		length := len(item)
		key := item[0].([]byte)
		fmt.Println("Key:", string(key)
		fmt.Println("		")
		for i := 1; i < length; i++ {
			value := item[i].([]byte)
			fmt.Print("  ", string(value))
		}
		fmt.Println("")
	}
#### Set
	
	
	ay, err := goredis.Value(c.Do("FULLSCAN", "default:test:", "HASH", "count", 100))
	if err != nil {
		fmt.Println(err)
		return
	}
	cursor := ay[0].([]byte);
	fmt.Println("Cursor:", string(cursor))
	a, err := goredis.MultiBulk(ay[1], nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	for idx, _ := range a {
		item := a[idx].([]interface{})
		length := len(item)
		key := item[0].([]byte)
		fmt.Println("Key:", string(key)
		fmt.Println("		")
		for i := 1; i < length; i++ {
			value := item[i].([]byte)
			fmt.Print("  ", string(value))
		}
		fmt.Println("")
	}
	
#### ZSet

	ay, err := goredis.Value(c.Do("FULLSCAN", "default:test:", "HASH", "count", 100))
	if err != nil {
		fmt.Println(err)
		return
	}
	cursor := ay[0].([]byte);
	fmt.Println("Cursor:", string(cursor))
	a, err := goredis.MultiBulk(ay[1], nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	for idx, _ := range a {
		item := a[idx].([]interface{})
		length := len(item)
		key := item[0].([]byte)
		fmt.Println("Key:", string(key)
		for i = 1; i < length; i++ {
			pair := item[i].([]interface{})
			if len(pair) != 2 {
				fmt.Println("length is not 2")
				return
			}
			zvalue := pair[0].([]byte)
			zscore := pair[1].([]byte)
			fmt.Println("		Value:", string(zvalue), "; Score:", string(zscore))
		}
	}