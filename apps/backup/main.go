package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	sdk "github.com/absolute8511/go-zanredisdb"
)

var (
	flagSet  = flag.NewFlagSet("backup", flag.ExitOnError)
	dataDir  = flagSet.String("data_dir", "data", "back up data dir name")
	lookup   = flagSet.String("lookup", "", "lookup list, split by ','")
	ns       = flagSet.String("ns", "", "namespace of backup")
	set      = flagSet.String("set", "", "table name of backup")
	backType = flagSet.String("type", "all", "which type you want to backup,split by ',' for multiple")
	pass     = flagSet.String("pass", "", "password of zankv")
)

var (
	backTypes []string
	wg        sync.WaitGroup
)

const (
	MAGIC = "ZANKV"
	VER   = "0001"
)

func help() {
	fmt.Println("Usage:")
	fmt.Println("\t", os.Args[0], "[-data_dir backup] -lookup lookuplist -ns namespace -table table_name -type all|kv[,hash,set,zset,list]")
	os.Exit(0)
}

func checkParameter() {
	if len(*lookup) <= 0 {
		fmt.Println("Error:must specify the lookup list")
		help()
	}

	if len(*ns) <= 0 {
		fmt.Println("Error:must specify the namespace")
		help()
	}

	if len(*set) <= 0 {
		fmt.Println("Error:must specify the table name")
		help()
	}

	if len(*dataDir) <= 0 {
		fmt.Println("Error:must specify the back up data dir, default is 'data_dir'")
		help()
	}

	if len(*backType) <= 0 {
		fmt.Println("Error:must specify the backup type")
		help()
	}

	types := strings.Split(*backType, ",")
	hasAll := false
	hasOther := false

	for _, t := range types {
		t = strings.ToLower(strings.TrimSpace(t))
		switch t {
		case "all":
			hasAll = true
		case "kv", "hash", "set", "zset", "list":
			backTypes = append(backTypes, t)
			hasOther = true
		default:
			fmt.Println("Error:unsupport type")
			help()
		}
	}

	if hasAll && hasOther {
		fmt.Println("Error:all is conflict with other type")
		help()
	}

	if hasAll {
		backTypes = append(backTypes, []string{"kv", "hash", "set", "zset", "list"}...)
	}
}

func kvbackup(ch chan []byte, file *os.File, client *sdk.ZanRedisClient) {
	tp := []byte{0}
	n, err := file.Write(tp)
	if err != nil {
		fmt.Printf("write kv type error. [err=%v]\n", err)
		return
	}
	if n != 1 {
		fmt.Printf("write kv type length error. [n=%d]\n", n)
		return
	}

	lenBuf := make([]byte, 4)
	for res := range ch {
		keyLen := len(res)
		binary.BigEndian.PutUint32(lenBuf, uint32(keyLen))
		n, err := file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write key's len error.[ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		if n != 4 {
			fmt.Printf("write key's len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		n, err = file.Write(res)
		if err != nil {
			fmt.Printf("write key error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != keyLen {
			fmt.Printf("write key length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		splits := bytes.Split(res, []byte(":"))
		if len(splits) != 2 {
			fmt.Printf("key error. [ns=%s, key=%s]\n", *ns, string(res))
			break
		}
		val, err := client.KVGet(*set, splits[1])
		if err != nil {
			fmt.Printf("get value error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		valLen := len(val)
		binary.BigEndian.PutUint32(lenBuf, uint32(valLen))
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write val's len error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), val, err)
			break
		}
		if n != 4 {
			fmt.Printf("write val's len length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), val, n)
			break
		}

		n, err = file.Write(val)
		if err != nil {
			fmt.Printf("write val error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), val, err)
			break
		}
		if n != valLen {
			fmt.Printf("write val length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), val, n)
			break
		}
	}
}

func hbackup(ch chan []byte, file *os.File, client *sdk.ZanRedisClient) {
	lenBuf := make([]byte, 4)
	var itemNum uint32
	for res := range ch {
		keyLen := len(res)
		binary.BigEndian.PutUint32(lenBuf, uint32(keyLen))
		n, err := file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write key's len error.[ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		if n != 4 {
			fmt.Printf("write key's len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		n, err = file.Write(res)
		if err != nil {
			fmt.Printf("write key error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != keyLen {
			fmt.Printf("write key length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		splits := bytes.Split(res, []byte(":"))
		if len(splits) != 2 {
			fmt.Printf("key error. [ns=%s, key=%s]\n", *ns, string(res))
			break
		}

		val, err := client.HGetAll(*set, splits[1])
		if err != nil {
			fmt.Printf("hget all error, [ns=%s, key=%s, err=%v", *ns, string(res), err)
			break
		}

		itemNum = uint32(len(val))
		if itemNum%2 != 0 {
			fmt.Printf("hget return value number error. [ns=%s, key=%s, itemnum=%d]\n", *ns, string(res), itemNum)
			break
		}
		itemNum = itemNum / 2
		binary.BigEndian.PutUint32(lenBuf, uint32(itemNum))
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write hash number error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != 4 {
			fmt.Printf("write hash number length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		for _, c := range val {
			item := c.([]byte)
			itemLen := len(item)
			binary.BigEndian.PutUint32(lenBuf, uint32(itemLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write val's len error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
				break
			}
			if n != 4 {
				fmt.Printf("write val's len length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
				break
			}
			n, err = file.Write(item)
			if err != nil {
				fmt.Printf("write val error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
				break
			}
			if n != itemLen {
				fmt.Printf("write val length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
				break
			}
		}
	}
}

func sbackup(ch chan []byte, file *os.File, client *sdk.ZanRedisClient) {
	lenBuf := make([]byte, 4)
	var itemNum uint32
	for res := range ch {
		keyLen := len(res)
		binary.BigEndian.PutUint32(lenBuf, uint32(keyLen))
		n, err := file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write key's len error.[ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		if n != 4 {
			fmt.Printf("write key's len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		n, err = file.Write(res)
		if err != nil {
			fmt.Printf("write key error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != keyLen {
			fmt.Printf("write key length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		splits := bytes.Split(res, []byte(":"))
		if len(splits) != 2 {
			fmt.Printf("key error. [ns=%s, key=%s]\n", *ns, string(res))
			break
		}

		val, err := client.SMembers(*set, splits[1])
		if err != nil {
			fmt.Printf("get value error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		itemNum = uint32(len(val))
		binary.BigEndian.PutUint32(lenBuf, uint32(itemNum))
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write smembers len error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != 4 {
			fmt.Printf("write smembers len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		for _, c := range val {
			item := c.([]byte)
			itemLen := len(item)
			binary.BigEndian.PutUint32(lenBuf, uint32(itemLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write val's len error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
				break
			}
			if n != 4 {
				fmt.Printf("write val's len length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
				break
			}
			n, err = file.Write(item)
			if err != nil {
				fmt.Printf("write val error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
				break
			}
			if n != itemLen {
				fmt.Printf("write val length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
				break
			}
		}
	}
}

func zbackup(ch chan []byte, file *os.File, client *sdk.ZanRedisClient) {
	lenBuf := make([]byte, 4)

	var itemNum uint32
	for res := range ch {
		keyLen := len(res)
		binary.BigEndian.PutUint32(lenBuf, uint32(keyLen))
		n, err := file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write key's len error.[ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		if n != 4 {
			fmt.Printf("write key's len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		n, err = file.Write(res)
		if err != nil {
			fmt.Printf("write key error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != keyLen {
			fmt.Printf("write key length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		splits := bytes.Split(res, []byte(":"))
		if len(splits) != 2 {
			fmt.Printf("key error. [ns=%s, key=%s]\n", *ns, string(res))
			break
		}

		//record the len pos
		// and write 0 into it
		// we will complete later
		lenIdx, err := file.Seek(0, 2)
		if err != nil {
			fmt.Printf("seek error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		itemNum = 0
		binary.BigEndian.PutUint32(lenBuf, itemNum)
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write lrange len error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != 4 {
			fmt.Printf("write lrange len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		LenPerReq := 100
		start := 0
		for {
			val, err := client.ZRange(*set, splits[1], start, start+LenPerReq, true)
			if err != nil {
				fmt.Printf("get zrange error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
				return
			}

			for _, c := range val {
				item := c.([]byte)
				valLen := len(item)
				binary.BigEndian.PutUint32(lenBuf, uint32(valLen))
				n, err = file.Write(lenBuf)
				if err != nil {
					fmt.Printf("write val's len error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
					break
				}
				if n != 4 {
					fmt.Printf("write val's len length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
					break
				}
				n, err = file.Write(item)
				if err != nil {
					fmt.Printf("write val error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
					break
				}
				if n != valLen {
					fmt.Printf("write val length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
					break
				}
				itemNum++
			}

			if len(val) < LenPerReq {
				break
			} else {
				start += LenPerReq + 1
			}
		}
		//jump to the reallen write pos
		_, err = file.Seek(lenIdx, 0)
		//jump to the current of file
		defer file.Seek(0, 2)
		if err != nil {
			fmt.Printf("file seek error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(itemNum))
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write lrange len error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != 4 {
			fmt.Printf("write lrange len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}
	}
}

func lbackup(ch chan []byte, file *os.File, client *sdk.ZanRedisClient) {
	lenBuf := make([]byte, 4)
	var itemNum uint32
	for res := range ch {
		keyLen := len(res)
		binary.BigEndian.PutUint32(lenBuf, uint32(keyLen))
		n, err := file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write key's len error.[ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		if n != 4 {
			fmt.Printf("write key's len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		n, err = file.Write(res)
		if err != nil {
			fmt.Printf("write key error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != keyLen {
			fmt.Printf("write key length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}

		splits := bytes.Split(res, []byte(":"))
		if len(splits) != 2 {
			fmt.Printf("key error. [ns=%s, key=%s]\n", *ns, string(res))
			break
		}
		//record the len pos
		// and write 0 into it
		// we will complete later
		lenIdx, err := file.Seek(0, 2)
		if err != nil {
			fmt.Printf("seek error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}

		itemNum = 0
		binary.BigEndian.PutUint32(lenBuf, itemNum)
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write lrange len error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != 4 {
			fmt.Printf("write lrange len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}
		LenPerReq := 100
		start := 0
		for {
			val, err := client.LRange(*set, splits[1], start, start+LenPerReq)
			if err != nil {
				fmt.Printf("get value error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
				return
			}

			for _, c := range val {

				item := c.([]byte)
				valLen := len(item)
				binary.BigEndian.PutUint32(lenBuf, uint32(valLen))
				n, err = file.Write(lenBuf)
				if err != nil {
					fmt.Printf("write val's len error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
					break
				}
				if n != 4 {
					fmt.Printf("write val's len length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
					break
				}
				n, err = file.Write(item)
				if err != nil {
					fmt.Printf("write val error. [ns=%s, key=%s, val=%v, err=%v]\n", *ns, string(res), item, err)
					break
				}
				if n != valLen {
					fmt.Printf("write val length error. [ns=%s, key=%s, val=%v, len=%d]\n", *ns, string(res), item, n)
					break
				}

				itemNum++
			}

			if len(val) < LenPerReq {
				break
			} else {
				start += LenPerReq + 1
			}
		}
		//jump to the reallen write pos
		_, err = file.Seek(lenIdx, 0)
		//jump to the current of file
		defer file.Seek(0, 2)
		if err != nil {
			fmt.Printf("file seek error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(itemNum))
		n, err = file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write lrange len error. [ns=%s, key=%s, err=%v]\n", *ns, string(res), err)
			break
		}
		if n != 4 {
			fmt.Printf("write lrange len length error. [ns=%s, key=%s, len=%d]\n", *ns, string(res), n)
			break
		}
	}
}

func backup(t string) {
	defer wg.Add(-1)

	lookupList := strings.Split(*lookup, ",")

	conf := &sdk.Conf{
		LookupList:   lookupList,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		TendInterval: 100,
		Namespace:    *ns,
		Password:     *pass,
	}
	client := sdk.NewZanRedisClient(conf)
	client.Start()
	defer client.Stop()

	ch := make(chan []byte)
	go client.AdvScanChannel(t, *set, ch)
	path := fmt.Sprintf("%s/%s:%s:%s:%s.db", *dataDir, t, time.Now().Format("2006-01-02"), *ns, *set)
	var file *os.File
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(path)
			if err != nil {
				fmt.Printf("create file error. [path=%s, err=%v]\n", path, err)
				return
			}
		}
	} else {
		fmt.Printf("file is already exist [path=%s]\n", path)
		return
	}

	defer file.Close()
	n, err := file.WriteString(MAGIC)
	if err != nil {
		fmt.Println("write magic error, ", err)
		return
	}

	if n != len(MAGIC) {
		fmt.Println("write magic length not equal, ", len(MAGIC))
		return
	}

	n, err = file.WriteString(VER)
	if err != nil {
		fmt.Println("write version error, ", err)
		return
	}
	if n != len(VER) {
		fmt.Println("write version length not equal, ", len(VER))
		return
	}

	length := uint32(len(*ns))
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, length)
	n, err = file.Write(lenBuf)
	if err != nil {
		fmt.Printf("write namespace's len  error. [ns=%s, err=%v]\n", *ns, err)
		return
	}
	if n != 4 {
		fmt.Printf("write namespace's len length not equal. [ns=%s, n=%d]\n", *ns, n)
		return
	}

	n, err = file.WriteString(*ns)
	if err != nil {
		fmt.Printf("write namespace error. [ns=%s, err=%v]\n", *ns, n)
		return
	}
	if uint32(n) != length {
		fmt.Printf("write namespace length not equal. [ns=%s, n=%d]\n", *ns, n)
	}

	switch t {
	case "kv":
		kvbackup(ch, file, client)
	case "hash":
		hbackup(ch, file, client)
	case "set":
		sbackup(ch, file, client)
	case "zset":
		zbackup(ch, file, client)
	case "list":
		lbackup(ch, file, client)
	}
}

func main() {

	flagSet.Parse(os.Args[1:])

	checkParameter()

	f, err := os.Stat(*dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(*dataDir, 0777)
			if err != nil {
				fmt.Println("create path error. ", err)
				return
			}
			f, err = os.Stat(*dataDir)
		} else {
			fmt.Println("get file stat error. ", err)
			return
		}
	}
	if !f.IsDir() {
		fmt.Println("file is already exist and is not dir. ", *dataDir)
		return
	}

	for _, t := range backTypes {
		wg.Add(1)
		go func(t string) {
			backup(t)
		}(t)
	}

	wg.Wait()
	fmt.Println("backup finished.")
}
