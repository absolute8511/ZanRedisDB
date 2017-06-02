package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	sdk "github.com/absolute8511/go-zanredisdb"
)

var (
	flagSet = flag.NewFlagSet("restore", flag.ExitOnError)
	data    = flagSet.String("data", "data", "back up data dir name")
	lookup  = flagSet.String("lookup", "", "lookup list, split by ','")
	ns      = flagSet.String("ns", "", "namespace of restore")
	set     = flagSet.String("set", "", "table name of restore")
	pass    = flagSet.String("pass", "", "password of zankv")
)
var (
	oriNS string
)

const (
	MAGIC = "ZANKV"
	VER   = "0001"
)

func help() {
	fmt.Println("Usage:")
	fmt.Println("\t", os.Args[0], "[-data restore] -lookup lookuplist -ns namespace -table table_name ")
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

	if len(*data) <= 0 {
		fmt.Println("Error:must specify the back up data dir, default is 'data_dir'")
		help()
	}
}

func kvrestore(file *os.File, client *sdk.ZanRedisClient) {
	lenBuf := make([]byte, 4)
	var key []byte
	var value []byte
	var length uint32
	var n int
	var err error
	var total uint64
	var realKey []byte
	for {
		n, err = file.Read(lenBuf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Printf("read key length error. [err=%v]\n", err)
				break
			}
		}
		if n != 4 {
			fmt.Printf("read key length, length not equal.[n=%d]\n", n)
			break
		}

		length = binary.BigEndian.Uint32(lenBuf)

		key = key[:0]
		key = make([]byte, length)
		n, err = file.Read(key)
		if err != nil {
			fmt.Printf("read key error.[err=%v]\n", err)
			break
		}

		if uint32(n) != length {
			fmt.Printf("read key length not equal.[key=%s, n=%d, length=%d]\n", string(key), n, length)
			break
		}

		n, err = file.Read(lenBuf)
		if err != nil {
			fmt.Printf("read key length error. [err=%v]\n", err)
			break
		}
		if n != 4 {
			fmt.Printf("read key length, length not equal.[n=%d]\n", n)
			break
		}

		length = binary.BigEndian.Uint32(lenBuf)
		value = value[:0]
		value = make([]byte, length)
		n, err = file.Read(value)
		if err != nil {
			fmt.Printf("read value error.[key=%s, err=%v]\n", string(key), err)
			break
		}

		if uint32(n) != length {
			fmt.Printf("read value length not equal.[key=%s, value=%v, n=%d, length=%d]\n", string(key), value, n, length)
			break
		}

		realKey = realKey[:0]
		realKey = append(realKey, []byte(oriNS)...)
		realKey = append(realKey, []byte(":")...)
		realKey = append(realKey, key...)

		val, err := client.KVGet(*set, realKey)
		if err == nil {
			fmt.Printf("key is already exsits in kv.[key=%s, realKey=%s, val=%v]\n", string(key), realKey, val)
			continue
		}

		err = client.KVSet(*set, realKey, val)
		if err != nil {
			fmt.Printf("restore error. [key=%s, realKey=%s, val=%v, err=%v]\n", key, realKey, val, err)
			continue
		}
		total++
	}
	fmt.Printf("restore finished. [total=%d]\n", total)
}

func hrestore(file *os.File, client *sdk.ZanRedisClient) {

}

func srestore(file *os.File, client *sdk.ZanRedisClient) {

}

func zrestore(file *os.File, client *sdk.ZanRedisClient) {

}

func lrestore(file *os.File, client *sdk.ZanRedisClient) {

}

func restore() {

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

	file, err := os.Open(*data)
	if err != nil {
		fmt.Printf("open file error. [path=%s, err=%v]\n", *data, err)
		return
	}

	defer file.Close()

	f, err := file.Stat()
	if err != nil {
		fmt.Printf("get file stat error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if f.IsDir() {
		fmt.Printf("%s is a dir not a file.", *data)
		return
	}

	magic := make([]byte, 5)

	n, err := file.Read(magic)
	if err != nil {
		fmt.Printf("read magic error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != len(MAGIC) {
		fmt.Printf("read magic length not equal. [n=%d]\n", n)
		return
	}

	if string(magic) != MAGIC {
		fmt.Printf("magic is not right.[magic=%s]\n", string(magic))
		return
	}

	version := make([]byte, 4)
	n, err = file.Read(version)
	if err != nil {
		fmt.Printf("read version error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != len(VER) {
		fmt.Printf("read version length not equal. [n=%d]\n", n)
		return
	}

	if string(version) != VER {
		fmt.Printf("version is not right.[ver=%s]\n", string(version))
		return
	}

	lenBuf := make([]byte, 4)
	n, err = file.Read(lenBuf)
	if err != nil {
		fmt.Printf("read origin namespace's len error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != 4 {
		fmt.Printf("read origin namespace's len length not equal. [path=%s, n=%d]\n", *data, n)
		return
	}

	length := binary.BigEndian.Uint32(lenBuf)

	oriNSBuf := make([]byte, length)
	n, err = file.Read(oriNSBuf)
	if err != nil {
		fmt.Printf("read origin namespace error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if uint32(n) != length {
		fmt.Printf("read origin namespace length not equal. [path=%s, ns=%s, n=%d, len=%d]\n", *data, string(oriNSBuf), n, length)
		return
	}

	oriNS = string(oriNSBuf)

	tp := make([]byte, 1)

	n, err = file.Read(tp)
	if err != nil {
		fmt.Printf("read type error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != 1 {
		fmt.Printf("read type length not equal. [n=%d]\n", n)
		return
	}

	t := tp[0]

	switch t {
	case 0:
		kvrestore(file, client)
	case 1:
		hrestore(file, client)
	case 2:
		srestore(file, client)
	case 3:
		zrestore(file, client)
	case 4:
		lrestore(file, client)
	default:
		fmt.Printf("unsupport type. [type=%d]\n", t)
	}
}

func main() {

	flagSet.Parse(os.Args[1:])

	checkParameter()

	restore()
	fmt.Println("restore finished.")
}
