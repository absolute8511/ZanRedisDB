package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/absolute8511/redigo/redis"
	sdk "github.com/youzan/go-zanredisdb"
)

var (
	flagSet = flag.NewFlagSet("restore", flag.ExitOnError)
	data    = flagSet.String("data", "data", "back up data dir name")
	lookup  = flagSet.String("lookup", "", "lookup list, split by ','")
	ns      = flagSet.String("ns", "", "namespace of restore")
	table   = flagSet.String("table", "", "table name of restore")
	qps     = flagSet.Int("qps", 1000, "qps")
	pass    = flagSet.String("pass", "", "password of zankv")
)
var (
	oriNS    string
	oriTable string
	tm       time.Duration
)

const (
	MAGIC = "ZANKV"
	VER   = "0001"
)

func help() {
	log.Println("Usage:")
	log.Println("\t", os.Args[0], "[-data restore] -lookup lookuplist -ns namespace -table table_name [-qps 1000]")
	os.Exit(0)
}

func checkParameter() {
	if len(*lookup) <= 0 {
		log.Println("Error:must specify the lookup list")
		help()
	}

	if len(*data) <= 0 {
		log.Println("Error:must specify the back up data dir, default is 'data_dir'")
		help()
	}
}

func readLen(file *os.File, lenBuf [4]byte) (int, error) {
	n, err := file.Read(lenBuf[:])
	if err != nil {
		if err == io.EOF {
			return 0, err
		} else {
			log.Printf("read key length error. [err=%v]\n", err)
			return 0, err
		}
	}
	if n != len(lenBuf) {
		log.Printf("read key length, length not equal.[n=%d]\n", n)
		return 0, errors.New("read length not match")
	}
	length := int(binary.BigEndian.Uint32(lenBuf[:]))
	return length, nil
}

func readLenAndBody(file *os.File, lenBuf [4]byte, key []byte) ([]byte, error) {
	length, err := readLen(file, lenBuf)
	if err != nil {
		return nil, err
	}

	if length <= len(key) {
		key = key[:length]
	} else {
		key = make([]byte, length)
	}
	n, err := file.Read(key)
	if err != nil {
		log.Printf("read key error.[err=%v]\n", err)
		return key, err
	}

	if n != length {
		log.Printf("read key length not equal.[key=%s, n=%d, length=%d]\n", string(key), n, length)
		return key, err
	}
	return key, nil
}

func kvrestore(file *os.File, client *sdk.ZanRedisClient) {
	var lenBuf [4]byte
	var key []byte
	var value []byte
	var err error
	var total uint64
	var existed uint64
	for {
		key, err = readLenAndBody(file, lenBuf, key)
		if err != nil {
			log.Printf("read key error. [err=%v]\n", err)
			break
		}

		value, err = readLenAndBody(file, lenBuf, value)
		if err != nil {
			log.Printf("read key error. [err=%v]\n", err)
			break
		}

		val, err := client.KVSetNX(oriTable, key, value)
		if err != nil {
			log.Printf("restore error. [key=%s, val=%v, err=%v]\n", key, val, err)
			break
		}
		if val == 0 {
			//log.Printf("key is already exsits in kv.[key=%s]\n", string(key))
			existed++
		}
		total++
		if total%1000 == 0 {
			if tm > 0 {
				time.Sleep(tm * 100)
			}
			fmt.Print(".")
		}
		if total%10000 == 0 {
			fmt.Printf("%d(%d)", total, existed)
		}
	}
	log.Printf("restore finished. [total=%d, existed=%d]\n", total, existed)
}

func hrestore(file *os.File, client *sdk.ZanRedisClient) {
	var lenBuf [4]byte
	var key []byte
	var field []byte
	var value []byte
	var fieldNum int
	var err error
	var total uint64
	var existed uint64
	for {
		key, err = readLenAndBody(file, lenBuf, key)
		if err != nil {
			log.Printf("read key error. [err=%v]\n", err)
			break
		}

		fieldNum, err = readLen(file, lenBuf)
		if err != nil {
			log.Printf("read error. [err=%v]\n", err)
			break
		}

		for i := 0; i < fieldNum; i++ {
			field, err = readLenAndBody(file, lenBuf, field)
			if err != nil {
				log.Printf("read error. [err=%v]\n", err)
				break
			}
			value, err = readLenAndBody(file, lenBuf, value)
			if err != nil {
				log.Printf("read error. [err=%v]\n", err)
				break
			}
			pk := sdk.NewPKey(oriNS, oriTable, key)
			val, err := redis.Int(client.DoRedis("hsetnx", pk.ShardingKey(), true, pk.RawKey, field, value))
			if err != nil {
				log.Printf("restore error. [key=%s, val=%v, err=%v]\n", key, val, err)
				break
			}
			if val == 0 {
				existed++
			}
		}

		total++
		if total%1000 == 0 {
			if tm > 0 {
				time.Sleep(tm * 100)
			}
			fmt.Print(".")
		}
		if total%10000 == 0 {
			fmt.Printf("%d(%d)", total, existed)
		}
	}
	log.Printf("restore finished. [total=%d, existed=%d]\n", total, existed)
}

func srestore(file *os.File, client *sdk.ZanRedisClient) {

}

func zrestore(file *os.File, client *sdk.ZanRedisClient) {

}

func lrestore(file *os.File, client *sdk.ZanRedisClient) {

}

func restore() {
	file, err := os.Open(*data)
	if err != nil {
		log.Printf("open file error. [path=%s, err=%v]\n", *data, err)
		return
	}

	defer file.Close()

	f, err := file.Stat()
	if err != nil {
		log.Printf("get file stat error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if f.IsDir() {
		log.Printf("%s is a dir not a file.", *data)
		return
	}

	magic := make([]byte, 5)

	n, err := file.Read(magic)
	if err != nil {
		log.Printf("read magic error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != len(MAGIC) {
		log.Printf("read magic length not equal. [n=%d]\n", n)
		return
	}

	if string(magic) != MAGIC {
		log.Printf("magic is not right.[magic=%s]\n", string(magic))
		return
	}

	version := make([]byte, 4)
	n, err = file.Read(version)
	if err != nil {
		log.Printf("read version error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != len(VER) {
		log.Printf("read version length not equal. [n=%d]\n", n)
		return
	}

	if string(version) != VER {
		log.Printf("version is not right.[ver=%s]\n", string(version))
		return
	}

	lenBuf := make([]byte, 4)
	n, err = file.Read(lenBuf)
	if err != nil {
		log.Printf("read origin namespace's len error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != 4 {
		log.Printf("read origin namespace's len length not equal. [path=%s, n=%d]\n", *data, n)
		return
	}

	length := binary.BigEndian.Uint32(lenBuf)

	oriNSBuf := make([]byte, length)
	n, err = file.Read(oriNSBuf)
	if err != nil {
		log.Printf("read origin namespace error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if uint32(n) != length {
		log.Printf("read origin namespace length not equal. [path=%s, ns=%s, n=%d, len=%d]\n", *data, string(oriNSBuf), n, length)
		return
	}
	if len(*ns) > 0 {
		oriNS = *ns
	} else {
		oriNS = string(oriNSBuf)
	}

	n, err = file.Read(lenBuf)
	if err != nil {
		log.Printf("read origin table's len error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != 4 {
		log.Printf("read origin table's len length not equal. [path=%s, n=%d]\n", *data, n)
		return
	}

	length = binary.BigEndian.Uint32(lenBuf)

	oriTableBuf := make([]byte, length)
	n, err = file.Read(oriTableBuf)
	if err != nil {
		log.Printf("read origin table error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if uint32(n) != length {
		log.Printf("read origin table length not equal. [path=%s, table=%s, n=%d, len=%d]\n", *data, string(oriTableBuf), n, length)
		return
	}
	if len(*table) > 0 {
		oriTable = *table
	} else {
		oriTable = string(oriTableBuf)
	}

	tp := make([]byte, 1)

	n, err = file.Read(tp)
	if err != nil {
		log.Printf("read type error. [path=%s, err=%v]\n", *data, err)
		return
	}

	if n != 1 {
		log.Printf("read type length not equal. [n=%d]\n", n)
		return
	}

	t := tp[0]

	lookupList := strings.Split(*lookup, ",")

	conf := &sdk.Conf{
		LookupList:   lookupList,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		TendInterval: 100,
		Namespace:    oriNS,
		Password:     *pass,
	}
	client := sdk.NewZanRedisClient(conf)
	client.Start()
	defer client.Stop()

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
		log.Printf("unsupport type. [type=%d]\n", t)
	}
}

func main() {

	flagSet.Parse(os.Args[1:])

	checkParameter()

	tm = time.Duration(1000000 / *qps) * time.Microsecond

	restore()
	log.Println("restore finished.")
}
