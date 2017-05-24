package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	sdk "github.com/absolute8511/go-zanredisdb"
)

var (
	flagSet   = flag.NewFlagSet("backup", flag.ExitOnError)
	dataDir   = flagSet.String("data_dir", "backup", "back up data dir name")
	lookup    = flagSet.String("lookup", "", "lookup list, split by ','")
	ns        = flagSet.String("ns", "", "namespace of backup")
	tableName = flagSet.String("table", "", "table name of backup")
	backType  = flagSet.String("type", "all", "which type you want to backup,split by ',' for multiple")
	pass      = flagSet.String("pass", "", "password of zankv")
)

var (
	backTypes []string
	wg        sync.WaitGroup
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

	if len(*tableName) <= 0 {
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

func backup(namespace, table, t string) {
	defer wg.Add(-1)
	fmt.Println(ns, table, t)

	lookupList := strings.Split(*lookup, ",")

	conf := &sdk.Conf{
		LookupList:   lookupList,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		TendInterval: 100,
		Namespace:    namespace,
		Password:     *pass,
	}
	client := sdk.NewZanRedisClient(conf)
	client.Start()
	defer client.Stop()

	ch := make(chan []byte)
	switch t {
	case "kv":
		go client.KVScanChannel(table, ch)
	case "hash":
		go client.HScanChannel(table, ch)
	case "set":
		go client.SScanChannel(table, ch)
	case "zset":
		go client.ZScanChannel(table, ch)
	case "list":
		go client.LScanChannel(table, ch)
	default:
		fmt.Println("Error: unsupport type")

	}

	for res := range ch {
		fmt.Println("###", string(res))
	}
}

func main() {

	flagSet.Parse(os.Args[1:])

	checkParameter()

	for _, t := range backTypes {
		wg.Add(1)
		go backup(*ns, *tableName, t)
	}

	wg.Wait()

	fmt.Println("backup finished.")
}
