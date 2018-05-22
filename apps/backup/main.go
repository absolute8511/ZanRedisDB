package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	sdk "github.com/youzan/go-zanredisdb"
)

var (
	flagSet  = flag.NewFlagSet("backup", flag.ExitOnError)
	dataDir  = flagSet.String("data_dir", "data", "back up data dir name")
	lookup   = flagSet.String("lookup", "", "lookup list, split by ','")
	ns       = flagSet.String("ns", "", "namespace of backup")
	table    = flagSet.String("table", "", "table name of backup")
	backType = flagSet.String("type", "all", "which type you want to backup,split by ',' for multiple")
	qps      = flagSet.Int("qps", 1000, "qps")
	pass     = flagSet.String("pass", "", "password of zankv")
)

var (
	backTypes []string
	wg        sync.WaitGroup
	tm        time.Duration
)

var (
	errWriteLen = errors.New("write item length error")
)

const (
	MAGIC = "ZANKV"
	VER   = "0001"
)

func help() {
	fmt.Println("Usage:")
	fmt.Println("\t", os.Args[0], "[-data_dir backup] -lookup lookuplist -ns namespace -table table_name -type all|kv[,hash,set,zset,list] [-qps 100] ")
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

	if len(*table) <= 0 {
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

type writeFunc func(key []byte, item []interface{}, file *os.File) error

func backupCommon(tp []byte, ch chan interface{}, file *os.File, f writeFunc) {
	n, err := file.Write(tp)
	if err != nil {
		fmt.Printf("write kv type error. [err=%v]\n", err)
		return
	}
	if n != 1 {
		fmt.Printf("write kv type length error. [n=%d]\n", n)
		return
	}

	totalCnt := int64(0)
	start := time.Now()
	defer func() {
		fmt.Printf("total backuped %v, cost time: %v\n", totalCnt, time.Since(start))
	}()
	for c := range ch {
		v := c.([]interface{})
		length := len(v)
		for i := 0; i < length; i++ {
			item := v[i].([]interface{})
			lenBuf := make([]byte, 4)
			key := item[0].([]byte)
			keyLen := len(key)
			binary.BigEndian.PutUint32(lenBuf, uint32(keyLen))
			n, err := file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write key's len error.[ns=%s, table=%s, key=%s, err=%v]\n", *ns, *table, string(key), err)
				return
			}

			if n != 4 {
				fmt.Printf("write key's len length error. [ns=%s, table=%s, key=%s, len=%d]\n", *ns, *table, string(key), n)
				return
			}

			n, err = file.Write(key)
			if err != nil {
				fmt.Printf("write key error. [ns=%s, table=%s, key=%s, err=%v]\n", *ns, *table, string(key), err)
				return
			}
			if n != keyLen {
				fmt.Printf("write key length error. [ns=%s, table=%s, key=%s, len=%d]\n", *ns, *table, string(key), n)
				return
			}
			err = f(key, item[1:], file)
			if err != nil {
				break
			}
			totalCnt++
			if totalCnt%100 == 0 {
				time.Sleep(tm * 100)
			}
			if totalCnt%10000 == 0 {
				fmt.Printf("current backuped %v\n", totalCnt)
			}
		}
	}
}

func kvbackup(ch chan interface{}, file *os.File, client *sdk.ZanRedisClient) {
	tp := []byte{0}
	backupCommon(tp, ch, file, func(key []byte, item []interface{}, file *os.File) error {
		lenBuf := make([]byte, 4)
		value := item[0].([]byte)
		valLen := len(value)
		binary.BigEndian.PutUint32(lenBuf, uint32(valLen))
		n, err := file.Write(lenBuf)
		if err != nil {
			fmt.Printf("write val's len error. [ns=%s, table=%s, key=%s, val=%v, err=%v]\n", *ns, *table, string(key), value, err)
			return err
		}
		if n != 4 {
			fmt.Printf("write val's len length error. [ns=%s, table=%s, key=%s, val=%v, len=%d]\n", *ns, *table, string(key), value, n)
			return errWriteLen
		}

		n, err = file.Write(value)
		if err != nil {
			fmt.Printf("write val error. [ns=%s, table=%s, key=%s, val=%v, err=%v]\n", *ns, *table, string(key), value, err)
			return err
		}
		if n != valLen {
			fmt.Printf("write val length error. [ns=%s, table=%s, key=%s, val=%v, len=%d]\n", *ns, *table, string(key), value, n)
			return errWriteLen
		}
		return nil
	})
}

func hbackup(ch chan interface{}, file *os.File, client *sdk.ZanRedisClient) {
	tp := []byte{1}
	backupCommon(tp, ch, file, func(key []byte, item []interface{}, file *os.File) error {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(item)))
		n, err := file.Write(lenBuf)

		if err != nil {
			fmt.Printf("write field-value count  error.[ns=%s, table=%s, key=%s, err=%v]\n", *ns, *table, string(key), err)
			return err
		}

		if n != 4 {
			fmt.Printf("write field-value count length error. [ns=%s, table=%s, key=%s, count=%d, len=%d]\n",
				*ns, *table, string(key), len(item), n)
			return errWriteLen
		}

		for i := 0; i < len(item); i++ {
			fv := item[i].([]interface{})
			field := fv[0].([]byte)
			fieldLen := len(field)
			binary.BigEndian.PutUint32(lenBuf, uint32(fieldLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write field's len error. [ns=%s, table=%s, key=%s, field=%v, err=%v]\n", *ns, *table, string(key), field, err)
				return err
			}
			if n != 4 {
				fmt.Printf("write field's len length error. [ns=%s, table=%s, key=%s, field=%v, len=%d]\n", *ns, *table, string(key), field, n)
				return errWriteLen
			}

			n, err = file.Write(field)
			if err != nil {
				fmt.Printf("write field error. [ns=%s, table=%s, key=%s, field=%v, err=%v]\n", *ns, *table, string(key), field, err)
				return err
			}
			if n != fieldLen {
				fmt.Printf("write field length error. [ns=%s, table=%s, key=%s, field=%v, len=%d]\n", *ns, *table, string(key), field, n)
				return errWriteLen
			}

			value := fv[1].([]byte)
			valLen := len(value)
			binary.BigEndian.PutUint32(lenBuf, uint32(valLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write val's len error. [ns=%s, table=%s, key=%s, val=%v, err=%v]\n", *ns, *table, string(key), value, err)
				return err
			}
			if n != 4 {
				fmt.Printf("write val's len length error. [ns=%s, table=%s, key=%s, val=%v, len=%d]\n", *ns, *table, string(key), value, n)
				return errWriteLen
			}

			n, err = file.Write(value)
			if err != nil {
				fmt.Printf("write val error. [ns=%s, table=%s, key=%s, val=%v, err=%v]\n", *ns, *table, string(key), value, err)
				return err
			}
			if n != valLen {
				fmt.Printf("write val length error. [ns=%s, table=%s, key=%s, val=%v, len=%d]\n", *ns, *table, string(key), value, n)
				return errWriteLen
			}
		}

		return nil
	})

}

func lbackup(ch chan interface{}, file *os.File, client *sdk.ZanRedisClient) {
	tp := []byte{2}
	backupCommon(tp, ch, file, func(key []byte, item []interface{}, file *os.File) error {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(item)))
		n, err := file.Write(lenBuf)

		if err != nil {
			fmt.Printf("write list count  error.[ns=%s, table=%s, key=%s, err=%v]\n", *ns, *table, string(key), err)
			return err
		}

		if n != 4 {
			fmt.Printf("write list count length error. [ns=%s, table=%s, key=%s, count=%d, len=%d]\n",
				*ns, *table, string(key), len(item), n)
			return errWriteLen
		}

		for i := 0; i < len(item); i++ {
			value := item[i].([]byte)
			valLen := len(value)
			binary.BigEndian.PutUint32(lenBuf, uint32(valLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write val's len error. [ns=%s, table=%s, key=%s, val=%v, err=%v]\n", *ns, *table, string(key), value, err)
				return err
			}
			if n != 4 {
				fmt.Printf("write val's len length error. [ns=%s, table=%s, key=%s, val=%v, len=%d]\n", *ns, *table, string(key), value, n)
				return errWriteLen
			}

			n, err = file.Write(value)
			if err != nil {
				fmt.Printf("write val error. [ns=%s, table=%s, key=%s, val=%v, err=%v]\n", *ns, *table, string(key), value, err)
				return err
			}
			if n != valLen {
				fmt.Printf("write val length error. [ns=%s, table=%s, key=%s, val=%v, len=%d]\n", *ns, *table, string(key), value, n)
				return errWriteLen
			}
		}

		return nil
	})

}

func sbackup(ch chan interface{}, file *os.File, client *sdk.ZanRedisClient) {
	tp := []byte{3}
	backupCommon(tp, ch, file, func(key []byte, item []interface{}, file *os.File) error {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(item)))
		n, err := file.Write(lenBuf)

		if err != nil {
			fmt.Printf("write member count  error.[ns=%s, table=%s, key=%s, err=%v]\n", *ns, *table, string(key), err)
			return err
		}

		if n != 4 {
			fmt.Printf("write member count length error. [ns=%s, table=%s, key=%s, count=%d, len=%d]\n", *ns, *table, string(key), len(item), n)
			return errWriteLen
		}

		for i := 0; i < len(item); i++ {
			member := item[i].([]byte)
			memberLen := len(member)
			binary.BigEndian.PutUint32(lenBuf, uint32(memberLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write member's len error. [ns=%s, table=%s, key=%s, member=%v, err=%v]\n", *ns, *table, string(key), member, err)
				return err
			}
			if n != 4 {
				fmt.Printf("write member's len length error. [ns=%s, table=%s, key=%s, member=%v, len=%d]\n", *ns, *table, string(key), member, n)
				return errWriteLen
			}

			n, err = file.Write(member)
			if err != nil {
				fmt.Printf("write member error. [ns=%s, table=%s, key=%s, member=%v, err=%v]\n", *ns, *table, string(key), member, err)
				return err
			}
			if n != memberLen {
				fmt.Printf("write member length error. [ns=%s, table=%s, key=%s, member=%v, len=%d]\n", *ns, *table, string(key), member, n)
				return errWriteLen
			}
		}

		return nil
	})

}

func zbackup(ch chan interface{}, file *os.File, client *sdk.ZanRedisClient) {
	tp := []byte{4}
	backupCommon(tp, ch, file, func(key []byte, item []interface{}, file *os.File) error {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(item)))
		n, err := file.Write(lenBuf)

		if err != nil {
			fmt.Printf("write member-score count  error.[ns=%s, table=%s, key=%s, err=%v]\n", *ns, *table, string(key), err)
			return err
		}

		if n != 4 {
			fmt.Printf("write member-score count length error. [ns=%s, table=%s, key=%s, count=%d, len=%d]\n",
				*ns, *table, string(key), len(item), n)
			return errWriteLen
		}

		for i := 0; i < len(item); i++ {
			ms := item[i].([]interface{})
			member := ms[0].([]byte)
			memberLen := len(member)
			binary.BigEndian.PutUint32(lenBuf, uint32(memberLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write member's len error. [ns=%s, table=%s, key=%s, member=%v, err=%v]\n", *ns, *table, string(key), member, err)
				return err
			}
			if n != 4 {
				fmt.Printf("write member's len length error. [ns=%s, table=%s, key=%s, member=%v, len=%d]\n", *ns, *table, string(key), member, n)
				return errWriteLen
			}

			n, err = file.Write(member)
			if err != nil {
				fmt.Printf("write member error. [ns=%s, table=%s, key=%s, member=%v, err=%v]\n", *ns, *table, string(key), member, err)
				return err
			}
			if n != memberLen {
				fmt.Printf("write member length error. [ns=%s, table=%s, key=%s, member=%v, len=%d]\n", *ns, *table, string(key), member, n)
				return errWriteLen
			}

			score := ms[1].([]byte)
			scoreLen := len(score)
			binary.BigEndian.PutUint32(lenBuf, uint32(scoreLen))
			n, err = file.Write(lenBuf)
			if err != nil {
				fmt.Printf("write score's len error. [ns=%s, table=%s, key=%s, score=%v, err=%v]\n", *ns, *table, string(key), score, err)
				return err
			}
			if n != 4 {
				fmt.Printf("write score's len length error. [ns=%s, table=%s, key=%s, score=%v, len=%d]\n", *ns, *table, string(key), score, n)
				return errWriteLen
			}

			n, err = file.Write(score)
			if err != nil {
				fmt.Printf("write score error. [ns=%s, table=%s, key=%s, score=%v, err=%v]\n", *ns, *table, string(key), score, err)
				return err
			}
			if n != scoreLen {
				fmt.Printf("write score length error. [ns=%s, table=%s, key=%s, score=%v, len=%d]\n", *ns, *table, string(key), score, n)
				return errWriteLen
			}
		}

		return nil
	})
}

func backup(t string) {
	defer wg.Done()

	lookupList := strings.Split(*lookup, ",")

	conf := &sdk.Conf{
		LookupList:   lookupList,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		TendInterval: 100,
		Namespace:    *ns,
		Password:     *pass,
	}
	client := sdk.NewZanRedisClient(conf)
	client.Start()
	defer client.Stop()

	stopCh := make(chan struct{})
	defer close(stopCh)
	ch := client.DoFullScanChannel(t, *table, stopCh)
	path := path.Join(*dataDir, fmt.Sprintf("%s:%s:%s:%s.db", t, time.Now().Format("2006-01-02"), *ns, *table))
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

	defer func() {
		file.Sync()
		file.Close()
	}()
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

	//write namespace
	length := uint32(len(*ns))
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, length)
	n, err = file.Write(lenBuf)
	if err != nil {
		fmt.Printf("write namespace's len  error. [ns=%s, table=%s, err=%v]\n", *ns, *table, err)
		return
	}
	if n != 4 {
		fmt.Printf("write namespace's len length not equal. [ns=%s, table=%s, n=%d]\n", *ns, *table, n)
		return
	}

	n, err = file.WriteString(*ns)
	if err != nil {
		fmt.Printf("write namespace error. [ns=%s, table=%s, err=%v]\n", *ns, *table, n)
		return
	}
	if uint32(n) != length {
		fmt.Printf("write namespace length not equal. [ns=%s, table=%s, n=%d]\n", *ns, *table, n)
	}

	//write table
	length = uint32(len(*table))
	lenBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, length)
	n, err = file.Write(lenBuf)
	if err != nil {
		fmt.Printf("write namespace's len  error. [ns=%s, table=%s, err=%v]\n", *ns, *table, err)
		return
	}
	if n != 4 {
		fmt.Printf("write namespace's len length not equal. [ns=%s, table=%s, n=%d]\n", *ns, *table, n)
		return
	}

	n, err = file.WriteString(*table)
	if err != nil {
		fmt.Printf("write namespace error. [ns=%s, table=%s, err=%v]\n", *ns, *table, n)
		return
	}
	if uint32(n) != length {
		fmt.Printf("write namespace length not equal. [ns=%s, table=%s, n=%d]\n", *ns, *table, n)
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
			err = os.Mkdir(*dataDir, 0755)
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

	tm = time.Duration(1000000 / *qps) * time.Microsecond

	for _, t := range backTypes {
		wg.Add(1)
		go func(t string) {
			backup(t)
		}(t)
	}

	wg.Wait()
	fmt.Println("backup finished.")
}
