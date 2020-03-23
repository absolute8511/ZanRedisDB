package rockredis

import (
	"bytes"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func TestHashCodec(t *testing.T) {
	key := []byte("key")
	field := []byte("field")

	ek := hEncodeSizeKey(key)
	if k, err := hDecodeSizeKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	}

	ek = hEncodeHashKey([]byte("test"), key, field)
	if table, k, f, err := hDecodeHashKey(ek); err != nil {
		t.Fatal(err)
	} else if string(k) != "key" {
		t.Fatal(string(k))
	} else if string(f) != "field" {
		t.Fatal(string(f))
	} else if string(table) != "test" {
		t.Fatal(string(table))
	}
}

func TestDBHash(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	key := []byte("test:testdb_hash_a")

	tn := time.Now().UnixNano()
	r1 := common.KVRecord{
		Key:   []byte("a"),
		Value: []byte("hello world 1"),
	}
	r2 := common.KVRecord{
		Key:   []byte("b"),
		Value: []byte("hello world 2"),
	}
	err := db.HMset(tn, key, r1, r2)
	assert.Nil(t, err)

	if n, err := db.HSet(tn, false, key, []byte("d"), []byte("hello world 2")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	v1, _ := db.HGet(key, []byte("a"))
	v2, _ := db.HGet(key, []byte("b"))
	if string(v1) != "hello world 1" {
		t.Error(v1)
	}
	if string(v2) != "hello world 2" {
		t.Error(v2)
	}
	ay, _ := db.HMget(key, []byte("a"), []byte("b"))

	if string(v1) != string(ay[0]) {
		t.Error(ay[0])
	}

	if string(v2) != string(ay[1]) {
		t.Error(ay[1])
	}

	length, err := db.HLen(key)
	if err != nil {
		t.Error(err)
	}
	if length != 3 {
		t.Errorf("length should be 2: %v", length)
	}
	n, results, _ := db.HGetAll(key)
	time.Sleep(time.Second)
	assert.Equal(t, int(n), len(results))
	if string(results[0].Rec.Key) != "a" {
		t.Error(results)
	}
	if string(results[0].Rec.Value) != "hello world 1" {
		t.Error(results)
	}
	if string(results[1].Rec.Key) != "b" {
		t.Error(results)
	}

	if string(results[1].Rec.Value) != "hello world 2" {
		t.Error(results)
	}

	n, results, _ = db.HKeys(key)
	assert.Equal(t, int(n), len(results))

	if string(results[0].Rec.Key) != "a" {
		t.Error(results)
	}
	if string(results[1].Rec.Key) != "b" {
		t.Error(results)
	}
	n, results, _ = db.HValues(key)
	if string(results[0].Rec.Value) != "hello world 1" {
		t.Error(results)
	}
	if string(results[1].Rec.Value) != "hello world 2" {
		t.Error(results)
	}

	if n, err := db.HSet(tn, true, key, []byte("b"), []byte("hello world changed nx")); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal(n)
	}
	v2, _ = db.HGet(key, []byte("b"))
	if string(v2) != "hello world 2" {
		t.Error(v2)
	}

	if n, err := db.HSet(tn, true, key, []byte("c"), []byte("hello world c")); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatal(n)
	}

	v3, _ := db.HGet(key, []byte("c"))
	if string(v3) != "hello world c" {
		t.Error(v3)
	}
}

func TestHashKeyExists(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:hkeyexists_test")
	v, err := db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 0 {
		t.Fatal("invalid value ", v)
	}

	if _, err := db.HSet(0, false, key, []byte("hello"), []byte("world")); err != nil {
		t.Fatal(err.Error())
	}

	v, err = db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 1 {
		t.Fatal("invalid value ", v)
	}
	if _, err := db.HSet(0, false, key, []byte("hello2"), []byte("world2")); err != nil {
		t.Fatal(err.Error())
	}
	db.HDel(key, []byte("hello"))
	v, err = db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 1 {
		t.Fatal("invalid value ", v)
	}
	db.HClear(key)
	v, err = db.HKeyExists(key)
	if err != nil {
		t.Fatal(err.Error())
	}
	if v != 0 {
		t.Fatal("invalid value ", v)
	}
}

func TestHashKeyIncrBy(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()
	key := []byte("test:hkey_incr_test")
	if _, err := db.HSet(0, false, key, []byte("hello"), []byte("0")); err != nil {
		t.Fatal(err.Error())
	}

	r, _ := db.HIncrBy(0, key, []byte("hello"), 3)
	if r != 3 {
		t.Error(r)
	}
	r, _ = db.HIncrBy(0, key, []byte("hello"), -6)
	if r != -3 {
		t.Error(r)
	}
}

func TestHashIndexLoad(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	assert.Equal(t, 0, len(db.indexMgr.tableIndexes))
	var hindex HsetIndex
	hindex.Table = []byte("test_index_table")
	hindex.Name = []byte("index1")
	hindex.IndexField = []byte("index_test_field")
	hindex.Unique = 0
	hindex.ValueType = StringV

	err := db.indexMgr.AddHsetIndex(db, &hindex)
	assert.Nil(t, err)

	var hindex2 HsetIndex
	hindex2.Table = []byte("test_index_table")
	hindex2.Name = []byte("index2")
	hindex2.IndexField = []byte("index_test_field2")
	hindex2.Unique = 0
	hindex2.ValueType = Int64V

	err = db.indexMgr.AddHsetIndex(db, &hindex2)
	assert.Nil(t, err)

	var hindex3 HsetIndex
	hindex3.Table = []byte("test_index_table3")
	hindex3.Name = []byte("index3")
	hindex3.IndexField = []byte("index_test_field3")
	hindex3.Unique = 0
	hindex3.ValueType = Int64V

	err = db.indexMgr.AddHsetIndex(db, &hindex3)
	assert.Nil(t, err)

	db.indexMgr.Close()
	err = db.indexMgr.LoadIndexes(db)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(db.indexMgr.tableIndexes))
	tindexes, err := db.indexMgr.GetHsetIndex(string(hindex.Table), string(hindex.IndexField))
	assert.Nil(t, err)
	assert.Equal(t, hindex.Name, tindexes.Name)

	tindexes, err = db.indexMgr.GetHsetIndex(string(hindex2.Table), string(hindex2.IndexField))
	assert.Nil(t, err)
	assert.Equal(t, hindex2.Name, tindexes.Name)

	tindexes, err = db.indexMgr.GetHsetIndex(string(hindex3.Table), string(hindex3.IndexField))
	assert.Nil(t, err)
	assert.Equal(t, hindex3.Name, tindexes.Name)
}

func TestHashIndexBuildAndClean(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	assert.Equal(t, 0, len(db.indexMgr.tableIndexes))
	var hindex HsetIndex
	hindex.Table = []byte("test_index_table")
	hindex.Name = []byte("index1")
	hindex.IndexField = []byte("index_test_field")
	hindex.Unique = 0
	hindex.ValueType = StringV

	key := []byte(string(hindex.Table) + ":testdb_hash_a")
	db.HSet(0, false, key, hindex.IndexField, []byte("1"))
	key = []byte(string(hindex.Table) + ":testdb_hash_b")
	db.HSet(0, false, key, hindex.IndexField, []byte("2"))
	key = []byte(string(hindex.Table) + ":testdb_hash_c")
	db.HSet(0, false, key, hindex.IndexField, []byte("2"))

	err := db.indexMgr.AddHsetIndex(db, &hindex)
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex.Table), string(hindex.IndexField), BuildingIndex)
	assert.Nil(t, err)
	// wait until building done
	buildStart := time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		hindex, err := db.indexMgr.GetHsetIndex(string(hindex.Table), string(hindex.IndexField))
		assert.Nil(t, err)
		if hindex.State == BuildDoneIndex {
			break
		} else if time.Since(buildStart) > time.Second*10 {
			t.Errorf("building index timeout")
			break
		}
	}
	condAll := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	_, cnt, _, err := db.HsetIndexSearch(hindex.Table, hindex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, 3, int(cnt))

	var hindex2 HsetIndex
	hindex2.Table = []byte("test_index_table")
	hindex2.Name = []byte("index2")
	hindex2.IndexField = []byte("index_test_field2")
	hindex2.Unique = 0
	hindex2.ValueType = Int64V

	err = db.indexMgr.AddHsetIndex(db, &hindex2)
	assert.Nil(t, err)

	var hindex3 HsetIndex
	hindex3.Table = []byte("test_index_table3")
	hindex3.Name = []byte("index3")
	hindex3.IndexField = []byte("index_test_field3")
	hindex3.Unique = 0
	hindex3.ValueType = Int64V

	err = db.indexMgr.AddHsetIndex(db, &hindex3)
	assert.Nil(t, err)

	key = []byte(string(hindex2.Table) + ":testdb_hash_a")
	db.HSet(0, false, key, hindex2.IndexField, []byte("1"))
	key = []byte(string(hindex2.Table) + ":testdb_hash_b")
	db.HSet(0, false, key, hindex2.IndexField, []byte("2"))
	key = []byte(string(hindex2.Table) + ":testdb_hash_c")
	db.HSet(0, false, key, hindex2.IndexField, []byte("2"))

	key = []byte(string(hindex3.Table) + ":testdb_hash_a")
	db.HSet(0, false, key, hindex3.IndexField, []byte("1"))
	key = []byte(string(hindex3.Table) + ":testdb_hash_b")
	db.HSet(0, false, key, hindex3.IndexField, []byte("2"))
	key = []byte(string(hindex3.Table) + ":testdb_hash_c")
	db.HSet(0, false, key, hindex3.IndexField, []byte("2"))
	key = []byte(string(hindex3.Table) + ":testdb_hash_d")
	db.HSet(0, false, key, hindex3.IndexField, []byte("3"))

	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex2.Table), string(hindex2.IndexField), BuildingIndex)
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex3.Table), string(hindex3.IndexField), BuildingIndex)
	assert.Nil(t, err)
	// wait until building done
	buildStart = time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		hindex2, err := db.indexMgr.GetHsetIndex(string(hindex.Table), string(hindex.IndexField))
		assert.Nil(t, err)
		hindex3, err := db.indexMgr.GetHsetIndex(string(hindex.Table), string(hindex.IndexField))
		assert.Nil(t, err)
		if hindex2.State == BuildDoneIndex && hindex3.State == BuildDoneIndex {
			break
		} else if time.Since(buildStart) > time.Second*10 {
			t.Errorf("building index timeout")
			break
		}
	}

	_, cnt, _, err = db.HsetIndexSearch(hindex2.Table, hindex2.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, 3, int(cnt))

	_, cnt, _, err = db.HsetIndexSearch(hindex3.Table, hindex3.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, 4, int(cnt))

	// clean index
	deletedIndex1, err := db.indexMgr.GetHsetIndex(string(hindex.Table), string(hindex.IndexField))
	assert.Nil(t, err)
	deletedIndex2, err := db.indexMgr.GetHsetIndex(string(hindex2.Table), string(hindex2.IndexField))
	assert.Nil(t, err)
	deletedIndex3, err := db.indexMgr.GetHsetIndex(string(hindex3.Table), string(hindex3.IndexField))
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex.Table), string(hindex.IndexField), DeletedIndex)
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex2.Table), string(hindex2.IndexField), DeletedIndex)
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex3.Table), string(hindex3.IndexField), DeletedIndex)
	assert.Nil(t, err)

	buildStart = time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		_, err1 := db.indexMgr.GetHsetIndex(string(hindex.Table), string(hindex.IndexField))
		_, err2 := db.indexMgr.GetHsetIndex(string(hindex2.Table), string(hindex2.IndexField))
		_, err3 := db.indexMgr.GetHsetIndex(string(hindex3.Table), string(hindex3.IndexField))
		if err1 == ErrIndexNotExist && err2 == ErrIndexNotExist && err3 == ErrIndexNotExist {
			break
		} else if time.Since(buildStart) > time.Second*10 {
			t.Errorf("clean index timeout")
			break
		}
	}

	cnt, _, err = deletedIndex1.SearchRec(db, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))

	cnt, _, err = deletedIndex2.SearchRec(db, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))

	cnt, _, err = deletedIndex3.SearchRec(db, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))
}

func TestHashIndexStringV(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	var hindex HsetIndex
	hindex.Table = []byte("test")
	hindex.Name = []byte("index1")
	hindex.IndexField = []byte("index_test_stringfield")
	hindex.Unique = 0
	hindex.ValueType = StringV

	err := db.indexMgr.AddHsetIndex(db, &hindex)
	assert.Nil(t, err)

	pkCnt := 20
	inputPKList := make([][]byte, 0, pkCnt)
	inputFVList := make([][]byte, 0, pkCnt)
	// 0, 1, 10, 11, 12, ..., 19, 2, 3, 4, ..., 9
	inputFVList = append(inputFVList, []byte("fv0"))
	inputFVList = append(inputFVList, []byte("fv1"))
	for i := 10; i < 20; i++ {
		inputFVList = append(inputFVList, []byte("fv"+strconv.Itoa(i)))
	}
	for i := 2; i < 10; i++ {
		inputFVList = append(inputFVList, []byte("fv"+strconv.Itoa(i)))
	}

	for i := 0; i < pkCnt; i++ {
		inputPKList = append(inputPKList, []byte("test:key"+strconv.Itoa(i)))
	}
	for i, pk := range inputPKList {
		err = db.hsetIndexAddRec(pk, hindex.IndexField, inputFVList[i], db.wb)
		assert.Nil(t, err)
	}
	db.CommitBatchWrite()
	condAll := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}

	condEqual := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: true,
		EndKey:       inputFVList[0],
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}
	condLess := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       inputFVList[len(inputFVList)-1],
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condLessEq := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       inputFVList[len(inputFVList)-1],
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}
	condLessMid := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       inputFVList[len(inputFVList)/2],
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condGt := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: false,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condGtEq := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: true,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condGtMidEq := &IndexCondition{
		StartKey:     inputFVList[len(inputFVList)/2],
		IncludeStart: true,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	_, cnt, pkList, err := db.HsetIndexSearch(hindex.Table, hindex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))
	for i := 0; i < len(inputPKList); i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		assert.Equal(t, inputFVList[i], pkList[i].IndexValue)
	}

	t.Log(condEqual)
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condEqual, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, int(cnt))
	t.Log(condEqual)
	assert.Equal(t, condEqual.StartKey, pkList[0].IndexValue)
	assert.Equal(t, inputPKList[0], pkList[0].PKey)

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLess, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		assert.Equal(t, 1, bytes.Compare(condLess.EndKey, pkList[i].IndexValue))
	}
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLessEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))
	for i := 0; i < len(inputPKList); i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		comp := bytes.Compare(condLess.EndKey, pkList[i].IndexValue)
		assert.True(t, comp == 1 || comp == 0)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGt, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
		assert.Equal(t, -1, bytes.Compare(condGt.StartKey, pkList[i].IndexValue))
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGtEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))
	for i := 0; i < len(inputPKList); i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		comp := bytes.Compare(condLess.StartKey, pkList[i].IndexValue)
		assert.True(t, comp == -1 || comp == 0)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLessMid, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)/2, int(cnt))
	for i := 0; i < len(inputPKList)/2; i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		comp := bytes.Compare(condLessMid.EndKey, pkList[i].IndexValue)
		assert.True(t, comp == 1)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGtMidEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)/2, int(cnt))
	for i := 0; i < len(inputPKList)/2; i++ {
		assert.Equal(t, inputPKList[i+len(inputPKList)/2], pkList[i].PKey)
		comp := bytes.Compare(condGtMidEq.StartKey, pkList[i].IndexValue)
		assert.True(t, comp == -1 || comp == 0)
	}

	db.hsetIndexRemoveRec(inputPKList[0], hindex.IndexField, inputFVList[0], db.wb)
	db.CommitBatchWrite()
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condEqual, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLess, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-2, int(cnt))
	for i := 0; i < len(inputPKList)-2; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLessEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGt, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGtEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}
}

func TestHashIndexStringVPrefix(t *testing.T) {
}

func TestHashIndexStringVMultiEqual(t *testing.T) {
}

func TestHashIndexStringVUnique(t *testing.T) {
}

func TestHashIndexInt64V(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	var hindex HsetIndex
	hindex.Table = []byte("test")
	hindex.Name = []byte("index1")
	hindex.IndexField = []byte("index_test_int64field")
	hindex.Unique = 0
	hindex.ValueType = Int64V

	err := db.indexMgr.AddHsetIndex(db, &hindex)
	assert.Nil(t, err)

	pkCnt := 20
	inputPKList := make([][]byte, 0, pkCnt)
	inputFVList := make([][]byte, 0, pkCnt)
	inputFVIntList := make([]int64, 0, pkCnt)

	for i := 0; i < pkCnt; i++ {
		inputPKList = append(inputPKList, []byte("test:key"+strconv.Itoa(i)))
		inputFVList = append(inputFVList, []byte(strconv.Itoa(i)))
		inputFVIntList = append(inputFVIntList, int64(i))
	}

	db.wb.Clear()
	for i, pk := range inputPKList {
		db.hsetIndexAddRec(pk, hindex.IndexField, inputFVList[i], db.wb)
	}
	db.eng.Write(db.defaultWriteOpts, db.wb)
	condAll := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condEqual := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: true,
		EndKey:       inputFVList[0],
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}
	condLess := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       inputFVList[len(inputFVList)-1],
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condLessEq := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       inputFVList[len(inputFVList)-1],
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}
	condLessMid := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       inputFVList[len(inputFVList)/2],
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condGt := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: false,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condGtEq := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: true,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	condGtMidEq := &IndexCondition{
		StartKey:     inputFVList[len(inputFVList)/2],
		IncludeStart: true,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}

	_, cnt, pkList, err := db.HsetIndexSearch(hindex.Table, hindex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))
	for i := 0; i < len(inputPKList); i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
	}
	t.Log(pkList)

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condEqual, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, inputPKList[0], pkList[0].PKey)
	assert.Equal(t, inputFVIntList[0], pkList[0].IndexIntValue)

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLess, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		assert.True(t, pkList[i].IndexIntValue < inputFVIntList[pkCnt-1])
	}
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLessEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))
	for i := 0; i < len(inputPKList); i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		assert.True(t, pkList[i].IndexIntValue <= inputFVIntList[pkCnt-1])
	}
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLessMid, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)/2, int(cnt))
	for i := 0; i < len(inputPKList)/2; i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		assert.True(t, pkList[i].IndexIntValue < inputFVIntList[pkCnt/2])
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGt, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
		assert.True(t, pkList[i].IndexIntValue > inputFVIntList[0])
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGtEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))
	for i := 0; i < len(inputPKList); i++ {
		assert.Equal(t, inputPKList[i], pkList[i].PKey)
		assert.True(t, pkList[i].IndexIntValue >= inputFVIntList[0])
	}
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGtMidEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)/2, int(cnt))
	for i := 0; i < len(inputPKList)/2; i++ {
		assert.Equal(t, inputPKList[i+pkCnt/2], pkList[i].PKey)
		assert.True(t, pkList[i].IndexIntValue >= inputFVIntList[pkCnt/2])
	}

	db.wb.Clear()
	db.hsetIndexRemoveRec(inputPKList[0], hindex.IndexField, inputFVList[0], db.wb)
	db.eng.Write(db.defaultWriteOpts, db.wb)
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condEqual, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLess, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-2, int(cnt))
	for i := 0; i < len(inputPKList)-2; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}
	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condLessEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGt, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}

	_, cnt, pkList, err = db.HsetIndexSearch(hindex.Table, hindex.IndexField, condGtEq, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))
	for i := 0; i < len(inputPKList)-1; i++ {
		assert.Equal(t, inputPKList[i+1], pkList[i].PKey)
	}
}

func TestHashUpdateWithIndex(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	var hindex HsetIndex
	hindex.Table = []byte("test")
	hindex.Name = []byte("index1")
	hindex.IndexField = []byte("index_test_int64field")
	hindex.Unique = 0
	hindex.ValueType = Int64V

	intIndex := hindex
	err := db.indexMgr.AddHsetIndex(db, &intIndex)
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex.Table), string(hindex.IndexField), ReadyIndex)
	assert.Nil(t, err)

	hindex.Table = []byte("test")
	hindex.Name = []byte("index2")
	hindex.IndexField = []byte("index_test_stringfield")
	hindex.Unique = 0
	hindex.ValueType = StringV

	stringIndex := hindex
	err = db.indexMgr.AddHsetIndex(db, &stringIndex)
	assert.Nil(t, err)
	err = db.indexMgr.UpdateHsetIndexState(db, string(hindex.Table), string(hindex.IndexField), ReadyIndex)
	assert.Nil(t, err)

	inputPKList := make([][]byte, 0, 3)
	inputPKList = append(inputPKList, []byte("test:testhindex_key1"))
	inputPKList = append(inputPKList, []byte("test:testhindex_key2"))
	inputPKList = append(inputPKList, []byte("test:testhindex_key3"))

	for i, key := range inputPKList {
		v := strconv.Itoa(i + 1)
		db.HSet(0, false, key, intIndex.IndexField, []byte(v))
	}

	condAll := &IndexCondition{
		StartKey:     nil,
		IncludeStart: false,
		EndKey:       nil,
		IncludeEnd:   false,
		Offset:       0,
		Limit:        -1,
	}
	_, cnt, _, err := db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))

	condEqual2 := &IndexCondition{
		StartKey:     []byte("2"),
		IncludeStart: true,
		EndKey:       []byte("2"),
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}

	_, cnt, pkList, err := db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condEqual2, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, 1, len(pkList))
	assert.Equal(t, inputPKList[1], pkList[0].PKey)
	assert.Equal(t, int64(2), pkList[0].IndexIntValue)
	// update field to new and check index update
	db.HSet(0, false, inputPKList[1], intIndex.IndexField, []byte("5"))

	_, cnt, _, err = db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condEqual2, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))
	condEqual5 := &IndexCondition{
		StartKey:     []byte("5"),
		IncludeStart: true,
		EndKey:       []byte("5"),
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}
	_, cnt, pkList5, err := db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condEqual5, false)
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, 1, len(pkList5))
	assert.Equal(t, pkList[0].PKey, pkList5[0].PKey)
	assert.Equal(t, int64(5), pkList5[0].IndexIntValue)

	_, cnt, _, err = db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))

	// hincrby
	db.HIncrBy(0, inputPKList[1], intIndex.IndexField, 1)
	_, cnt, _, err = db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condEqual5, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))
	condEqual6 := &IndexCondition{
		StartKey:     []byte("6"),
		IncludeStart: true,
		EndKey:       []byte("6"),
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}
	_, cnt, pkList6, err := db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condEqual6, false)
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, 1, len(pkList6))
	assert.Equal(t, pkList[0].PKey, pkList6[0].PKey)
	assert.Equal(t, int64(6), pkList6[0].IndexIntValue)

	// hmset test
	// hdel
	// hclear

	inputFVList := make([][]byte, 0, 3)
	inputFVList = append(inputFVList, []byte("fv1"))
	inputFVList = append(inputFVList, []byte("fv2"))
	inputFVList = append(inputFVList, []byte("fv3"))
	for i, pk := range inputPKList {
		err = db.HMset(0, pk, common.KVRecord{stringIndex.IndexField, inputFVList[i]})
		assert.Nil(t, err)
	}

	_, cnt, _, err = db.HsetIndexSearch(stringIndex.Table, stringIndex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList), int(cnt))

	condEqual0 := &IndexCondition{
		StartKey:     inputFVList[0],
		IncludeStart: true,
		EndKey:       inputFVList[0],
		IncludeEnd:   true,
		Offset:       0,
		Limit:        -1,
	}

	_, cnt, pkList, err = db.HsetIndexSearch(stringIndex.Table, stringIndex.IndexField, condEqual0, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, int(cnt))
	assert.Equal(t, 1, len(pkList))
	assert.Equal(t, inputPKList[0], pkList[0].PKey)

	db.HDel(inputPKList[0], stringIndex.IndexField)
	_, cnt, _, err = db.HsetIndexSearch(stringIndex.Table, stringIndex.IndexField, condEqual0, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, int(cnt))

	_, cnt, _, err = db.HsetIndexSearch(stringIndex.Table, stringIndex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))

	db.HClear(inputPKList[1])

	_, cnt, _, err = db.HsetIndexSearch(intIndex.Table, intIndex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-1, int(cnt))

	_, cnt, _, err = db.HsetIndexSearch(stringIndex.Table, stringIndex.IndexField, condAll, false)
	assert.Nil(t, err)
	assert.Equal(t, len(inputPKList)-2, int(cnt))
}
