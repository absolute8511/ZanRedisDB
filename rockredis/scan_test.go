package rockredis

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/common"
)

func fillScanKeysForType(t *testing.T, dt string, total int, insertFunc func([]byte, string)) ([][]byte, [][]byte) {
	keyList1 := make([][]byte, 0, total*2)
	keyList2 := make([][]byte, 0, total*2)
	for i := 0; i < total; i++ {
		k1 := fmt.Sprintf("test:test_%s_scan_key_%05d", dt, i)
		k2 := fmt.Sprintf("test2:test2_%s_scan_key_%05d", dt, i)
		keyList1 = append(keyList1, []byte(k1))
		keyList2 = append(keyList2, []byte(k2))
	}
	for i := 0; i < total; i++ {
		k1 := fmt.Sprintf("test:test_%s_scan_key_longlonglonglonglonglong_%05d", dt, i)
		k2 := fmt.Sprintf("test2:test2_%s_scan_key_longlonglonglonglonglong_%05d", dt, i)
		keyList1 = append(keyList1, []byte(k1))
		keyList2 = append(keyList2, []byte(k2))
	}
	for _, key := range keyList1 {
		insertFunc(key, "test")
	}
	for _, key := range keyList2 {
		insertFunc(key, "test2")
	}
	return keyList1, keyList2
}

func runAndCheckScan(t *testing.T, db *RockDB, dataType common.DataType, total int, keyList1 [][]byte, keyList2 [][]byte) {
	allKeys := make([][]byte, 0, total*2*2)
	allKeys = append(allKeys, keyList2...)
	allKeys = append(allKeys, keyList1...)
	allRevKeys := make([][]byte, 0, total*2*2)
	for i := len(allKeys) - 1; i >= 0; i-- {
		allRevKeys = append(allRevKeys, allKeys[i])
	}
	revKeyList2 := make([][]byte, 0, total*2)
	for i := len(keyList2) - 1; i >= 0; i-- {
		revKeyList2 = append(revKeyList2, keyList2[i])
	}
	type args struct {
		dataType common.DataType
		cursor   []byte
		count    int
		match    string
		reverse  bool
	}
	tests := []struct {
		name    string
		args    args
		want    [][]byte
		wantErr bool
	}{
		{"scan_test", args{dataType, []byte("test:"), total * 4, "", false}, keyList1, false},
		{"scan_test2", args{dataType, []byte("test2:"), total * 4, "", false}, allKeys, false},
		{"scan_test_limit", args{dataType, []byte("test:"), total, "", false}, keyList1[:total], false},
		{"revscan_test", args{dataType, []byte("test:"), total * 4, "", true}, revKeyList2, false},
		{"revscan_test2", args{dataType, []byte("test2:"), total * 4, "", true}, make([][]byte, 0), false},
		{"revscan_test_all", args{dataType, keyList1[len(keyList1)-1], total * 4, "", true}, allRevKeys[1:], false},
		{"revscan_test_all_limit", args{dataType, keyList1[len(keyList1)-1], total, "", true}, allRevKeys[1 : total+1], false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.Scan(tt.args.dataType, tt.args.cursor, tt.args.count, tt.args.match, tt.args.reverse)
			if (err != nil) != tt.wantErr {
				t.Errorf("RockDB.Scan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("RockDB.Scan() length = %v, want %v", len(got), len(tt.want))
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RockDB.Scan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRockDB_ScanKV(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 100
	keyList1, keyList2 := fillScanKeysForType(t, "kv", total, func(key []byte, prefix string) {
		err := db.KVSet(0, key, key)
		assert.Nil(t, err)
	})

	runAndCheckScan(t, db, common.KV, total, keyList1, keyList2)
}

func TestRockDB_ScanHash(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 100
	keyList1, keyList2 := fillScanKeysForType(t, "hash", total, func(key []byte, prefix string) {
		_, err := db.HSet(0, false, key, []byte(prefix+":a"), key)
		assert.Nil(t, err)
		_, err = db.HSet(0, false, key, []byte(prefix+":b"), key)
		assert.Nil(t, err)
	})

	runAndCheckScan(t, db, common.HASH, total, keyList1, keyList2)
}

func TestRockDB_ScanList(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 100
	keyList1, keyList2 := fillScanKeysForType(t, "list", total, func(key []byte, prefix string) {
		_, err := db.LPush(0, key, key, key)
		assert.Nil(t, err)
	})

	runAndCheckScan(t, db, common.LIST, total, keyList1, keyList2)
}

func TestRockDB_ScanSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 100
	keyList1, keyList2 := fillScanKeysForType(t, "set", total, func(key []byte, prefix string) {
		_, err := db.SAdd(0, key, []byte(prefix+":a"), []byte(prefix+":b"))
		assert.Nil(t, err)
	})

	runAndCheckScan(t, db, common.SET, total, keyList1, keyList2)
}

func TestRockDB_ScanZSet(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 100
	keyList1, keyList2 := fillScanKeysForType(t, "zset", total, func(key []byte, prefix string) {
		_, err := db.ZAdd(0, key, common.ScorePair{1, []byte(prefix + ":a")},
			common.ScorePair{2, []byte(prefix + ":b")})
		assert.Nil(t, err)
	})

	runAndCheckScan(t, db, common.ZSET, total, keyList1, keyList2)
}

func TestRockDB_HashScan(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 50
	dt := "hash"
	fieldList1 := make([][]byte, 0, total*2)
	fieldList2 := make([][]byte, 0, total*2)
	for i := 0; i < total; i++ {
		k1 := fmt.Sprintf("test:%v:%5d", dt, i)
		k2 := fmt.Sprintf("test2:%v:%5d", dt, i)
		fieldList1 = append(fieldList1, []byte(k1))
		fieldList2 = append(fieldList2, []byte(k2))
	}
	for _, f := range fieldList1 {
		db.HSet(0, false, []byte("test:test"), f, f)
	}
	for _, f := range fieldList2 {
		db.HSet(0, false, []byte("test:test2"), f, f)
	}
	fvs, err := db.HScan([]byte("test:test"), []byte(""), total*4, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList1), len(fvs))
	for i, fv := range fvs {
		assert.Equal(t, fv.Key, fieldList1[i])
		assert.Equal(t, fv.Key, fv.Value)
	}
	fvs, err = db.HScan([]byte("test:test"), []byte(""), total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fvs))
	fvs, err = db.HScan([]byte("test:test"), fieldList1[len(fieldList1)-1], total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList1)-1, len(fvs))
	for i, fv := range fvs {
		assert.Equal(t, fv.Key, fieldList1[len(fieldList1)-2-i])
		assert.Equal(t, fv.Key, fv.Value)
	}
	fvs, err = db.HScan([]byte("test:test2"), []byte(""), total*4, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList2), len(fvs))
	for i, fv := range fvs {
		assert.Equal(t, fv.Key, fieldList2[i])
		assert.Equal(t, fv.Key, fv.Value)
	}
	fvs, err = db.HScan([]byte("test:test2"), []byte(""), total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fvs))
	fvs, err = db.HScan([]byte("test:test2"), fieldList2[len(fieldList2)-1], total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList2)-1, len(fvs))
}

func TestRockDB_SetScan(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 50
	dt := "set"
	fieldList1 := make([][]byte, 0, total*2)
	fieldList2 := make([][]byte, 0, total*2)
	for i := 0; i < total; i++ {
		k1 := fmt.Sprintf("test:%v:%5d", dt, i)
		k2 := fmt.Sprintf("test2:%v:%5d", dt, i)
		fieldList1 = append(fieldList1, []byte(k1))
		fieldList2 = append(fieldList2, []byte(k2))
	}
	for _, f := range fieldList1 {
		_, err := db.SAdd(0, []byte("test:test"), f)
		assert.Nil(t, err)
	}
	for _, f := range fieldList2 {
		_, err := db.SAdd(0, []byte("test:test2"), f)
		assert.Nil(t, err)
	}
	mems, err := db.SScan([]byte("test:test"), []byte(""), total*4, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList1), len(mems))
	assert.Equal(t, fieldList1, mems)

	mems, err = db.SScan([]byte("test:test"), []byte(""), total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mems))
	mems, err = db.SScan([]byte("test:test"), fieldList1[len(fieldList1)-1], total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList1)-1, len(mems))
	for i, v := range mems {
		assert.Equal(t, v, fieldList1[len(fieldList1)-2-i])
	}
	mems, err = db.SScan([]byte("test:test2"), []byte(""), total*4, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList2), len(mems))
	for i, v := range mems {
		assert.Equal(t, v, fieldList2[i])
	}
	mems, err = db.SScan([]byte("test:test2"), []byte(""), total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mems))
	mems, err = db.SScan([]byte("test:test2"), fieldList2[len(fieldList2)-1], total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList2)-1, len(mems))
}

func TestRockDB_ZsetScan(t *testing.T) {
	db := getTestDB(t)
	defer os.RemoveAll(db.cfg.DataDir)
	defer db.Close()

	total := 50
	dt := "zset"
	fieldList1 := make([][]byte, 0, total*2)
	fieldList2 := make([][]byte, 0, total*2)
	for i := 0; i < total; i++ {
		k1 := fmt.Sprintf("test:%v:%5d", dt, i)
		k2 := fmt.Sprintf("test2:%v:%5d", dt, i)
		fieldList1 = append(fieldList1, []byte(k1))
		fieldList2 = append(fieldList2, []byte(k2))
	}
	for _, f := range fieldList1 {
		db.ZAdd(0, []byte("test:test"), common.ScorePair{1, f})
	}
	for _, f := range fieldList2 {
		db.ZAdd(0, []byte("test:test2"),
			common.ScorePair{2, f})
	}
	fvs, err := db.ZScan([]byte("test:test"), []byte(""), total*4, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList1), len(fvs))
	for i, fv := range fvs {
		assert.Equal(t, fv.Member, fieldList1[i])
		assert.Equal(t, fv.Score, float64(1))
	}
	fvs, err = db.ZScan([]byte("test:test"), []byte(""), total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fvs))
	fvs, err = db.ZScan([]byte("test:test"), fieldList1[len(fieldList1)-1], total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList1)-1, len(fvs))
	for i, fv := range fvs {
		assert.Equal(t, fv.Member, fieldList1[len(fieldList1)-2-i])
		assert.Equal(t, fv.Score, float64(1))
	}
	fvs, err = db.ZScan([]byte("test:test2"), []byte(""), total*4, "", false)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList2), len(fvs))
	for i, fv := range fvs {
		assert.Equal(t, fv.Member, fieldList2[i])
		assert.Equal(t, fv.Score, float64(2))
	}
	fvs, err = db.ZScan([]byte("test:test2"), []byte(""), total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fvs))
	fvs, err = db.ZScan([]byte("test:test2"), fieldList2[len(fieldList2)-1], total*4, "", true)
	assert.Nil(t, err)
	assert.Equal(t, len(fieldList2)-1, len(fvs))
}
