package rockredis

import (
	"bytes"
	"errors"
	"strings"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var (
	jSep                = byte(':')
	errJsonPathNotArray = errors.New("json path is not array")
)

func checkJsonValueSize(value []byte) error {
	if len(value) > MaxValueSize*2 {
		return errValueSize
	}

	return nil
}

func convertJsonPath(path []byte) string {
	if path == nil {
		return ""
	}
	// handle the compatible between redis json and sjson/gjson lib
	jpath := string(path)
	strings.TrimSpace(jpath)
	if len(jpath) > 0 && jpath[0] == '.' {
		jpath = jpath[1:]
	}
	return jpath
}

func encodeJsonKey(table []byte, key []byte) ([]byte, error) {
	ek := make([]byte, 1)
	pos := 0
	ek[pos] = JSONType
	pos++
	var err error
	ek, err = EncodeMemCmpKey(ek[:pos], table, jSep, key)
	return ek, err
}

func decodeJsonKey(ek []byte) ([]byte, []byte, error) {
	pos := 0
	if pos+1 > len(ek) || ek[pos] != JSONType {
		return nil, nil, errKVKey
	}

	pos++
	rets, err := Decode(ek[pos:], 3)
	if err != nil {
		return nil, nil, err
	}
	table, _ := rets[0].([]byte)
	rk, _ := rets[2].([]byte)
	return table, rk, nil
}

func encodeJsonStartKey(table []byte) ([]byte, error) {
	return encodeJsonKey(table, nil)
}

func encodeJsonStopKey(table []byte, key []byte) []byte {
	ek := make([]byte, 1)
	pos := 0
	ek[pos] = JSONType
	pos++
	ek, _ = EncodeMemCmpKey(ek[:pos], table, jSep+1, nil)
	return ek
}

func (db *RockDB) jSetPath(jdata []byte, path string, value []byte) ([]byte, error) {
	if len(path) == 0 {
		return value, nil
	}
	return sjson.SetRawBytes(jdata, path, value)
}

func (db *RockDB) getOldJson(table []byte, rk []byte) ([]byte, []byte, bool, error) {
	if err := checkKeySize(rk); err != nil {
		return nil, nil, false, err
	}
	ek, err := encodeJsonKey(table, rk)
	if err != nil {
		return nil, nil, false, err
	}
	oldV, err := db.eng.GetBytesNoLock(db.defaultReadOpts, ek)
	if err != nil {
		return ek, nil, false, err
	}
	if oldV == nil {
		return ek, oldV, false, nil
	}
	if len(oldV) >= tsLen {
		oldV = oldV[:len(oldV)-tsLen]
	}
	return ek, oldV, true, nil
}

func (db *RockDB) JSet(ts int64, key []byte, path []byte, value []byte) (int64, error) {
	if !gjson.Valid(string(value)) {
		return 0, errors.New("invalid json value")
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}

	// index lock should before any db read or write since it may be changed by indexing
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	var index *JsonIndex
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
		index = tableIndexes.GetJsonIndexNoLock(string(path))
	}

	ek, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return 0, err
	}

	db.wb.Clear()
	oldV, err = db.jSetPath(oldV, convertJsonPath(path), value)
	if err != nil {
		return 0, err
	}
	// json value can be two times large since it can be read partially.
	if err := checkJsonValueSize(oldV); err != nil {
		return 0, err
	}
	if !gjson.Valid(string(oldV)) {
		return 0, err
	}
	// TODO: update index for path
	_ = index
	if !isExist {
		db.IncrTableKeyCount(table, 1, db.wb)
	}
	tsBuf := PutInt64(ts)
	oldV = append(oldV, tsBuf...)
	db.wb.Put(ek, oldV)
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	if isExist {
		return 0, err
	}
	return 1, err
}

func (db *RockDB) JMset(ts int64, key []byte, args ...common.KVRecord) error {
	if len(args) >= MAX_BATCH_NUM {
		return errTooMuchBatchSize
	}
	if len(args) == 0 {
		return nil
	}
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return err
	}
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	ek, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return err
	}

	db.wb.Clear()

	for i := 0; i < len(args); i++ {
		path := args[i].Key
		oldV, err = db.jSetPath(oldV, convertJsonPath(path), args[i].Value)
		if tableIndexes != nil {
			if index := tableIndexes.GetJsonIndexNoLock(string(path)); index != nil {
				//oldPathV := gjson.GetBytes(oldV, string(path))
				//err = index.UpdateRec(oldPathV, args[i].Value, key, db.wb)
				//if err != nil {
				//	return err
				//}
			}
		}
	}
	if err := checkJsonValueSize(oldV); err != nil {
		return err
	}
	if !gjson.Valid(string(oldV)) {
		return err
	}
	tsBuf := PutInt64(ts)
	oldV = append(oldV, tsBuf...)
	db.wb.Put(ek, oldV)
	if !isExist {
		db.IncrTableKeyCount(table, 1, db.wb)
	}
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return err
}

func (db *RockDB) JMGet(path []byte, keys ...[]byte) ([]string, error) {
	return nil, nil
}

func (db *RockDB) JGet(key []byte, paths ...[]byte) ([]string, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	_, oldV, _, err := db.getOldJson(table, rk)
	if err != nil {
		return nil, err
	}
	tmp := make([]string, len(paths))
	for i, path := range paths {
		tmp[i] = convertJsonPath(path)
	}
	rets := gjson.GetManyBytes(oldV, tmp...)
	for i := 0; i < len(tmp); i++ {
		if tmp[i] == "" {
			tmp[i] = string(oldV)
		} else {
			tmp[i] = rets[i].String()
		}
	}
	return tmp, nil
}

func (db *RockDB) JDel(ts int64, key []byte, path []byte) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}

	ek, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return 0, err
	}
	if !isExist {
		return 0, nil
	}

	jpath := convertJsonPath(path)
	db.wb.Clear()
	if jpath == "" {
		// delete whole json
		db.wb.Delete(ek)
		db.IncrTableKeyCount(table, -1, db.wb)
	} else {
		newV, err := sjson.DeleteBytes(oldV, jpath)
		if err != nil {
			return 0, err
		}
		if bytes.Equal(newV, oldV) {
			return 0, nil
		}
		oldV = newV
		tsBuf := PutInt64(ts)
		oldV = append(oldV, tsBuf...)
		db.wb.Put(ek, oldV)
	}
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return 1, err
}

func (db *RockDB) JKeyExists(key []byte) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	sk, _ := encodeJsonKey(table, rk)
	v, err := db.eng.GetBytes(db.defaultReadOpts, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *RockDB) JArrayAppend(ts int64, key []byte, path []byte, jsons ...[]byte) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}
	ek, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return 0, err
	}
	jpath := convertJsonPath(path)
	oldPath := gjson.GetBytes(oldV, jpath)
	if jpath == "" {
		oldPath = gjson.ParseBytes(oldV)
	}
	arrySize := 0
	if oldPath.Exists() && !oldPath.IsArray() {
		return 0, errJsonPathNotArray
	}
	arrySize = len(oldPath.Array())
	if jpath == "" {
		jpath = "-1"
	} else {
		jpath += ".-1"
	}
	for _, json := range jsons {
		oldV, err = db.jSetPath(oldV, jpath, json)
		if err != nil {
			return 0, err
		}
		arrySize++
	}
	db.wb.Clear()
	if err := checkJsonValueSize(oldV); err != nil {
		return 0, err
	}
	if !gjson.Valid(string(oldV)) {
		return 0, err
	}
	tsBuf := PutInt64(ts)
	oldV = append(oldV, tsBuf...)
	db.wb.Put(ek, oldV)
	if !isExist {
		db.IncrTableKeyCount(table, 1, db.wb)
	}
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return int64(arrySize), err
}

func (db *RockDB) JArrayPop(ts int64, key []byte, path []byte) (string, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return "", err
	}
	tableIndexes := db.indexMgr.GetTableIndexes(string(table))
	if tableIndexes != nil {
		tableIndexes.Lock()
		defer tableIndexes.Unlock()
	}
	ek, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return "", err
	}
	if !isExist {
		return "", nil
	}
	jpath := convertJsonPath(path)
	var oldJson gjson.Result
	if jpath == "" {
		oldJson = gjson.ParseBytes(oldV)
	} else {
		oldJson = gjson.GetBytes(oldV, jpath)
	}
	if !oldJson.Exists() {
		return "", nil
	}
	if !oldJson.IsArray() {
		dbLog.Infof("pop not array: %v, %v", oldV, oldJson)
		return "", errJsonPathNotArray
	}
	arrySize := len(oldJson.Array())
	if arrySize == 0 {
		return "", nil
	}
	if jpath == "" {
		jpath = "-1"
	} else {
		jpath += ".-1"
	}
	poped := oldJson.Array()[arrySize-1].String()
	oldV, err = sjson.DeleteBytes(oldV, jpath)
	if err != nil {
		return "", err
	}
	db.wb.Clear()
	tsBuf := PutInt64(ts)
	oldV = append(oldV, tsBuf...)
	db.wb.Put(ek, oldV)
	err = db.eng.Write(db.defaultWriteOpts, db.wb)
	return poped, err
}

func (db *RockDB) JArrayLen(key []byte, path []byte) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	_, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return 0, err
	}
	if !isExist {
		return 0, nil
	}
	jpath := convertJsonPath(path)
	var jsonData gjson.Result
	if jpath == "" {
		jsonData = gjson.ParseBytes(oldV)
	} else {
		jsonData = gjson.GetBytes(oldV, jpath)
	}
	if !jsonData.Exists() || !jsonData.IsArray() {
		return 0, nil
	}
	length := len(jsonData.Array())
	return int64(length), nil
}

func (db *RockDB) JObjLen(key []byte, path []byte) (int64, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return 0, err
	}
	_, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return 0, err
	}
	if !isExist {
		return 0, nil
	}
	jpath := convertJsonPath(path)
	var jsonData gjson.Result
	if jpath == "" {
		jsonData = gjson.ParseBytes(oldV)
	} else {
		jsonData = gjson.GetBytes(oldV, jpath)
	}
	if !jsonData.Exists() || !jsonData.IsObject() {
		return 0, nil
	}
	length := len(jsonData.Map())
	return int64(length), nil
}

func (db *RockDB) JObjKeys(key []byte, path []byte) ([]string, error) {
	table, rk, err := extractTableFromRedisKey(key)
	if err != nil {
		return nil, err
	}
	_, oldV, isExist, err := db.getOldJson(table, rk)
	if err != nil {
		return nil, err
	}
	if !isExist {
		return nil, nil
	}
	jpath := convertJsonPath(path)
	var jsonData gjson.Result
	if jpath == "" {
		jsonData = gjson.ParseBytes(oldV)
	} else {
		jsonData = gjson.GetBytes(oldV, jpath)
	}
	if !jsonData.Exists() || !jsonData.IsObject() {
		return nil, nil
	}
	kvs := jsonData.Map()
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return keys, nil
}
