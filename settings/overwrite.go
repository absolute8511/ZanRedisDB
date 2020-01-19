package settings

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
)

// RootSettingDir is the setting config file root directory
var RootSettingDir string

func getConfig(fn string) map[string]interface{} {
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return nil
	}
	m := map[string]interface{}{}
	b, err := ioutil.ReadFile(filepath.Clean(fn))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	return m
}

func overwriteSettingsWithFile(s interface{}, fn string) {
	cfg := getConfig(path.Join(RootSettingDir, fn))
	rd := reflect.Indirect(reflect.ValueOf(s))
	rt := rd.Type()
	tagMapper := make(map[string]string, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		jn := f.Tag.Get("json")
		pos := strings.Index(jn, ",")
		if pos != -1 {
			jn = jn[:pos]
		}
		tagMapper[jn] = f.Name
	}
	overwriteSettings(cfg, rd, tagMapper)
}

func getField(key string, rd reflect.Value, tagMapper map[string]string) reflect.Value {
	field := rd.FieldByName(key)
	if field.IsValid() {
		return field
	}
	// find field from json tag
	tn, ok := tagMapper[key]
	if ok {
		field = rd.FieldByName(tn)
	}
	return field
}

func overwriteSettings(cfg map[string]interface{}, rd reflect.Value, tagMapper map[string]string) {
	for key, val := range cfg {
		field := getField(key, rd, tagMapper)
		if field.IsValid() {
			switch field.Type().String() {
			case "uint64":
				field.SetUint(uint64(val.(float64)))
			case "bool":
				field.SetBool(val.(bool))
			case "string":
				field.SetString(val.(string))
			default:
			}
		}
	}
}
