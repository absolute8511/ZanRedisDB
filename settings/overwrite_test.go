package settings

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOverwriteSettings(t *testing.T) {
	tmpDir := os.TempDir()
	t.Logf("tmp: %v", tmpDir)
	os.MkdirAll(tmpDir, 0777)
	RootSettingDir = tmpDir
	defer os.RemoveAll(tmpDir)
	testSettings := `{"TestInt":1, "TestStr":"str", "TestBool":true}`
	err := ioutil.WriteFile(path.Join(tmpDir, "soft-settings.json"), []byte(testSettings), 0777)
	assert.Nil(t, err)
	err = ioutil.WriteFile(path.Join(tmpDir, "static-settings.json"), []byte(testSettings), 0777)
	assert.Nil(t, err)

	s1 := getSoftSettings()
	s2 := getStaticSettings()
	assert.Equal(t, uint64(1), s1.TestInt)
	assert.Equal(t, true, s1.TestBool)
	assert.Equal(t, "str", s1.TestStr)
	assert.Equal(t, uint64(1), s2.TestInt)
	assert.Equal(t, true, s2.TestBool)
	assert.Equal(t, "str", s2.TestStr)

	testSettings = `{"test_int":1, "test_str":"str", "test_bool":true}`
	err = ioutil.WriteFile(path.Join(tmpDir, "soft-settings.json"), []byte(testSettings), 0777)
	assert.Nil(t, err)
	err = ioutil.WriteFile(path.Join(tmpDir, "static-settings.json"), []byte(testSettings), 0777)
	assert.Nil(t, err)

	s3 := getSoftSettings()
	s4 := getStaticSettings()
	assert.Equal(t, uint64(1), s3.TestInt)
	assert.Equal(t, true, s3.TestBool)
	assert.Equal(t, "str", s3.TestStr)
	assert.Equal(t, uint64(1), s4.TestInt)
	assert.Equal(t, true, s4.TestBool)
	assert.Equal(t, "str", s4.TestStr)
}
