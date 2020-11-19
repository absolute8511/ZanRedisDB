package main

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/server"
)

func TestAppConfigParse(t *testing.T) {
	flagSet.Parse([]string{})

	var configFile server.ConfigFile
	d, err := ioutil.ReadFile("../../default.conf")
	assert.Nil(t, err)
	err = json.Unmarshal(d, &configFile)
	assert.Nil(t, err)

	serverConf := configFile.ServerConf
	server.NewServer(serverConf)
}
