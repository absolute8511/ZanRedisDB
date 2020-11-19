package main

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/stretchr/testify/assert"
	"github.com/youzan/ZanRedisDB/pdserver"
)

func TestAppConfigParse(t *testing.T) {
	flagSet.Parse([]string{})

	configFile := "../../pdserver/pdconf.example.conf"
	var cfg map[string]interface{}
	_, err := toml.DecodeFile(configFile, &cfg)
	if err != nil {
		t.Fatalf("ERROR: failed to load config file %s - %s", configFile, err.Error())
	}
	opts := pdserver.NewServerConfig()
	options.Resolve(opts, flagSet, cfg)
	pdserver.NewServer(opts)
	assert.Equal(t, "v2", opts.BalanceVer)
}
