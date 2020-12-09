package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRangeMarshal(t *testing.T) {
	dt := DeleteTableRange{
		StartFrom: []byte("teststart"),
		EndTo:     []byte("testend"),
	}
	t.Logf("%s", getRangeStr(dt))
	out := DeleteTableRange{}
	json.Unmarshal([]byte(getRangeStr(dt)), &out)
	assert.Equal(t, dt, out)
	assert.Equal(t, []byte("teststart"), out.StartFrom)
}
