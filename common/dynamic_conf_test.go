package common

import (
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDumpDynamicConf(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		// Add test cases.
		{"dump", []string{"check_raft_timeout:5", "check_snap_timeout:60",
			"empty_int:0",
			"max_remote_recover:2",
			"slow_limiter_half_open_sec:15",
			"slow_limiter_refuse_cost_ms:600",
			"slow_limiter_switch:1",
			"test_str:test_str"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DumpDynamicConf(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DumpDynamicConf() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetIntDynamicConf(t *testing.T) {
	type args struct {
		k string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		//Add test cases.
		{"get default check_snap_timeout", args{ConfCheckSnapTimeout}, 60},
		{"get changed check_snap_timeout", args{ConfCheckSnapTimeout}, 2},
		{"get non exist", args{"noexist"}, 0},
		{"get after set non exist", args{"noexist-set"}, 0},
	}
	changedCalled := 0
	RegisterConfChangedHandler(ConfCheckSnapTimeout, func(nv interface{}) {
		_, ok := nv.(int)
		assert.True(t, ok)
		changedCalled++
	})
	SetIntDynamicConf("noexist-set", 2)
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetIntDynamicConf(tt.args.k); got != tt.want {
				t.Errorf("GetIntDynamicConf() = %v, want %v", got, tt.want)
			}
			SetIntDynamicConf(ConfCheckSnapTimeout, 2)
			assert.Equal(t, i+1, changedCalled)
		})
	}
}

func TestGetStrDynamicConf(t *testing.T) {
	type args struct {
		k string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"get default test_str", args{"test_str"}, "test_str"},
		{"get changed test_str", args{"test_str"}, "test_str_changed"},
		{"get non exist", args{"noexist"}, ""},
		{"get after set non exist", args{"noexist-set"}, "set-noexist"},
	}
	changedCalled := 0
	RegisterConfChangedHandler("test_str", func(nv interface{}) {
		_, ok := nv.(string)
		assert.True(t, ok)
		changedCalled++
	})
	SetStrDynamicConf("noexist-set", "set-noexist")
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStrDynamicConf(tt.args.k); got != tt.want {
				t.Errorf("GetStrDynamicConf() = %v, want %v", got, tt.want)
			}
			SetStrDynamicConf("test_str", "test_str_changed")
			assert.Equal(t, i+1, changedCalled)
		})
	}
}

func TestIsConfSetted(t *testing.T) {
	type args struct {
		k string
	}
	tests := []struct {
		pre  func()
		name string
		args args
		want bool
	}{
		{nil, "check default check_snap_timeout", args{"check_snap_timeout"}, true},
		{func() { SetIntDynamicConf("check_snap_timeout", 0) }, "check empty check_snap_timeout", args{"check_snap_timeout"}, false},
		{nil, "check non exist", args{"noexist"}, false},
		{nil, "check empty str conf", args{"empty_str"}, false},
		{nil, "check empty int conf", args{"empty_int"}, false},
		{nil, "check non exist str", args{"noexist-set-str"}, false},
		{func() { SetStrDynamicConf("noexist-set-str", "v") }, "check after set non exist str", args{"noexist-set-str"}, true},
	}
	SetStrDynamicConf("empty_str", "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.pre != nil {
				tt.pre()
			}
			if got := IsConfSetted(tt.args.k); got != tt.want {
				t.Errorf("IsConfSetted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfRace(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000000; i++ {
			GetIntDynamicConf("check_snap_timeout")
			GetIntDynamicConf("check_raft_timeout")
			GetStrDynamicConf("test_str")
			GetIntDynamicConf("noexist")
			GetStrDynamicConf("noexist")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000000; i++ {
			SetIntDynamicConf("check_snap_timeout", i)
			SetIntDynamicConf("check_raft_timeout", i)
			SetStrDynamicConf("test_str", strconv.Itoa(i))
			SetIntDynamicConf("noexist", i)
			SetStrDynamicConf("noexist", "v")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000000; i++ {
			GetIntDynamicConf("check_snap_timeout")
			GetIntDynamicConf("check_raft_timeout")
			GetStrDynamicConf("test_str")
			GetIntDynamicConf("noexist")
			GetStrDynamicConf("noexist")
		}
	}()
	wg.Wait()
}
