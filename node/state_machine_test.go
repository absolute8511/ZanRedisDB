package node

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_handleReuseOldCheckpoint(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("sm-test-%d", time.Now().UnixNano()))
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	t.Logf("dir:%v\n", tmpDir)
	terms := []uint64{
		0x0001,
		0x0002,
		0x0003,
		0x011a,
		0x021a,
		0x031a,
	}
	indexs := []uint64{
		0x01000,
		0x02000,
		0x03000,
		0x0a000,
		0x0b000,
		0x0c000,
	}
	cntIdx := 10
	type args struct {
		srcInfo    string
		localPath  string
		term       uint64
		index      uint64
		skipReuseN int
	}
	destPath1 := path.Join(tmpDir, fmt.Sprintf("%016x-%016x", 1, 1))
	destPath2 := path.Join(tmpDir, fmt.Sprintf("%016x-%016x", 2, 2))
	lastPath := path.Join(tmpDir, fmt.Sprintf("%04x-%05x", terms[len(terms)-1], int(indexs[len(indexs)-1])+cntIdx-1))
	firstPath := path.Join(tmpDir, fmt.Sprintf("%04x-%05x", terms[0], indexs[0]))
	mid1Path := path.Join(tmpDir, fmt.Sprintf("%04x-%05x", terms[len(terms)-1], int(indexs[len(indexs)-1])+cntIdx/2))
	mid2Path := path.Join(tmpDir, fmt.Sprintf("%04x-%05x", terms[len(terms)-2], int(indexs[len(indexs)-2])+cntIdx/2))
	mid3Path := path.Join(tmpDir, fmt.Sprintf("%04x-%05x", terms[len(terms)-3], int(indexs[len(indexs)-3])+cntIdx/2))
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{"src match in first", args{"matchfirst", tmpDir, 1, 1, 0}, lastPath, destPath1},
		{"src match in last", args{"matchlast", tmpDir, 1, 1, 0}, firstPath, destPath1},
		{"src not match", args{"notmatch", tmpDir, 1, 1, 0}, "", destPath1},
		{"src match in middle", args{"matchmiddle", tmpDir, 1, 1, 0}, mid1Path, destPath1},
		{"src match in same with old", args{"matchfirst", tmpDir, 0x01ba, 0x02ab46c3, 0}, lastPath, path.Join(tmpDir, "00000000000001ba-0000000002ab46c3")},
		{"src match first skip 1", args{"matchfirst", tmpDir, 2, 2, 1}, "", destPath2},
		{"src match last skip 1", args{"matchlast", tmpDir, 2, 2, 1}, "", destPath2},
		{"src match middle skip 1", args{"matchmiddle", tmpDir, 2, 2, 1}, mid2Path, destPath2},
		{"src match middle skip 2", args{"matchmiddle", tmpDir, 2, 2, 2}, mid3Path, destPath2},
		{"src match middle skip all", args{"matchmiddle", tmpDir, 2, 2, len(terms)}, "", destPath2},
	}

	// source info
	for i := 0; i < len(terms); i++ {
		for j := 0; j < cntIdx; j++ {
			idx := indexs[i] + uint64(j)
			p := path.Join(tmpDir, fmt.Sprintf("%04x-%05x", terms[i], idx))
			err := os.MkdirAll(p, 0777)
			assert.Nil(t, err)
			if i == 0 && j == 0 {
				postFileSync(p, "matchlast")
				t.Logf("gen: %v, %v", p, "matchlast")
			} else if j == cntIdx/2 {
				postFileSync(p, "matchmiddle")
				t.Logf("gen: %v, %v", p, "matchmiddle")
			} else if i == len(terms)-1 && j == cntIdx-1 {
				postFileSync(p, "matchfirst")
				t.Logf("gen: %v, %v", p, "matchfirst")
			} else {
				t.Logf("gen: %v, no src info", p)
			}
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := handleReuseOldCheckpoint(tt.args.srcInfo, tt.args.localPath, tt.args.term, tt.args.index, tt.args.skipReuseN)
			if got != tt.want {
				t.Errorf("handleReuseOldCheckpoint() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("handleReuseOldCheckpoint() got1 = %v, want %v", got1, tt.want1)
			}
			// rename back to normal to test next
			if got != "" {
				os.Rename(got1, got)
			}
		})
	}
}
