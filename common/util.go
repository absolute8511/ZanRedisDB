package common

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/absolute8511/redcon"
)

const (
	// api used by data node
	APIAddNode        = "/cluster/node/add"
	APIAddLearnerNode = "/cluster/node/addlearner"
	APIRemoveNode     = "/cluster/node/remove"
	APIGetMembers     = "/cluster/members"
	APIGetLeader      = "/cluster/leader"
	APICheckBackup    = "/cluster/checkbackup"
	APIGetIndexes     = "/schema/indexes"
	APINodeAllReady   = "/node/allready"
	// check if the namespace raft node is synced and can be elected as leader immediately
	APIIsRaftSynced = "/cluster/israftsynced"
	APITableStats   = "/tablestats"

	// below api for pd
	APIGetSnapshotSyncInfo = "/pd/snapshot_sync_info"
)

const (
	NamespaceTableSeperator = byte(':')
)

func GetIPv4ForInterfaceName(ifname string) string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		if inter.Name == ifname {
			if addrs, err := inter.Addrs(); err == nil {
				for _, addr := range addrs {
					switch ip := addr.(type) {
					case *net.IPNet:
						if ip.IP.DefaultMask() != nil {
							return ip.IP.String()
						}
					}
				}
			}
		}
	}
	return ""
}

// do not use middle '-' which is used as join for name and partition
var validNamespaceTableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

const (
	InternalPrefix = "##"
)

func IsValidNamespaceName(ns string) bool {
	return isValidNameString(ns)
}

func IsInternalTableName(tb string) bool {
	return strings.HasPrefix(tb, InternalPrefix)
}

func isValidNameString(name string) bool {
	if len(name) > 255 || len(name) < 1 {
		return false
	}
	return validNamespaceTableNameRegex.MatchString(name)
}

func CutNamesapce(rawKey []byte) ([]byte, error) {
	index := bytes.IndexByte(rawKey, NamespaceTableSeperator)
	if index <= 0 {
		return nil, ErrInvalidRedisKey
	}
	realKey := rawKey[index+1:]
	return realKey, nil
}

func ExtractNamesapce(rawKey []byte) (string, []byte, error) {
	index := bytes.IndexByte(rawKey, NamespaceTableSeperator)
	if index <= 0 {
		return "", nil, ErrInvalidRedisKey
	}
	namespace := string(rawKey[:index])
	realKey := rawKey[index+1:]
	return namespace, realKey, nil
}

func GetNsDesp(ns string, part int) string {
	return ns + "-" + strconv.Itoa(part)
}

func GetNamespaceAndPartition(fullNamespace string) (string, int) {
	splits := strings.SplitN(fullNamespace, "-", 2)
	if len(splits) != 2 {
		return "", 0
	}
	namespace := splits[0]
	pid, err := strconv.Atoi(splits[1])
	if err != nil {
		return "", 0
	}

	return namespace, pid
}

func DeepCopyCmd(cmd redcon.Command) redcon.Command {
	var newCmd redcon.Command
	newCmd.Raw = append(newCmd.Raw, cmd.Raw...)
	for i := 0; i < len(cmd.Args); i++ {
		var tmp []byte
		tmp = append(tmp, cmd.Args[i]...)
		newCmd.Args = append(newCmd.Args, tmp)
	}
	return newCmd
}

func IsMergeScanCommand(cmd string) bool {
	if len(cmd) < 4 {
		return false
	}
	switch len(cmd) {
	case 4:
		if (cmd[0] == 's' || cmd[0] == 'S') &&
			(cmd[1] == 'c' || cmd[1] == 'C') &&
			(cmd[2] == 'a' || cmd[2] == 'A') &&
			(cmd[3] == 'n' || cmd[3] == 'N') {
			return true
		}
	case 7:
		if (cmd[0] == 'a' || cmd[0] == 'A') &&
			(cmd[1] == 'd' || cmd[1] == 'D') &&
			(cmd[2] == 'v' || cmd[2] == 'V') &&
			(cmd[3] == 's' || cmd[3] == 'S') &&
			(cmd[4] == 'c' || cmd[4] == 'C') &&
			(cmd[5] == 'a' || cmd[5] == 'A') &&
			(cmd[6] == 'n' || cmd[6] == 'N') {
			return true
		}
		if (cmd[0] == 'r' || cmd[0] == 'R') &&
			(cmd[1] == 'e' || cmd[1] == 'E') &&
			(cmd[2] == 'v' || cmd[2] == 'V') &&
			(cmd[3] == 's' || cmd[3] == 'S') &&
			(cmd[4] == 'c' || cmd[4] == 'C') &&
			(cmd[5] == 'a' || cmd[5] == 'A') &&
			(cmd[6] == 'n' || cmd[6] == 'N') {
			return true
		}
	case 8:
		if (cmd[0] == 'f' || cmd[0] == 'F') &&
			(cmd[1] == 'u' || cmd[1] == 'U') &&
			(cmd[2] == 'l' || cmd[2] == 'L') &&
			(cmd[3] == 'l' || cmd[3] == 'L') &&
			(cmd[4] == 's' || cmd[4] == 'S') &&
			(cmd[5] == 'c' || cmd[5] == 'C') &&
			(cmd[6] == 'a' || cmd[6] == 'A') &&
			(cmd[7] == 'n' || cmd[7] == 'N') {
			return true
		}
	case 10:
		if (cmd[0] == 'a' || cmd[0] == 'A') &&
			(cmd[1] == 'd' || cmd[1] == 'D') &&
			(cmd[2] == 'v' || cmd[2] == 'V') &&
			(cmd[3] == 'r' || cmd[3] == 'R') &&
			(cmd[4] == 'e' || cmd[4] == 'E') &&
			(cmd[5] == 'v' || cmd[5] == 'V') &&
			(cmd[6] == 's' || cmd[6] == 'S') &&
			(cmd[7] == 'c' || cmd[7] == 'C') &&
			(cmd[8] == 'a' || cmd[8] == 'A') &&
			(cmd[9] == 'n' || cmd[9] == 'N') {
			return true
		}
	}

	return false
}

func IsFullScanCommand(cmd string) bool {
	if (cmd[0] == 'f' || cmd[0] == 'F') &&
		(cmd[1] == 'u' || cmd[1] == 'U') &&
		(cmd[2] == 'l' || cmd[2] == 'L') &&
		(cmd[3] == 'l' || cmd[3] == 'L') &&
		(cmd[4] == 's' || cmd[4] == 'S') &&
		(cmd[5] == 'c' || cmd[5] == 'C') &&
		(cmd[6] == 'a' || cmd[6] == 'A') &&
		(cmd[7] == 'n' || cmd[7] == 'N') {
		return true
	}
	return false
}

func IsMergeIndexSearchCommand(cmd string) bool {
	if len(cmd) != len("hidx.from") {
		return false
	}
	return strings.ToLower(cmd) == "hidx.from"
}

func IsMergeKeysCommand(cmd string) bool {
	lcmd := strings.ToLower(cmd)
	return lcmd == "plset" || lcmd == "exists" || lcmd == "del"
}

func IsMergeCommand(cmd string) bool {
	if IsMergeScanCommand(cmd) {
		return true
	}

	if IsMergeIndexSearchCommand(cmd) {
		return true
	}

	if IsMergeKeysCommand(cmd) {
		return true
	}

	return false
}

func BuildCommand(args [][]byte) redcon.Command {
	// build a pipeline command
	bufSize := 128
	if len(args) > 5 {
		bufSize = 256
	}
	buf := make([]byte, 0, bufSize)
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')

	poss := make([]int, 0, len(args)*2)
	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(arg)), 10)...)
		buf = append(buf, '\r', '\n')
		poss = append(poss, len(buf), len(buf)+len(arg))
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}

	// reformat a new command
	var ncmd redcon.Command
	ncmd.Raw = buf
	ncmd.Args = make([][]byte, len(poss)/2)
	for i, j := 0, 0; i < len(poss); i, j = i+2, j+1 {
		ncmd.Args[j] = ncmd.Raw[poss[i]:poss[i+1]]
	}
	return ncmd
}

// This will copy file as hard link, if failed it will failover to do the file content copy
// Used this method when both the src and dst file content will never be changed to save disk space
func CopyFileForHardLink(src, dst string) error {
	// open source file
	sfi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !sfi.Mode().IsRegular() {
		return fmt.Errorf("non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}

	// open dest file
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// file doesn't exist
		err := os.MkdirAll(filepath.Dir(dst), DIR_PERM)
		if err != nil {
			return err
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return nil
		}
		// link will failed if dst exist, so remove it first
		os.Remove(dst)
	}
	if err = os.Link(src, dst); err == nil {
		return nil
	}
	err = copyFileContents(src, dst)
	if err != nil {
		return err
	}
	os.Chmod(dst, sfi.Mode())
	return nil
}

// copyFileContents copies the contents.
// all contents will be replaced by the source .
func copyFileContents(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	// we remove dst to avoid override the hard link file content which may affect the origin linked file
	err = os.Remove(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		cerr := dstFile.Close()
		if err == nil {
			err = cerr
		}
	}()

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	err = dstFile.Sync()
	return err
}

func CopyFile(src, dst string, override bool) error {
	sfi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !sfi.Mode().IsRegular() {
		return fmt.Errorf("copyfile: non-regular source file %v (%v)", sfi.Name(), sfi.Mode().String())
	}
	_, err = os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		if !override {
			return nil
		}
	}
	return copyFileContents(src, dst)
}
