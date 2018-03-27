package common

import (
	"bytes"
	"net"
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

var validNamespaceTableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

const (
	InternalPrefix = "##"
)

func IsValidNamespaceName(ns string) bool {
	return isValidNameString(ns)
}

func IsValidTableName(tb []byte) bool {
	return isValidName(tb)
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

func isValidName(name []byte) bool {
	if len(name) > 255 || len(name) < 1 {
		return false
	}
	return validNamespaceTableNameRegex.Match(name)
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
