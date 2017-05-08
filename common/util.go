package common

import (
	"bytes"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

const (
	APIAddNode     = "/cluster/node/add"
	APIGetMembers  = "/cluster/members"
	APIGetLeader   = "/cluster/leader"
	APICheckBackup = "/cluster/checkbackup"
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
	}
	return false
}

func IsMergeCommand(cmd string) bool {
	if IsMergeScanCommand(cmd) {
		return true
	}

	return false
}
