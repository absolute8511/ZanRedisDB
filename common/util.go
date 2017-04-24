package common

import (
	"net"
	"regexp"
	"strconv"
	"strings"
)

const (
	APIAddNode     = "/cluster/node/add"
	APIGetMembers  = "/cluster/members"
	APIGetLeader   = "/cluster/leader"
	APICheckBackup = "/cluster/checkbackup"
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
