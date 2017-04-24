package common

import (
	"bytes"
	"encoding/gob"
	"net"
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

func IsValidNamespaceName(ns string) bool {
	return true
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

func DeepCopy(src interface{}) (dst interface{}, err error) {
	var buf bytes.Buffer
	if err = gob.NewEncoder(&buf).Encode(src); err != nil {
		return
	}

	err = gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
	return
}
