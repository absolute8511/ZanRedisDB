package common

import (
	"net"
)

const (
	APIAddNode     = "/cluster/node/add"
	APIGetMembers  = "/cluster/members/:namespace"
	APIGetLeader   = "/cluster/leader/:namespace"
	APICheckBackup = "/cluster/checkbackup/:namespace"
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
