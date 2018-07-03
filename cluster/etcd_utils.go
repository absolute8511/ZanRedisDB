package cluster

import (
	"net/url"

	"github.com/coreos/etcd/client"
)

const (
	ErrCodeEtcdNotReachable    = 501
	ErrCodeUnhandledHTTPStatus = 502
)

var (
	lockID   int64
	pid      int
	hostname string
	ip       string
)

func initEtcdPeers(machines []string) error {
	for i, ep := range machines {
		u, err := url.Parse(ep)
		if err != nil {
			return err
		}
		if u.Scheme == "" {
			u.Scheme = "http"
		}
		machines[i] = u.String()
	}
	return nil
}

func IsEtcdNotReachable(err error) bool {
	if cErr, ok := err.(client.Error); ok {
		return cErr.Code == ErrCodeEtcdNotReachable
	}
	return false
}

func IsEtcdWatchExpired(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeEventIndexCleared)
}
