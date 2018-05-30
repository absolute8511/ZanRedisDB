package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

func NewDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

// stores the result in the value pointed to by ret(must be a pointer)
func APIRequest(method string, endpoint string, body io.Reader, timeout time.Duration, ret interface{}) (int, error) {
	httpclient := &http.Client{Transport: NewDeadlineTransport(timeout)}
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return 0, err
	}
	req.Header.Add("Accept", "application/zanredisdb; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("req %v error %v",
			endpoint, err.Error())
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return resp.StatusCode, fmt.Errorf("req %v read body error %v",
			endpoint, err.Error())
	}

	// return status code and error info if not status ok
	// no error means if is status ok
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("req %v got error response %s %q", endpoint, resp.Status, respBody)
	}

	if ret == nil {
		return resp.StatusCode, nil
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}
	err = json.Unmarshal(respBody, ret)
	if err != nil {
		err = errors.New(err.Error() + string(respBody))
	}
	return resp.StatusCode, err
}
