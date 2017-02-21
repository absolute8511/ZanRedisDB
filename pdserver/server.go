package pdserver

import (
	"github.com/absolute8511/ZanRedisDB/cluster"
	"github.com/absolute8511/ZanRedisDB/common"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
)

var sLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("pdserver"))

func SetLogger(level int32, logger common.Logger) {
	sLog.SetLevel(level)
	sLog.Logger = logger
}

func SLogger() *common.LevelLogger {
	return sLog
}

type Server struct {
	conf    *ServerConfig
	stopC   chan struct{}
	wg      sync.WaitGroup
	router  http.Handler
	pdCoord *cluster.PDCoordinator
}

func NewServer(conf *ServerConfig) *Server {
	myNode := &cluster.NodeInfo{
		NodeIP: conf.BroadcastAddr,
	}

	if conf.ClusterID == "" {
		sLog.Fatalf("cluster id can not be empty")
	}
	if conf.BroadcastInterface != "" {
		myNode.NodeIP = common.GetIPv4ForInterfaceName(conf.BroadcastInterface)
	}
	if myNode.NodeIP == "" {
		myNode.NodeIP = conf.BroadcastAddr
	} else {
		conf.BroadcastAddr = myNode.NodeIP
	}
	if myNode.NodeIP == "0.0.0.0" || myNode.NodeIP == "" {
		sLog.Errorf("can not decide the broadcast ip: %v", myNode.NodeIP)
		os.Exit(1)
	}
	_, myNode.HttpPort, _ = net.SplitHostPort(conf.HTTPAddress)
	if conf.ReverseProxyPort != "" {
		myNode.HttpPort = conf.ReverseProxyPort
	}

	sLog.Infof("Start with broadcast ip:%s", myNode.NodeIP)
	myNode.ID = cluster.GenNodeID(myNode, "pd")

	clusterOpts := &cluster.Options{}
	var err error
	if len(conf.BalanceInterval) == 2 {
		clusterOpts.BalanceStart, err = strconv.Atoi(conf.BalanceInterval[0])
		if err != nil {
			sLog.Errorf("invalid balance interval: %v", err)
			os.Exit(1)
		}
		clusterOpts.BalanceEnd, err = strconv.Atoi(conf.BalanceInterval[1])
		if err != nil {
			sLog.Errorf("invalid balance interval: %v", err)
			os.Exit(1)
		}
	}
	s := &Server{
		conf:    conf,
		stopC:   make(chan struct{}),
		pdCoord: cluster.NewPDCoordinator(conf.ClusterID, myNode, clusterOpts),
	}

	r := cluster.NewPDEtcdRegister(conf.ClusterLeadershipAddresses)
	s.pdCoord.SetRegister(r)

	return s
}

func (self *Server) Stop() {
	close(self.stopC)
	self.pdCoord.Stop()
	self.wg.Wait()
	sLog.Infof("server stopped")
}

func (self *Server) Start() {
	err := self.pdCoord.Start()
	if err != nil {
		sLog.Errorf("FATAL: start coordinator failed - %s", err)
		os.Exit(1)
	}

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.serveHttpAPI(self.conf.HTTPAddress, self.stopC)
	}()
}
