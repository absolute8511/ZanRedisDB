package main

import (
	"flag"
	"fmt"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/pdserver"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/absolute8511/glog"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
)

var (
	flagSet = flag.NewFlagSet("placedriver", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress        = flagSet.String("http-address", "0.0.0.0:18001", "<addr>:<port> to listen on for HTTP clients")
	broadcastAddress   = flagSet.String("broadcast-address", "", "address of this lookupd node, (default to the OS hostname)")
	broadcastInterface = flagSet.String("broadcast-interface", "", "address of this lookupd node, (default to the OS hostname)")
	reverseProxyPort   = flagSet.String("reverse-proxy-port", "", "<port> for reverse proxy")

	clusterLeadershipAddresses = flagSet.String("cluster-leadership-addresses", "", " the cluster leadership server list")
	clusterID                  = flagSet.String("cluster-id", "test-cluster", "the cluster id used for separating different nsq cluster.")

	logLevel        = flagSet.Int("log-level", 1, "log verbose level")
	logDir          = flagSet.String("log-dir", "", "directory for log file")
	balanceInterval = common.StringArray{}
)

func init() {
	flagSet.Var(&balanceInterval, "balance-interval", "the balance time interval")
}

type program struct {
	placedriver *pdserver.Server
}

func main() {
	defer glog.Flush()
	prg := &program{}
	if err := svc.Run(prg, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGINT); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	glog.InitWithFlag(flagSet)

	flagSet.Parse(os.Args[1:])
	if *showVersion {
		fmt.Println(common.VerString("placedriver"))
		os.Exit(0)
	}

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := pdserver.NewServerConfig()
	options.Resolve(opts, flagSet, cfg)
	if opts.LogDir != "" {
		glog.SetGLogDir(opts.LogDir)
	}
	glog.StartWorker(time.Second * 2)
	daemon := pdserver.NewServer(opts)

	daemon.Start()
	p.placedriver = daemon
	return nil
}

func (p *program) Stop() error {
	if p.placedriver != nil {
		p.placedriver.Stop()
	}
	return nil
}
