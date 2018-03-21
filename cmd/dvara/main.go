package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	"log"

	"github.com/kongxinchi/dvara"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/op/go-logging"
)

func main() {
	if err := Main(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func InitLogger(conf *dvara.Conf) (*logging.Logger, error) {
	logger := logging.MustGetLogger("dvara")

	logLevel, err := logging.LogLevel(conf.LogLevel)
	if err != nil {
		return nil, err
	}
	logging.SetLevel(logLevel, "dvara")

	logFile := conf.LogFile
	logIO, err := os.OpenFile(logFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.ModeType)
	if err != nil {
		return nil, err
	}

	backend := logging.NewLogBackend(logIO, "", log.Ldate | log.Lmicroseconds)
	backendLeveled := logging.AddModuleLevel(backend)
	backendLeveled.SetLevel(logLevel, "")
	logger.SetBackend(backendLeveled)
	return logger, nil
}

func Main() error {
	configPath := flag.String("config", "", "config file path")
	flag.Parse()

	conf, err := dvara.InitConf(*configPath)

	logger, err := InitLogger(conf)
	if err != nil {
		return err
	}

	replicaSet := dvara.ReplicaSet {
		ProxyConfigs:            conf.ProxyConfigs,
		MessageTimeout:          time.Duration(conf.MessageTimeout) * time.Second,
		ClientIdleTimeout:       time.Duration(conf.ClientIdleTimeout) * time.Second,
		ServerIdleTimeout:       time.Duration(conf.ServerIdleTimeout) * time.Second,
		ServerClosePoolSize:     conf.ServerClosePoolSize,
		MaxConnections:          conf.MaxConnections,
		MaxPerClientConnections: conf.MaxPerClientConnections,
	}

	var graph inject.Graph
	err = graph.Provide(
		&inject.Object{Value: logger},
		&inject.Object{Value: &replicaSet},
	)
	if err != nil {
		return err
	}
	if err := graph.Populate(); err != nil {
		return err
	}
	objects := graph.Objects()

	if err := startstop.Start(objects, logger); err != nil {
		return err
	}
	defer startstop.Stop(objects, logger)

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	return nil
}
