package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kongxinchi/dvara"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/facebookgo/stats"
)

func main() {
	if err := Main(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Main() error {
	configPath := flag.String("config", "", "config file path")
	flag.Parse()

	conf, err := dvara.InitConf(*configPath)


	replicaSet := dvara.ReplicaSet{
		ProxyConfigs:            conf.ProxyConfigs,
		MessageTimeout:          time.Duration(conf.MessageTimeout) * time.Second,
		ClientIdleTimeout:       time.Duration(conf.ClientIdleTimeout) * time.Second,
		ServerIdleTimeout:       time.Duration(conf.ServerIdleTimeout) * time.Second,
		ServerClosePoolSize:     conf.ServerClosePoolSize,
		GetLastErrorTimeout:     time.Duration(conf.GetLastErrorTimeout) * time.Second,
		MaxConnections:          conf.MaxConnections,
		MaxPerClientConnections: conf.MaxPerClientConnections,
	}

	var statsClient stats.HookClient
	var log stdLogger
	var graph inject.Graph
	err = graph.Provide(
		&inject.Object{Value: &log},
		&inject.Object{Value: &replicaSet},
		&inject.Object{Value: &statsClient},
	)
	if err != nil {
		return err
	}
	if err := graph.Populate(); err != nil {
		return err
	}
	objects := graph.Objects()

	if err := startstop.Start(objects, &log); err != nil {
		return err
	}
	defer startstop.Stop(objects, &log)

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	return nil
}
