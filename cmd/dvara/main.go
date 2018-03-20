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


	//messageTimeout := flag.Duration("message_timeout", 2*time.Minute, "timeout for one message to be proxied")
	//clientIdleTimeout := flag.Duration("client_idle_timeout", time.Second, "idle timeout for client connections")
	//serverIdleTimeout := flag.Duration("server_idle_timeout", 1*time.Hour, "idle timeout for  server connections")
	//serverClosePoolSize := flag.Uint("server_close_pool_size", 100, "number of goroutines that will handle closing server connections")
	//getLastErrorTimeout := flag.Duration("get_last_error_timeout", time.Minute, "timeout for getLastError pinning")
	//maxPerClientConnections := flag.Uint("max_per_client_connections", 100, "maximum number of connections per client")
	//maxConnections := flag.Uint("max_connections", 100, "maximum number of connections per mongo")
	//portStart := flag.Int("port_start", 6000, "start of port range")
	//portEnd := flag.Int("port_end", 6010, "end of port range")
	//addrs := flag.String("addrs", "localhost:27017", "comma separated list of mongo addresses")
	// TODO 从配置中获取监听端口，地址，用户名，密码




	replicaSet := dvara.ReplicaSet{
		//Addrs:                   *addrs,
		//PortStart:               *portStart,
		//PortEnd:                 *portEnd,
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
