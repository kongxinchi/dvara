package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kongxinchi/dvara"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/sevlyar/go-daemon"
	"syscall"
	"path/filepath"
)

var (
	signal = flag.String("s", "", `send signal to the daemon stop — graceful shutdown`)
	configPath = flag.String("config", "", "config file path")
)

func waitChildExit(child *os.Process, reborn chan bool) {
	ps, err := child.Wait()
	if err != nil {
		fmt.Println("child exit unexpected:", err)
		reborn <- true
	}
	if !ps.Success() {
		fmt.Println("child exit unexpected:", ps)
		reborn <- true
	}
	reborn <- false
}

func main() {
	flag.Parse()
	daemon.AddCommand(daemon.StringFlag(signal, "stop"), syscall.SIGTERM, termHandler)

	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	
	context := &daemon.Context{
		PidFileName: filepath.Join(dir, "run.pid"),
		PidFilePerm: 0644,
		LogFileName: filepath.Join(dir, "run.log"),
		LogFilePerm: 060,
		WorkDir:     "./",
		Umask:       027,
		//Args:        []string{"-config", *configPath},
	}

	if len(daemon.ActiveFlags()) > 0 {
		d, err := context.Search()
		if err != nil {
			fmt.Println("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		for {
			err := d.Signal(syscall.Signal(0))
			if err != nil {
				break
			}
			fmt.Printf("waiting %d exit...\n", d.Pid)
			time.Sleep(time.Second)
		}
		return
	}

	child, err := context.Reborn()
	if err != nil {
		fmt.Printf("Unable to run: %s\n", err)
	}

	if child != nil {
		// supervisor in parent process
		reborn := make(chan bool)

		go waitChildExit(child, reborn)

		for v := range reborn {
			if !v {
				break
			}
			child, _ = context.Reborn()
			go waitChildExit(child, reborn)
		}

	} else {

		defer context.Release()

		if err := Main(); err != nil {
			fmt.Println(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func Main() error {

	conf, err := dvara.InitConf(*configPath)
	if err != nil {
		fmt.Printf("Unable load config: %s\n", err)
	}

	logger := dvara.InitLogger(conf)

	proxyManager := &dvara.ProxyManager{
		ProxyConfigs:            conf.ProxyConfigs,
		MessageTimeout:          time.Duration(conf.MessageTimeout) * time.Second,
		ClientIdleTimeout:       time.Duration(conf.ClientIdleTimeout) * time.Second,
		MaxPerClientConnections: conf.MaxPerClientConnections,
		MaxServerConnections:    int32(conf.MaxServerConnections),
		MaxResponseWait:         int32(conf.MaxResponseWait),
		Debug:                   conf.Debug,
	}

	var graph inject.Graph
	err = graph.Provide(
		&inject.Object{Value: logger},
		&inject.Object{Value: proxyManager},
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

	go proxyManager.HttpProfListen()

	err = daemon.ServeSignals()
	if err != nil {
		return err
	}
	return nil
}

func termHandler(sig os.Signal) error {
	return daemon.ErrStop
}
