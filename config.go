package dvara

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"go.uber.org/zap"
)

type Conf struct {
	MessageTimeout          uint         `yaml:"message_timeout"`
	ClientIdleTimeout       uint         `yaml:"client_idle_timeout"`
	MaxPerClientConnections uint         `yaml:"max_per_client_connections"`
	MaxServerConnections    uint         `yaml:"max_server_connections"`
	MaxResponseWait         uint         `yaml:"max_response_wait"`
	LogConfig               zap.Config   `yaml:"log"`
	ProxyConfigs            []ProxyConf  `yaml:"proxies"`
	InfluxdbConf            InfluxdbConf `yaml:"influxdb"`
	Debug                   bool         `yaml:"debug"`
	Deployment              string       `yaml:"deployment"`
	App                     string       `yaml:"app"`
}

type ProxyConf struct {
	Listen   string `yaml:"listen"`
	Server   string `yaml:"server"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type InfluxdbConf struct {
	Server     string `yaml:"server"`
	DB         string `yaml:"db"`
	AppName    string `yaml:"app_name"`
}

func InitConf(path string) (*Conf, error) {

	conf := &Conf{}
	err := conf.loadConf(path)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (c *Conf) loadConf(path string) error {

	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		return err
	}
	return nil
}