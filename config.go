package dvara

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

type Conf struct {
	MessageTimeout          uint        `yaml:"message_timeout"`
	ClientIdleTimeout       uint        `yaml:"client_idle_timeout"`
	ServerIdleTimeout       uint        `yaml:"server_idle_timeout"`
	ServerClosePoolSize     uint        `yaml:"server_close_pool_size"`
	GetLastErrorTimeout     uint        `yaml:"get_last_error_timeout"`
	MaxPerClientConnections uint        `yaml:"max_per_client_connections"`
	MaxConnections          uint        `yaml:"max_connections"`
	ProxyConfigs            []ProxyConf `yaml:"proxies"`
}

type ProxyConf struct {
	Listen   string `yaml:"listen"`
	Server   string `yaml:"server"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
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