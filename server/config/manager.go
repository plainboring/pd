package config

import (
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/server/kv"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"path"
	"sync"
)

// ConfigManager persist and distribute the config of pd and tikv
type ConfigManager struct {
	mu sync.Mutex
	rootPath string
	member   string
	client   *clientv3.Client
}

// NewConfigManager creates a new ConfigManager.
func NewConfigManager(client *clientv3.Client, rootPath string, member string) *ConfigManager {
	return &ConfigManager{
		rootPath: rootPath,
		client:   client,
		member:   member,
	}
}

func (c *ConfigManager) GetPDConfig() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	configPath := path.Join(c.rootPath, "pd")

	return c.getConfig(configPath)
}

func (c *ConfigManager) GetTikvConfig() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	configPath := path.Join(c.rootPath, "tikv")

	return c.getConfig(configPath)
}

func (c *ConfigManager) getConfig(configPath string) (string, error) {
	resp,err := etcdutil.EtcdKVGet(c.client, configPath)
	if err != nil {
		return "", errors.WithStack(err)
	}

	if len(resp.Kvs) == 0 {
		return "",nil
	}

	return string(resp.Kvs[0].Value), nil
}

func (c *ConfigManager) UpdatePDConfig(config string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	configPath := path.Join(c.rootPath, "pd")

	return c.saveConfig(configPath, config)
}

func (c *ConfigManager) UpdateTikvConfig(config string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	configPath := path.Join(c.rootPath, "tikv")

	return c.saveConfig(configPath, config)
}

func (c *ConfigManager) saveConfig(configPath, configContent string) error {
	leaderPath := path.Join(c.rootPath, "leader")
	txn := kv.NewSlowLogTxn(c.client).If(append([]clientv3.Cmp{}, clientv3.Compare(clientv3.Value(leaderPath), "=", c.member))...)
	resp, err := txn.Then(clientv3.OpPut(configPath, configContent)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.New("save config failed, maybe we lost leader")
	}

	return nil
}
