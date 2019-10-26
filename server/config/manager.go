package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/server/kv"
	"github.com/pkg/errors"
	cfgclient "github.com/plainboring/config_client/pkg/tikv"
	"go.etcd.io/etcd/clientv3"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
)

// ConfigManager persist and distribute the config of pd and tikv
type ConfigManager struct {
	mu sync.Mutex
	rootPath string
	member   string

	tikvEntryIndex int32
	client   *clientv3.Client

	tikvConfigs map[uint64]*tikvConfig

	option *ScheduleOption
}

type tikvConfig struct {
	store_id uint64
	config *cfgclient.Config
	appliedIndex int32
}

// NewConfigManager creates a new ConfigManager.
func NewConfigManager(client *clientv3.Client, rootPath string, member string) *ConfigManager {
	cfg := &ConfigManager{
		rootPath: rootPath,
		client:   client,
		member:   member,
		tikvConfigs: make(map[uint64]*tikvConfig),
	}

	return cfg
}

func (c *ConfigManager) InitConfigManager()  {
	c.mu.Lock()
	defer c.mu.Unlock()
	l,err := c.getTikvConfigList()
	if err != nil {
		panic(err)
	}

	c.tikvEntryIndex = int32(len(l))
}

func (c *ConfigManager) NewTikvConfigReport(store_id uint64, config string)  {
	c.mu.Lock()
	raft_store := cfgclient.Config{}
	_,err := toml.Decode(config, &raft_store)
	if err != nil {
		panic(err)
	}
	c.tikvConfigs[store_id] = &tikvConfig{
		store_id: store_id,
		config: &raft_store,
		appliedIndex: 0,
	}
	c.mu.Unlock()
}

func (c *ConfigManager) GetTikvEntries(store_id uint64) []*configpb.ConfigEntry {
	entries,err := c.getTikvConfigList()
	if err != nil || len(entries) == 0 {
		return nil
	}

	var changed []*configpb.ConfigEntry
	c.mu.Lock()
	cfg,ok := c.tikvConfigs[store_id]
	if !ok {
		panic("there are no tikv store here")
	}

	if cfg.appliedIndex < c.tikvEntryIndex {
		cfg.appliedIndex = c.tikvEntryIndex
		changed = entries[cfg.appliedIndex:]
		c.applyChange(store_id, changed)
	}
	c.mu.Unlock()

	return changed
}

//TODO get config by store_id
func (c *ConfigManager) GetTikvConfig() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var cfg *cfgclient.Config
	for _,t_cfg := range c.tikvConfigs {
		cfg = t_cfg.config
	}
	if cfg == nil {
		return "",errors.New("there are no tikv in memory")
	}

	tikv_config := []byte{}
	err := toml.NewEncoder(bytes.NewBuffer(tikv_config)).Encode(cfg)
	if err != nil {
		return "",err
	}

	return string(tikv_config),nil
}

func (c *ConfigManager) UpdatePDConfig(entry *configpb.ConfigEntry, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	buf := []byte{}
	if err := toml.NewEncoder(bytes.NewBuffer(buf)).Encode(map[string]string{entry.Name:entry.Value}); err != nil {
		panic(err)
	}

	switch entry.Subsystem[0] {
	case "schedule":
		_,err = toml.Decode(string(buf), &cfg.Schedule)
	case "replication":
		_,err = toml.Decode(string(buf), &cfg.Replication)
	case "pd-server":
		_,err = toml.Decode(string(buf), &cfg.PDServerCfg)
	default:
		return errors.New("unkown subsystem")
	}

	return err
}


func (c *ConfigManager) UpdateTikvConfig(entry *configpb.ConfigEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	configPath := path.Join(c.rootPath, "tikv")

	if err := c.saveConfig(configPath, entry); err != nil {
		return err
	}

	atomic.AddInt32(&c.tikvEntryIndex, 1)
	return nil
}

func (c *ConfigManager) applyChange(store_id uint64, entry []*configpb.ConfigEntry) {
	cfg := c.tikvConfigs[store_id]

	for _,e := range entry {
		num_value,num_err := strconv.ParseInt(e.Value, 10, 64)
		bool_value,bool_err := strconv.ParseBool(e.Value)

		switch e.Name {
		case "raft-heartbeat-ticks":
			if num_err == nil {
				cfg.config.Raftstore.RaftHeartbeatTicks = num_value
			}
		case "raft-election-timeout-ticks":
			if num_err == nil {
				cfg.config.Raftstore.RaftElectionTimeoutTicks = num_value
			}
		case "raft-log-gc-threshold":
			if num_err == nil {
				cfg.config.Raftstore.RaftLogGCThreshold = num_value
			}
		case "raft-log-gc-count-limit":
			if num_err == nil {
				cfg.config.Raftstore.RaftLogGCCountLimit = num_value
			}
		case "region-compact-check-step":
			if num_err == nil {
				cfg.config.Raftstore.RegionCompactCheckStep = num_value
			}
		case "region-compact-min-tombstones":
			if num_err == nil {
				cfg.config.Raftstore.RegionCompactMinTombstones = num_value
			}
		case "region-compact-tombstones-percent":
			if num_err == nil {
				cfg.config.Raftstore.RegionCompactTombstonesPercent = num_value
			}
		case "notify-capacity":
			if num_err == nil {
				cfg.config.Raftstore.NotifyCapacity = num_value
			}
		case "messages-per-tick":
			if num_err == nil {
				cfg.config.Raftstore.MessagesPerTick = num_value
			}
		case "leader-transfer-max-log-lag":
			if num_err == nil {
				cfg.config.Raftstore.LeaderTransferMaxLogLag = num_value
			}
		case "merge-max-log-gap":
			if num_err == nil {
				cfg.config.Raftstore.MergeMaxLogGap = num_value
			}
		case "apply-max-batch-size":
			if num_err == nil {
				cfg.config.Raftstore.ApplyMaxBatchSize = num_value
			}
		case "apply-pool-size":
			if num_err == nil {
				cfg.config.Raftstore.ApplyPoolSize = num_value
			}
		case "store-max-batch-size":
			if num_err == nil {
				cfg.config.Raftstore.StoreMaxBatchSize = num_value
			}
		case "store-pool-size":
			if num_err == nil {
				cfg.config.Raftstore.StorePoolSize = num_value
			}
		case "sync-log":
			if bool_err == nil {
				cfg.config.Raftstore.SyncLog = bool_value
			}
		case "right-derive-when-split":
			if bool_err == nil {
				cfg.config.Raftstore.RightDeriveWhenSplit = bool_value
			}
		case "allow-remove-leader":
			if bool_err == nil {
				cfg.config.Raftstore.AllowRemoveLeader = bool_value
			}
		case "use-delete-range":
			if bool_err == nil {
				cfg.config.Raftstore.UseDeleteRange = bool_value
			}
		case "hibernate-regions":
			if bool_err == nil {
				cfg.config.Raftstore.HibernateRegions = bool_value
			}
		default:
			buf := bytes.NewBuffer([]byte{})
			if err := toml.NewEncoder(buf).Encode(map[string]string{e.Name: e.Value}); err != nil {
				panic(err)
			}
			fmt.Println(buf.String(), buf.Len())
			if err := toml.Unmarshal(buf.Bytes(), &cfg.config.Raftstore); err != nil {
				panic(err)
			}
		}
	}

	c.tikvConfigs[store_id] = cfg
}

func (c *ConfigManager) saveConfig(configPath string, entry *configpb.ConfigEntry) error {
	entries,err := c.getTikvConfigList()
	if err != nil {
		return err
	}

	entries = append(entries, entry)
	data,err := json.Marshal(entries)
	if err != nil {
		return err
	}

	leaderPath := path.Join(c.rootPath, "leader")
	txn := kv.NewSlowLogTxn(c.client).If(append([]clientv3.Cmp{}, clientv3.Compare(clientv3.Value(leaderPath), "=", c.member))...)
	resp, err := txn.Then(clientv3.OpPut(configPath, string(data))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}

	if !resp.Succeeded {
		return errors.New("save config failed, maybe we lost leader")
	}

	return nil
}

func (c *ConfigManager) getTikvConfigList() ([]*configpb.ConfigEntry,error) {
	kvResp,err := kv.NewEtcdKVBase(c.client, c.rootPath).Load("tikv")
	if err != nil {
		return nil,err
	}
	if kvResp == "" {
		return nil,nil
	}
	entries := []*configpb.ConfigEntry{}
	err = json.Unmarshal([]byte(kvResp), &entries)
	if err != nil {
		return nil,err
	}
	return entries,nil
}
