package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/kv"
	"github.com/pkg/errors"
	cfgclient "github.com/plainboring/config_client/pkg/tikv"
	"go.etcd.io/etcd/clientv3"
	"path"
	"strconv"
	"sync"
)

// ConfigManager persist and distribute the config of pd and tikv
type ConfigManager struct {
	mu sync.Mutex
	rootPath string
	member   string

	//index_mu sync.Mutex
	//tikvEntryIndex map[uint64]

	client   *clientv3.Client
	baseKV kv.Base
	tikvConfigs map[uint64]*tikvConfig

	option *ScheduleOption
}

type tikvConfig struct {
	store_id uint64
	config *cfgclient.Config
	appliedIndex int
}

// NewConfigManager creates a new ConfigManager.
func NewConfigManager(client *clientv3.Client, rootPath string, member string) *ConfigManager {
	cfg := &ConfigManager{
		rootPath: rootPath,
		client:   client,
		member:   member,
		tikvConfigs: make(map[uint64]*tikvConfig),
		baseKV: kv.NewEtcdKVBase(client, rootPath),
	}

	return cfg
}

//func (c *ConfigManager) InitConfigManager()  {
//	c.mu.Lock()
//	defer c.mu.Unlock()
//	l,err := c.getTikvConfigList()
//	if err != nil {
//		panic(err)
//	}
//
//	c.tikvEntryIndex = int32(len(l))
//}
//
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
	entries,err := c.getTikvConfigList(store_id)
	if err != nil || len(entries) == 0 {
		return nil
	}

	var changed []*configpb.ConfigEntry
	c.mu.Lock()
	cfg,ok := c.tikvConfigs[store_id]
	if !ok {
		panic("there are no tikv store here")
	}

	if cfg.appliedIndex < len(entries) {
		changed = entries[cfg.appliedIndex:]
		c.applyChange(store_id, changed)
		cfg.appliedIndex = len(entries)
	}
	c.mu.Unlock()

	return changed
}

//TODO get config by store_id
func (c *ConfigManager) GetTikvConfig(store_id uint64) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cfg,ok := c.tikvConfigs[store_id]
	if !ok {
		return "",errors.New("there are no tikv in memory")
	}

	tikv_config := bytes.NewBuffer([]byte{})
	err := toml.NewEncoder(tikv_config).Encode(cfg.config)
	if err != nil {
		return "",err
	}

	return tikv_config.String(),nil
}

func (c *ConfigManager) UpdatePDConfig(entry *configpb.ConfigEntry, cfg *Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	buf := bytes.NewBuffer([]byte{})
	if err := toml.NewEncoder(buf).Encode(map[string]string{entry.Name:entry.Value}); err != nil {
		panic(err)
	}

	switch entry.Subsystem[0] {
	case "schedule":
		DecodeIntoConfigSchedule(entry, cfg)
	case "replication":
		DecodeIntoConfigReplication(entry, cfg)
	case "pd-server":
		DecodeIntoConfigPDServer(entry,cfg)
	default:
		return errors.New("unkown subsystem")
	}

	return err
}

func DecodeIntoConfigSchedule(entry *configpb.ConfigEntry, cfg *Config)  {
	uint64_value,uint64_err := strconv.ParseUint(entry.Value, 10, 64)
	float64_value,float64_err := strconv.ParseFloat(entry.Value, 64)
	bool_value,bool_err := strconv.ParseBool(entry.Value)

	switch entry.Name {
	//uint64
	case "max-snapshot-count" :
		if uint64_err == nil {
			cfg.Schedule.MaxSnapshotCount = uint64_value
		}
	case "max-pending-peer-count":
		if uint64_err == nil {
			cfg.Schedule.MaxPendingPeerCount = uint64_value
		}
	case "max-merge-region-size":
		if uint64_err == nil {
			cfg.Schedule.MaxMergeRegionSize = uint64_value
		}
	case "max-merge-region-keys":
		if uint64_err == nil {
			cfg.Schedule.MaxMergeRegionKeys = uint64_value
		}
	case "leader-schedule-limit":
		if uint64_err == nil {
			cfg.Schedule.LeaderScheduleLimit = uint64_value
		}
	case "region-schedule-limit":
		if uint64_err == nil {
			cfg.Schedule.RegionScheduleLimit = uint64_value
		}
	case "replica-schedule-limit":
		if uint64_err == nil {
			cfg.Schedule.ReplicaScheduleLimit = uint64_value
		}
	case "merge-schedule-limit":
		if uint64_err == nil {
			cfg.Schedule.MergeScheduleLimit = uint64_value
		}
	case "hot-region-schedule-limit":
		if uint64_err == nil {
			cfg.Schedule.HotRegionScheduleLimit = uint64_value
		}
	case "hot-region-cache-hits-threshol":
		if uint64_err == nil {
			cfg.Schedule.HotRegionCacheHitsThreshold = uint64_value
		}
	case "scheduler-max-waiting-operator":
		if uint64_err == nil {
			cfg.Schedule.SchedulerMaxWaitingOperator = uint64_value
		}
	//float64
	case "store-balance-rate":
		if float64_err == nil {
			cfg.Schedule.StoreBalanceRate = float64_value
		}
	case "tolerant-size-ratio":
		if float64_err == nil {
			cfg.Schedule.TolerantSizeRatio = float64_value
		}
	case "low-space-ratio":
		if float64_err == nil {
			cfg.Schedule.LowSpaceRatio = float64_value
		}
	case "high-space-ratio":
		if float64_err == nil {
			cfg.Schedule.HighSpaceRatio = float64_value
		}
	//bool
	case "enable-one-way-merge":
		if bool_err == nil {
			cfg.Schedule.EnableOneWayMerge = bool_value
		}
	case "disable-raft-learner":
		if bool_err == nil {
			cfg.Schedule.DisableLearner = bool_value
		}
	case "disable-remove-down-replica":
		if bool_err == nil {
			cfg.Schedule.DisableRemoveDownReplica = bool_value
		}
	case "disable-replace-offline-replica":
		if bool_err == nil {
			cfg.Schedule.DisableReplaceOfflineReplica = bool_value
		}
	case "disable-make-up-replica":
		if bool_err == nil {
			cfg.Schedule.DisableMakeUpReplica = bool_value
		}
	case "disable-remove-extra-replica":
		if bool_err == nil {
			cfg.Schedule.DisableRemoveExtraReplica = bool_value
		}
	case "disable-location-replacement":
		if bool_err == nil {
			cfg.Schedule.DisableLocationReplacement = bool_value
		}
	case "disable-namespace-relocation":
		if bool_err == nil {
			cfg.Schedule.DisableNamespaceRelocation = bool_value
		}
	//other
	default:
		buf := bytes.NewBuffer([]byte{})
		if err := toml.NewEncoder(buf).Encode(map[string]string{entry.Name:entry.Value}); err != nil {
			log.Error(err.Error())
		}
		if _,err := toml.Decode(buf.String(), &cfg.Schedule); err != nil {
			log.Error(err.Error())
		}
	}
}

func DecodeIntoConfigReplication(entry *configpb.ConfigEntry, cfg *Config)  {
	switch entry.Name {
	case "max-replicas":
		uint64_value,uint64_err := strconv.ParseUint(entry.Value, 10, 64)
		if uint64_err == nil {
			cfg.Replication.MaxReplicas = uint64_value
		}
	case "strictly-match-label":
		bool_value,bool_err := strconv.ParseBool(entry.Value)
		if bool_err != nil {
			cfg.Replication.StrictlyMatchLabel = bool_value
		}
	}
}

func DecodeIntoConfigPDServer(entry *configpb.ConfigEntry, cfg *Config)  {
	switch entry.Name {
	case "use-region-storage":
		bool_value,bool_err := strconv.ParseBool(entry.Value)
		if bool_err != nil {
			cfg.PDServerCfg.UseRegionStorage = bool_value
		}
	}
}

func (c *ConfigManager) UpdateTikvConfig(store_id uint64, entry *configpb.ConfigEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	configPath := path.Join(c.rootPath, "tikv")

	if err := c.saveConfig(configPath, store_id, entry); err != nil {
		return err
	}

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

func (c *ConfigManager) saveConfig(configPath string,store_id uint64, entry *configpb.ConfigEntry) error {
	entries,err := c.getTikvConfigList(store_id)
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

	store_path := path.Join(configPath, strconv.FormatUint(store_id, 10))
	resp, err := txn.Then(clientv3.OpPut(store_path, string(data))).Commit()
	if err != nil {
		return errors.WithStack(err)
	}

	if !resp.Succeeded {
		return errors.New("save config failed, maybe we lost leader")
	}

	return nil
	//return c.baseKV.Save(store_path, string(data))
}

func (c *ConfigManager) getTikvConfigList(store_id uint64) ([]*configpb.ConfigEntry,error) {
	kvResp,err := c.baseKV.Load(path.Join("tikv", strconv.FormatUint(store_id, 10)))
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
