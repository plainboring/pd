package config

import (
	"bytes"
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

func (c *ConfigManager) NewTikvConfigReport(store_id uint64, config string)  {
	raft_store := &cfgclient.Config{}
	_,err := toml.Decode(config, raft_store)
	if err != nil {
		panic(err)
	}
	log.Info(fmt.Sprintf("%+v", raft_store))
	cfg := c.GetLatestTikvConfig(store_id)
	if cfg == nil {
		if err := c.SaveTikvConfigIfNotExist(store_id, config); err != nil {
			log.Error(err.Error())
		}
	} else {
		raft_store = cfg
	}
	//log.Info(config)
	log.Info(fmt.Sprintf("record %v tikv config %+v", store_id, raft_store))
	c.mu.Lock()
	c.tikvConfigs[store_id] = &tikvConfig{
		store_id: store_id,
		config: raft_store,
	}
	c.mu.Unlock()
}

func (c *ConfigManager) GetLatestTikvConfig(store_id uint64) *cfgclient.Config {
	configPath := path.Join("tikv", strconv.FormatUint(store_id, 10))
	cfg,err := c.baseKV.Load(configPath)
	if err != nil || cfg == "" {
		return nil
	}

	raft_store := &cfgclient.Config{}
	if _,err = toml.Decode(cfg, raft_store); err != nil {
		panic(err)
	}
	return raft_store
}

func (c *ConfigManager) ApplyNewConfigForTikv(store_id uint64, entry  *configpb.ConfigEntry) {
	latest_config := c.GetLatestTikvConfig(store_id)
	if latest_config == nil {
		return
	}
	switch entry.Subsystem[0] {
	case "server":
		c.DecodeTikvServerConfig(latest_config, entry)
	case "storage":
		c.DecodeTikvStorageConfig(latest_config, entry)
	case "raftstore":
		c.DecodeTikvRaftStorageConfig(latest_config, entry)
	case "storage,block-cache":
		c.DecodeTikvStorageBlockCacheConfig(latest_config, entry)
	}

	buf := bytes.NewBuffer([]byte{})
	if err := toml.NewEncoder(buf).Encode(latest_config); err != nil {
		log.Error(err.Error())
	}

	if err := c.SaveTikvConfig(store_id, buf.String()); err != nil {
		log.Error(err.Error())
	}
}

func (c *ConfigManager) SaveTikvConfigIfNotExist(store_id uint64, config string) error {
	configPath := path.Join("tikv", strconv.FormatUint(store_id, 10))
	cfg,err := c.baseKV.Load(configPath)
	if err != nil {
		return err
	}
	if cfg != "" {
		return nil
	}

	return c.SaveTikvConfig(store_id, config)
}

func (c *ConfigManager) SaveTikvConfig(store_id uint64, config string) error {
	configPath := path.Join("tikv", strconv.FormatUint(store_id, 10))
	return c.baseKV.Save(configPath, config)
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

func (c *ConfigManager) DecodeTikvServerConfig(cfg *cfgclient.Config, entry *configpb.ConfigEntry)  {
	num_value,num_err := strconv.ParseInt(entry.Value, 10, 64)
	bool_value,bool_err := strconv.ParseBool(entry.Value)
	switch entry.Name {
	case "grpc-concurrency":
		if num_err == nil {
			cfg.Server.GrpcConcurrency = num_value
		}
	case "grpc-concurrent-stream":
		if num_err == nil {
			cfg.Server.GrpcConcurrentStream = num_value
		}
	case "grpc-raft-conn-num":
		if num_err == nil {
			cfg.Server.GrpcRaftConnNum = num_value
		}
	case "concurrent-send-snap-limit":
		if num_err == nil {
			cfg.Server.ConcurrentSendSnapLimit = num_value
		}
	case "concurrent-recv-snap-limit":
		if num_err == nil {
			cfg.Server.ConcurrentRecvSnapLimit = num_value
		}
	case "end-point-recursion-limit":
		if num_err == nil {
			cfg.Server.EndPointRecursionLimit = num_value
		}
	case "end-point-stream-channel-size":
		if num_err == nil {
			cfg.Server.EndPointStreamChannelSize = num_value
		}
	case "end-point-batch-row-limit":
		if num_err == nil {
			cfg.Server.EndPointBatchRowLimit = num_value
		}
	case "end-point-stream-batch-row-limit":
		if num_err == nil {
			cfg.Server.EndPointStreamBatchRowLimit = num_value
		}
	case "stats-concurrency":
		if num_err == nil {
			cfg.Server.StatsConcurrency = num_value
		}
	case "heavy-load-threshold":
		if num_err == nil {
			cfg.Server.HeavyLoadThreshold = num_value
		}
	case "end-point-enable-batch-if-possible":
		if bool_err == nil {
			cfg.Server.EndPointEnableBatchIfPossible = bool_value
		}
	case "labels":
		//skip
	default:
		buf := bytes.NewBuffer([]byte{})
		if err := toml.NewEncoder(buf).Encode(map[string]string{entry.Name: entry.Value}); err != nil {
			panic(err)
		}
		fmt.Println(buf.String(), buf.Len())
		if err := toml.Unmarshal(buf.Bytes(), &cfg.Server); err != nil {
			panic(err)
		}
	}
}

func (c *ConfigManager) DecodeTikvStorageConfig(cfg *cfgclient.Config, entry *configpb.ConfigEntry)  {
	num_value,num_err := strconv.ParseInt(entry.Value, 10, 64)
	switch entry.Name {
	case "max-key-size":
		if num_err == nil {
			cfg.Storage.MaxKeySize = num_value
		}
	case "scheduler-notify-capacity":
		if num_err == nil {
			cfg.Storage.SchedulerNotifyCapacity = num_value
		}
	case "scheduler-concurrency":
		if num_err == nil {
			cfg.Storage.SchedulerConcurrency = num_value
		}
	case "scheduler-worker-pool-size":
		if num_err == nil {
			cfg.Storage.SchedulerWorkerPoolSize = num_value
		}
	default:
		buf := bytes.NewBuffer([]byte{})
		if err := toml.NewEncoder(buf).Encode(map[string]string{entry.Name: entry.Value}); err != nil {
			panic(err)
		}
		fmt.Println(buf.String(), buf.Len())
		if err := toml.Unmarshal(buf.Bytes(), &cfg.Storage); err != nil {
			panic(err)
		}
	}
}

func (c *ConfigManager)  DecodeTikvStorageBlockCacheConfig(cfg *cfgclient.Config, entry *configpb.ConfigEntry) {
	num_value,num_err := strconv.ParseInt(entry.Value, 10, 64)
	float64_value,float64_err := strconv.ParseFloat(entry.Value, 64)
	bool_value,bool_err := strconv.ParseBool(entry.Value)

	switch entry.Name {
	case "shared":
		if bool_err == nil {
			cfg.Storage.BlockCache.Shared = bool_value
		}
	case "strict-capacity-limit":
		if bool_err == nil {
			cfg.Storage.BlockCache.StrictCapacityLimit = bool_value
		}
	//int64
	case "num-shard-bits":
		if num_err == nil {
			cfg.Storage.BlockCache.NumShardBits = num_value
		}
	//float64
	case "high-pri-pool-ratio":
		if float64_err == nil {
			cfg.Storage.BlockCache.HighPriPoolRatio = float64_value
		}
	default:
		buf := bytes.NewBuffer([]byte{})
		if err := toml.NewEncoder(buf).Encode(map[string]string{entry.Name: entry.Value}); err != nil {
			panic(err)
		}
		fmt.Println(buf.String(), buf.Len())
		if err := toml.Unmarshal(buf.Bytes(), &cfg.Storage.BlockCache); err != nil {
			panic(err)
		}
	}

}

func (c *ConfigManager) DecodeTikvRaftStorageConfig(cfg *cfgclient.Config, entry *configpb.ConfigEntry)  {
	num_value,num_err := strconv.ParseInt(entry.Value, 10, 64)
	bool_value,bool_err := strconv.ParseBool(entry.Value)

	switch entry.Name {
	case "raft-heartbeat-ticks":
		if num_err == nil {
			cfg.Raftstore.RaftHeartbeatTicks = num_value
		}
	case "raft-election-timeout-ticks":
		if num_err == nil {
			cfg.Raftstore.RaftElectionTimeoutTicks = num_value
		}
	case "raft-log-gc-threshold":
		if num_err == nil {
			cfg.Raftstore.RaftLogGCThreshold = num_value
		}
	case "raft-log-gc-count-limit":
		if num_err == nil {
			cfg.Raftstore.RaftLogGCCountLimit = num_value
		}
	case "region-compact-check-step":
		if num_err == nil {
			cfg.Raftstore.RegionCompactCheckStep = num_value
		}
	case "region-compact-min-tombstones":
		if num_err == nil {
			cfg.Raftstore.RegionCompactMinTombstones = num_value
		}
	case "region-compact-tombstones-percent":
		if num_err == nil {
			cfg.Raftstore.RegionCompactTombstonesPercent = num_value
		}
	case "notify-capacity":
		if num_err == nil {
			cfg.Raftstore.NotifyCapacity = num_value
		}
	case "messages-per-tick":
		if num_err == nil {
			cfg.Raftstore.MessagesPerTick = num_value
		}
	case "leader-transfer-max-log-lag":
		if num_err == nil {
			cfg.Raftstore.LeaderTransferMaxLogLag = num_value
		}
	case "merge-max-log-gap":
		if num_err == nil {
			cfg.Raftstore.MergeMaxLogGap = num_value
		}
	case "apply-max-batch-size":
		if num_err == nil {
			cfg.Raftstore.ApplyMaxBatchSize = num_value
		}
	case "apply-pool-size":
		if num_err == nil {
			cfg.Raftstore.ApplyPoolSize = num_value
		}
	case "store-max-batch-size":
		if num_err == nil {
			cfg.Raftstore.StoreMaxBatchSize = num_value
		}
	case "store-pool-size":
		if num_err == nil {
			cfg.Raftstore.StorePoolSize = num_value
		}
	case "sync-log":
		if bool_err == nil {
			cfg.Raftstore.SyncLog = bool_value
		}
	case "right-derive-when-split":
		if bool_err == nil {
			cfg.Raftstore.RightDeriveWhenSplit = bool_value
		}
	case "allow-remove-leader":
		if bool_err == nil {
			cfg.Raftstore.AllowRemoveLeader = bool_value
		}
	case "use-delete-range":
		if bool_err == nil {
			cfg.Raftstore.UseDeleteRange = bool_value
		}
	case "hibernate-regions":
		if bool_err == nil {
			cfg.Raftstore.HibernateRegions = bool_value
		}
	default:
		buf := bytes.NewBuffer([]byte{})
		if err := toml.NewEncoder(buf).Encode(map[string]string{entry.Name: entry.Value}); err != nil {
			panic(err)
		}
		fmt.Println(buf.String(), buf.Len())
		if err := toml.Unmarshal(buf.Bytes(), &cfg.Raftstore); err != nil {
			panic(err)
		}
	}
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