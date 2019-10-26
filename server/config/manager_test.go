package config

import (
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/server/kv"
	cfgclient "github.com/plainboring/config_client/pkg/tikv"
	"reflect"
	"testing"
)

func TestTIKVReload(t *testing.T)  {
	manager := ConfigManager{
		tikvConfigs: make(map[uint64]*tikvConfig),
	}

	manager.tikvConfigs[1] = &tikvConfig{
		store_id: 1,
		config: &cfgclient.Config{},
		appliedIndex: 0,
	}

	ens := []*configpb.ConfigEntry{
		{
			Name: "raft-log-gc-count-limit",
			Value: "1000",
		},
		{
			Name: "hibernate-regions",
			Value: "true",
		},
		{
			Name: "raft-store-max-leader-lease",
			Value: "string",
		},
	}

	manager.applyChange(1, ens)
	if manager.tikvConfigs[1].config.Raftstore.RaftLogGCCountLimit != 1000{
		t.Fatal(manager.tikvConfigs[1].config.Raftstore.RaftLogGCCountLimit)
	}

	if !manager.tikvConfigs[1].config.Raftstore.HibernateRegions {
		t.Fatal(manager.tikvConfigs[1].config.Raftstore.HibernateRegions)
	}
	if manager.tikvConfigs[1].config.Raftstore.RaftStoreMaxLeaderLease != "string" {
		t.Fatal(manager.tikvConfigs[1].config.Raftstore.RaftStoreMaxLeaderLease)
	}
}

func TestTikvWorkLoad(t *testing.T) {
	var store_id uint64 = 1
	var other_store_id uint64 = 2

	manager := ConfigManager{
		tikvConfigs: make(map[uint64]*tikvConfig),
		baseKV: kv.NewMemoryKV(),
	}

	//tikv putstore and report itself
	initConfig := cfgclient.Config{}
	initConfig.Raftstore.RaftLogGCCountLimit = 10
	buf := bytes.NewBuffer([]byte{})
	if err := toml.NewEncoder(buf).Encode(initConfig); err != nil {
		t.Fatal(err)
	}
	//t.Log(buf.String())
	manager.NewTikvConfigReport(store_id, buf.String())
	manager.NewTikvConfigReport(other_store_id, buf.String())

	//tidb get tikv config
	str,err := manager.GetTikvConfig(store_id)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(str)

	newCfg := cfgclient.Config{}
	if _,err := toml.Decode(str, &newCfg); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(initConfig, newCfg) {
		t.Fatal(str)
		t.Fatal(initConfig, newCfg)
	}

	//tidb update tikv config
	entry := &configpb.ConfigEntry{
		Subsystem: []string{},
		Name: "raft-log-gc-count-limit",
		Value: "100",
	}

	if err := manager.UpdateTikvConfig(store_id, entry); err != nil {
		t.Fatal(err)
	}
	//if manager.tikvConfigs[store_id].config.Raftstore.RaftLogGCCountLimit != 100 {
	//	t.Fatal(manager.tikvConfigs[store_id].config.Raftstore.RaftLogGCCountLimit)
	//}

	//tikv heartbeat and receive entry
	entries := manager.GetTikvEntries(store_id)
	if len(entries) != 1 {
		t.Fatal(entries)
	}
	if entries[0].Name != entry.Name || entries[0].Value != entry.Value {
		t.Fatal(entries[0], entry)
	}
	entries = manager.GetTikvEntries(other_store_id)
	if len(entries) != 0 {
		t.Fatal(entries)
	}

	//tikv heartbear and receive nothing
	entries = manager.GetTikvEntries(store_id)
	if len(entries) != 0 {
		t.Fatal(entries)
	}

	//tidb get tikv config
	str,err = manager.GetTikvConfig(store_id)
	if err != nil {
		t.Fatal(err)
	}
	newCfg1 := cfgclient.Config{}
	if _,err := toml.Decode(str, &newCfg1); err != nil {
		t.Fatal(err)
	}
	if newCfg1.Raftstore.RaftLogGCCountLimit != 100 {
		t.Fatal(newCfg1.Raftstore.RaftLogGCCountLimit)
	}
}

func TestNewTikvUpdate(t *testing.T)  {
	manager := ConfigManager{
		tikvConfigs: make(map[uint64]*tikvConfig),
		baseKV: kv.NewMemoryKV(),
	}

	var store_id uint64 = 1
	initConfig := cfgclient.Config{}
	initConfig.Raftstore.RaftLogGCCountLimit = 100
	buf := bytes.NewBuffer([]byte{})
	if err := toml.NewEncoder(buf).Encode(initConfig); err != nil {
		t.Fatal(err)
	}

	manager.NewTikvConfigReport(store_id, buf.String())

	cfg := manager.GetLatestTikvConfig(store_id)
	if cfg.Raftstore.RaftLogGCCountLimit != 100 {
		t.Fatal(cfg.Raftstore.RaftLogGCCountLimit)
	}

	manager.ApplyNewConfigForTikv(store_id, &configpb.ConfigEntry{Subsystem:[]string{"raftstore"}, Name:"store-pool-size",Value:"100"})
	cfg = manager.GetLatestTikvConfig(store_id)
	if cfg.Raftstore.StorePoolSize != 100 {
		t.Fatal(cfg.Raftstore.RaftLogGCCountLimit)
	}

}