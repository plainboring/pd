package config

import (
	"github.com/pingcap/kvproto/pkg/configpb"
	cfgclient "github.com/plainboring/config_client/pkg/tikv"
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