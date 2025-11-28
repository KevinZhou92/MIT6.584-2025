package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/util"
	tester "6.5840/tester1"
)

const (
	SHARD_CONFIG_NAME string = "ShardConfig"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	err := sck.IKVClerk.Put(SHARD_CONFIG_NAME, cfg.String(), rpc.Tversion(0))
	if err != rpc.OK {
		panic("InitConfig failed to put shard config in kv service")
	}

}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	// Step 1: Find out changing shards
	curShardConfig := sck.Query()
	newShardConfig := new

	movedShards := make([]shardcfg.Tshid, 0)
	for shardId, groupId := range curShardConfig.Shards {
		newGroupId := newShardConfig.Shards[shardId]
		if groupId == newGroupId {
			continue
		}
		movedShards = append(movedShards, shardcfg.Tshid(shardId))
	}
	util.Debug(util.Info, "###Changing shard config from %v to %v, moved shards %v", curShardConfig, newShardConfig, movedShards)

	// Step 2: Freeze shard
	for _, shardId := range movedShards {
		groupId := curShardConfig.Shards[shardId]
		groupClerk := shardgrp.MakeClerk(sck.clnt, curShardConfig.Groups[groupId])
		groupClerk.FreezeShard(shardId, curShardConfig.Num)
	}

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	shardConfig, version, err := sck.IKVClerk.Get(SHARD_CONFIG_NAME)
	if err != rpc.OK {
		panic("Can't query shard config in kv service")
	}

	util.Debug(util.Info, "@@@Retrieved shard config %s with version %d", shardConfig, version)

	return shardcfg.FromString(shardConfig)
}
