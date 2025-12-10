package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	SHARD_CONFIG_NAME      string = "ShardConfig"
	NEXT_SHARD_CONFIG_NAME string = "NextShardConfig"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu sync.Mutex
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
	curConfig := sck.Query()

	nextConfig, _, err := sck.IKVClerk.Get(NEXT_SHARD_CONFIG_NAME)
	if err != rpc.OK && err != rpc.ErrMaybe {
		panic("[Shard Ctrl] InitController failed to get next shard config in kv service")
	}

	newShardConfig := shardcfg.FromString(nextConfig)
	if curConfig.Num == newShardConfig.Num {
		// log.Println("[Shard Ctrl] InitController: current config matches next config, no recovery needed")
		return
	}
	sck.ChangeConfigTo(newShardConfig)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	// log.Printf("[Shard Ctrl] Initialize shard controller with config %v\n", cfg)
	err := sck.IKVClerk.Put(SHARD_CONFIG_NAME, cfg.String(), rpc.Tversion(0))
	if err != rpc.OK && err != rpc.ErrMaybe {
		log.Printf("[Shard Ctrl] InitConfig failed to put shard config in kv service %v", err)
		panic("InitConfig failed to put shard config in kv service")
	}
	err = sck.IKVClerk.Put(NEXT_SHARD_CONFIG_NAME, cfg.String(), rpc.Tversion(0))
	if err != rpc.OK && err != rpc.ErrMaybe {
		log.Printf("[Shard Ctrl] InitConfig failed to put next shard config in kv service %v", err)
		panic("InitConfig failed to put next shard config in kv service")
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	sck.mu.Lock()
	defer sck.mu.Unlock()

	// Step 1: Find out changing shards
	shardData := make(map[shardcfg.Tshid][]byte)
	curShardConfig := sck.Query()
	newShardConfig := new
	installedShard := make(map[shardcfg.Tshid]bool)

	if newShardConfig.Num != curShardConfig.Num+1 {
		// log.Printf("ChangeConfigTo called with invalid config num %d, current config num %d\n", newShardConfig.Num, curShardConfig.Num)
		return
	}

	err := sck.PutNextConfigIfNotPresent(newShardConfig, curShardConfig)
	if err != rpc.OK && err != rpc.ErrMaybe {
		// log.Println("[Shard Ctrl] There is already an ongoing config change")
		return
	}

	movedShards := make([]shardcfg.Tshid, 0)
	for shardId, groupId := range curShardConfig.Shards {
		newGroupId := newShardConfig.Shards[shardId]
		if groupId == newGroupId {
			continue
		}
		movedShards = append(movedShards, shardcfg.Tshid(shardId))
	}
	// log.Printf("[Shard Ctrl] Changing shard config from %v to %v, moved shards %v\n", curShardConfig, newShardConfig, movedShards)

	// Step 2: Freeze shard
	// log.Printf("[Shard Ctrl] Start freeze shards for shards %v\n", movedShards)
	for _, shardId := range movedShards {
		groupId := curShardConfig.Shards[shardId]
		groupClerk := shardgrp.MakeClerk(sck.clnt, curShardConfig.Groups[groupId])

		data, err := groupClerk.FreezeShard(shardId, newShardConfig.Num)
		if err != rpc.OK {
			continue
			// log.Printf("[Shard Ctrl] Failed to freeze shard %d\n", shardId)
		}
		shardData[shardId] = data
	}
	// log.Printf("[Shard Ctrl] Finished freeze shards for shards %v\n", movedShards)

	// Step 3: Install shard
	// log.Printf("[Shard Ctrl] Start install shards for shards %v\n", movedShards)
	for _, shardId := range movedShards {
		groupId := newShardConfig.Shards[shardId]
		groupClerk := shardgrp.MakeClerk(sck.clnt, newShardConfig.Groups[groupId])

		err := groupClerk.InstallShard(shardId, shardData[shardId], newShardConfig.Num)
		if err != rpc.OK {
			continue
			// log.Printf("[Shard Ctrl] Failed to install shard %d\n", shardId)
		}
		installedShard[shardId] = true
	}
	// log.Printf("[Shard Ctrl] Finished install shards for shards %v\n", movedShards)

	// Step 4: Delete shard
	// log.Printf("[Shard Ctrl] Start delete shards for shards %v\n", movedShards)
	for _, shardId := range movedShards {
		groupId := curShardConfig.Shards[shardId]
		groupClerk := shardgrp.MakeClerk(sck.clnt, curShardConfig.Groups[groupId])

		if !installedShard[shardId] {
			// log.Printf("[Shard Ctrl] Shard %d not installed at new group, skip delete\n", shardId)
			continue
		}

		err := groupClerk.DeleteShard(shardId, newShardConfig.Num)
		if err != rpc.OK {
			// log.Printf("[Shard Ctrl] Failed to delete shard %d\n", shardId)
		}
	}
	// log.Printf("[Shard Ctrl] Finished delete shards for shards %v\n", movedShards)
	err = sck.IKVClerk.Put(SHARD_CONFIG_NAME, newShardConfig.String(), rpc.Tversion(curShardConfig.Num))
	if err != rpc.OK && err != rpc.ErrMaybe {
		// log.Printf("[Shard Ctrl] ChangeConfigTo failed to update shard config from %v to newShardConfig %v with num %d due to err %v\n", curShardConfig, newShardConfig, curShardConfig.Num, err)
		// panic("[Shard Ctrl] ChangeConfigTo failed to update shard config in kv service")
	}
	// log.Printf("[Shard Ctrl] Finished change config for shards %v\n", movedShards)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	shardConfig, version, err := sck.IKVClerk.Get(SHARD_CONFIG_NAME)
	if err != rpc.OK {
		panic("[Shard Ctrl] Can't query shard config in kv service for version" + string(version))
	}

	// log.Printf("@@@Retrieved shard config %s with version %d", shardConfig, version)

	return shardcfg.FromString(shardConfig)
}

func (sck *ShardCtrler) QueryNextConfig() *shardcfg.ShardConfig {
	shardConfig, version, err := sck.IKVClerk.Get(NEXT_SHARD_CONFIG_NAME)
	if err != rpc.OK {
		panic("[Shard Ctrl] Can't query next shard config in kv service for version" + string(version))
	}

	// log.Printf("@@@Retrieved shard config %s with version %d", shardConfig, version)

	return shardcfg.FromString(shardConfig)
}

func (sck *ShardCtrler) PutNextConfigIfNotPresent(newShardConfig *shardcfg.ShardConfig, curShardConfig *shardcfg.ShardConfig) rpc.Err {
	nextConfig := sck.QueryNextConfig()

	if nextConfig.Num == newShardConfig.Num && nextConfig.String() != newShardConfig.String() {
		return rpc.ErrVersion
	}

	if nextConfig.Num == curShardConfig.Num+1 && nextConfig.String() == newShardConfig.String() {
		return rpc.OK
	}
	// Store the next config in kvsrv since the current next config is the same as the current config
	err := sck.IKVClerk.Put(NEXT_SHARD_CONFIG_NAME, newShardConfig.String(), rpc.Tversion(curShardConfig.Num))

	return err

}
