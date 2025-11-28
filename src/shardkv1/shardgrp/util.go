package shardgrp

import (
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

func GetShardGroupClerk(key string, shardConfig *shardcfg.ShardConfig, clnt *tester.Clnt) *Clerk {
	shardId := shardcfg.Key2Shard(key)
	_, servers, _ := shardConfig.GidServers(shardId)
	// log.Printf("key %v shardId %v gid %v servers %v ok %v\n", key, shardId, gid, servers, ok)

	shardGroupClerk := MakeClerk(clnt, servers)

	return shardGroupClerk
}
