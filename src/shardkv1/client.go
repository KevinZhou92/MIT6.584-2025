package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	var err rpc.Err
	var val string
	var version rpc.Tversion

	for {
		shardGroupClerk := shardgrp.GetShardGroupClerk(key, ck.sck.Query(), ck.clnt)
		val, version, err = shardGroupClerk.Get(key)

		if err == rpc.OK || err == rpc.ErrNoKey {
			// log.Printf("===shardkv Clerk Get key %v value %v version %v succeeded\n", key, val, version)
			break
		}

		if err == rpc.ErrWrongGroup {
			time.Sleep(5 * time.Millisecond)
			// log.Printf("===shardkv Clerk Get key %v wrong group, retrying after sleep\n", key)
			continue
		}
	}

	return val, version, err
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	var err rpc.Err
	retry := 0
	for {
		shardGroupClerk := shardgrp.GetShardGroupClerk(key, ck.sck.Query(), ck.clnt)
		err = shardGroupClerk.Put(key, value, version)

		if err == rpc.OK || err == rpc.ErrMaybe || err == rpc.ErrVersion {
			// log.Printf("+++shardkv Clerk Put key %v value %v version %v succeeded\n", key, value, version)
			break
		}

		if err == rpc.ErrWrongGroup {
			// log.Printf("+++shardkv Clerk Put key %v value %v version %v wrong group, retrying after sleep\n", key, value, version)
			time.Sleep(5 * time.Millisecond)
			retry += 1
			continue
		}
		// log.Printf("+++shardkv Clerk Put key %v value %v version %v failed with err %v\n", key, value, version, err)
	}

	// We need to check retry from the client side because the shardgroup might be using stale info
	// and return ErrVersion even though the Put actually succeeded.
	// Here an example scenario:
	// 1. Client does a Put(key, value1, version 1) which succeeds but shard group client didn't receive the response. And the shard was moved to a new group.
	// 2. Client retries the put which fails with ErrWrongGroup at shardgroup due to stale config.
	// 3. Client queries new config and does Put(key, value1, version 1) to new group which failed and returns ErrVersion.
	// In this case the Put actually succeeded in step 1 so we return ErrMaybe to the client.
	if retry > 0 && err == rpc.ErrVersion {
		err = rpc.ErrMaybe
	}

	return err
}
