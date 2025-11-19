package kvraft

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	clientId  string
	leaderIdx int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		servers:   servers,
		clientId:  uuid.New().String(),
		leaderIdx: -1,
	}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	var args rpc.GetArgs
	var reply rpc.GetReply

	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		args = rpc.GetArgs{Key: key}
		reply = rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.Get", &args, &reply)
		if !ok {
			// log.Printf("==clerk Get RPC to server %d failed\n", server)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			// log.Printf("==clerk Get RPC to a non leader server %d failed\n", server)
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
			// log.Printf("==clerk Get RPC to a leader server %d succeeded\n", server)
			ck.leaderIdx = serverIdx
			break
		}
	}

	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	var reply rpc.PutReply

	restransmit := false
	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		reply = rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.Put", &args, &reply)
		if !ok {
			// log.Printf("==clerk Put RPC to server %d failed\n", server)
			restransmit = true
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			// log.Printf("==clerk Put RPC to a non leader server %d failed\n", server)
			continue
		}

		if reply.Err == rpc.OK {
			// log.Printf("==clerk Put RPC to a leader server %d succeeded\n", server)
			ck.leaderIdx = serverIdx
			break
		}

		if reply.Err == rpc.ErrVersion {
			// log.Printf("==clerk Put RPC to a leader server %d failed since there is a different version\n", server)
			if restransmit {
				reply.Err = rpc.ErrMaybe
			}
			ck.leaderIdx = serverIdx
			break
		}
	}
	// log.Printf("Put %v took %v", args, dur)

	return reply.Err
}
