package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1
const Pending = "PENDING"
const kTimeout = time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	Key       string
	Value     string // optional
	RequestId RequestId
}

func OpAppend(key, value string, r RequestId) Op {
	return Op{Append, key, value, r}
}

func OpGet(key string, r RequestId) Op {
	return Op{Get, key, "", r}
}

func OpPut(key, value string, r RequestId) Op {
	return Op{Put, key, value, r}
}

type RaftKV struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state     map[string]string    // kv map
	completed map[string]uint32    // clerkId -> max SeqNum
	gets      map[RequestId]string // result of gets
	cv        *sync.Cond
}

func (kv *RaftKV) lock(msg string) {
	DPrintf("%v get lock at kv-%v", kv.me, msg)
	kv.cv.L.Lock()
	DPrintf("%v got lock at kv-%v", kv.me, msg)
}

func (kv *RaftKV) unlock(msg string) {
	DPrintf("%v unlocked at kv-%v", kv.me, msg)
	kv.cv.L.Unlock()
}

func (kv *RaftKV) start(req RequestId, op Op) bool {
	index, term, isLeader := kv.rf.Start(op)
	if op.OpType == Get {
		kv.lock("start - add pending")
		kv.gets[req] = Pending
		kv.unlock("start - add pending")
	}
	DPrintf("%v, %v, %v := kv.rf.Start(%v)", index, term, isLeader, op)
	if !isLeader {
		return false
	}
	done := make(chan struct{})
	go func() {
		kv.lock("start")
		defer kv.unlock("start")
		for kv.completed[req.ClerkId] < req.SeqNum {
			kv.cv.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(kTimeout):
		if op.OpType == Get {
			kv.lock("start - del pending")
			delete(kv.gets, req)
			kv.unlock("start - del pending")
		}
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer func() { DPrintf("%v Get sending %v", kv.me, *reply) }()
	if kv.requestAlreadyCompleted(args.RequestId) {
		DPrintf("request already completed")
		reply.R = Reply{false, "Request already completed"}
		return
	}
	op := OpGet(args.Key, args.RequestId)
	isLeader := kv.start(args.RequestId, op)
	if !isLeader {
		reply.R = Reply{true, ""}
	} else {
		kv.lock("get")
		defer kv.unlock("get")
		reply.Value = kv.gets[args.RequestId]
		if reply.Value == Pending {
			panic(fmt.Sprintf("Get(%v) from request %v is %v", args.Key, args.RequestId, Pending))
		}
		delete(kv.gets, args.RequestId)
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v PutAppend args=%v", kv.me, *args)
	if kv.requestAlreadyCompleted(args.RequestId) {
		DPrintf("request already completed")
		reply.R = Reply{false, "Request already completed"}
		return
	}
	var op Op
	switch args.Op {
	case "Append":
		op = OpAppend(args.Key, args.Value, args.RequestId)
	case "Put":
		op = OpPut(args.Key, args.Value, args.RequestId)
	}
	isLeader := kv.start(args.RequestId, op)
	if !isLeader {
		reply.R = Reply{true, ""}
	}
}

// Assumes that a clerk can have atmost one outstanding request at a time
func (kv *RaftKV) apply() {
	for applyMsg := range kv.applyCh {
		op := applyMsg.Command.(Op)
		kv.lock(fmt.Sprintf("apply(%v)", op))
		switch op.OpType {
		case Append:
			kv.state[op.Key] += op.Value
		case Get:
			if _, ok := kv.gets[op.RequestId]; ok {
				kv.gets[op.RequestId] = kv.state[op.Key]
			}
		case Put:
			kv.state[op.Key] = op.Value
		}
		// TODO more sophistication below, max() or ???
		kv.completed[op.RequestId.ClerkId] = op.RequestId.SeqNum
		kv.cv.Signal()
		kv.unlock("apply")
	}
}

func (kv *RaftKV) requestAlreadyCompleted(r RequestId) bool {
	kv.lock("requestAlreadyCompleted")
	defer kv.unlock("requestAlreadyCompleted")
	if seqNum, ok := kv.completed[r.ClerkId]; ok {
		return r.SeqNum <= seqNum
	}
	return false
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.completed = make(map[string]uint32)
	kv.gets = make(map[RequestId]string)
	mu := sync.Mutex{}
	kv.cv = sync.NewCond(&mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()

	return kv
}
