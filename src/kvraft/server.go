package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

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
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state     map[string]string
	completed map[string]uint32
	gets      map[RequestId]string
	cv        *sync.Cond
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.requestAlreadyCompleted(args.RequestId) {
		reply.Value = kv.gets[args.RequestId]
		return
	}
	op := OpGet(args.Key, args.RequestId)
	kv.cv.L.Lock()
	defer kv.cv.L.Unlock()
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("%v, %v, %v := kv.rf.Start(%v)", index, term, isLeader, op)
	if !isLeader {
		reply.R = Reply{true, ""}
		return
	}
	for kv.completed[args.RequestId.ClerkId] < args.RequestId.SeqNum {
		kv.cv.Wait()
	}
	reply.Value = kv.gets[args.RequestId]
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v PutAppend args=%v", kv.me, *args)
	if kv.requestAlreadyCompleted(args.RequestId) {
		DPrintf("request already completed")
		return
	}
	var op Op
	switch args.Op {
	case "Append":
		op = OpAppend(args.Key, args.Value, args.RequestId)
	case "Put":
		op = OpPut(args.Key, args.Value, args.RequestId)
	}
	DPrintf("get lock")
	kv.cv.L.Lock()
	DPrintf("got lock")
	defer kv.cv.L.Unlock()
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("%v, %v, %v := kv.rf.Start(%v)", index, term, isLeader, op)
	if !isLeader {
		reply.R = Reply{true, ""}
		return
	}
	for kv.completed[args.RequestId.ClerkId] < args.RequestId.SeqNum {
		kv.cv.Wait()
	}
}

// Assumes that a clerk can have atmost one outstanding request at a time
func (kv *RaftKV) apply() {
	for applyMsg := range kv.applyCh {
		op := applyMsg.Command.(Op)
		kv.cv.L.Lock()
		switch op.OpType {
		case Append:
			kv.state[op.Key] += op.Value
		case Get:
			kv.gets[op.RequestId] = kv.state[op.Key]
		case Put:
			kv.state[op.Key] = op.Value
		}
		// TODO more sophistication below, max() or ???
		kv.completed[op.RequestId.ClerkId] = op.RequestId.SeqNum
		kv.cv.Signal()
		kv.cv.L.Unlock()
	}
}

func (kv *RaftKV) requestAlreadyCompleted(r RequestId) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	kv.cv = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()

	return kv
}
