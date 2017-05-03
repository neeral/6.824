package raftkv

import (
	"crypto/rand"
	"labrpc"
	"log"
	"math/big"
	"os/exec"
	"strings"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     string
	seqNum uint32
	leader int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	if len(servers) < 1 {
		log.Fatal("There must be at least one server")
	}
	uuid, err := exec.Command("uuidgen").Output()
	if err != nil {
		log.Fatal(err)
	}
	ck.id = string(uuid)
	ck.id = strings.TrimSpace(ck.id)
	ck.id = "ck" + ck.id[:2]
	ck.seqNum = 0
	ck.leader = 0 // to begin with
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	requestId := ck.nextRequestId()
	args := GetArgs{key, requestId}

	for {
		l := ck.getLeader()
		for i := 0; i < len(ck.servers); i++ {
			j := (l + i) % len(ck.servers)
			reply := GetReply{}
			ok := ck.servers[j].Call("RaftKV.Get", &args, &reply)
			if ok {
				ck.updateLeader(int32(j), reply.R)
				if !reply.R.WrongLeader &&
					!reply.R.IsErr() {
					DPrintf("%v Get(%v) = (%v, %v)", ck.id, key, ok, reply.R)
					return reply.Value
				}
			}
			DPrintf("%v Get(%v) = (%v, %v)", ck.id, key, ok, reply.R)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := ck.nextRequestId()
	args := PutAppendArgs{key, value, op, requestId}
	// TODO implement retries
	for {
		l := ck.getLeader()
		DPrintf("%v leader = %v", ck.id, l)
		for i := 0; i < len(ck.servers); i++ {
			j := (l + i) % len(ck.servers)
			reply := PutAppendReply{}
			DPrintf("%v sending Put %v->%v to %v", ck.id, args.Key, args.Value, j)
			ok := ck.servers[j].Call("RaftKV.PutAppend", &args, &reply)
			if ok {
				ck.updateLeader(int32(j), reply.R)
				if !reply.R.WrongLeader &&
					!reply.R.IsErr() {
					DPrintf("%v PutAppend(%v) = (%v, %v)", ck.id, key, ok, reply)
					return
				}
			}
			DPrintf("  %v PutAppend(%v) = (%v, %v)", ck.id, key, ok, reply)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) nextRequestId() RequestId {
	n := atomic.AddUint32(&ck.seqNum, 1)
	return RequestId{ck.id, n}
}

func (ck *Clerk) getLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

// if WrongLeader, randomly update leader
func (ck *Clerk) updateLeader(sentTo int32, r Reply) {
	if r.WrongLeader {
		maybeNewLeader := (int(atomic.LoadInt32(&ck.leader)) + 1) % len(ck.servers)
		atomic.CompareAndSwapInt32(&ck.leader, sentTo, int32(maybeNewLeader))
	} else {
		atomic.CompareAndSwapInt32(&ck.leader, sentTo, int32(sentTo))
	}

}
