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
	id, _id string
	seqNum  uint32
	leader  int32
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
	ck._id = string(uuid)
	ck._id = strings.TrimSpace(ck._id)
	ck.id = "ck" + ck._id[:3]
	DPrintf("%v New clerk %v created", ck.id, ck._id)
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
			DPrintf("%v sending Get(%v) to %v", ck.id, args.Key, j)
			ok := ck.servers[j].Call("RaftKV.Get", &args, &reply)
			if ok {
				ck.updateLeader(l, j, reply.R)
				if !reply.R.WrongLeader &&
					!reply.R.IsErr() {
					DPrintf("%v Get(%v) = (%v, %v)", ck.id, key, ok, reply)
					return reply.Value
				}
			}
			DPrintf("%v Get(%v) = (%v, %v)", ck.id, key, ok, reply)
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
		for i := 0; i < len(ck.servers); i++ {
			j := (l + i) % len(ck.servers)
			reply := PutAppendReply{}
			DPrintf("%v sending Put %v->%v to %v", ck.id, args.Key, args.Value, j)
			ok := ck.servers[j].Call("RaftKV.PutAppend", &args, &reply)
			if ok {
				ck.updateLeader(l, j, reply.R)
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
	return RequestId{ck._id, n}
}

func (ck *Clerk) getLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

// if WrongLeader, randomly update leader
func (ck *Clerk) updateLeader(oldLeader, sentTo int, r Reply) {
	if !r.WrongLeader && oldLeader != sentTo {
		atomic.CompareAndSwapInt32(&ck.leader, int32(oldLeader), int32(sentTo))
		DPrintf("%v leader %v->%v", ck.id, oldLeader, sentTo)
	}

}
