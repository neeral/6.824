package raftkv

import (
	"fmt"
)

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId RequestId
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId RequestId
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type RequestId struct {
	clerkId string
	seqNum  int
}

func makeRequestId(clerkId string, seqNum int) RequestId {
	return RequestId{clerkId, seqNum}
}

func (r *RequestId) String() string {
	return fmt.Sprintf("%v:::%v", r.clerkId, r.seqNum)
}

type OpType int

const (
	Append OpType = iota
	Get
	Put
)

func (o OpType) String() string {
	switch o {
	case Append:
		return "Append"
	case Get:
		return "Get"
	case Put:
		return "Put"
	default:
		return "Unknown OpType"
	}
}
