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
	R Reply
}
type Reply struct {
	WrongLeader bool
	Err         Err
}

func (r Reply) String() string {
	s := "Yes"
	if r.WrongLeader {
		s = "No"
	}
	if r.IsErr() {
		s += fmt.Sprintf("(err=%v)", r.Err)
	}
	return s
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId RequestId
}

type GetReply struct {
	R     Reply
	Value string
}

type RequestId struct {
	ClerkId string
	SeqNum  uint32
}

func (par PutAppendReply) String() string {
	return fmt.Sprintf("%v", par.R)
}
func (gr GetReply) String() string {
	return fmt.Sprintf("%v[%v]", gr.R, gr.Value)
}
func (r RequestId) String() string {
	return fmt.Sprintf("%v:::%v", r.ClerkId, r.SeqNum)
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

/*
type ErrReply interface {
	IsErr() bool
}
*/
func (r *Reply) IsErr() bool {
	return r.Err.IsErr()
}

/*
func (gr *GetReply) IsErr() bool {
	return gr.Err.IsErr()
}
*/
func (e Err) IsErr() bool {
	return len(e) > 0
}

/*
type ReplyWithWrongLeader interface {
	WrongLeader() bool
}
*/
/*
func (par *PutAppendReply) WrongLeader() bool {
	return par.WrongLeader
}

func (gr *GetReply) WrongLeader() bool {
	return gr.WrongLeader()
}
*/
