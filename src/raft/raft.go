package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       NodeState
	currentTerm int
	votedFor    *int          // ptr so it can be nil
	logs        []LogEntry    // append-only log, latest entry is at the end
	heartbeat   time.Time     // timestamp of last received heartbeat
	timeout     time.Duration // election timeout
}

type NodeState int

func (ns NodeState) String() string {
	switch ns {
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	default:
		return "Unknown NodeState"
	}
}

const (
	Candidate NodeState = iota
	Follower
	Leader
)

type LogEntry struct {
	Term  int
	Index int
	Entry []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d received RequestVote from %d in term %d\n", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term >= rf.currentTerm &&
		(rf.votedFor == nil || *rf.votedFor == args.CandidateId) &&
		rf.isAtleastAsUptodate(args) {
		reply.VoteGranted = true
		rf.votedFor = new(int)
		*rf.votedFor = args.CandidateId
	}
}

// checkTerm returns true if check caused conversion to Follower
// if RPC request or response contains term T > currentTerm
//   set currentTerm = T
//   convert to Follower
func (rf *Raft) checkTerm(receivedTerm int) bool {
	if receivedTerm > rf.currentTerm {
		rf.currentTerm = receivedTerm
		DPrintf("%v %v -> Follower", rf.me, rf.state)
		rf.state = Follower
		rf.votedFor = nil
		return true
	}
	return false
}

// RequestVoteArgs is at least as up-to-date as receiver's log
func (rf *Raft) isAtleastAsUptodate(args *RequestVoteArgs) bool {
	if len(rf.logs) == 0 {
		return true
	}
	lastEntry := rf.logs[len(rf.logs)-1]
	return lastEntry.Term < args.LastLogTerm || (lastEntry.Term == args.LastLogTerm && lastEntry.Index <= args.LastLogIndex)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d sendRequestVote to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// randomTimeout interval between 150 and 300ms
func randomTimeout() time.Duration {
	r := rand.Intn(150) + 150
	return time.Duration(r) * time.Millisecond
}

func (rf *Raft) promoteToCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = new(int)
	*rf.votedFor = rf.me
	rf.heartbeat = time.Now()
	rf.timeout = randomTimeout()

	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.logs) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		last := rf.logs[len(rf.logs)-1]
		args.LastLogIndex = last.Index
		args.LastLogTerm = last.Term
	}

	DPrintf("%d asking for votes\n", rf.me)
	// send RequestVote in parallel to all servers
	ch := make(chan RequestVoteReply) //, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)
			// TODO handle ok=false by retrying
			ok = ok
			DPrintf("%d received vote reply from %v ok? %v %v term %v\n", rf.me, server, ok, reply.VoteGranted, reply.Term)
			ch <- reply
		}(i)
	}

	// process replies
	var start time.Time = time.Now()
	remaining := randomTimeout()
	received_votes := 1 // self-vote
	yes_votes := 1      // self-vote
	majority := len(rf.peers)/2 + 1
	rf.mu.Unlock()
	select {
	case <-time.After(remaining):
		rf.mu.Lock()
		if rf.state != Candidate {
			DPrintf("%d not a Candidate anymore", rf.me)
			break
		}
		DPrintf("%d timed out waiting for responses as Candidate\n", rf.me)
		// TODO start new election
		rf.promoteToCandidate()
	case reply := <-ch:
		rf.mu.Lock()
		if rf.state != Candidate {
			DPrintf("%d not a Candidate anymore", rf.me)
			break
		}
		remaining = start.Add(remaining).Sub(time.Now())
		received_votes += 1
		if ok := rf.checkTerm(reply.Term); ok {
			DPrintf("%d Candidate->Follower\n", rf.me)
			break
		}
		if reply.VoteGranted {
			yes_votes += 1
		}
		if yes_votes >= majority {
			DPrintf("%d has won election %d\n", rf.me, rf.currentTerm)
			rf.state = Leader
			rf.sendHeartbeats()
			break
		}
	}
}

// electLeader should be run in a separate goroutine. It continually checks
// whether no heartbeats have been received by the election timeout window.
// On receiving a heartbeat, it resets the election timeout. If none received,
// promotes to Candidate state. If already a Leader, sends heartbeats.
func (rf *Raft) electLeader() {
	for true {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		timeout := randomTimeout()
		if state == Leader {
			timeout /= 2
		}
		select {
		case <-time.After(timeout):
			rf.mu.Lock()

			switch state {
			case Candidate:
				// Qu how would it get to this state?
			case Follower:
				if time.Since(rf.heartbeat) > rf.timeout {
					DPrintf("%d timed out", rf.me)
					rf.promoteToCandidate()
				}
			case Leader:
				// TODO handle return value from heartbeats
				// Qu how frequently to send heartbeats
				rf.sendHeartbeats()
			}
			rf.mu.Unlock()
		}
	}
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// commenting out things i'll need later
	//	PrevLogIndex int
	//	PrevLogTerm  int
	Entries []LogEntry
	//	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d AppendEntries handler for %v %v\n", rf.me, args, reply)
	rf.checkTerm(args.Term)
	rf.heartbeat = time.Now()
	rf.timeout = randomTimeout()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// TODO add other checks and handling
	reply.Success = true
}

func (rf *Raft) sendHeartbeats() bool {
	if rf.state != Leader {
		DPrintf("%d is not leader but attempted sending heartbeats", rf.me)
		return false
	}
	DPrintf("%v sendHeartbeats", rf.me)
	// TODO handle replies, especially missing reply
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.Entries = make([]LogEntry, 0)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, args, &reply)
			// TODO use $ok and $reply
		}(i)
	}
	return true // maybe?
}

// send AppendEntries RPC to server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.timeout = randomTimeout()
	rand.Seed(int64(me))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electLeader()

	return rf
}
