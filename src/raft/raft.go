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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
	applyCV   *sync.Cond          // Conditional variable to protect shared access to applying log entries

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state NodeState
	// Persistent state
	currentTerm int
	votedFor    *int       // ptr so it can be nil
	logs        []LogEntry // append-only log, latest entry is at the end
	// Volatile state
	heartbeat   time.Time     // timestamp of last received heartbeat
	timeout     time.Duration // election timeout
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
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

const DebugLock = 0
const DebugEncoding = 0

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) Lock() {
	if DebugLock > 0 && Debug > 0 {
		DPrintf("%v get lock", rf.me)
		debug.PrintStack()
	}
	rf.mu.Lock()
	if DebugLock > 0 {
		DPrintf("%v got lock", rf.me)
	}
}

func (rf *Raft) unlock() {
	if DebugLock > 0 && Debug > 0 {
		DPrintf("%v unlocked", rf.me)
		debug.PrintStack()
	}
	rf.mu.Unlock()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	b := rf.votedFor == nil
	e.Encode(b)
	if !b {
		e.Encode(*rf.votedFor)
	}
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	if DebugEncoding > 0 {
		DPrintf("%v encoded (%v,%v,[-,]%v)", rf.me, rf.currentTerm, b, len(rf.logs))
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	decode(rf.me, d, &rf.currentTerm)
	var b bool
	decode(rf.me, d, &b)
	if !b {
		rf.votedFor = new(int)
		decode(rf.me, d, rf.votedFor)
	}
	decode(rf.me, d, &rf.logs)
	if DebugEncoding > 0 {
		DPrintf("%v decoded (%v,%v,[-,]%v)", rf.me, rf.currentTerm, b, len(rf.logs))
	}
}

func decode(me int, d *gob.Decoder, e interface{}) {
	if err := d.Decode(e); err != nil {
		log.Fatalf("%v decode error: %v", me, err)
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

func (rvr RequestVoteReply) String() string {
	voteGranted := "No"
	if rvr.VoteGranted {
		voteGranted = "Yes"
	}
	return fmt.Sprintf("%v(%v)", voteGranted, rvr.Term)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d received RequestVote from %d in term %d\n", rf.me, args.CandidateId, args.Term)
	rf.Lock()
	defer rf.unlock()
	defer rf.persist()
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term >= rf.currentTerm &&
		(rf.votedFor == nil || *rf.votedFor == args.CandidateId) &&
		rf.isAtleastAsUptodate(args) {
		reply.VoteGranted = true
		rf.votedFor = new(int)
		*rf.votedFor = args.CandidateId
		rf.heartbeat = time.Now()
		rf.timeout = randomTimeout()
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
	if lastEntry, ok := rf.lastEntry(); ok {
		return lastEntry.Term < args.LastLogTerm || (lastEntry.Term == args.LastLogTerm && lastEntry.Index <= args.LastLogIndex)
	}
	return true
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
	DPrintf("%v %v -> Candidate", rf.me, rf.state)
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
	rf.persist()
	DPrintf("%d asking for votes for term %v", rf.me, rf.currentTerm)
	// send RequestVote in parallel to all servers
	ch := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, &reply)
			// TODO handle ok=false by retrying
			if ok {
				DPrintf("%d received vote reply from %v %v", rf.me, server, reply)
				ch <- reply
			} else {
				DPrintf("%d received vote reply from %v timed out from term %v", rf.me, server, args.Term)
			}
		}(i)
	}

	// process replies
	var start time.Time = time.Now()
	remaining := randomTimeout()
	yes_votes := 1 // self-vote
	majority := len(rf.peers)/2 + 1
	rf.unlock()
	for {
		select {
		case <-time.After(remaining):
			rf.Lock()
			if rf.state != Candidate {
				DPrintf("%d not a Candidate anymore", rf.me)
				return
			}
			DPrintf("%d timed out waiting for responses as Candidate in term %v\n", rf.me, args.Term)
			rf.promoteToCandidate() // start a new election
			return
		case reply := <-ch:
			rf.Lock()
			if rf.state != Candidate {
				DPrintf("%d not a Candidate anymore", rf.me)
				return
			}
			remaining = start.Add(remaining).Sub(time.Now())
			if deposed := rf.checkTerm(reply.Term); deposed {
				return
			}
			if reply.VoteGranted {
				yes_votes += 1
			}
			if yes_votes >= majority {
				DPrintf("%d has won election %d", rf.me, rf.currentTerm)
				rf.state = Leader
				lastIndex := 0
				if lastEntry, ok := rf.lastEntry(); ok {
					lastIndex = lastEntry.Index
				}
				for i := range rf.peers {
					rf.nextIndex[i] = lastIndex + 1
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = lastIndex
				rf.sendHeartbeats()
				return
			}
			rf.unlock()
		}
	}
}

func (rf *Raft) lastEntry() (last LogEntry, ok bool) {
	if len(rf.logs) == 0 {
		ok = false
		return
	}
	return rf.logs[len(rf.logs)-1], true
}

// mainLoop should be run in a separate goroutine. It continually checks
// whether no heartbeats have been received by the election timeout window.
// On receiving a heartbeat, it resets the election timeout. If none received,
// promotes to Candidate state. If already a Leader, sends heartbeats.
func (rf *Raft) mainLoop() {
	for true {
		rf.Lock()
		state := rf.state
		rf.unlock()
		timeout := randomTimeout()
		if state == Leader {
			timeout /= 3
		}
		select {
		case <-time.After(timeout):
			rf.Lock()

			switch rf.state {
			case Candidate:
				// Qu how would it get to this state?
			case Follower:
				// if previous state was Leader, has a smaller timeout
				if state == Follower &&
					time.Since(rf.heartbeat) > rf.timeout {
					DPrintf("%d timed out", rf.me)
					rf.promoteToCandidate()
				}
			case Leader:
				// TODO handle return value from heartbeats
				// Qu how frequently to send heartbeats
				rf.sendHeartbeats()
			}
			rf.unlock()
		}
	}
}

func findLogEntryWithIndex(logs []LogEntry, idx int) (*LogEntry, bool) {
	for _, entry := range logs {
		if entry.Index == idx {
			return &entry, true
		}
	}
	return nil, false
}

// applyLogEntries should be run in a separate goroutine. It continually checks
// whether there are any committed log entries that have not been applied, and
// applies these.
func (rf *Raft) applyLogEntries(applyCh chan<- ApplyMsg) {
	for true {
		rf.Lock()           // accessing state in rf
		rf.applyCV.L.Lock() // waiting on CV
		for rf.commitIndex <= rf.lastApplied {
			rf.unlock() // release while waiting
			rf.applyCV.Wait()
			rf.Lock()
		}
		rf.applyCV.L.Unlock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			logEntry, ok := findLogEntryWithIndex(rf.logs, i)
			if !ok {
				DPrintf("%v unable to find entry with index %v", rf.me, i)
				break
			}
			applyMsg := ApplyMsg{}
			applyMsg.Index = logEntry.Index
			applyMsg.Command = logEntry.Command
			DPrintf("%v <- applyCh", rf.me)
			applyCh <- applyMsg
			rf.lastApplied += 1
		}
		rf.unlock()
	}
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingIndex int
	ConflictingTerm  *int
}

func (aer AppendEntriesReply) String() string {
	if aer.Success {
		return fmt.Sprintf("Yes(%v)", aer.Term)
	} else if aer.ConflictingTerm == nil {
		return fmt.Sprintf("No(%v, @%v)", aer.Term, aer.ConflictingIndex)
	} else {
		return fmt.Sprintf("No(%v, %v@%v)", aer.Term, *aer.ConflictingTerm, aer.ConflictingIndex)
	}
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.unlock()
	defer rf.persist()

	DPrintf("%d AppendEntries handler for %v", rf.me, *args)
	rf.checkTerm(args.Term)
	rf.heartbeat = time.Now()
	rf.timeout = randomTimeout()
	reply.Term = rf.currentTerm

	// demote Candidate if receives message from new leader
	if args.Term == rf.currentTerm &&
		rf.state == Candidate {
		DPrintf("%v Candidate -> Follower", rf.me)
		rf.state = Follower
		rf.votedFor = nil
	}

	// 1. terms differ
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("%v responding with %v", rf.me, *reply)
		return
	}

	// 2. prev log entry differs
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex-1 >= len(rf.logs) {
			reply.Success = false
			reply.ConflictingIndex = Max(len(rf.logs), MinIndex)
			reply.ConflictingTerm = nil
			DPrintf("%v responding with %v", rf.me, *reply)
			return
		}
		prevLogEntry := rf.logs[args.PrevLogIndex-1]
		if prevLogEntry.Index != args.PrevLogIndex {
			panic(fmt.Sprintf("LogEntry at %v has index %v", args.PrevLogIndex, prevLogEntry.Index))
		}
		if prevLogEntry.Term != args.PrevLogTerm {
			reply.Success = false
			entry, _ := rf.findFirstLogEntryWithTerm(prevLogEntry.Term)
			reply.ConflictingIndex = entry.Index
			reply.ConflictingTerm = new(int)
			*reply.ConflictingTerm = prevLogEntry.Term
			DPrintf("%v responding with %v", rf.me, *reply)
			return
		}
	}

	reply.Success = true

	// 3. existing entry conflicts
	if len(args.Entries) > 0 {
		appendFrom := 0

		for _, newEntry := range args.Entries {
			idx := newEntry.Index - 1
			if idx >= len(rf.logs) {
				break
			}
			// delete conflicting and all that follow
			if rf.logs[idx].Term != newEntry.Term {
				rf.logs = rf.logs[:idx]
				DPrintf("Deleted conflicting entries (%v) and all that follow: %v", idx, rf.logs)
			} else {
				appendFrom += 1
			}
		}

		// 4. append
		rf.logs = append(rf.logs, args.Entries[appendFrom:]...)
		DPrintf("%v has len(logs)=%v after append: %v", rf.me, len(rf.logs), rf.logs)
	}

	// 5. commit indices differ
	if args.LeaderCommit > rf.commitIndex {
		indexOfLastNewEntry := -1
		if lastEntry, ok := rf.lastEntry(); ok {
			indexOfLastNewEntry = lastEntry.Index
		}
		rf.commitIndex = Min(args.LeaderCommit, indexOfLastNewEntry)
		rf.applyCV.L.Lock()
		defer rf.applyCV.L.Unlock()
		rf.applyCV.Signal()
	}
}

func createBasicAppendEntriesArgs(rf *Raft) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	return args
}

func (rf *Raft) sendHeartbeats() bool {
	if rf.state != Leader {
		DPrintf("%d is not leader but attempted sending heartbeats", rf.me)
		return false
	}
	DPrintf("%v sendHeartbeats", rf.me)
	args := createBasicAppendEntriesArgs(rf)
	return rf.sendAppendEntriesToAll(&args)
}

func updateCommitIndex(rf *Raft) {
	if middle, ok := Middle(rf.matchIndex); ok && middle > 0 {
		DPrintf("updateCommitIndex middle=%v commitIndex=%v matchIndex=%v %v", middle, rf.commitIndex, rf.matchIndex, rf.me)
		entry := rf.logs[middle-1]
		if middle > rf.commitIndex &&
			entry.Term == rf.currentTerm {
			DPrintf("%v updateCommitIndex %v->%v", rf.me, rf.commitIndex, middle)
			rf.commitIndex = middle
			rf.applyCV.L.Lock()
			defer rf.applyCV.L.Unlock()
			rf.applyCV.Signal()
		}
	}
}

// matchIndexIs enforces that updates to marchIndex are monotonically
// increasing.
func (rf *Raft) matchIndexIs(server, newValue int) {
	// occurs if receive an out-of-order message
	if rf.matchIndex[server] > newValue {
		DPrintf("%v non-monotonically increasing update to matchIndex[%v]: %v -> %v", rf.me, server, rf.matchIndex[server], newValue)
		return
	}
	rf.matchIndex[server] = newValue
}

func (rf *Raft) findFirstLogEntryWithTerm(term int) (*LogEntry, bool) {
	for _, entry := range rf.logs {
		if entry.Term == term {
			return &entry, true
		}
	}
	return nil, false
}

func (rf *Raft) findLastLogEntryWithTerm(term *int) (*LogEntry, bool) {
	if term == nil {
		return nil, false
	}
	found := false
	var prev LogEntry
	for _, entry := range rf.logs {
		if entry.Term == *term {
			found = true
		} else if found {
			break
		}
		prev = entry
	}
	return &prev, found
}

func (rf *Raft) sendAppendEntriesToAll(template *AppendEntriesArgs) bool {
	end := 0
	if lastEntry, ok := rf.lastEntry(); ok {
		end = lastEntry.Index
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{}
		*args = *template // copy of data structure
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
		if len(rf.logs) > 0 && rf.nextIndex[i] > 1 {
			entry := rf.logs[rf.nextIndex[i]-2]
			args.PrevLogIndex = entry.Index
			args.PrevLogTerm = entry.Term
		}

		var entries []LogEntry
		DPrintf("%v append [%v,%v)", rf.me, rf.nextIndex[i]-1, end)
		for j := rf.nextIndex[i] - 1; j < end; j++ {
			entries = append(entries, rf.logs[j])
		}
		args.Entries = entries
		DPrintf("len(logs)=%v\tnextIndex=%v end=%v sending %v log entries to %v", len(rf.logs), rf.nextIndex[i], end, len(entries), i)

		go func(server int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				DPrintf("%v AppendEntries reply timed out from %v for %v", rf.me, server, *args)
				// TODO handle failure
				return
			}
			DPrintf("%v AppendEntries %v reply from %v for %v", rf.me, reply, server, *args)
			rf.Lock()
			defer rf.unlock()
			if rf.state != Leader {
				DPrintf("%d not a Leader anymore", rf.me)
				return
			}
			// check term
			if deposed := rf.checkTerm(reply.Term); deposed {
				return
			}
			// check success
			if reply.Success && len(args.Entries) > 0 {
				lastEntry := args.Entries[len(args.Entries)-1]
				rf.nextIndex[server] = lastEntry.Index + 1
				rf.matchIndexIs(server, lastEntry.Index)
			} else if !reply.Success {
				DPrintf("%v processing No() - begin nextIndex[%v]=%v", rf.me, server, rf.nextIndex[server])
				// an old message was received from a previous term
				if reply.Term < rf.currentTerm {
					// ignore this message
				} else if entry, ok := rf.findLastLogEntryWithTerm(reply.ConflictingTerm); ok {
					DPrintf("%v findLastLogEntryWithTerm(%v)=%v", rf.me, *reply.ConflictingTerm, *entry)
					rf.nextIndex[server] = entry.Index + 1
				} else {
					rf.nextIndex[server] = reply.ConflictingIndex
				}
				DPrintf("%v processing No() - end nextIndex[%v]=%v", rf.me, server, rf.nextIndex[server])
				// TODO retry
			} else {
				rf.nextIndex[server] = args.PrevLogIndex + 1
				rf.matchIndexIs(server, args.PrevLogIndex)
			}
			updateCommitIndex(rf)
		}(i, args)
	}
	return true
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
	// Your code here (2B).
	rf.Lock()
	defer rf.unlock()

	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return -1, term, isLeader
	}

	index := 1
	if lastEntry, ok := rf.lastEntry(); ok {
		index = lastEntry.Index + 1
	}

	// append command to logs
	entry := LogEntry{}
	entry.Index = index
	entry.Term = term
	entry.Command = command
	rf.logs = append(rf.logs, entry)
	rf.matchIndexIs(rf.me, index)
	rf.persist()
	DPrintf("Start(%v)", entry.Command)

	// create AppendEntriesArg and send RPC, then return
	args := createBasicAppendEntriesArgs(rf)
	rf.sendAppendEntriesToAll(&args)

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
	atomic.StoreInt32(&Debug, 0)
	rf.Lock()
	defer rf.unlock()
	rf.state = Follower
	rf.timeout = 10 * time.Minute // won't bother us for some time
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
	atomic.StoreInt32(&Debug, kDebug)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.timeout = randomTimeout()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rand.Seed(int64(me))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.mainLoop()

	m := sync.Mutex{}
	rf.applyCV = sync.NewCond(&m)
	go rf.applyLogEntries(applyCh)

	return rf
}
