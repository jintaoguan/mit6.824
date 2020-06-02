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
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

var HeartbeatInterval = 150 * time.Millisecond
var ElectionTimeoutLower = 400
var ElectionTimeoutUpper = 600

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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
	state          int
	currentTerm    int
	votedFor       int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == STATE_LEADER
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// DPrintf("%v vote to %v, local term %v, rpc term %v", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.changeToFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true

	} else if rf.votedFor != -1 {
		reply.VoteGranted = false
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// handle heartbeat or new data
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v term %v receive heartbeat, rpc term %v, ", rf.me, rf.currentTerm, args.Term)
	if args.Term >= rf.currentTerm {
		// DPrintf("=====> %v found itself not the leader any more. local term %v, remote term %v", rf.me, rf.currentTerm, args.Term)
		// DPrintf("%v starts to change state to FOLLOWER", rf.me)
		rf.changeToFollower(args.Term)
	} else if args.Term < rf.currentTerm {
		// reply.Term = rf.currentTerm
		reply.Success = false
	}

}

// this RPC can be used to send heartbeat or new data
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == STATE_LEADER
	if isLeader {
		// if this is the leader, keep taking new log
		newLogEntry := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, newLogEntry)
		DPrintf("%v gets new log. command %v", rf.me, command)
	}

	return index, term, isLeader
}

// func (rf *Raft) getLastLogIndex() int {
// 	return len(rf.log) - 1
// }

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// DPrintf("!!!!! %v raft server killed.", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = 0
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
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
	rf.mu.Lock()
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	// DPrintf("!!!!! %v raft server startup(). state %v, local term %v", rf.me, rf.state, rf.currentTerm)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.mu.Unlock()

	// main loop
	go func(rf *Raft) {
		for {
			select {
			// election timeout
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				// DPrintf("%v election Timer timeout local term %v, local state %v", rf.me, rf.currentTerm, rf.state)
				rf.changeToCandidate()
				rf.mu.Unlock()

			// heartbeat timeout
			// if this is a leader, it is time to send another heartbeat to everyone else
			// if this is a follower/candidate ...
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				// DPrintf("%v heartbeat Timer timeout local term %v, local state %v", rf.me, rf.currentTerm, rf.state)
				if rf.state == STATE_LEADER {
					rf.broadcastHeartbeat()
				}
				rf.mu.Unlock()
			}
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startElection() {
	DPrintf("%v starts election, new term %v", rf.me, rf.currentTerm+1)
	rf.currentTerm = rf.currentTerm + 1
	// election can fail and this is for the next election
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	request := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	var voteCount int32
	for i := range rf.peers {
		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt32(&voteCount, 1)
			continue
		}
		go func(serverId int) {

			reply := RequestVoteReply{}
			if rf.sendRequestVote(serverId, &request, &reply) {
				if rf.state != STATE_CANDIDATE {
					return
				} else if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
					// DPrintf("%v receives vote from %v, new term: %v", rf.me, serverId, rf.currentTerm)
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
						rf.changeToLeader()
						// DPrintf("=====> %v becomes LEADER, new term: %v", rf.me, rf.currentTerm)
					}
				} else if reply.Term > rf.currentTerm {
					rf.changeToFollower(reply.Term)
				}
			} else {
				// DPrintf("%v send request vote to %d failed", rf.me, serverId)
			}
		}(i)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("--------> %v broadcast heartbeat %v", rf.me, len(rf.log))
	// send heartbeat to everyone to claim the leadership
	rf.heartbeatTimer.Reset(HeartbeatInterval)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			// DPrintf("%v broadcast heartbeat to %v", rf.me, serverId)
			entries := make([]LogEntry, 0)
			if rf.getLastLogIndex() >= 0 {
				entries = append(entries)
			}
			request := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// PrevLogIndex: rf.log[rf.nextIndex[serverId]].Index,
				// PrevLogTerm:  rf.log[rf.nextIndex[serverId]].Term,
				// Entries:      append([]LogEntry{}, rf.log[rf.nextIndex[serverId]:]...),
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(serverId, &request, &reply) {
				// DPrintf("%v received heartbeat reply, local term %v, remote term %v", rf.me, rf.currentTerm, reply.Term)
				if reply.Term > rf.currentTerm {
					// DPrintf("=====> %v found itself not the leader any more. local term %v, remote term %v", rf.me, rf.currentTerm, reply.Term)
					rf.changeToFollower(reply.Term)
				}
				if reply.Success {
					rf.matchIndex[serverId] = request.PrevLogIndex + len(request.Entries)
					rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
					rf.commitLogs()
				}
				// } else {
				// 	rf.nextIndex[serverId] = rf.nextIndex[serverId] - 1
				// }
			} else {
				// DPrintf("%v broadcast heartbeat to %d failed", rf.me, serverId)
			}
		}(i)
	}
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Index
}

// sort the matchIndex to find the lowest match index among the majorities that have higher match index
// the leader's new commit index will be the new match index
func (rf *Raft) commitLogs() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	matchIndexCopy := make([]int, len(rf.peers))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy)
	nextCommitIndex := matchIndexCopy[len(matchIndexCopy)/2]
	// IMPORTANT: make sure all the commits have to be in the same term
	if nextCommitIndex > rf.commitIndex && rf.log[nextCommitIndex].Term == rf.currentTerm {
		rf.commitIndex = nextCommitIndex
	}
}

func (rf *Raft) changeToFollower(term int) {
	rf.state = STATE_FOLLOWER
	// DPrintf("%v becomes FOLLOWER, local term %v", rf.me, rf.currentTerm)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.heartbeatTimer.Stop()
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
}

func (rf *Raft) changeToCandidate() {
	rf.state = STATE_CANDIDATE
	// DPrintf("%v becomes CANDIDATE, local term %v", rf.me, rf.currentTerm)
	rf.startElection()
}

func (rf *Raft) changeToLeader() {
	rf.state = STATE_LEADER
	DPrintf("=====> %v becomes LEADER, local term %v", rf.me, rf.currentTerm)
	rf.heartbeatTimer.Reset(HeartbeatInterval)
	rf.electionTimer.Stop()
	rf.broadcastHeartbeat()

}

func randTimeDuration(lower int, upper int) time.Duration {
	randNum := rand.Int31n(int32(upper)-int32(lower)+int32(1)) + int32(lower)
	return time.Duration(randNum) * time.Millisecond
}
