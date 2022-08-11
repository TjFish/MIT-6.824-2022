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
	"fmt"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         *LogEntries

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	config *RaftConfig
	state  ServerState

	lock    *sync.Mutex
	mTicker *Ticker

	applyCh chan ApplyMsg
}

func (rf *Raft) String() string {
	return fmt.Sprintf("{[%v] state:%v, currentTerm:%v, votedFor:%v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, mTicker:%v, "+
		"log:%+v}",
		rf.me, rf.state, rf.currentTerm, rf.votedFor,
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.mTicker, rf.log.log)
}

type ServerState string

const (
	ILeader    ServerState = "Leader"
	IFollower  ServerState = "Follower"
	ICandidate ServerState = "Candidate"
)

type RaftConfig struct {
	heartbeatPeriods   time.Duration
	minElectionTimeout time.Duration
	maxElectionTimeout time.Duration
	checkPeriods       time.Duration
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Index:%d,Term:%d}", l.Index, l.Term)
}

type LogEntries struct {
	log []*LogEntry
}

func makeLogEntries() *LogEntries {
	l := &LogEntries{}
	l.log = []*LogEntry{{0, 0, nil}}
	return l
}

func (l *LogEntries) get(index int) *LogEntry {
	if index >= len(l.log) {
		return nil
	}
	return l.log[index]
}

func (l *LogEntries) last() *LogEntry {
	return l.log[len(l.log)-1]
}

func (l *LogEntries) after(index int) []*LogEntry {
	//if index == 0 {
	//	return make([]*LogEntry, 0)
	//}
	copyData := make([]*LogEntry, l.last().Index-index)
	copy(copyData, l.log[index+1:])
	return copyData
}

func (l *LogEntries) rewrite(startIndex int, newEntrys []*LogEntry) {
	//Debug(dDrop, "rewrite +%v, +%v, +%v", startIndex, newEntrys)
	i := 0
	for i = 0; i < len(newEntrys); i++ {
		newEntry := newEntrys[i]
		exitEntry := l.get(startIndex + i)
		if exitEntry == nil {
			break
		}
		if newEntry.Term != exitEntry.Term {
			//entry conflicts
			l.log = l.log[:startIndex+i]
			break
		}
	}
	if i < len(newEntrys) {
		copyData := make([]*LogEntry, len(newEntrys))
		copy(copyData, newEntrys)
		l.log = append(l.log, copyData[i:]...)
	}
	return
}

func (l *LogEntries) append(term int, command interface{}) {
	newEntry := &LogEntry{Term: term, Command: command, Index: len(l.log)}
	l.log = append(l.log, newEntry)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader = false
	// Your code here (2A).
	rf.lock.Lock()
	term = rf.currentTerm
	if rf.state == ILeader {
		isleader = true
	}
	rf.lock.Unlock()
	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) (ok bool, reply *RequestVoteReply) {
//	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
//	if ok {
//		// todo check term
//		if reply.VoteGranted {
//			rf.voteNum += 1
//		}
//	}
//	return ok, reply
//}

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
	rf.lock.Lock()

	if rf.killed() || rf.state != ILeader {
		rf.lock.Unlock()
		return index, term, false
	}
	Info(dInfo, "[%v] start %+v", rf.me, rf)

	rf.log.append(rf.currentTerm, command)
	index = rf.log.last().Index
	term = rf.log.last().Term
	isLeader = true

	rf.lock.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.applyCh = applyCh
	rf.lock = &sync.Mutex{}
	rf.mTicker = &Ticker{}
	rf.mTicker.reset()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.log = makeLogEntries()
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = IFollower
	rf.votedFor = -1
	rf.config = &RaftConfig{
		heartbeatPeriods:   150 * time.Millisecond,
		minElectionTimeout: 300 * time.Millisecond,
		maxElectionTimeout: 600 * time.Millisecond,
		checkPeriods:       20 * time.Millisecond,
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.committer()
	go rf.applier()
	rf.initHeartBeater()
	rf.initLogReplicator()
	Info(dInfo, "Make: %d", rf.me)
	return rf
}
