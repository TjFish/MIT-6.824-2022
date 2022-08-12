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
	"6.824/labgob"
	"bytes"
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

	lastIncludedIndex int
	lastIncludedTerm  int
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
	return fmt.Sprintf("{Index:%d, Term:%d}", l.Index, l.Term)
}

type LogEntries struct {
	log        []*LogEntry
	startIndex int
}

func makeLogEntries(startIndex int) *LogEntries {
	l := &LogEntries{}
	l.log = []*LogEntry{}
	l.startIndex = startIndex
	return l
}

func (l *LogEntries) get(index int) *LogEntry {
	i := index - l.startIndex
	if i >= len(l.log) || i < 0 {
		return nil
	}
	return l.log[i]
}

func (l *LogEntries) last() *LogEntry {
	return l.log[len(l.log)-1]
}

//return log which Index > index
func (l *LogEntries) after(index int) []*LogEntry {
	copyData := make([]*LogEntry, l.last().Index-index)
	i := index - l.startIndex
	copy(copyData, l.log[i+1:])
	return copyData
}

// return log which Index<= index
func (l *LogEntries) before(index int) []*LogEntry {
	i := index - l.startIndex
	copyData := make([]*LogEntry, i+1)
	copy(copyData, l.log[:i+1])
	return copyData
}

func (l *LogEntries) rewrite(index int, newEntrys []*LogEntry) {
	//Debug(dDrop, "rewrite +%v, +%v, +%v", startIndex, newEntrys)
	i := 0
	for i = 0; i < len(newEntrys); i++ {
		newEntry := newEntrys[i]
		exitEntry := l.get(index + i)
		if exitEntry == nil {
			break
		}
		if newEntry.Term != exitEntry.Term {
			//entry conflicts
			l.log = l.before(exitEntry.Index - 1)
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
	newIndex := len(l.log) + l.startIndex
	newEntry := &LogEntry{Term: term, Command: command, Index: newIndex}
	l.log = append(l.log, newEntry)
}

// 截取>=index的log
func (l *LogEntries) trim(index int) {
	l.log = l.after(index - 1)
	l.startIndex = index
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log.before(rf.log.last().Index))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []*LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logs) != nil {
		Debug(dError, "Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		rf.log = makeLogEntries(lastIncludedIndex)
		for _, log := range logs {
			rf.log.append(log.Term, log.Command)
		}
	}
	Debug(dPersist, "%v, %v, %v", currentTerm, votedFor, logs)
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
	rf.lock.Lock()
	defer rf.lock.Unlock()
	Info(dSnap, "[%v]before Snapshot,index:%v %v", rf.me, index, rf)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log.get(index).Term
	rf.log.trim(index)
	rf.persister.SaveStateAndSnapshot(nil, snapshot)
	rf.persist()
	Info(dSnap, "[%v]after Snapshot index:%v  %v", rf.me, index, rf)
	//rf.persister.SaveStateAndSnapshot(nil, snapshot)
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
	rf.lock.Lock()

	if rf.killed() || rf.state != ILeader {
		rf.lock.Unlock()
		return index, term, false
	}
	Info(dInfo, "[%v] start %+v", rf.me, rf)

	rf.log.append(rf.currentTerm, command)
	rf.persist()
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
	rf.log = makeLogEntries(0)
	rf.log.append(0, nil)
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
