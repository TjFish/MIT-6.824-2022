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
	mu        sync.Mutex          // Lock to protect shared access to this peer's role
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         *LogCache

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role          Role // 当前状态
	electionTimer *Timer

	applyCh chan ApplyMsg
}

func (rf *Raft) String() string {
	return fmt.Sprintf("{[%v] role:%v, currentTerm:%v, votedFor:%v, "+
		"commitIndex:%v, lastApplied:%v, nextIndex:%v, matchIndex:%v, electionTimer:%v, "+
		"log:%+v}",
		rf.me, rf.role, rf.currentTerm, rf.votedFor,
		rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex, rf.electionTimer, rf.log)
}

type Role string

const (
	ILeader    Role = "Leader"
	IFollower  Role = "Follower"
	ICandidate Role = "Candidate"
)

const (
	HeartBeatTimeout   = 150 * time.Millisecond
	MinElectionTimeout = 300 * time.Millisecond
	MaxElectionTimeout = 600 * time.Millisecond
	CheckPeriods       = 20 * time.Millisecond //检查频率
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Index:%d, Term:%d}", l.Index, l.Term)
}

// LogCache 具有以下几个性质
// 1. Logs[0]存放 LastIncludedIndex,LastIncludedIndex 的logEntry
// 2. Logs[]永远不为空，因为Logs[0]永远有值
// 3. last()返回最后一条LogEntry
type LogCache struct {
	Logs              []*LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func makeLogCache(lastIncludedIndex int, lastIncludedTerm int) *LogCache {
	l := &LogCache{}
	l.Logs = []*LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm, Command: nil}}
	l.LastIncludedIndex = lastIncludedIndex
	l.LastIncludedTerm = lastIncludedTerm
	return l
}

func (l *LogCache) get(index int) *LogEntry {
	i := index - l.LastIncludedIndex
	if i >= len(l.Logs) || i < 0 {
		return nil
	}
	return l.Logs[i]
}

func (l *LogCache) last() *LogEntry {
	return l.Logs[len(l.Logs)-1]
}

//return logs which Index > index
func (l *LogCache) after(index int) []*LogEntry {
	copyData := make([]*LogEntry, l.last().Index-index)
	i := index - l.LastIncludedIndex
	copy(copyData, l.Logs[i+1:])
	return copyData
}

func (l *LogCache) rewrite(index int, newEntries []*LogEntry) {
	i := 0
	for i = 0; i < len(newEntries); i++ {
		newEntry := newEntries[i]
		exitEntry := l.get(index + i)
		if exitEntry == nil {
			break
		}
		if newEntry.Term != exitEntry.Term {
			//entry conflicts
			l.Logs = l.Logs[:exitEntry.Index-l.LastIncludedIndex]
			break
		}
	}
	if i < len(newEntries) {
		copyData := make([]*LogEntry, len(newEntries))
		copy(copyData, newEntries)
		l.Logs = append(l.Logs, copyData[i:]...)
	}
	return
}

func (l *LogCache) append(newEntry ...*LogEntry) {
	l.Logs = append(l.Logs, newEntry...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader = false
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.role == ILeader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any role?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log *LogCache

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		Debug(dError, "Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = rf.log.LastIncludedIndex
		//rf.lastApplied = rf.log.LastIncludedIndex
	}
	Debug(dPersist, "[%v]%v, %v, %v, %v", rf.me, currentTerm, votedFor, log, rf)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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

	if rf.killed() || rf.role != ILeader {
		return index, term, false
	}
	Info(dInfo, "[%v] start %+v", rf.me, rf)

	newLogEntry := &LogEntry{
		Index:   rf.log.last().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log.append(newLogEntry)
	rf.persist()

	index = rf.log.last().Index
	term = rf.log.last().Term
	isLeader = true

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

// lastApplied初始化后，只有本线程会更新lastApplied
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		// 根据快照更新状态，一般出现的情况有两种:
		// 1.奔溃重启时，自己使用快照恢复
		// 2.收到Install Snapshot RPC，安装快照
		// 由于当前状态小于快照状态，直接更新为快照状态
		if rf.lastApplied < rf.log.LastIncludedIndex {
			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.log.LastIncludedTerm,
				SnapshotIndex: rf.log.LastIncludedIndex,
			}
			rf.lastApplied = rf.log.LastIncludedIndex
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			continue
		}

		// 根据commitIndex更新状态
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			logEntry := rf.log.get(rf.lastApplied)
			Info(dLog, "[%v] applier %+v %+v", rf.me, rf.lastApplied, rf)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			continue
		}
		rf.mu.Unlock()
		time.Sleep(CheckPeriods)
	}
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
	rf.applyCh = applyCh
	rf.electionTimer = &Timer{}
	rf.electionTimer.reset()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.log = makeLogCache(0, 0)
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = IFollower
	rf.votedFor = -1
	// initialize from role persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Info(dInfo, "[%v] Make: %v", rf)

	// start ticker goroutine to start elections
	go rf.applier()        // for apply log -- all
	go rf.ticker()         // for election -- candidate
	go rf.committer()      // for commit log -- leader
	rf.initHeartBeater()   // for heartBeat -- leader
	rf.initLogReplicator() // for log replicate -- leader
	return rf
}
