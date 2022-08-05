package raft

import (
	"math/rand"
	"sync"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	electionTimeout := rf.generateRandomizedElectionTimeout()

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//这个协程的目的是持续检查是否满足选举条件
		// 一旦满足条件则启动选举协程
		//1. 处于Leader状态，一切正常
		//2.1 处于Follower状态，则判断心跳是否超时
		//2.2 处于Candidate状态，则判断选举是否超时
		rf.lock.Lock()
		rf.mTicker.elapsed()

		//Debug(dLeader, "electionTimeout %v", electionTimeout)
		if rf.state == ILeader {
			rf.mTicker.reset()
		}

		if rf.state == IFollower {
			//计时器超时，开始选举
			if rf.mTicker.isTimeOut(electionTimeout) {
				rf.startElection()
				electionTimeout = rf.generateRandomizedElectionTimeout()
			}
		}

		if rf.state == ICandidate {
			//选举超时
			if rf.mTicker.isTimeOut(electionTimeout) {
				rf.startElection()
				electionTimeout = rf.generateRandomizedElectionTimeout()
			}
		}

		rf.lock.Unlock()
		time.Sleep(electionTimeout / 3)
	}
}

func (rf *Raft) generateRandomizedElectionTimeout() time.Duration {
	randRange := rf.config.maxElectionTimeout - rf.config.minElectionTimeout
	return rf.config.minElectionTimeout + time.Duration(rand.Intn(int(randRange)))
}

//On conversion to candidate, start election:
//• Increment currentTerm
//• Vote for self
//• Reset election timer
//• Send RequestVote RPCs to all other servers
//• If votes received from majority of servers: become leader
//• If AppendEntries RPC received from new leader: convert to follower
//• If election timeout elapses: start new election
func (rf *Raft) startElection() {
	//Debug("dInfo", "[%v] startElection, %+v", rf.me, rf)
	rf.state = ICandidate
	rf.currentTerm += 1
	electionTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.mTicker.reset()

	args := &RequestVoteArgs{}
	args.Term = electionTerm
	args.CandidateId = rf.me
	//args.LastLogIndex = len(rf.log)
	//args.LastLogTerm = rf.log[len(rf.log)-1].Term
	cond := sync.NewCond(rf.lock)
	go rf.waitForVote(cond, electionTerm)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, cond)
	}
}

// 这个协程是计票员，目的是等待其他server的投票结果，进行汇总
func (rf *Raft) waitForVote(cond *sync.Cond, electionTerm int) {
	//等待回复并计票，如果超过多数则成为leader
	majority := len(rf.peers)/2 + 1
	counter := 1
	for {
		rf.lock.Lock()
		cond.Wait()
		// currentTerm已经发生变化，这次选举过期失效
		if rf.currentTerm > electionTerm {
			rf.lock.Unlock()
			return
		}
		// 不是Candidate，说明要么成为了Leader，要么是Follower，这次选举结束
		if rf.state != ICandidate {
			rf.lock.Unlock()
			return
		}
		counter += 1
		//计票是大多数，变为leader
		if counter >= majority {
			rf.toLeader()
			rf.lock.Unlock()
			return
		}
		rf.lock.Unlock()
	}
}

// 这个协程目的是向指定的server发送RequestVote RPC消息，并处理其回复
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, cond *sync.Cond) (ok bool, reply *RequestVoteReply) {
	reply = &RequestVoteReply{}
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.lock.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.toFollower()
		}
		rf.lock.Unlock()
		if reply.VoteGranted {
			cond.Signal()
		}
	}
	return ok, reply
}

// RequestVote RPC handle
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.toFollower()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	rf.lock.Unlock()
	return
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}
