package raft

import (
	"math/rand"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	electionTimeout := rf.getRandElectionTimeout()

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//这个协程的目的是持续检查是否满足选举条件
		// 一旦满足条件则启动选举协程
		//1. 处于Leader状态，一切正常
		//2.1 处于Follower状态，则判断心跳是否超时
		//2.2 处于Candidate状态，则判断选举是否超时
		rf.mu.Lock()
		rf.electionTimer.elapsed()

		//Debug(dLeader, "electionTimeout %v", electionTimeout)
		if rf.role == ILeader {
			rf.electionTimer.reset()
		}

		if rf.role == IFollower || rf.role == ICandidate {
			if rf.electionTimer.isTimeOut(electionTimeout) {
				rf.startElection()
				electionTimeout = rf.getRandElectionTimeout()
			}
		}

		rf.mu.Unlock()
		time.Sleep(CheckPeriods)
	}
}

func (rf *Raft) getRandElectionTimeout() time.Duration {
	randRange := MaxElectionTimeout - MinElectionTimeout
	return MinElectionTimeout + time.Duration(rand.Intn(int(randRange)))
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
	Info(dInfo, "[%v] startElection, %+v", rf.me, rf)
	rf.role = ICandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer.reset()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.last().Index,
		LastLogTerm:  rf.log.last().Term,
	}
	//得票数
	grantedCount := 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, &grantedCount)
	}
}

// 这个协程目的是向指定的server发送RequestVote RPC消息，并处理其回复
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, grantedCount *int) (ok bool, reply *RequestVoteReply) {
	reply = &RequestVoteReply{}
	Trace(dVote, "[%v]sendRequestVote [%v], %+v", rf.me, server, args)
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//处理RPC回复
	if ok {
		majority := len(rf.peers)/2 + 1
		// 公共检查
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = IFollower
			rf.votedFor = -1
			rf.persist()
			return
		}

		// 任期已经改变，此次选举结束
		// 本身状态不是Candidate，说明要么成为了Leader，要么是Follower，此次选举结束
		if rf.currentTerm != args.Term || rf.role != ICandidate {
			return
		}

		//处理消息回复
		if reply.VoteGranted {
			*grantedCount = *grantedCount + 1
			if *grantedCount >= majority {
				rf.toLeader()
			}
		}
	}
	return ok, reply
}

// RequestVote RPC handle
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Trace(dVote, "[%v]RequestVote from [%v], %+v, %+v", rf.me, args.CandidateId, args, reply, rf)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = IFollower
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastEntry := rf.log.last()

		if lastEntry.Term < args.LastLogTerm ||
			(lastEntry.Term == args.LastLogTerm && lastEntry.Index <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.electionTimer.reset()
			rf.persist()
		}

	}
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
