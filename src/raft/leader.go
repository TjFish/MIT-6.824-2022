package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int         //leader’s term
	LeaderId     int         //so follower can redirect clients
	PrevLogIndex int         //index of log entry immediately preceding new  ones
	PrevLogTerm  int         //term of prevLogIndex entry
	Entries      []*LogEntry //log entries to store (empty for heartbeat  may send more than one for efficiency)
	LeaderCommit int         //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) (ok bool, reply *AppendEntriesReply) {
	reply = &AppendEntriesReply{}
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.lock.Lock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.toFollower()
		}
		rf.lock.Unlock()
	}
	return ok, reply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock.Lock()
	reply.Term = rf.currentTerm
	// 公共检查
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.lock.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	// election
	rf.toFollower()

	reply.Success = true

	rf.lock.Unlock()
	return
}

func (rf *Raft) heartBeater() {
	for rf.killed() == false {
		rf.lock.Lock()
		if rf.state == ILeader {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			//args.LeaderCommit = rf.commitIndex
			for server, _ := range rf.peers {
				if server == rf.me {
					continue
				}
				go rf.sendAppendEntries(server, args)
			}
		}
		rf.lock.Unlock()
		time.Sleep(rf.config.heartbeatPeriods)
	}
}
