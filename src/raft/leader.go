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

func (rf *Raft) toLeader() {
	rf.state = ILeader
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log.last().Index + 1
		rf.matchIndex[i] = 0
	}
	//send initial empty AppendEntries RPC
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		args := rf.newAppendEntriesArgs(server)
		go rf.sendAppendEntries(server, args)
	}
	Info(dLeader, "[%v] i am leader%+v", rf.me, rf)
}

func (rf *Raft) initHeartBeater() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.heartBeater(server)
	}
}

func (rf *Raft) heartBeater(server int) {
	for rf.killed() == false {
		rf.lock.Lock()
		if rf.state == ILeader {
			args := rf.newAppendEntriesArgs(server)
			go rf.sendAppendEntries(server, args)
		}
		rf.lock.Unlock()
		time.Sleep(rf.config.heartbeatPeriods)
	}
}

func (rf *Raft) initLogReplicator() {
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.logReplicator(server)
	}
}

//这个协程等待条件cond，被唤醒时触发一次log同步任务
func (rf *Raft) logReplicator(server int) {
	for rf.killed() == false {
		rf.lock.Lock()
		if rf.state == ILeader && rf.log.last().Index >= rf.nextIndex[server] {
			args := rf.newAppendEntriesArgs(server)
			go rf.sendAppendEntries(server, args)
		}
		rf.lock.Unlock()
		time.Sleep(rf.config.checkPeriods)
	}
}

func (rf *Raft) newAppendEntriesArgs(server int) (args *AppendEntriesArgs) {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log.get(prevLogIndex).Term
	entries := rf.log.after(prevLogIndex)

	args = &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) (ok bool, reply *AppendEntriesReply) {
	reply = &AppendEntriesReply{}
	Trace(dTrace, "[%v]sendAppendEntries[%v] %+v %+v %+v", rf.me, server, args)
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.lock.Lock()

	defer rf.lock.Unlock()
	// 处理RPC回复
	if ok {

		// 公共检查
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = IFollower
			rf.votedFor = -1
			return
		}

		// 过期消息
		if rf.currentTerm != args.Term {
			return
		}

		// 自身状态已经改变
		if rf.state != ILeader {
			return
		}

		//开始处理回复结果
		//注意，同一时间可能收到多个AppendEntries RPC的回复，消息可能重复、乱序
		//这意味着rf.nextIndex,rf.matchIndex的状态可能已经被改变
		//有几种方法处理
		// 1. 修改幂等：rf.nextIndex,rf.matchIndex的修改从原始请求参数中取值
		// 2. 判断取最优值：首先判断数据是否被其他回复协程修改，如果修改了，比对取最优值
		// 目前采用第一种方法，实现起来简单些
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		} else {
			rf.nextIndex[server] = Max(args.PrevLogIndex, 1)
			// retry
			retryArgs := rf.newAppendEntriesArgs(server)
			go rf.sendAppendEntries(server, retryArgs)
		}
	}
	return ok, reply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	defer Trace(dTrace, "[%v]AppendEntries[%v] %+v %+v %+v", rf.me, args.LeaderId, args, reply, rf)

	// 公共检查
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = IFollower
		rf.votedFor = args.LeaderId
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.mTicker.reset()
	rf.state = IFollower
	rf.votedFor = args.LeaderId

	//rf.toFollower()

	prevLog := rf.log.get(args.PrevLogIndex)
	if prevLog == nil || prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//Debug(dInfo, "[%v] AppendEntries %+v %+v %+v", rf.me, prevLog, args)

	rf.log.rewrite(args.PrevLogIndex+1, args.Entries)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.log.last().Index)
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	return
}

func (rf *Raft) committer() {
	for rf.killed() == false {
		rf.lock.Lock()
		if rf.state == ILeader {
			for N := rf.commitIndex + 1; N <= rf.log.last().Index; N++ {
				majority := len(rf.peers)/2 + 1
				counter := 1
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						counter++
					}
				}

				if counter >= majority && rf.log.get(N).Term == rf.currentTerm {
					rf.commitIndex = N
					Debug(dCommit, "[%v]commitIndex %+v", rf.me, rf)
				}
			}
		}
		rf.lock.Unlock()
		time.Sleep(rf.config.checkPeriods)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.lock.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			logEntry := rf.log.get(rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: rf.lastApplied,
			}
			Info(dLog, "[%v] applier %+v %+v", rf.me, applyMsg.CommandIndex, rf)
			rf.lock.Unlock()
			rf.applyCh <- applyMsg
			//go func() {
			//	Debug(dLog, "%+v", applyMsg.CommandIndex)
			//	rf.applyCh <- applyMsg
			//}()
			rf.lock.Lock()
		}
		rf.lock.Unlock()
		time.Sleep(rf.config.checkPeriods)
	}
}