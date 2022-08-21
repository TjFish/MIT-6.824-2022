package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 已经生成过Snapshot
	if index < rf.log.LastIncludedIndex {
		return
	}
	lastIncludedIndex := index
	lastIncludedTerm := rf.log.get(index).Term
	rf.saveStateAndSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot)
}

func (rf *Raft) saveStateAndSnapshot(lastIncludedIndex, lastIncludedTerm int, snapshot []byte) {
	Info(dSnap, "[%v]before Snapshot,index:%v %v", rf.me, lastIncludedIndex, rf)
	retainLogs := rf.log.after(lastIncludedIndex)
	rf.log = makeLogCache(lastIncludedIndex, lastIncludedTerm)
	rf.log.append(retainLogs...)
	state := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	Info(dSnap, "[%v]after Snapshot index:%v  %v", rf.me, lastIncludedIndex, rf)
}

func (rf *Raft) newInstallSnapshotArgs() (args *InstallSnapshotArgs) {
	snapshot := rf.persister.ReadSnapshot()
	args = &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Data:              snapshot,
		Done:              true,
	}
	return args
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) (ok bool, reply *InstallSnapshotReply) {
	reply = &InstallSnapshotReply{}
	Trace(dTrace, "[%v]sendInstallSnapshot[%v] %+v %+v %+v", rf.me, server, args)
	ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 处理RPC回复
	if ok {
		// 公共检查
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = IFollower
			rf.votedFor = -1
			rf.persist()
			return
		}

		// 过期消息 || 自身状态已经改变
		if rf.currentTerm != args.Term || rf.role != ILeader {
			return
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok, reply
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Trace(dTrace, "[%v]InstallSnapshot[%v] %+v %+v %+v", rf.me, args.LeaderId, args, reply, rf)

	// 公共检查
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = IFollower
		rf.votedFor = args.LeaderId
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// InstallSnapshot 与 appendEntries 类似，收到消息证明leader还活着
	rf.electionTimer.reset()
	rf.role = IFollower
	rf.votedFor = args.LeaderId
	rf.persist()

	reply.Term = rf.currentTerm
	entry := rf.log.get(args.LastIncludedIndex)
	// 保存更新的log
	if entry != nil && entry.Term == args.LastIncludedTerm {
		rf.saveStateAndSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
		rf.commitIndex = args.LastIncludedIndex
		return
	}

	// 抛弃掉已有log
	rf.log = makeLogCache(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.saveStateAndSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	rf.commitIndex = args.LastIncludedIndex
	return
}
