package raft

func (rf *Raft) newInstallSnapshotArgs() (args *InstallSnapshotArgs) {
	snapshot := rf.persister.ReadSnapshot()
	args = &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
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
	if entry != nil && entry.Term == args.LastIncludedTerm {
		rf.log.trim(args.LastIncludedIndex)
		rf.persist()
		return
	}

	rf.log = makeLogEntries(args.LastIncludedIndex)
	rf.log.append(args.LastIncludedTerm, nil)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()

	go func() {
		applyMsg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyCh <- applyMsg
	}()
	return
}
