package shardctrler

import (
	"6.824/raft"
	"bytes"
	"encoding/gob"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	persister    *raft.Persister
	maxraftstate int
	// Your data here.

	configs          []Config // indexed by config num
	duplicateTable   *DuplicateTable
	lastApplied      int
	notifyHandleCh   map[int64]chan bool
	notifyStopCh     chan bool
	notifySnapshotCh chan bool
}

type Op struct {
	// Your data here.
	RequestId
	HandleId    int64
	Type        string
	RequestArgs interface{}
}

func (sc *ShardCtrler) baseRequest(op *Op, args *BaseArgs, reply *BaseReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	//defer DPrintf("[%v]Get %+v, %+v, %+v", sc.me, args, reply, sc)

	// 不是leader
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 重复消息
	if isDuplicate := sc.duplicateTable.check(args.RequestId); isDuplicate {
		reply.Err = OK
		return
	}

	handleCh := make(chan bool, 1)
	sc.notifyHandleCh[op.HandleId] = handleCh

	// 发出command
	sc.rf.Start(op)
	sc.mu.Unlock()

	// command 已经发出，等待raft提交
	waitTimer := time.NewTimer(WaitCmdTimeOut)
	defer waitTimer.Stop()
	select {
	case <-handleCh: // 收到回复
		sc.mu.Lock()
		delete(sc.notifyHandleCh, op.HandleId)
		reply.Err = OK
		sc.duplicateTable.add(op.RequestId)
		return
	case <-waitTimer.C: // 超时
		sc.mu.Lock()
		delete(sc.notifyHandleCh, op.HandleId)
		reply.Err = ErrTimeOut
		return
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Type: "Join", RequestArgs: args,
		RequestId: args.RequestId, HandleId: nrand()}
	sc.baseRequest(&op, &args.BaseArgs, &reply.BaseReply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Type: "Leave", RequestArgs: args,
		RequestId: args.RequestId, HandleId: nrand()}
	sc.baseRequest(&op, &args.BaseArgs, &reply.BaseReply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Type: "Move", RequestArgs: args,
		RequestId: args.RequestId, HandleId: nrand()}
	sc.baseRequest(&op, &args.BaseArgs, &reply.BaseReply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Type: "Query", RequestArgs: args,
		RequestId: args.RequestId, HandleId: nrand()}
	sc.baseRequest(&op, &args.BaseArgs, &reply.BaseReply)
	if reply.Err == OK {
		sc.mu.Lock()
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.lastConfig()
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.notifyStopCh)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

type DuplicateTable struct {
	Table map[int64]*RequestId
}

func makeDuplicateTable() *DuplicateTable {
	dt := &DuplicateTable{}
	dt.Table = make(map[int64]*RequestId)
	return dt
}

func (dt *DuplicateTable) check(requestId RequestId) (isDuplicate bool) {
	duplicateEntry := dt.Table[requestId.ClientId]
	if duplicateEntry != nil && duplicateEntry.Seq == requestId.Seq {
		return true
	}
	return false
}

func (dt *DuplicateTable) add(requestId RequestId) {
	dt.Table[requestId.ClientId] = &requestId
}

func (sc *ShardCtrler) applier() {
	// Your code here.
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.mu.Lock()
		//DPrintf("[%v]applier %+v, %+v", sc.me, msg, sc)

		if msg.CommandValid {
			op := (msg.Command).(*Op)
			sc.lastApplied = msg.CommandIndex

			if isDuplicate := sc.duplicateTable.check(op.RequestId); !isDuplicate {
				switch op.Type {
				case "Join":
					args := op.RequestArgs.(*JoinArgs)
					newConfig := Config{}
					DeepCopy(&newConfig, sc.lastConfig())
					newConfig.Num = len(sc.configs)
					for k, v := range args.Servers {
						newConfig.Groups[k] = v
					}
					sc.configs = append(sc.configs, newConfig)
					sc.reAssign()
				case "Leave":
					args := op.RequestArgs.(*LeaveArgs)
					newConfig := Config{}
					DeepCopy(newConfig, sc.lastConfig())
					newConfig.Num = len(sc.configs)
					for _, gid := range args.GIDs {
						delete(newConfig.Groups, gid)
					}
					sc.configs = append(sc.configs, newConfig)
					sc.reAssign()
				case "Move":
					args := op.RequestArgs.(*MoveArgs)
					newConfig := Config{}
					DeepCopy(newConfig, sc.lastConfig())
					newConfig.Num = len(sc.configs)
					newConfig.Shards[args.Shard] = args.GID
					sc.configs = append(sc.configs, newConfig)
				case "Query":
					//args := op.RequestArgs.(QueryArgs)

				}
			}

			// 唤醒等待的handle
			if waitHandle := sc.notifyHandleCh[op.HandleId]; waitHandle != nil {
				sc.notify(waitHandle)
			}
			// check snapshot
			sc.notify(sc.notifySnapshotCh)
		}
		if msg.SnapshotValid {
			sc.readSnapshot(msg.Snapshot)
		}
		sc.mu.Unlock()

	}
}

func DeepCopy(dst, src interface{}) error {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(src); err != nil {
		return err
	}

	return gob.NewDecoder(&buffer).Decode(dst)
}

func (sc *ShardCtrler) lastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

//重新分配shard的分配，尽可能的减少移动
func (sc *ShardCtrler) reAssign() {
	currentConfig := sc.lastConfig()
	groupNum := len(currentConfig.Groups)
	shardNum := NShards
	baseNum := shardNum / groupNum
	overflowNum := shardNum % groupNum

	// 初始化
	groupAssignedNum := make(map[int]int, groupNum)
	groupQueue := make([]int, groupNum+1)
	index := 1
	for gid, _ := range currentConfig.Groups {
		groupAssignedNum[gid] = 0
		groupQueue[index] = gid
		index++
	}

	for i := 0; i < shardNum; i++ {
		// 将当前分配放置在队列首位，优先级最高，尽量不移动shard
		groupQueue[0] = currentConfig.Shards[i]
		// 找一个合适的group分配shard
		// 1. 已分配数 < baseNum
		// 2. 已分配数 = baseNum, 但还可以溢出 overflowNum >0
		for gid := range groupQueue {
			if _, ok := groupAssignedNum[gid]; ok {
				assignedNum := groupAssignedNum[gid]
				if assignedNum < baseNum {
					groupAssignedNum[gid]++
					currentConfig.Shards[i] = gid
					break
				}

				if assignedNum == baseNum && overflowNum > 0 {
					currentConfig.Shards[i] = gid
					groupAssignedNum[gid]++
					overflowNum--
					break
				}
			}
		}
	}

}

func (sc *ShardCtrler) snapshot() {
	sc.mu.Lock()
	raftStateSize := sc.persister.RaftStateSize()
	if raftStateSize > sc.maxraftstate && sc.maxraftstate != -1 {
		// Snapshot
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(sc.lastApplied)
		e.Encode(sc.duplicateTable)
		e.Encode(sc.configs)
		snapshot := w.Bytes()
		index := sc.lastApplied
		sc.mu.Unlock()
		sc.rf.Snapshot(index, snapshot)
	} else {
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) notify(ch chan bool) {
	go func() { ch <- true }()
}

func (sc *ShardCtrler) snapshotChecker() {
	checkTimer := time.NewTimer(CheckPeriods)
	defer checkTimer.Stop()

	for sc.killed() == false {
		select {
		case <-sc.notifyStopCh:
			return
		case <-checkTimer.C:
			sc.notify(sc.notifySnapshotCh)
		case <-sc.notifySnapshotCh:
			sc.snapshot()
			checkTimer.Reset(CheckPeriods)
		}
	}
}

func (sc *ShardCtrler) readSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var duplicateTable *DuplicateTable
	var configs []Config

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&duplicateTable) != nil ||
		d.Decode(&configs) != nil {
	} else {
		sc.lastApplied = lastApplied
		sc.duplicateTable = duplicateTable
		sc.configs = configs
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(BaseArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.persister = persister
	sc.duplicateTable = makeDuplicateTable()
	sc.maxraftstate = 10000
	//sc.readSnapshot(persister.ReadSnapshot())

	sc.notifyStopCh = make(chan bool, 5)
	sc.notifySnapshotCh = make(chan bool, 5)
	sc.notifyHandleCh = make(map[int64]chan bool)

	// Your code here.
	go sc.applier()
	//go sc.snapshotChecker()
	return sc
}
