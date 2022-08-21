package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Type      string
	ClientId  int64
	Seq       int64
	Result    string
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	db               map[string]string
	duplicateTable   *DuplicateTable
	lastApplied      int
	notifyHandleCh   map[int64]chan bool
	notifyStopCh     chan bool
	notifySnapshotCh chan bool
}

// 重复检测 start
type DuplicateTable struct {
	Table map[int64]*Op
}

func makeDuplicateTable() *DuplicateTable {
	dt := &DuplicateTable{}
	dt.Table = make(map[int64]*Op)
	return dt
}

func (dt *DuplicateTable) check(op Op) (isDuplicate bool, value string) {
	duplicateOp := dt.Table[op.ClientId]
	if duplicateOp != nil && duplicateOp.Seq == op.Seq {
		return true, duplicateOp.Result
	}
	return false, ""
}

func (dt *DuplicateTable) add(op Op) {
	dt.Table[op.ClientId] = &op
}

// 重复检测 end

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer DPrintf("[%v]Get %+v, %+v, %+v", kv.me, args, reply, kv)
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, Value: "", Type: "Get",
		ClientId: args.ClientId, Seq: args.Seq,
		RequestId: nrand(),
	}

	// 重复消息
	isDuplicate, value := kv.duplicateTable.check(op)
	if isDuplicate {
		reply.Err = OK
		reply.Value = value
		return
	}

	// 发出command
	kv.rf.Start(op)

	ch := make(chan bool, 1)
	kv.notifyHandleCh[op.RequestId] = ch
	kv.mu.Unlock()

	// command 已经发出，等待raft提交
	waitTimer := time.NewTimer(WaitCmdTimeOut)
	defer waitTimer.Stop()
	select {
	case <-ch: // 收到回复
		kv.mu.Lock()
		delete(kv.notifyHandleCh, op.RequestId)
		if kv.db[args.Key] == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Value = kv.db[args.Key]
			reply.Err = OK
		}
		return
	case <-waitTimer.C: // 超时
		kv.mu.Lock()
		delete(kv.notifyHandleCh, op.RequestId)
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer DPrintf("[%v]PutAppend %+v, %+v, %+v", kv.me, args, reply, kv)
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, Value: args.Value, Type: args.Op,
		ClientId: args.ClientId, Seq: args.Seq,
		RequestId: nrand(),
	}

	isDuplicate, _ := kv.duplicateTable.check(op)
	if isDuplicate {
		reply.Err = OK
		return
	}

	kv.rf.Start(op)
	ch := make(chan bool, 1)
	kv.notifyHandleCh[op.RequestId] = ch
	kv.mu.Unlock()

	// command 已经发出，等待raft提交
	waitTimer := time.NewTimer(WaitCmdTimeOut)
	defer waitTimer.Stop()
	select {
	case <-ch: // 收到回复
		kv.mu.Lock()
		delete(kv.notifyHandleCh, op.RequestId)
		reply.Err = OK
		return
	case <-waitTimer.C: // 超时
		kv.mu.Lock()
		delete(kv.notifyHandleCh, op.RequestId)
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) applier() {
	// Your code here.
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		DPrintf("[%v]applier %+v, %+v", kv.me, msg, kv)

		if msg.CommandValid {
			op := (msg.Command).(Op)
			kv.lastApplied = msg.CommandIndex

			isDuplicate, _ := kv.duplicateTable.check(op)
			if !isDuplicate {
				switch op.Type {
				case "Get":
					op.Result = kv.db[op.Key]
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					if kv.db[op.Key] == "" {
						kv.db[op.Key] = op.Value
					} else {
						kv.db[op.Key] = kv.db[op.Key] + op.Value
					}
				}
				kv.duplicateTable.add(op)
			}
			// 唤醒等待的handle
			waitHandle := kv.notifyHandleCh[op.RequestId]
			if waitHandle != nil {
				kv.notify(waitHandle)
			}
			// check snapshot
			kv.notify(kv.notifySnapshotCh)
		}
		if msg.SnapshotValid {
			kv.readSnapshot(msg.Snapshot)
		}
		kv.mu.Unlock()

	}
}

func (kv *KVServer) snapshot() {
	kv.mu.Lock()
	raftStateSize := kv.persister.RaftStateSize()
	if raftStateSize > kv.maxraftstate && kv.maxraftstate != -1 {
		// Snapshot
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.lastApplied)
		e.Encode(kv.duplicateTable)
		e.Encode(kv.db)
		snapshot := w.Bytes()
		index := kv.lastApplied
		kv.mu.Unlock()
		kv.rf.Snapshot(index, snapshot)
	} else {
		kv.mu.Unlock()
	}
}

func (kv *KVServer) notify(ch chan bool) {
	go func() { ch <- true }()
}

func (kv *KVServer) snapshotChecker() {
	checkTimer := time.NewTimer(CheckPeriods)
	defer checkTimer.Stop()

	for kv.killed() == false {
		select {
		case <-kv.notifyStopCh:
			return
		case <-checkTimer.C:
			kv.notify(kv.notifySnapshotCh)
		case <-kv.notifySnapshotCh:
			kv.snapshot()
			checkTimer.Reset(CheckPeriods)
		}
	}
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var duplicateTable *DuplicateTable
	var db map[string]string

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&duplicateTable) != nil ||
		d.Decode(&db) != nil {
		DPrintf("Decode error")
	} else {
		kv.lastApplied = lastApplied
		kv.duplicateTable = duplicateTable
		kv.db = db
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.notifyStopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.duplicateTable = makeDuplicateTable()
	kv.readSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.notifyStopCh = make(chan bool, 5)
	kv.notifySnapshotCh = make(chan bool, 5)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.notifyHandleCh = make(map[int64]chan bool)
	// You may need initialization code here.
	go kv.applier()
	go kv.snapshotChecker()
	DPrintf("[%v] StartKVServer %v", kv.me, kv)
	return kv
}
