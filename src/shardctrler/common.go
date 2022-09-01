package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrTimeOut     = "ErrTimeOut"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	ChangeLeaderPeriods = 20 * time.Millisecond
	WaitCmdTimeOut      = 3000 * time.Millisecond
	CheckPeriods        = 20 * time.Millisecond //检查频率
)

type Err string

type RequestId struct {
	ClientId int64
	Seq      int64
}

type BaseArgs struct {
	RequestId RequestId
}

type BaseReply struct {
	Err Err
}

type JoinArgs struct {
	BaseArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	BaseReply
}

type LeaveArgs struct {
	BaseArgs
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	BaseReply
}

type MoveArgs struct {
	BaseArgs
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	BaseReply
}

type QueryArgs struct {
	BaseArgs
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	BaseReply
	Config Config
}
