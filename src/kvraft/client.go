package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	DPrintf("Clerk is starting...")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 拿key的当前value，如果不存在就返回空。
// 如果遇到了错误，就会反复重试。
// 以ok := ck.servers[i].Call("KVServer.Get", &args, &reply)的方式发送RPC
// args和reply的类型(包括它们是否是指针)必须匹配RPC处理函数的参数声明的类型。
// 并且reply必须作为指针传递。
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key: key,
	}
	// send Get RPC to Leader
	reply := &GetReply{}
	for {
		over := false
		if ck.leaderId != -1 {
			DPrintf("ck send Get RPC while leaderId = %v", ck.leaderId)
			ok := ck.sendGet(ck.leaderId, args, reply)
			// 如果返回值为false，说明call fail或者发给了错误的kvserver
			// 就会重新发送
			if ok {
				over = true
			} else {
				// 反复执行
				ck.leaderId = -1
			}
		} else {
			DPrintf("ck send Get RPC while leaderId = -1")
			for i := range ck.servers {
				ok := ck.sendGet(i, args, reply)
				if ok {
					ck.leaderId = i
					over = true
					break
				}
				// 如果一直没能ok，就会反复执行
			}
			break
		}
		if over {
			break
		}
	}
	return ""
}

func (ck *Clerk) sendGet(i int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
	return ok
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 由Put和Append共享
// 以ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)的方式发送RPC
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
