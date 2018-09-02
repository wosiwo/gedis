package main

import (
	//"bufio"
	"fmt"
	//"io"
	"log"
	"net"
	"net/rpc"
	"sync"
	"./zset"
)


//
// RPC request/reply definitions
//

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
//hash
type HSetArgs struct {
	Key   string
	Field   string
	Value string
}

type HSetReply struct {
	Err Err
	Value string
}

type HGetArgs struct {
	Key string
	Field string
}

type HGetReply struct {
	Err   Err
	Value string
}

//zset
type ZAddArgs struct {
	Key   string
	Mem   string
	Score float64
}

type ZAddReply struct {
	Err Err
	Value string
}

type ZScoreArgs struct {
	Key string
	Mem string
}

type ZScoreReply struct {
	Err   Err
	Value float64
}

//
// Server
//

type KV struct {
	mu   sync.Mutex
	data map[string]string
}

type HASHTBVal struct {
	data map[string]string
}
type HASHTB struct {
	mu   sync.Mutex
	data map[string]*HASHTBVal
	//value []HASHTBVal
}
type ZSETAPI struct {
	mu   sync.Mutex
	data map[string]*zset.ZSetType
}
func server() {
	rpcs := rpc.NewServer()

	kv := new(KV)
	kv.data = map[string]string{}
	ht := new(HASHTB)
	ht.data = map[string]*HASHTBVal{}

	z := new(ZSETAPI)
	z.data = map[string]*zset.ZSetType{} //zset.New()
	rpcs.Register(kv)
	rpcs.Register(ht)
	rpcs.Register(z)


	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//持续循环，不退出进程
	for {
		conn, err := l.Accept()
		if err == nil {
			go rpcs.ServeConn(conn)
			//go handleConnection2(conn)
		} else {
			break
		}
	}
	l.Close()
	//go func() {
	//	for {
	//		conn, err := l.Accept()
	//		if err == nil {
	//			go rpcs.ServeConn(conn)
	//			//go handleConnection2(conn)
	//		} else {
	//			break
	//		}
	//	}
	//	l.Close()
	//}()
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.data[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[args.Key] = args.Value
	reply.Err = OK
	return nil
}
//HASH
func (ht *HASHTB) HGet(args *HGetArgs, reply *HGetReply) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hsval, ok := ht.data[args.Key]
	var  val string
	if(ok){
		val, ok = hsval.data[args.Field]
	}
	fmt.Printf("HGet key %s Field %s \n", args.Key,args.Field)
	fmt.Printf("HGet val %s\n", val)

	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func (ht *HASHTB) HSet(args *HSetArgs, reply *HSetReply) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	var hsval = new(HASHTBVal)
	hsval.data = make(map[string]string)
	hsval.data[args.Field] = args.Value
	ht.data[args.Key] = hsval

	fmt.Printf("HSet key %s Field %s Value %s\n", args.Key,args.Field,args.Value)
	reply.Err = OK
	return nil
}
//zset

func (z *ZSETAPI) ZScore(args *ZScoreArgs, reply *ZScoreReply) error {
	zval, ok := z.data[args.Key]
	var  val float64
	if(ok){
		val, ok = zval.Score(args.Mem)
	}
	fmt.Printf("HGet key %s Field %s \n", args.Key,args.Mem)
	fmt.Printf("HGet val %s\n", val)

	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = 0
	}
	return nil
}

func (z *ZSETAPI) ZAdd(args *ZAddArgs, reply *ZAddReply) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	zval := zset.New()
	zval.Add(args.Score,args.Mem)
	z.data[args.Key] = zval
	fmt.Printf("ZAdd key %s Score %s Mem %s\n", args.Key,args.Score,args.Mem)
	reply.Err = OK
	return nil
}

//
// main
//

func main() {
	server()

	//put("subject", "6.824")
	//fmt.Printf("Put(subject, 6.824) done\n")
	//fmt.Printf("get(subject) -> %s\n", get("subject"))
}