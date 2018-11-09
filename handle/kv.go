package handle

import (
	"../table"
	"strconv"

	//"bufio"
	"fmt"
	"sync"
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


type Args struct {
	Key string
	Mem string
	Mems[] string
}

type Reply struct {
	Err Err
	Value string
}
type PutReply struct {
	Err Err
	Value string
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
	Mu   sync.Mutex
	Data map[string]string
}
type HASHTBVal struct {
	Data map[string]string
}
type HASHTB struct {
	Mu   sync.Mutex
	Data map[string]*HASHTBVal
	//value []HASHTBVal
}
type ZSETAPI struct {
	Mu   sync.Mutex
	Data map[string]*table.ZSetType
}

type SETAPI struct {
	Mu   sync.Mutex
	Data map[string]*table.Set
}

//
//func server() {
//	rpcs := rpc.NewServer()
//
//	kv := new(KV)
//	kv.Data = map[string]string{}
//	ht := new(HASHTB)
//	ht.Data = map[string]*HASHTBVal{}
//
//	z := new(ZSETAPI)
//	z.Data = map[string]*zset.ZSetType{} //zset.New()
//	rpcs.Register(kv)
//	rpcs.Register(ht)
//	rpcs.Register(z)
//
//
//	l, e := net.Listen("tcp", ":1234")
//	if e != nil {
//		log.Fatal("listen error:", e)
//	}
//	//持续循环，不退出进程
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
//	//go func() {
//	//	for {
//	//		conn, err := l.Accept()
//	//		if err == nil {
//	//			go rpcs.ServeConn(conn)
//	//			//go handleConnection2(conn)
//	//		} else {
//	//			break
//	//		}
//	//	}
//	//	l.Close()
//	//}()
//}

func Get(kv *KV, args *GetArgs, reply *GetReply) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	val, ok := kv.Data[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func Put(kv *KV, args *PutArgs, reply *PutReply) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	kv.Data[args.Key] = args.Value
	reply.Err = OK
	return nil
}
//HASH
func HGet(ht *HASHTB, args *HGetArgs, reply *HGetReply) error {
	ht.Mu.Lock()
	defer ht.Mu.Unlock()
	hsval, ok := ht.Data[args.Key]
	var  val string
	if(ok){
		val, ok = hsval.Data[args.Field]
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

func HSet(ht *HASHTB, args *HSetArgs, reply *HSetReply) error {
	ht.Mu.Lock()
	defer ht.Mu.Unlock()
	var hsval = new(HASHTBVal)
	hsval.Data = make(map[string]string)
	hsval.Data[args.Field] = args.Value
	ht.Data[args.Key] = hsval

	fmt.Printf("HSet key %s Field %s Value %s\n", args.Key,args.Field,args.Value)
	reply.Err = OK
	return nil
}
//zset

func ZScore(z *ZSETAPI, args *ZScoreArgs, reply *ZScoreReply) error {
	zval, ok := z.Data[args.Key]
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

func ZAdd(z *ZSETAPI, args *ZAddArgs, reply *ZAddReply) error {
	z.Mu.Lock()
	defer z.Mu.Unlock()
	zval := table.New()
	zval.Add(args.Score,args.Mem)
	z.Data[args.Key] = zval
	fmt.Printf("ZAdd key %s Score %s Mem %s\n", args.Key,args.Score,args.Mem)
	reply.Err = OK
	return nil
}

func SAdd(s *SETAPI, args *Args, reply *Reply) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	var sval table.Set
	if _, ok := s.Data[args.Key]; !ok {
		//不存在存在
		sval = *table.NewSet(args.Mems...)
	}else{
		sval = *s.Data[args.Key]
		sval.HsVal.Add(args.Mems...)
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}
	s.Data[args.Key] = &sval
	fmt.Printf("SAdd key %s Score %s Mem \n", args.Key)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}

func SCard(s *SETAPI, args *Args, reply *Reply) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	var sval table.Set
	if _, ok := s.Data[args.Key]; !ok {
		//不存在存在
		reply.Value = "0"
	}else{
		sval = *s.Data[args.Key]
		count := sval.HsVal.Count()
		reply.Value = strconv.FormatInt(int64(count),10)
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}
	fmt.Printf("SCard key %s Count %s  \n", args.Key,reply.Value)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}
func SMembers(s *SETAPI, args *Args, reply *Reply) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	var sval table.Set
	if _, ok := s.Data[args.Key]; !ok {
		//不存在存在
		reply.Value = ""
	}else{
		sval = *s.Data[args.Key]
		reply.Value = sval.HsVal.SMembers()
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}
	fmt.Printf("SMembers key %s Value %s  \n", args.Key,reply.Value)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}

//
// main
//
//
//func main() {
//	server()
//
//	//put("subject", "6.824")
//	//fmt.Printf("Put(subject, 6.824) done\n")
//	//fmt.Printf("get(subject) -> %s\n", get("subject"))
//}