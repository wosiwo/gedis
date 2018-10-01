package handle

import (
	"../table"
	"strconv"

	//"bufio"
	"fmt"
	"sync"
)


//
//  request/reply definitions
//

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

/**
  值对象
 */
type ValueObj struct {
	Datatype int8		// 0:string 1:hash 2:set 3:zset
	Value interface{}
}
/**
 每个数据库节点的数据结构
 */
type RedisDB struct {
	Mu   sync.Mutex
	Dict map[string]ValueObj
}

type RedisServer struct {
	DBnum   int8
	DB[] RedisDB
}


type Err string

type Args struct {
	Key string
	Mem string
	Value string
	Field string
	Score float64
	Mems[] string
}

type Reply struct {
	Err Err
	Value string
}

//
// Server
//

type HASHTBVal struct {
	Data map[string]string
}


func (db *RedisDB) Get(args *Args, reply *Reply) error {


	val, ok := db.Dict[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = val.Value.(string)
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}
	return nil
}

func (db *RedisDB) Put(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()

	val := new(ValueObj)
	val.Value = args.Value
	val.Datatype = 0
	db.Dict[args.Key] = *val
	reply.Err = OK
	return nil
}
//HASH
func (db *RedisDB) HGet(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	hsObj, ok := db.Dict[args.Key]
	var  val string
	if(ok){
		hsval := hsObj.Value.(*HASHTBVal)
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

func (db *RedisDB) HSet(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	var hsval = new(HASHTBVal)
	var valObj = new(ValueObj)
	valObj.Datatype = 1


	hsval.Data = make(map[string]string)
	hsval.Data[args.Field] = args.Value
	valObj.Value = hsval

	db.Dict[args.Key] = *valObj

	fmt.Printf("HSet key %s Field %s Value %s\n", args.Key,args.Field,args.Value)
	reply.Err = OK
	return nil
}
//zset

func (db *RedisDB) ZScore(args *Args, reply *Reply) error {
	valObj := db.Dict[args.Key]
	zval, ok := valObj.Value.(*table.ZSetType)
	var  val float64
	if(ok){
		val, ok = zval.Score(args.Mem)
	}
	fmt.Printf("HGet key %s Field %s \n", args.Key,args.Mem)
	fmt.Printf("HGet val %s\n", val)

	if ok {
		reply.Err = OK
		reply.Value = strconv.FormatInt(int64(val),10)
	} else {
		reply.Err = ErrNoKey
		reply.Value = "0"
	}
	return nil
}

func (db *RedisDB) ZAdd(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	zval := table.New()
	zval.Add(args.Score,args.Mem)

	//值对象
	valObj := new(ValueObj)
	valObj.Value = zval
	valObj.Datatype = 3

	db.Dict[args.Key] = *valObj
	fmt.Printf("ZAdd key %s Score %s Mem %s\n", args.Key,args.Score,args.Mem)
	reply.Err = OK
	return nil
}

func (db *RedisDB) SAdd(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	var sval table.Set
	if _, ok := db.Dict[args.Key]; !ok {
		//不存在存在
		sval = *table.NewSet(args.Mems...)
	}else{
		valObj := db.Dict[args.Key]
		sval = valObj.Value.(table.Set)
		sval.HsVal.Add(args.Mems...)
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}
	//值对象
	valObj := new(ValueObj)
	valObj.Value = &sval
	valObj.Datatype = 2

	db.Dict[args.Key] = *valObj
	fmt.Printf("SAdd key %s Score %s Mem \n", args.Key)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}

func (db *RedisDB) SCard(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	var sval *table.Set
	if _, ok := db.Dict[args.Key]; !ok {
		//不存在存在
		reply.Value = "0"
	}else{
		valObj := db.Dict[args.Key]
		sval = valObj.Value.(*table.Set)
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
func (db *RedisDB) SMembers(args *Args, reply *Reply) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()
	var sval *table.Set
	if _, ok := db.Dict[args.Key]; !ok {
		//不存在存在
		reply.Value = ""
	}else{
		valObj := db.Dict[args.Key]
		sval = valObj.Value.(*table.Set)
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