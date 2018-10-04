package handle

import (
	"../table"
	"strconv"
	"container/list"
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
	Mu   sync.Mutex
	Datatype int8		// 0:string 1:hash 2:set 3:zset 4:list
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

func (db *RedisDB) Set(args *Args, reply *Reply) error {
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()

		valObj = *new(ValueObj)
		valObj.Value = args.Value
		valObj.Datatype = 0
	}else{
		valObj = db.Dict[args.Key]
		valObj.Value = args.Value
	}

	db.Dict[args.Key] = valObj
	reply.Err = OK
	return nil
}
//HASH
func (db *RedisDB) HGet(args *Args, reply *Reply) error {
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
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()

		var hsval = new(HASHTBVal)
		valObj = *new(ValueObj)
		valObj.Datatype = 1


		hsval.Data = make(map[string]string)
		hsval.Data[args.Field] = args.Value
		valObj.Value = hsval
	}else{
		valObj = db.Dict[args.Key]
		hsval := valObj.Value.(*HASHTBVal)
		hsval.Data[args.Field] = args.Value
	}


	db.Dict[args.Key] = valObj

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
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()

		zval := table.New()
		zval.Add(args.Score,args.Mem)

		//值对象
		valObj = *new(ValueObj)
		valObj.Value = zval
		valObj.Datatype = 3
	}else{
		valObj = db.Dict[args.Key]
		zval := valObj.Value.(*table.ZSetType)
		zval.Add(args.Score,args.Mem)
	}


	db.Dict[args.Key] = valObj
	fmt.Printf("ZAdd key %s Score %s Mem %s\n", args.Key,args.Score,args.Mem)
	reply.Err = OK
	return nil
}

func (db *RedisDB) SAdd(args *Args, reply *Reply) error {
	var sval *table.Set
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()
		//不存在存在
		sval = table.NewSet(args.Mems...)
		//值对象
		valObj = *new(ValueObj)
		valObj.Value = sval
		valObj.Datatype = 2
	}else{
		valObj = db.Dict[args.Key]
		sval = valObj.Value.(*table.Set)
		sval.HsVal.Add(args.Mems...)
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}


	db.Dict[args.Key] = valObj
	fmt.Printf("SAdd key %s Score %s Mem \n", args.Key)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}

func (db *RedisDB) SCard(args *Args, reply *Reply) error {
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

// list


func (db *RedisDB) LPush(args *Args, reply *Reply) error {
	var lval *list.List
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()
		//不存在存在
		lval = list.New()
		//值对象
		valObj = *new(ValueObj)
		valObj.Value = lval
		valObj.Datatype = 4
	}else{
		valObj = db.Dict[args.Key]
		lval = valObj.Value.(*list.List)
	}
	//将元素循环加入列表 头部
	for _, item := range args.Mems{
		lval.PushFront(item)
	}
	db.Dict[args.Key] = valObj
	fmt.Printf("LPush key %s Mem \n", args.Key)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}

func (db *RedisDB) LPop(args *Args, reply *Reply) error {
	if _, ok := db.Dict[args.Key]; !ok {
		reply.Value = ""
	}else{
		valObj := db.Dict[args.Key]
		lval := valObj.Value.(*list.List)
		reply.Value = ""
		if(lval.Len()>0){
			reply.Value = lval.Remove(lval.Front()).(string)
		}
	}

	fmt.Printf("LPush key %s Mem \n", args.Key)
	fmt.Println(args.Mems)
	reply.Err = OK
	return nil
}



func (db *RedisDB) Delete(args *Args, reply *Reply) error {
	if _, ok := db.Dict[args.Key]; !ok {
		reply.Value = "+OK"
	}else{
		delete(db.Dict,args.Key)
		reply.Value = "+OK"
	}

	fmt.Printf("Delete key %s  \n", args.Key)
	reply.Err = OK
	return nil
}

