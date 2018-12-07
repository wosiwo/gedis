package core

import (
	"./aof"
	"./config"
	"./table"
	"container/list"
	"errors"
	"fmt"
	"gedis/core/heap"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type HASHTBVal struct {
	Data map[string]string
}

/**
  值对象
*/
type ValueObj struct {
	Rw       sync.RWMutex
	Datatype int8 // 0:string 1:hash 2:set 3:zset 4:list
	Value    interface{}
}

/**
每个数据库节点的数据结构
*/
type GedisDB struct {
	Mu   sync.Mutex
	Dict map[string]*ValueObj
}

type GedisServer struct {
	DBnum      int
	Pid        int
	DB         []GedisDB
	WriteC     chan GdClient //写channel
	IsChannel  bool
	CmdBuffer  aof.CmdBuffer
	Commands   map[string]*GedisCommand
	AofPath    string
	AofLoadNum int
	DeleteHeap heap.Heap
}

//命令函数指针
type CommandProc func(c *GdClient)

type GedisCommand struct {
	Name    string
	IsWrite bool
	IsPing  bool
	Proc    CommandProc
}

//创建客户端
func (s *GedisServer) CreateClient() (c *GdClient) {
	c = new(GdClient)
	c.DBId = 0
	c.QueryBuf = ""
	return c
}

func (s *GedisServer) ProcessCommand(c *GdClient) error {
	//fmt.Println(cmd, cmdName, s)
	if c.Cmd != nil {
		call(c)
	} else {
		fmt.Println("(error) ERR unknown command ", c.CommandName)
		return errors.New("ProcessInputBuffer failed")
	}
	return nil
}

func (s *GedisServer) ClearExpireKey() {
	for {
		fmt.Println(s.DeleteHeap)
		if s.DeleteHeap.Length == 0 {
			break
		}
		node := s.DeleteHeap.List[1]
		fmt.Println(node)
		t := time.Now()
		now := t.Unix()
		fmt.Println(now)
		if node.Value <= now {
			s.DeleteHeap.GetTopHeap()
			fmt.Println(node.Key, " is cleaned!")
			c := GdClient{}
			c.Key = node.Key
			s.Del(&c)
			continue
		} else {
			break
		}
	}
}

func (s *GedisServer) Del(c *GdClient) {
	_, ok := s.DB[c.DBId].Dict[c.Key]
	if ok {
		delete(s.DB[c.DBId].Dict, c.Key)
	} else {
		addReplyBulk(c, "nil")
		fmt.Printf("%s is not exsit \n", c.Key)
	}
}

//get
func (s *GedisServer) Get(c *GdClient) {
	//fmt.Println(s.DB)
	//fmt.Println("s.DB[c.DBId]")
	//fmt.Println(s.DB[c.DBId])
	val, ok := s.DB[c.DBId].Dict[c.Key]
	defer func(ok bool) {
		if ok {
			val.Rw.RUnlock() //解除读锁
		}
	}(ok)
	if ok {
		val.Rw.RLock() //加读锁
		addReplyBulk(c, val.Value.(string))

	} else {
		addReplyBulk(c, "nil")
	}
}

//set
func (s *GedisServer) Set(c *GdClient) {
	var valObj *ValueObj
	db := &s.DB[c.DBId]
	Value := c.Argv[6]
	if len(c.Argv) >= 9 {
		t := time.Now()
		now := t.Unix()
		fmt.Println(c.Argv)
		second, err := strconv.ParseInt(c.Argv[8], 10, 64)
		if err != nil {
			fmt.Println("字符串转换成整数失败")
		}
		delTime := now + second
		node := heap.Node{}
		node.Value = delTime
		node.Key = c.Key
		s.DeleteHeap.InsertHeap(&node)
	}
	valObj, ok := db.Dict[c.Key]
	//defer func(ok bool) {
	//	if ok {
	//		valObj.Rw.Unlock() //解除写锁
	//	}
	//}(ok)
	if !ok {
		valObj = new(ValueObj)
		valObj.Value = Value
		valObj.Datatype = 0
		db.Dict[c.Key] = valObj
	} else {
		//valObj.Rw.Lock()
		//valObj = db.Dict[c.Key]
		valObj.Value = Value
	}

	addReplyBulk(c, "+OK")
}

//HASH
func (s *GedisServer) HGet(c *GdClient) {
	db := &s.DB[c.DBId]
	hsObj, ok := db.Dict[c.Key]
	var val string
	Field := c.Argv[6]
	defer func(ok bool) {
		if ok {
			hsObj.Rw.RUnlock() //解除读锁
		}
	}(ok)
	if ok {
		hsObj.Rw.RLock() //加读锁
		hsval := hsObj.Value.(*HASHTBVal)
		val, ok = hsval.Data[Field]
	}
	fmt.Printf("HGet key %s Field %s \n", c.Key, Field)
	fmt.Printf("HGet val %s\n", val)
	if ok {
		addReplyBulk(c, val)
	} else {
		addReplyBulk(c, "")
	}
}

func (s *GedisServer) HSet(c *GdClient) {
	var valObj *ValueObj
	db := &s.DB[c.DBId]
	Field := c.Argv[6]
	Value := c.Argv[8]
	valObj, ok := db.Dict[c.Key]
	defer func(ok bool) {
		if ok {
			valObj.Rw.Unlock() //解除写锁
		}
	}(ok)
	if !ok {
		defer db.Mu.Unlock()
		var hsval = new(HASHTBVal)
		valObj = new(ValueObj)
		valObj.Datatype = 1
		hsval.Data = make(map[string]string)
		hsval.Data[Field] = Value
		valObj.Value = hsval
		db.Dict[c.Key] = valObj
	} else {
		valObj.Rw.Lock() //加写锁
		//valObj = db.Dict[c.Key]
		hsval := valObj.Value.(*HASHTBVal)
		hsval.Data[Field] = Value
	}
	if s.IsChannel {
		s.WriteC <- *c
	}
	fmt.Printf("HSet key %s Field %s Value %s\n", c.Key, Field, Value)
	addReplyBulk(c, "+OK")
}

//zset
func (s *GedisServer) ZScore(c *GdClient) {
	db := &s.DB[c.DBId]
	valObj := db.Dict[c.Key]
	zval, ok := valObj.Value.(*table.ZSetType)
	Mem := c.Argv[6]
	var val float64

	defer func(ok bool) {
		if ok {
			valObj.Rw.RUnlock() //解除读锁
		}
	}(ok)

	if ok {
		valObj.Rw.RLock()
		val, ok = zval.Score(Mem)
	}
	fmt.Printf("HGet key %s Field %s \n", c.Key, Mem)
	fmt.Printf("HGet val %s\n", val)
	if s.IsChannel {
		s.WriteC <- *c
	}
	if ok {
		addReplyBulk(c, strconv.FormatInt(int64(val), 10))
	} else {
		addReplyBulk(c, "0")
	}
}

func (s *GedisServer) ZAdd(c *GdClient) {
	var valObj *ValueObj
	db := &s.DB[c.DBId]
	Score, _ := strconv.ParseFloat(c.Argv[6], 64)
	Mem := c.Argv[8]
	valObj, ok := db.Dict[c.Key]
	defer func(ok bool) {
		if ok {
			valObj.Rw.Unlock() //解除写锁
		}
	}(ok)
	if !ok {
		db.Mu.Lock() //创建新键值对时才使用全局锁
		defer db.Mu.Unlock()
		zval := table.New()
		zval.Add(Score, Mem)
		//值对象
		valObj = new(ValueObj)
		valObj.Value = zval
		valObj.Datatype = 3
		db.Dict[c.Key] = valObj
	} else {
		valObj.Rw.Lock()
		//valObj = db.Dict[c.Key]
		if valObj.Datatype != 3 { //判断键的类型
			addReplyBulk(c, "key exists but not zset")
			return
		}
		zval := valObj.Value.(*table.ZSetType)
		zval.Add(Score, Mem)
	}
	if s.IsChannel {
		s.WriteC <- *c
	}
	fmt.Printf("ZAdd key %s Score %s Mem %s\n", c.Key, Score, Mem)
	addReplyBulk(c, "+OK")
}

func (s *GedisServer) SAdd(c *GdClient) {
	var sval *table.Set
	var valObj *ValueObj
	db := &s.DB[c.DBId]
	var Mems []string
	//TODO 支持多个元素
	Mems = append(Mems, c.Argv[6])
	Mems = append(Mems, c.Argv[8])
	valObj, ok := db.Dict[c.Key]
	defer func(ok bool) {
		if ok {
			valObj.Rw.Unlock() //解除写锁
		}
	}(ok)
	if !ok {
		//db.Mu.Lock() //创建新键值对时才使用全局锁
		//defer db.Mu.Unlock()
		//不存在存在
		sval = table.NewSet(Mems...)
		//值对象
		valObj = new(ValueObj)
		valObj.Value = &sval
		valObj.Datatype = 2
		db.Dict[c.Key] = valObj
	} else {
		valObj.Rw.Lock()
		//valObj = db.Dict[c.Key]
		sval = valObj.Value.(*table.Set)
		sval.HsVal.Add(Mems...)
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}

	fmt.Printf("SAdd key %s Score %s Mem \n", c.Key)
	fmt.Println(Mems)
	if s.IsChannel {
		s.WriteC <- *c
	}
	addReplyBulk(c, "+OK")

}

func (s *GedisServer) SCard(c *GdClient) {
	var sval *table.Set
	db := &s.DB[c.DBId]
	var Value string
	if _, ok := db.Dict[c.Key]; !ok {
		//不存在存在
		Value = "0"
	} else {
		valObj := db.Dict[c.Key]
		sval = valObj.Value.(*table.Set)
		count := sval.HsVal.Count()
		Value = strconv.FormatInt(int64(count), 10)
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}
	fmt.Printf("SCard key %s Count %s  \n", c.Key, Value)
	addReplyBulk(c, Value)
}

func (s *GedisServer) SMembers(c *GdClient) {
	var sval *table.Set
	var valObj *ValueObj

	db := &s.DB[c.DBId]
	var Value string
	valObj, ok := db.Dict[c.Key]
	defer func(ok bool) {
		if ok {
			valObj.Rw.RUnlock() //解除读锁
		}
	}(ok)
	if !ok {
		//不存在存在
		Value = ""
	} else {
		valObj.Rw.RLock()
		//valObj := db.Dict[c.Key]
		sval = valObj.Value.(*table.Set)
		Value = sval.HsVal.SMembers()
	}

	//TODO 判断是否使用整数集合
	//if(sval.EncodeType == 0){
	//	sval.IntVal.Add(args.Mems...)
	//}else{
	//	sval.HsVal.Add(args.Mems...)
	//}
	fmt.Printf("SMembers key %s Value %s  \n", c.Key, Value)
	addReplyBulk(c, Value)
}

// list
func (s *GedisServer) LPush(c *GdClient) {
	var lval list.List
	var valObj *ValueObj
	db := &s.DB[c.DBId]
	var Mems []string
	//TODO 支持多个元素
	Mems = append(Mems, c.Argv[6])
	Mems = append(Mems, c.Argv[8])
	valObj, ok := db.Dict[c.Key]
	defer func(ok bool) {
		if ok {
			valObj.Rw.Unlock() //解除写锁
		}
	}(ok)
	if !ok {
		db.Mu.Lock() //创建新键值对时才使用全局锁
		defer db.Mu.Unlock()
		//不存在存在
		lval = *list.New()
		//值对象
		valObj = new(ValueObj)
		valObj.Value = &lval
		valObj.Datatype = 4
	} else {
		valObj.Rw.Lock()
		//valObj = db.Dict[c.Key]
		lval = valObj.Value.(list.List)
	}
	//将元素循环加入列表 头部
	for _, item := range Mems {
		lval.PushFront(item)
	}
	db.Dict[c.Key] = valObj
	if s.IsChannel {
		s.WriteC <- *c
	}
	fmt.Printf("LPush key %s Mem \n", c.Key)
	fmt.Println(Mems)
	addReplyBulk(c, "+OK")

}

func (s *GedisServer) LPop(c *GdClient) {
	db := &s.DB[c.DBId]
	var Value string
	valObj, ok := db.Dict[c.Key]
	defer func(ok bool) {
		if ok {
			valObj.Rw.RUnlock() //解除读锁
		}
	}(ok)
	if !ok {
		Value = ""
	} else {
		valObj.Rw.RLock()
		//valObj := db.Dict[c.Key]
		lval := valObj.Value.(*list.List)
		Value = ""
		if lval.Len() > 0 {
			Value = lval.Remove(lval.Front()).(string)
		}
	}
	if s.IsChannel {
		s.WriteC <- *c
	}
	fmt.Printf("LPush key %s Mem \n", c.Key)
	addReplyBulk(c, Value)
}

// 查找命令对应的执行函数
func (s *GedisServer) lookupCommand(name string) *GedisCommand {
	name = strings.ToUpper(name)
	if cmd, ok := s.Commands[name]; ok {
		return cmd
	}
	return nil
}

//调用命令的实现函数，执行命令
func call(c *GdClient) {
	c.Cmd.Proc(c)
}

//返回一个 数据
func addReplyBulk(c *GdClient, retValue string) {
	c.Buf = retValue
}

func (s *GedisServer) RunServer(conf *config.Config) {
	//fmt.Println(conf)
	//堆初始化
	s.DeleteHeap = heap.Heap{}
	node := heap.Node{}
	s.DeleteHeap.List = append(s.DeleteHeap.List, &node)
	//数据库初始化
	s.DBnum = 16
	s.Pid = os.Getpid()
	//是否开启channel
	s.IsChannel = conf.GetBoolDefault("isChannel", false)
	if s.IsChannel {
		s.WriteC = make(chan GdClient, 1)
		//是否开启channel 必须配置日志路径
		s.AofPath = conf.GetIStringDefault("aof_path", "./conf/aof.log")
		s.AofLoadNum = conf.GetIntDefault("aof_load_num", 1)
	}
	//gdServer.Commands = map[string]*core.GedisCommand{}	//命令数组
	initDB(s)
	//初始化命令哈希表
	initCommand(s)
	//加载aof日志
	if s.IsChannel {
		LoadData(s)
	}
}

func LoadData(gdServer *GedisServer) {
	fmt.Println("Loading Data......")
	c := gdServer.CreateClient()
	c.FakeFlag = true
	pros := aof.ReadAof(gdServer.AofPath)
	for _, v := range pros {
		c.QueryBuf = string(v)
		err := c.ProcessInputBuffer(gdServer)
		if err != nil {
			log.Println("ProcessInputBuffer err", err)
		}
		gdServer.ProcessCommand(c)
	}
	fmt.Println("Loading Data is finished")
}

func initDB(gdServer *GedisServer) {
	gdServer.DB = make([]GedisDB, gdServer.DBnum)
	for i := 0; i < gdServer.DBnum; i++ {
		gdServer.DB[i] = GedisDB{} //初始化
		gdServer.DB[i].Dict = map[string]*ValueObj{}
	}
}

func initCommand(gdServer *GedisServer) {
	getCommand := &GedisCommand{Name: "GET", Proc: gdServer.Get, IsWrite: false}
	setCommand := &GedisCommand{Name: "SET", Proc: gdServer.Set, IsWrite: true}
	hgetCommand := &GedisCommand{Name: "HGET", Proc: gdServer.HGet, IsWrite: false}
	hsetCommand := &GedisCommand{Name: "HSET", Proc: gdServer.HSet, IsWrite: true}
	zaddCommand := &GedisCommand{Name: "ZADD", Proc: gdServer.ZAdd, IsWrite: true}
	zscoreCommand := &GedisCommand{Name: "ZSCORE", Proc: gdServer.ZScore, IsWrite: false}
	saddCommand := &GedisCommand{Name: "SADD", Proc: gdServer.SAdd, IsWrite: true}
	scardCommand := &GedisCommand{Name: "SCARD", Proc: gdServer.SCard, IsWrite: false}
	smembersCommand := &GedisCommand{Name: "SMEMBERS", Proc: gdServer.SMembers, IsWrite: false}
	lpushCommand := &GedisCommand{Name: "LPUSH", Proc: gdServer.LPush, IsWrite: true}
	lpopCommand := &GedisCommand{Name: "LPOP", Proc: gdServer.LPop, IsWrite: true}
	comandComand := &GedisCommand{Name: "COMMAND", Proc: gdServer.Command, IsWrite: false}
	comandDel := &GedisCommand{Name: "DEL", Proc: gdServer.Del, IsWrite: true}

	gdServer.Commands = map[string]*GedisCommand{
		"GET":      getCommand,
		"SET":      setCommand,
		"HGET":     hgetCommand,
		"HSET":     hsetCommand,
		"ZADD":     zaddCommand,
		"ZSCORE":   zscoreCommand,
		"SADD":     saddCommand,
		"SCARD":    scardCommand,
		"SMEMBERS": smembersCommand,
		"LPUSH":    lpushCommand,
		"LPOP":     lpopCommand,
		"COMMAND":  comandComand,
		"DEL":      comandDel,
	}
}

func (s *GedisServer) Command(c *GdClient) {
	var Value string
	Value = "+OK"
	//fmt.Printf("LPush key %s Mem \n", c.Key)
	addReplyBulk(c, Value)
}
