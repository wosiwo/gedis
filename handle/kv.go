package handle

import (
	"../table"
	"container/list"
	"strconv"
	"strings"

	//"bufio"
	"fmt"
	"sync"
	"errors"
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
type GedisDB struct {
	Mu   sync.Mutex
	Dict map[string]ValueObj
}
type GedisServer struct {
	DBnum   int
	Pid     int
	DB[] GedisDB
	Commands         map[string]*GedisCommand
}

//命令函数指针
type CommandProc func(c *GdClient)

type GedisCommand struct {
	Name string
	Proc CommandProc
}

//每次连接，创建一个对应的客户端
type GdClient struct {
	Db       *GedisDB
	DBId	 int		//当前使用的数据库id
	QueryBuf string		//查询缓冲区

	Cmd      *GedisCommand
	Argv     []string
	Argc     int
	Key      string

	Reqtype  int		//请求的类型：内联命令还是多条命令
	Ctime    int		//客户端创建时间
	Buf      string		//回复缓冲区
	FakeFlag bool
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

//命令解析
func (c *GdClient) ProcessInputBuffer() error {
	cmdStrArr := strings.Split(c.QueryBuf, "\r\n")
	//fmt.Println(cmdStrArr)
	cmd := cmdStrArr[2]
	cmdLen := len(cmdStrArr)
	fmt.Println("cmdLen %d", cmdLen)
	cmd = strings.ToLower(cmd)
	fmt.Println("cmd " + cmd)

	c.Argc = cmdLen
	c.Argv = cmdStrArr
	return nil
}


//创建客户端
func (s *GedisServer) CreateClient() (c *GdClient) {
	c = new(GdClient)
	c.DBId = 0
	c.QueryBuf = ""
	return c
}

func (s *GedisServer) ProcessCommand(c *GdClient) error {
	if c.Argc<6 {
		return  nil
	}
	cmdName := c.Argv[2]
	c.Key = c.Argv[4]
	cmd := lookupCommand(cmdName,s)
	//fmt.Println(cmd, cmdName, s)
	if cmd != nil {
		c.Cmd = cmd
		call(c)
	} else {
		fmt.Println("(error) ERR unknown command '%s'", cmdName)
		return errors.New("ProcessInputBuffer failed")
	}

	return nil

}

// 查找命令对应的执行函数
func lookupCommand(name string, s *GedisServer) *GedisCommand {
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
func addReplyBulk(c *GdClient,retValue string){
	c.Buf = retValue
}



/**
****************************
	以下为数据操作方法
****************************
 */

//get
func (s *GedisServer) Get(c *GdClient) {
	//fmt.Println(s.DB)
	//fmt.Println("s.DB[c.DBId]")
	//fmt.Println(s.DB[c.DBId])
	val, ok := s.DB[c.DBId].Dict[c.Key]
	if ok {
		addReplyBulk(c,val.Value.(string))
	} else {
		addReplyBulk(c,"nil")
	}
}

//set
func (s *GedisServer) Set(c *GdClient)  {
	var valObj ValueObj
	db := &s.DB[c.DBId]
	Value := c.Argv[6]
	if _, ok := db.Dict[c.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()

		valObj = *new(ValueObj)
		valObj.Value = Value
		valObj.Datatype = 0
	}else{
		valObj = db.Dict[c.Key]
		valObj.Value = Value
	}

	db.Dict[c.Key] = valObj
	addReplyBulk(c,"+OK")
	//return nil
}
//HASH
func (db *GedisDB) HGet(args *Args, reply *Reply) error {
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

func (db *GedisDB) HSet(args *Args, reply *Reply) error {
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

func (db *GedisDB) ZScore(args *Args, reply *Reply) error {
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

func (db *GedisDB) ZAdd(args *Args, reply *Reply) error {
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

func (db *GedisDB) SAdd(args *Args, reply *Reply) error {
	var sval *table.Set
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()
		//不存在存在
		sval = table.NewSet(args.Mems...)
		//值对象
		valObj = *new(ValueObj)
		valObj.Value = &sval
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

func (db *GedisDB) SCard(args *Args, reply *Reply) error {
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
func (db *GedisDB) SMembers(args *Args, reply *Reply) error {
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


func (db *GedisDB) LPush(args *Args, reply *Reply) error {
	var lval list.List
	var valObj ValueObj
	if _, ok := db.Dict[args.Key]; !ok {
		db.Mu.Lock()	//创建新键值对时才使用全局锁
		defer db.Mu.Unlock()
		//不存在存在
		lval = *list.New()
		//值对象
		valObj = *new(ValueObj)
		valObj.Value = &lval
		valObj.Datatype = 4
	}else{
		valObj = db.Dict[args.Key]
		lval = valObj.Value.(list.List)
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

func (db *GedisDB) LPop(args *Args, reply *Reply) error {
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