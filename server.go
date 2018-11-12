package main

import (
	"./core"
	"./core/aof"
	"fmt"
	"net"
	"os"
	//"./table"
	"./core/config"
)

var gdServer = new(core.GedisServer)
var DBIndex int8
var confPath = "./conf/server.conf"

func coreArg() {
	//处理命令行参数
	argv := os.Args
	argc := len(os.Args)
	if argc > 2 {
		if argv[1] == "--v" || argv[1] == "-V" {
			fmt.Println("Gedis server v=1.3.0 bits=64") //输出版本号
			os.Exit(0)
		}
	}
}
func main() {
	//处理命令行参数
	coreArg()
	//读取配置
	conf, err := config.NewConfig(confPath)
	if err != nil {
		fmt.Printf("read config file err: %v", err)
		return
	}
	//数据库初始化
	DBIndex = 0
	gdServer.DBnum = 16
	gdServer.Pid = os.Getpid()
	//是否开启channel
	gdServer.IsChannel = conf.GetBoolDefault("isChannel", false)
	if gdServer.IsChannel {
		gdServer.WriteC = make(chan core.GdClient)
		//是否开启channel 必须配置日志路径
		gdServer.AofPath = conf.GetIStringDefault("aof_path", "./conf/aof.log")
	}

	//gdServer.Commands = map[string]*core.GedisCommand{}	//命令数组
	initDB(gdServer)
	//初始化命令哈希表
	initCommand(gdServer)
	var tcpAddr *net.TCPAddr
	host := conf.GetIStringDefault("hostname", "127.0.0.1")
	port := conf.GetIStringDefault("port", "9999")
	hostPort := net.JoinHostPort(host, port)
	fmt.Println(hostPort)
	tcpAddr, _ = net.ResolveTCPAddr("tcp", hostPort)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn, err)
		if gdServer.IsChannel {
			var c core.GdClient
			select {
			case c = <-gdServer.WriteC:
				//写入日志
				if c.FakeFlag == false {
					aof.AppendToFile(gdServer.AofPath, c.QueryBuf)
				}
			}
		}
	}
}

func tcpPipe(conn *net.TCPConn, err error) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	c := gdServer.CreateClient()

	//reader := bufio.NewReader(conn)
	//读取请求内容
	command := make([]byte, 1024)
	n, err := conn.Read(command)
	cmdstr := string(command[:n])
	fmt.Println("cmdstr" + cmdstr)

	c.QueryBuf = cmdstr    //命令
	c.ProcessInputBuffer() //命令解析

	err = gdServer.ProcessCommand(c) //执行命令
	//reply := parseCmd(cmdstr)
	if err != nil {
		fmt.Println("ProcessInputBuffer err", err)
		return
	}

	//返回数据给客户端
	SendReplyToClient(conn, c)
	//}
}

func initDB(gdServer *core.GedisServer) {
	gdServer.DB = make([]core.GedisDB, gdServer.DBnum)
	for i := 0; i < gdServer.DBnum; i++ {
		gdServer.DB[i] = core.GedisDB{} //初始化
		gdServer.DB[i].Dict = map[string]core.ValueObj{}
	}
}

func initCommand(gdServer *core.GedisServer) {
	getCommand := &core.GedisCommand{Name: "get", Proc: gdServer.Get}
	setCommand := &core.GedisCommand{Name: "get", Proc: gdServer.Set}
	hgetCommand := &core.GedisCommand{Name: "get", Proc: gdServer.HGet}
	hsetCommand := &core.GedisCommand{Name: "hset", Proc: gdServer.HSet}
	zaddCommand := &core.GedisCommand{Name: "zadd", Proc: gdServer.ZAdd}
	zscoreCommand := &core.GedisCommand{Name: "zscore", Proc: gdServer.ZScore}
	saddCommand := &core.GedisCommand{Name: "sadd", Proc: gdServer.SAdd}
	scardCommand := &core.GedisCommand{Name: "scard", Proc: gdServer.SCard}
	smembersCommand := &core.GedisCommand{Name: "smembers", Proc: gdServer.SMembers}
	lpushCommand := &core.GedisCommand{Name: "lpush", Proc: gdServer.LPush}
	lpopCommand := &core.GedisCommand{Name: "lpop", Proc: gdServer.LPop}

	gdServer.Commands = map[string]*core.GedisCommand{
		"get":      getCommand,
		"set":      setCommand,
		"hget":     hgetCommand,
		"hset":     hsetCommand,
		"zadd":     zaddCommand,
		"zscore":   zscoreCommand,
		"sadd":     saddCommand,
		"scard":    scardCommand,
		"smembers": smembersCommand,
		"lpush":    lpushCommand,
		"lpop":     lpopCommand,
	}
}

// 负责传送命令回复的写处理器
func SendReplyToClient(conn net.Conn, c *core.GdClient) {
	len := len(c.Buf)
	fmt.Println(c.Buf)
	rep := fmt.Sprintf("$%d\r\n%s\r\n", len, c.Buf)
	fmt.Println("replyVal " + c.Buf)

	conn.Write([]byte(rep))
}
