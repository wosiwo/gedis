package main

import (
	"./handle"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	//"./table"
	"./core/config"
)

var gdServer =  new(handle.GedisServer)
var DBIndex int8
var confPath = "./conf/server.conf"

func handleArg() {
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
	handleArg()
	//读取配置
	conf, err := config.NewConfig(confPath)
	if err != nil {
		fmt.Printf("read config file err: %v", err)
		return
	}
	//数据库初始化
	DBIndex = 0
	gdServer.Pid = os.Getpid()
	//gdServer.Commands = map[string]*handle.GedisCommand{}	//命令数组
	initDB(gdServer)

	var tcpAddr *net.TCPAddr
	host := conf.GetIStringDefault("hostname", "127.0.0.1")
	port := conf.GetIStringDefault("port", "9999")
	hostPort := net.JoinHostPort(host, port)
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
	command := make([]byte, 1024)
	n, err := conn.Read(command)
	cmdstr := string(command[:n])
	fmt.Println("cmdstr" + cmdstr)

	reply := parseCmd(cmdstr)

	b := []byte(reply)
	conn.Write(b)
	//}
}

func parseCmd(cmdStr string) string {
	cmdStrArr := strings.Split(cmdStr, "\r\n")

	cmd := cmdStrArr[2]
	cmdLen := len(cmdStrArr)
	fmt.Println("cmdLen %d", cmdLen)
	cmd = strings.ToLower(cmd)
	fmt.Println("cmd " + cmd)
	var replyVal string
	//var reply handle.GetReply
	key := cmdStrArr[4]

	//获取当前数据库
	db := gdServer.DB[DBIndex]
	switch {
	case cmd == "get" && cmdLen >= 6:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		db.Get(&reqArgs, &reply)
		replyVal = reply.Value
	case cmd == "set" && cmdLen >= 8:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		reqArgs.Value = cmdStrArr[6]
		db.Set(&reqArgs, &reply)
		fmt.Println("tt")
		replyVal = "+OK"
		fmt.Println(reply.Value)

	case cmd == "hget" && cmdLen >= 8:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		reqArgs.Field = cmdStrArr[6]
		db.HGet(&reqArgs, &reply)
		replyVal = reply.Value

	case cmd == "hset" && cmdLen >= 10:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		reqArgs.Field = cmdStrArr[6]
		reqArgs.Value = cmdStrArr[8]
		db.HSet(&reqArgs, &reply)
		replyVal = "+OK"
	case cmd == "zadd" && cmdLen >= 10:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		reqArgs.Mem = cmdStrArr[8]
		i, err := strconv.ParseFloat(cmdStrArr[6], 64)
		replyVal = "+Error"
		if err == nil {
			reqArgs.Score = i
			db.ZAdd(&reqArgs, &reply)
			replyVal = "+OK"
		}
	case cmd == "zscore" && cmdLen >= 8:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		reqArgs.Mem = cmdStrArr[6]
		db.ZScore(&reqArgs, &reply)
		replyVal = reply.Value
	case cmd == "sadd" && cmdLen >= 8:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		//TODO 支持多个元素
		reqArgs.Mems = append(reqArgs.Mems, cmdStrArr[6])
		reqArgs.Mems = append(reqArgs.Mems, cmdStrArr[8])
		db.SAdd(&reqArgs, &reply)
		replyVal = "+OK"
	case cmd == "scard" && cmdLen >= 6:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		db.SCard(&reqArgs, &reply)
		replyVal = reply.Value
	case cmd == "smembers" && cmdLen >= 6:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		db.SMembers(&reqArgs, &reply)
		replyVal = reply.Value
	case cmd == "lpush" && cmdLen >= 10:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		//TODO 支持多个元素
		reqArgs.Mems = append(reqArgs.Mems, cmdStrArr[6])
		reqArgs.Mems = append(reqArgs.Mems, cmdStrArr[8])

		db.LPush(&reqArgs, &reply)
		replyVal = "+OK"
	case cmd == "lpop" && cmdLen >= 6:
		var reqArgs handle.Args
		var reply handle.Reply
		reqArgs.Key = key
		db.LPop(&reqArgs, &reply)
		replyVal = reply.Value

	default:
		//var reply handle.GetReply
		replyVal = "+Error"
	}
	len := len(replyVal)
	fmt.Println(replyVal)
	rep := fmt.Sprintf("$%d\r\n%s\r\n", len, replyVal)
	fmt.Println("replyVal " + replyVal)
	return rep

}
