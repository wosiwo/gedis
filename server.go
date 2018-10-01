package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"./handle"
	//"./table"
)




var rdServer handle.RedisServer
var DBIndex int8
func main() {
	//数据库初始化
	DBIndex = 0
	rdServer.DBnum = 16
	rdServer.DB = make([]handle.RedisDB,rdServer.DBnum)
	rdServer.DB[DBIndex] = handle.RedisDB{}	//初始化
	rdServer.DB[DBIndex].Dict = map[string]handle.ValueObj{}
	var tcpAddr *net.TCPAddr

	tcpAddr, _ = net.ResolveTCPAddr("tcp", "127.0.0.1:9999")

	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)

	defer tcpListener.Close()

	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}

		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn,err)
	}

}

func tcpPipe(conn *net.TCPConn,err error) {
	ipStr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	//reader := bufio.NewReader(conn)
	command := make([]byte, 1024)
	n, err := conn.Read(command)
	cmdstr := string(command[:n])
	fmt.Println("cmdstr"+cmdstr)

	reply := parseCmd(cmdstr)
	//request := protocol.GetRequest(cmdstr)
	//for {
	//	message, err := reader.ReadString('\n')
	//	if err != nil {
	//		return
	//	}
	//
	//	fmt.Println(string(message))
	//	msg := time.Now().String() + "\n"
	b := []byte(reply)
	conn.Write(b)
	//}
}

func parseCmd(cmdStr string) string {
	cmdStrArr := strings.Split(cmdStr,"\r\n")

	cmd := cmdStrArr[2]
	cmdLen := len(cmdStrArr)
	fmt.Println("cmdLen %d",cmdLen)
	cmd = strings.ToLower(cmd)
	fmt.Println("cmd "+cmd)
	var replyVal string
	//var reply handle.GetReply
	key := cmdStrArr[4]

	//获取当前数据库
	db := rdServer.DB[DBIndex]
	switch {
		case cmd == "get"  && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			db.Get(&reqArgs,&reply)
			replyVal = reply.Value
		case cmd == "set" && cmdLen>=8 :
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			reqArgs.Value = cmdStrArr[6]
			db.Put(&reqArgs,&reply)
			fmt.Println("tt")
			replyVal = "+OK"
			fmt.Println(reply.Value)

	case cmd == "hget"  && cmdLen>=8 :
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			reqArgs.Field = cmdStrArr[6]
			db.HGet(&reqArgs,&reply)
			replyVal = reply.Value

	case cmd == "hset"  && cmdLen>=10 :
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			reqArgs.Field = cmdStrArr[6]
			reqArgs.Value = cmdStrArr[8]
			db.HSet(&reqArgs,&reply)
			replyVal = "+OK"
		case cmd == "zadd"  && cmdLen>=10:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			reqArgs.Mem = cmdStrArr[8]
			i, err := strconv.ParseFloat(cmdStrArr[6], 64)
			replyVal = "+Error"
			if err == nil {
				reqArgs.Score = i
				db.ZAdd(&reqArgs,&reply)
				replyVal = "+OK"
			}
		case cmd == "zscore"  && cmdLen>=8:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			reqArgs.Mem = cmdStrArr[6]
			db.ZScore(&reqArgs,&reply)
			replyVal = reply.Value
		case cmd == "sadd" && cmdLen>=8:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			//TODO 支持多个元素
			reqArgs.Mems = append(reqArgs.Mems,cmdStrArr[6])
			reqArgs.Mems = append(reqArgs.Mems,cmdStrArr[8])
			db.SAdd(&reqArgs,&reply)
			replyVal = "+OK"
		case cmd == "scard" && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			db.SCard(&reqArgs,&reply)
			replyVal = reply.Value
		case cmd == "smembers" && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			db.SMembers(&reqArgs,&reply)
			replyVal = reply.Value

	default:
			//var reply handle.GetReply
			replyVal = "+Error"
	}
	len := len(replyVal)
	fmt.Println(replyVal)
	rep := fmt.Sprintf( "$%d\r\n%s\r\n", len, replyVal)
	fmt.Println("replyVal "+replyVal)
	return rep

}