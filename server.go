package main

import "C"
import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"./handle"
	"time"

	//"./table"
)




var rdServer handle.RedisServer
var DBIndex int8

// 用来记录所有的客户端连接
//var ConnMap map[string]*net.TCPConn
func main() {
	//数据库初始化
	DBIndex = 0
	rdServer.DBnum = 16
	rdServer.DB = make([]handle.RedisDB,rdServer.DBnum)
	rdServer.DB[DBIndex] = handle.RedisDB{}	//初始化
	rdServer.DB[DBIndex].Dict = map[string]handle.ValueObj{}
	var tcpAddr *net.TCPAddr
	//ConnMap = make(map[string]*net.TCPConn)

	tcpAddr, _ = net.ResolveTCPAddr("tcp", "127.0.0.1:9999")

	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)

	defer tcpListener.Close()

	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}

		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		// 新连接加入map
		//ConnMap[tcpConn.RemoteAddr().String()] = tcpConn
		go handleConnection(tcpConn,err)
	}

}

/**
	处理连接
 */
func handleConnection(conn *net.TCPConn,err error) {
	Log("handleConnection")
	Log(conn)
	ipStr := conn.RemoteAddr().String()
	defer func(){
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			Log(conn.RemoteAddr().String(), " Fatal error: ", err)
			return
		}
		//解析请求执行相关读写操作
		cmdstr := string(buffer[:n])
		fmt.Println("cmdstr"+cmdstr)

		reply := handleRequest(cmdstr)
		b := []byte(reply)
		conn.Write(b)
		fmt.Println("reply "+reply)
		if("+Error" == reply){
			return
		}

		//心跳计时
		Data := buffer[:n]
		message := make(chan byte)
		go HeartBeating(conn, message, 30)
		// 检测每次是否有数据传入
		go GravelChannel(Data, message)
		Log(time.Now().Format("2006-01-02 15:04:05.0000000"), conn.RemoteAddr().String(), string(buffer[:n]))
	}

}
func GravelChannel(bytes []byte, mess chan byte) {
	for _, v := range bytes {
		mess <- v
	}
	close(mess)
}
func HeartBeating(conn net.Conn, bytes chan byte, timeout int) {
	select {
	case fk := <-bytes:
		Log(conn.RemoteAddr().String(), "心跳:第", string(fk), "times")
		conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
		break
	case <-time.After(5 * time.Second):
		Log("conn dead now")
		conn.Close()
	}
}
func Log(v ...interface{}) {
	fmt.Println(v...)
	return
}

func handleRequest(cmdStr string) string {
	cmdStrArr := strings.Split(cmdStr,"\r\n")

	cmd := cmdStrArr[2]
	cmdLen := len(cmdStrArr)
	if(cmdLen<=4){
		return "+Error"
	}
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
			db.Set(&reqArgs,&reply)
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
		case cmd == "lpush" && cmdLen>=10:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			//TODO 支持多个元素
			reqArgs.Mems = append(reqArgs.Mems,cmdStrArr[6])
			reqArgs.Mems = append(reqArgs.Mems,cmdStrArr[8])

			db.LPush(&reqArgs,&reply)
			replyVal = "+OK"
		case cmd == "lpop" && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			db.LPop(&reqArgs,&reply)
			replyVal = reply.Value
		case cmd == "del" && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			db.Delete(&reqArgs,&reply)
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