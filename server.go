package main

import (
	//"./protocol"
	"./handle"
	"fmt"
	"net"
	"strconv"
	"strings"
	"./table"
)

func main() {
	var tcpAddr *net.TCPAddr

	tcpAddr, _ = net.ResolveTCPAddr("tcp", "127.0.0.1:9999")

	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)

	defer tcpListener.Close()
	kv := new(handle.KV)
	kv.Data = map[string]string{}
	ht := new(handle.HASHTB)
	ht.Data = map[string]*handle.HASHTBVal{}

	z := new(handle.ZSETAPI)
	z.Data = map[string]*table.ZSetType{} //zset.New()

	s := new(handle.SETAPI)
	s.Data = map[string]*table.Set{}
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}

		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn,kv,ht,z,s,err)
	}

}

func tcpPipe(conn *net.TCPConn,kv *handle.KV,ht *handle.HASHTB,z *handle.ZSETAPI,s *handle.SETAPI,err error) {
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

	reply := parseCmd(cmdstr,kv,ht,z,s)
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

func parseCmd(cmdStr string,kv *handle.KV,ht *handle.HASHTB,z *handle.ZSETAPI,s *handle.SETAPI) string {
	cmdStrArr := strings.Split(cmdStr,"\r\n")

	cmd := cmdStrArr[2]
	cmdLen := len(cmdStrArr)
	fmt.Println("cmdLen %d",cmdLen)
	cmd = strings.ToLower(cmd)
	fmt.Println("cmd "+cmd)
	var replyVal string
	//var reply handle.GetReply
	key := cmdStrArr[4]
	switch {
		case cmd == "get"  && cmdLen>=6:
			var reqArgs handle.GetArgs
			var reply handle.GetReply
			reqArgs.Key = key
			handle.Get(kv, &reqArgs,&reply)
			replyVal = reply.Value
		case cmd == "set" && cmdLen>=8 :
			var reqArgs handle.PutArgs
			var reply handle.PutReply
			reqArgs.Key = key
			reqArgs.Value = cmdStrArr[6]
			handle.Put(kv, &reqArgs,&reply)
			fmt.Println("tt")
			replyVal = "+OK"
			fmt.Println(reply.Value)

	case cmd == "hget"  && cmdLen>=8 :
			var reqArgs handle.HGetArgs
			var reply handle.HGetReply
			reqArgs.Key = key
			reqArgs.Field = cmdStrArr[6]
			handle.HGet(ht, &reqArgs,&reply)
			replyVal = reply.Value

	case cmd == "hset"  && cmdLen>=10 :
			var reqArgs handle.HSetArgs
			var reply handle.HSetReply
			reqArgs.Key = key
			reqArgs.Field = cmdStrArr[6]
			reqArgs.Value = cmdStrArr[8]
			handle.HSet(ht, &reqArgs,&reply)
			replyVal = "+OK"
		case cmd == "zadd"  && cmdLen>=10:
			var reqArgs handle.ZAddArgs
			var reply handle.ZAddReply
			reqArgs.Key = key
			reqArgs.Mem = cmdStrArr[6]
			i, err := strconv.ParseFloat(cmdStrArr[8], 64)
			replyVal = "+Error"
			if err == nil {
				reqArgs.Score = i
				handle.ZAdd(z, &reqArgs,&reply)
				replyVal = "+OK"
			}
		case cmd == "zscore"  && cmdLen>=8:
			var reqArgs handle.ZScoreArgs
			var reply handle.ZScoreReply
			reqArgs.Key = key
			reqArgs.Mem = cmdStrArr[6]
			handle.ZScore(z, &reqArgs,&reply)
			replyVal = strconv.FormatFloat(reply.Value, 'f', 6, 64)
		case cmd == "sadd" && cmdLen>=8:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			//TODO 支持多个元素
			reqArgs.Mems = append(reqArgs.Mems,cmdStrArr[6])
			reqArgs.Mems = append(reqArgs.Mems,cmdStrArr[8])
			handle.SAdd(s, &reqArgs,&reply)
			replyVal = "+OK"
		case cmd == "scard" && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			handle.SCard(s, &reqArgs,&reply)
			replyVal = reply.Value
		case cmd == "smembers" && cmdLen>=6:
			var reqArgs handle.Args
			var reply handle.Reply
			reqArgs.Key = key
			handle.SMembers(s, &reqArgs,&reply)
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