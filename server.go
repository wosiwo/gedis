package main

import (
	"./core"
	"./core/aof"
	"./core/config"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
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
	/*---- 读取配置----*/
	conf, err := config.NewConfig(confPath)
	if err != nil {
		fmt.Printf("read config file err: %v", err)
		return
	}
	/*---- 监听信号 平滑退出 ----*/
	sc := make(chan os.Signal)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go sigHandler(sc)
	/*---- 处理消费队列 ----*/
	go consumeWrite()
	/*---- server初始化 ----*/
	gdServer.RunServer(conf)
	/*---- 监听请求 ---- */
	var tcpAddr *net.TCPAddr
	host := conf.GetIStringDefault("hostname", "127.0.0.1")
	port := conf.GetIStringDefault("port", "9999")
	hostPort := net.JoinHostPort(host, port)
	tcpAddr, _ = net.ResolveTCPAddr("tcp", hostPort)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	/*---- 循环接受请求 ---- */
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		//fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		/*---- 循环处理请求 ---- */
		go handleConnection(tcpConn,2) //, err
	}
}

//消费写操作
func consumeWrite() {
	for c := range gdServer.WriteC {
		gdServer.ProcessCommand(&c) //执行命令
		//写入日志
		if c.FakeFlag == false {
			SendReplyToClient(c.Conn, &c)
			gdServer.CmdBuffer.Cmd += c.QueryBuf
			gdServer.CmdBuffer.Num++
			//fmt.Println(gdServer.AofLoadNum)
			if gdServer.CmdBuffer.Num > gdServer.AofLoadNum {
				aof.AppendToFile(gdServer.AofPath, gdServer.CmdBuffer.Cmd)
				gdServer.CmdBuffer.Cmd = ""
				gdServer.CmdBuffer.Num = 0
			}
		}
	}
}

//长连接入口
func handleConnection(conn *net.TCPConn,timeout int) {

	buffer := make([]byte, 2048)
	for {
		n, err := conn.Read(buffer)

		if err != nil &&  err == io.EOF {	//客户端关闭连接
			conn.Close()
			conn = nil
			return
		}
		if err != nil {
			fmt.Println(conn.RemoteAddr().String(), " connection error: ", err)
			return
		}
		cmdstr := string(buffer[:n])
		cmdlen := len(cmdstr)
		if cmdlen<6 {
			continue
		}
		messnager := make(chan string)
		//心跳计时
		go HeartBeating(conn,messnager,timeout)
		//go handleCommand(conn,cmdstr)	//执行客户端上传的命令
		//检测每次Client是否有数据传来
		messnager <- cmdstr
		//Log( "receive data length:",n)
		//Log(conn.RemoteAddr().String(), "receive data string:", cmdstr)

	}
}

//心跳计时，根据GravelChannel判断Client是否在设定时间内发来信息
func HeartBeating(conn *net.TCPConn, readerChannel chan string,timeout int) {
	select {
	case cmdstr := <-readerChannel:
		//Log(conn.RemoteAddr().String(), "receive data string:", cmdstr)
		//conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
		//conn.SetReadDeadline(time.Now().Add(time.Duration(5) * time.Second))
		handleCommand(conn,cmdstr)	//执行客户端上传的命令
		break
	case <-time.After(time.Second*5):
		//Log("It's really weird to get Nothing!!!")
		conn.Close()
	}

}

func Log(v ...interface{}) {
	fmt.Println(v...)
}

//协程处理连接
func handleCommand(conn *net.TCPConn,cmdstr string) { //err
	var err error
	c := gdServer.CreateClient()
	//读取请求内容
	//command := make([]byte, 1024)
	//n, err := conn.Read(command)
	//cmdstr := string(command[:n])
	//fmt.Println("cmdstr" + cmdstr)
	c.QueryBuf = cmdstr    //命令
	c.Conn = conn          //绑定连接
	c.ProcessInputBuffer() //命令解析
	if c.Argc<6 {
		//返回数据给客户端
		c := gdServer.CreateClient()
		c.Buf = "+error"
		SendReplyToClient(conn, c)
		return
	}
	if strings.ToUpper(c.Argv[2]) == "SET"  {
		if gdServer.IsChannel {
			//fmt.Println(c)
			gdServer.WriteC <- *c
			return
		} else {
			err = gdServer.ProcessCommand(c) //执行命令
		}
	} else {
		err = gdServer.ProcessCommand(c) //执行命令
	}
	//reply := parseCmd(cmdstr)
	if err != nil {
		fmt.Println("ProcessInputBuffer err", err)
		return
	}
	//返回数据给客户端
	SendReplyToClient(conn, c)
}

// 负责传送命令回复的写处理器
func SendReplyToClient(conn net.Conn, c *core.GdClient) {
	len := len(c.Buf)
	//fmt.Println(c.Buf)
	rep := fmt.Sprintf("$%d\r\n%s\r\n", len, c.Buf)
	//fmt.Println("replyVal " + c.Buf)
	conn.Write([]byte(rep))
	//conn.Close()
}

//信号处理
func sigHandler(c chan os.Signal) {
	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			exitHandler()
		default:
			fmt.Println("signal ", s)
		}
	}
}

//退出处理
func exitHandler() {
	gdServer.CmdBuffer.Mut.Lock()
	aof.AppendToFile(gdServer.AofPath, gdServer.CmdBuffer.Cmd)
	gdServer.CmdBuffer.Mut.Unlock()
	fmt.Println("exiting smoothly ...")
	fmt.Println("bye ")
	os.Exit(0)
}
