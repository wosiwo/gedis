package main

import (
	"./core"
	"./core/aof"
	"./core/config"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
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
	/*---- 监听信号 平滑退出 ----*/
	sc := make(chan os.Signal)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go sigHandler(sc)
	//处理消费队列
	go consumeWrite()
	//初始化
	gdServer.RunServer(conf)
	//监听端口
	var tcpAddr *net.TCPAddr
	host := conf.GetIStringDefault("hostname", "127.0.0.1")
	port := conf.GetIStringDefault("port", "9999")
	hostPort := net.JoinHostPort(host, port)
	tcpAddr, _ = net.ResolveTCPAddr("tcp", hostPort)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		//循环接受请求
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		//fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go tcpPipe(tcpConn, err)
	}
}

//消费写操作
func consumeWrite() {
	for c := range gdServer.WriteC {
		//写入日志
		if c.FakeFlag == false {
			gdServer.CmdBuffer.Mut.Lock()
			gdServer.CmdBuffer.Cmd += c.QueryBuf
			gdServer.CmdBuffer.Num++
			//fmt.Println(gdServer.AofLoadNum)
			if gdServer.CmdBuffer.Num > gdServer.AofLoadNum {
				aof.AppendToFile(gdServer.AofPath, gdServer.CmdBuffer.Cmd)
				gdServer.CmdBuffer.Cmd = ""
				gdServer.CmdBuffer.Num = 0
			}
			gdServer.CmdBuffer.Mut.Unlock()
		}
	}
}

//多线程处理连接
func tcpPipe(conn *net.TCPConn, err error) {
	//ipStr := conn.RemoteAddr().String()
	defer func() {
		//fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	c := gdServer.CreateClient()
	//读取请求内容
	command := make([]byte, 1024)
	n, err := conn.Read(command)
	cmdstr := string(command[:n])
	//fmt.Println("cmdstr" + cmdstr)
	c.QueryBuf = cmdstr              //命令
	c.ProcessInputBuffer()           //命令解析
	err = gdServer.ProcessCommand(c) //执行命令
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
