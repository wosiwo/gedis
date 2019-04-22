package main

import (
	"./core"
	"./core/aof"
	"./core/config"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	//reuse "github.com/libp2p/go-reuseport"
	//_ "net/http/pprof"
)
var gdServer = new(core.GedisServer)
var DBIndex int8
var confPath = "./conf/server.conf"
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
func listenPort(conf *config.Config,num int,deferFunc func()){
	defer func() {
		deferFunc()
	}()
	/*---- 监听请求 ---- */
	osName := runtime.GOOS
	archName := runtime.GOARCH
	fmt.Println(osName)
	fmt.Println(archName)
	//switch runtime.GOOS { case "darwin": case "windows": case "linux": }
	//ifReUsePort := {"darwin":0}
	if osName != "darwin" {	//TODO linux下开启多协程监听同个端口
		net.USE_SO_REUSEPORT = true
	}

	host := conf.GetIStringDefault("hostname", "127.0.0.1")
	port := conf.GetIStringDefault("port", "9999")
	hostPort := net.JoinHostPort(host, port)
	tcpAddr, _ := net.ResolveTCPAddr("tcp", hostPort)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	//net.ListenTCP
	//tcpListener, _ := reuse.Listen("tcp", hostPort)
	//tcpListener = tcpListener.(*TCPListener)
	//&TCPListener{fd}
	//tcpListener.
	defer tcpListener.Close()
	/*---- 循环接受请求 ---- */
	for {
		conn, err := tcpListener.Accept()
		//conn, err := tcpListener.Accept()
		if err != nil {
			continue
		}
		//fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		/*---- 循环处理请求 ---- */
		go handleConnection(conn,num) //, err
	}
}
func listenUinxSocket(num int,deferFunc func()){
	defer func() {
		deferFunc()
	}()
	defer func(){
		fmt.Println("Enter defer function.")
		if p := recover(); p != nil {
			fmt.Printf("panic: %s\n", p)
		}
		fmt.Println("Exit defer function.")
	}()
	listener, _ := net.Listen("unix", "/tmp/gedis.sock")
	defer listener.Close()
	/*---- 循环接受请求 ---- */
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		//fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		/*---- 循环处理请求 ---- */
		go handleConnection(conn,num) //, err
	}

}
func main() {
	defer func(){
		fmt.Println("Enter defer function.")
		if p := recover(); p != nil {
			fmt.Printf("panic: %s\n", p)
		}
		fmt.Println("Exit defer function.")
	}()
	// 引发 panic。
	//go func() {
	//	panic(errors.New("something wrong"))
	//}()
	//fmt.Println("Exit function main.")

	//procs := runtime.GOMAXPROCS(8)	//查看利用的核心数

	//性能分析
	flag.Parse()
	//fmt.Println(procs)

	if *cpuprofile != "" {
		fmt.Println(cpuprofile)
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
	/*---- 处理新建key ----*/
	go consumeWrite()
	/*---- 处理日志写入 ----*/
	go consumeLog()
	/*---- server初始化 ----*/
	gdServer.RunServer(conf)

	//在使用WaitGroup值实现一对多的 goroutine 协作流程
	var wg sync.WaitGroup
	wg.Add(2)

	//多个协程监听端口
	//go listenPort(conf,1)
	go listenUinxSocket(2,wg.Done)
	go listenPort(conf,3,wg.Done)

	wg.Wait()

}

//消费写操作
func consumeWrite() {
	for c := range gdServer.WriteC {
		gdServer.ProcessCommand(&c) //串行执行新建key命令
	}
}
//串行写入日志 TODO 根据key哈希到不同的消费协程中
func consumeLog() {
	for c := range gdServer.LogC {
		//写入日志
		if c.FakeFlag == false {
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
func handleConnection(conn net.Conn,num int) {
	defer func(){
		fmt.Println("Enter defer function.")
		if p := recover(); p != nil {
			fmt.Printf("panic: %s\n", p)
		}
		fmt.Println("Exit defer function.")
	}()

	c := gdServer.CreateClient()
	c.Cn = conn //命令
	buffer := make([]byte, 1024)
	for {
		//log.Print("Receive command '")
		n, err := conn.Read(buffer)
		//fmt.Println(n)
		switch {
		case err == io.EOF:
			fmt.Println(num)
			fmt.Println("Reached EOF - close this connection.\n   ---")
			return
		case err != nil:
			log.Println("\nError reading command", err)
			return
		}
		cmdstr := string(buffer[:n])
		//fmt.Println(cmdstr)
		c.QueryBuf = cmdstr            //命令
		c.ProcessInputBuffer(gdServer) //命令解析
		//fmt.Println(c)
		if c.Cmd.Proc == nil {
			fmt.Println(c)
			core.SendReplyToClient(c)
			continue
		}
		//TODO 手动进行心跳检查
		//除了主从同步，正常客户端连接在没有收到服务端响应时，不应发送第二次请求，所以这里不再开启新的协程
		handleCommand(c)
	}
}

//处理命令
func handleCommand(c *core.GdClient) {
	//fmt.Println(c)
	var err error
	if c.Cmd.IsWrite && c.IsNew {	//创建新key才需要窜行执行
		gdServer.WriteC <- *c
		return
	} else {
		err = gdServer.ProcessCommand(c) //执行命令
	}
	//reply := parseCmd(cmdstr)
	if err != nil {
		fmt.Println("ProcessInputBuffer err", err)
		return
	}
	//返回数据给客户端
	core.SendReplyToClient(c)
}


//信号处理
func sigHandler(c chan os.Signal) {
	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			pprof.StopCPUProfile()
			fmt.Println("exit")
			exitHandler()
		default:
			fmt.Println("signal ", s)
		}
	}
}

//退出处理
func exitHandler() {
	if gdServer.IsLogChannel {
		gdServer.CmdBuffer.Mut.Lock()
		aof.AppendToFile(gdServer.AofPath, gdServer.CmdBuffer.Cmd)
		gdServer.CmdBuffer.Mut.Unlock()
	}
	fmt.Println("exiting smoothly ...")
	fmt.Println("bye ")
	os.Exit(0)
}
