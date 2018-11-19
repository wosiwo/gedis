package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)
var sendNum = 0

func sendGetTest() (int){
	hostPort := "127.0.0.1:9998"

	tcpAddr, err := net.ResolveTCPAddr("tcp", hostPort)
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)
	defer conn.Close()
	//log.Println(tcpAddr, conn.LocalAddr(), conn.RemoteAddr())

	rep := fmt.Sprintf("*2\r\n$3\r\nget\r\n$1\r\na\r\n")
	n := 0
	for i := 0; i < 10000; i++ {
		send2Server(rep, conn)
		buff := make([]byte, 1024)
		n, _ = conn.Read(buff)
		sendNum = sendNum+1

	}
	fmt.Println(n)
	return n
}
func main() {
	//hostPort := "127.0.0.1:9998"



	millis := getmillis()
	fmt.Println(millis)
	n := 0
	//循环读取1w次
	for i := 0; i < 100; i++ {
		go sendGetTest()
	}

	for sendNum<1000000{
		fmt.Println(sendNum)
	}

	fmt.Println(n)
	millis2 := getmillis()
	fmt.Println(millis2)
	dif := millis2-millis
	fmt.Println(dif)


}

func getmillis()(int64){
	now := time.Now()
	//secs := now.Unix()
	nanos := now.UnixNano()
	// 微秒手动除以一个数值来获取毫秒
	millis := nanos / 1000000
	return millis
}
func send2Server(msg string, conn net.Conn) (n int, err error) {

	//fmt.Println("proto encode", p, string(p))
	n, err = conn.Write([]byte(msg))
	return n, err
}
func checkError(err error) {
	if err != nil {
		log.Println("err ", err.Error())
		os.Exit(1)
	}
}
