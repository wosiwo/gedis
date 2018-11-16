package core

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

//每次连接，创建一个对应的客户端
type GdClient struct {
	Db          *GedisDB
	DBId        int               //当前使用的数据库id
	QueryBuf    string            //查询缓冲区
	Cmd         *GedisCommand     //命令方法
	Argv        []string          //解析参数
	Argc        int               //参数长度
	CommandName string            //命令名字
	Key         string            //存储的key
	ReqType     int               //请求的类型：内联命令还是多条命令
	CTime       int               //客户端创建时间
	Buf         string            //回复缓冲区
	FakeFlag    bool              //是否是假客户端
	Cn          net.Conn       //所属连接
	RW          *bufio.ReadWriter //所属连接
	IsNew       bool              //是否创建操作
}

//命令解析
func (c *GdClient) ProcessInputBuffer(s *GedisServer) error {
	cmdStrArr := strings.Split(c.QueryBuf, "\r\n")
	cmdLen := len(cmdStrArr)
	//fmt.Println("cmdLen %d", cmdLen)
	if len(cmdStrArr) > 1 {
		//fmt.Println(cmdStrArr)
		//fmt.Println(len(cmdStrArr))
		c.Argc = cmdLen
		c.Argv = cmdStrArr
		c.CommandName = c.Argv[2]
		if len(cmdStrArr) >= 6 {
			c.Key = c.Argv[4]
		} else {
			fmt.Println(cmdStrArr)
		}
		c.Cmd = s.lookupCommand(c.CommandName)
		if _, ok := s.DB[c.DBId].Dict[c.Key]; ok {
			c.IsNew = false
		} else {
			c.IsNew = true
		}
	} else {
		c.Cmd = &GedisCommand{}
		c.Cmd.IsWrite = false
		c.Cmd.IsPing = false
		fmt.Println(cmdStrArr)
	}
	//fmt.Println(c)
	return nil
}
