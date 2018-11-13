package core

import (
	"strings"
)

//每次连接，创建一个对应的客户端
type GdClient struct {
	Db       *GedisDB
	DBId     int    //当前使用的数据库id
	QueryBuf string //查询缓冲区

	Cmd  *GedisCommand
	Argv []string
	Argc int
	Key  string

	ReqType  int    //请求的类型：内联命令还是多条命令
	CTime    int    //客户端创建时间
	Buf      string //回复缓冲区
	FakeFlag bool
}

//命令解析
func (c *GdClient) ProcessInputBuffer() error {
	cmdStrArr := strings.Split(c.QueryBuf, "\r\n")
	cmdLen := len(cmdStrArr)
	//fmt.Println("cmdLen %d", cmdLen)
	if len(cmdStrArr) > 1 {
		//cmd := cmdStrArr[2]
		//cmd = strings.ToLower(cmd)
		//fmt.Println("cmd " + cmd)
		c.Argc = cmdLen
		c.Argv = cmdStrArr
	} else {
		//fmt.Println(cmdStrArr)
	}
	return nil
}
