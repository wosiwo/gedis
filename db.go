package main

import(
	"./handle"
)

func initDB(gdServer *handle.GedisServer){
	for i := 0; i < gdServer.DBnum; i++ {
		gdServer.DB[i] = handle.GedisDB{} //初始化
		gdServer.DB[i].Dict = map[string]handle.ValueObj{}
	}
}



func initCommand(gdServer *handle.GedisServer){
	getCommand := &handle.GedisCommand{Name: "get", Proc: gdServer.Get}
	//setCommand := &handle.GedisCommand{Name: "set", Proc: gdServer.Set}

	gdServer.Commands = map[string]*handle.GedisCommand{
		"get" : getCommand,
	}
}