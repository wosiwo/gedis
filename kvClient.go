package main

import (
	"fmt"
	"log"
	"net/rpc"
)

//
// RPC request/reply definitions
//

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

//hash
type HSetArgs struct {
	Key   string
	Field string
	Value string
}

type HSetReply struct {
	Err Err
}

type HGetArgs struct {
	Key   string
	Field string
}

type HGetReply struct {
	Err   Err
	Value string
}

//zset
type ZAddArgs struct {
	Key   string
	Mem   string
	Score float64
}

type ZAddReply struct {
	Err   Err
	Value string
}

type ZScoreArgs struct {
	Key string
	Mem string
}

type ZScoreReply struct {
	Err   Err
	Value float64
}

//
// Client
//

func connect() *rpc.Client {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

func get(key string) string {
	client := connect()
	args := GetArgs{"subject"}
	reply := GetReply{}
	err := client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

func put(key string, val string) {
	client := connect()
	args := PutArgs{"subject", "6.824"}
	reply := PutReply{}
	err := client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

//hash
func hset(key string, field string, val string) {
	client := connect()
	args := HSetArgs{key, field, val}
	reply := PutReply{}
	err := client.Call("HASHTB.HSet", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

func hget(key string, field string) string {
	client := connect()
	args := HGetArgs{key, field}
	reply := HGetReply{}
	err := client.Call("HASHTB.HGet", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

//zset
func zadd(key string, member string, score float64) {
	client := connect()
	args := ZAddArgs{key, member, score}
	reply := ZAddReply{}
	err := client.Call("ZSETAPI.ZAdd", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
}

func zscore(key string, member string) float64 {
	client := connect()
	args := ZScoreArgs{key, member}
	reply := ZScoreReply{}
	err := client.Call("ZSETAPI.ZScore", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

//
// main
//

func main() {

	//put("subject", "6.824")
	//fmt.Printf("Put(subject, 6.824) done\n")
	//fmt.Printf("get(subject) -> %s\n", get("subject"))

	//hset("keyname","fname", "6.824")
	//fmt.Printf("get(keyname) -> %s\n", hget("keyname","fname"))

	zadd("keyname", "fname", 6.824)
	fmt.Printf("get(keyname) -> %f\n", zscore("keyname", "fname"))

}
