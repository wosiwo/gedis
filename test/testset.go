package main

import(
	"../table"
	"fmt"
)

func main(){
	s := table.NewSet("a","b")
	fmt.Println(s.HsVal.Count())
}
