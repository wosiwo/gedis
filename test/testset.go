package main

import (
	"../core/table"
	"fmt"
)

func main() {
	s := table.NewSet("a", "b")
	fmt.Println(s.HsVal.Count())
}
