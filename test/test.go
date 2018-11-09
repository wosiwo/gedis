package main

import (
	"fmt"
	"strconv"
	//"../table"
)
/**
  判断是否所有元素都是int
 */
func CheckIfInt(items... string) (bool,[]int,[]string) {
	ifAllInt := true
	var intItems[] int
	var strItems[] string
	for _, v := range items {
		vint, err := strconv.Atoi(v)
		//fmt.Println(v)
		//fmt.Println(err)
		if(err == nil){
			intItems = append(intItems, vint)
		}else{

			ifAllInt = false
			strItems = append(strItems, v)
		}

	}
	return ifAllInt,intItems,strItems
}

func main(){
	ifInt,intItems,strItems := CheckIfInt("1","2","a")

	fmt.Println("ifInt: " + strconv.FormatBool(ifInt))
	fmt.Println(intItems)
	fmt.Println(strItems)

}
