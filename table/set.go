package table

import (
	"fmt"
)

const DEFAULT_MAX_INTHashSet_LEN = 512

type Set struct {
	EncodeType int		//0:整数集合 1:哈希
	HsVal HashSet
	IntVal IntSet
}

type People struct {
	Name string
}
// 函数实现了原来应有功能
func (p People) Say() {
	fmt.Printf("Hi, I'm %s. \n",p.Name)
}



// 新建集合对象
// 可以传入初始元素
func NewSet(items ...interface{}) *Set {
	ifInt,intItems,strItems := CheckIfInt(items...)
	if(len(items)<=DEFAULT_MAX_INTHashSet_LEN && ifInt){
		s := &Set{
			EncodeType: 0,
			IntVal: *NewIntSet(intItems...),
		}
		s.IntVal.Add(intItems...)
		return s
	}
	//不符合整数集合条件时，使用字典
	s := &Set{
		EncodeType: 1,
		HsVal: *NewHashSet(strItems...),
	}
	s.HsVal.Add(strItems...)
	return s
}

/**
  判断是否所有元素都是int
 */
func CheckIfInt(items... interface{}) (bool,[]int,[]string) {
	ifAllInt := true
	var intItems[] int
	var strItems[] string
	for _, v := range items {
		switch  v :=v.(type) {
		case int:
			intItems = append(intItems, v)
		case string:
			ifAllInt = false
			strItems = append(strItems, v)
		default:
			ifAllInt = false
			//break
		}
	}
	return ifAllInt,intItems,strItems
}
