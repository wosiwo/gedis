package table

import (
	//"fmt"
	"sort"
	"sync"
)

type IntSet struct {
	sync.RWMutex
	Val map[int]bool
}

// 新建集合对象
// 可以传入初始元素
func NewIntSet(items ...int) *IntSet {
	s := &IntSet{
		Val: make(map[int]bool, len(items)),
	}
	s.Add(items...)
	return s
}

// 创建副本
func (s *IntSet) Duplicate() *IntSet {
	s.Lock()
	defer s.Unlock()
	r := &IntSet{
		Val: make(map[int]bool, len(s.Val)),
	}
	for e := range s.Val {
		r.Val[e] = true
	}
	return r
}

// 添加元素
func (s *IntSet) Add(items ...int) {
	s.Lock()
	defer s.Unlock()
	for _, v := range items {
		s.Val[v] = true
	}
}

// 删除元素
func (s *IntSet) IntSetRemove(items ...int) {
	s.Lock()
	defer s.Unlock()
	for _, v := range items {
		delete(s.Val, v)
	}
}

// 判断元素是否存在
func (s *IntSet) IntSetHas(items ...int) bool {
	s.RLock()
	defer s.RUnlock()
	for _, v := range items {
		if _, ok := s.Val[v]; !ok {
			return false
		}
	}
	return true
}

// 统计元素个数
func (s *IntSet) IntSetCount() int {
	s.Lock()
	defer s.Unlock()
	return len(s.Val)
}

// 清空集合
func (s *IntSet) IntSetClear() {
	s.Lock()
	defer s.Unlock()
	s.Val = map[int]bool{}
}

// 空集合判断
func (s *IntSet) IntSetEmpty() bool {
	s.Lock()
	defer s.Unlock()
	return len(s.Val) == 0
}

// 获取元素列表（无序）
func (s *IntSet) IntSetList() []int {
	s.RLock()
	defer s.RUnlock()
	list := make([]int, 0, len(s.Val))
	for item := range s.Val {
		list = append(list, item)
	}
	return list
}

// 获取元素列表（有序）
func (s *IntSet) IntSetSortedList() []int {
	s.RLock()
	defer s.RUnlock()
	list := make([]int, 0, len(s.Val))
	for item := range s.Val {
		list = append(list, item)
	}
	sort.Ints(list)
	return list
}

// 并集
// 获取 s 与参数的并集，结果存入 s
func (s *IntSet) IntSetUnion(IntSets ...*IntSet) {
	// 为了防止多例程死锁，不能同时锁定两个集合
	// 所以这里没有锁定 s，而是创建了一个临时集合
	r := s.Duplicate()
	// 获取并集
	for _, IntSet := range IntSets {
		IntSet.Lock()
		for e := range IntSet.Val {
			r.Val[e] = true
		}
		IntSet.Unlock()
	}
	// 将结果转入 s
	s.Lock()
	defer s.Unlock()
	s.Val = map[int]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}

// 并集（函数）
// 获取所有参数的并集，并返回
func IntSetUnion(IntSets ...*IntSet) *IntSet {
	// 处理参数数量
	if len(IntSets) == 0 {
		return NewIntSet()
	} else if len(IntSets) == 1 {
		return IntSets[0]
	}
	// 获取并集
	r := IntSets[0].Duplicate()
	for _, IntSet := range IntSets[1:] {
		IntSet.Lock()
		for e := range IntSet.Val {
			r.Val[e] = true
		}
		IntSet.Unlock()
	}
	return r
}

// 差集
// 获取 s 与所有参数的差集，结果存入 s
func (s *IntSet) IntSetMinus(IntSets ...*IntSet) {
	// 为了防止多例程死锁，不能同时锁定两个集合
	// 所以这里没有锁定 s，而是创建了一个临时集合
	r := s.Duplicate()
	// 获取差集
	for _, IntSet := range IntSets {
		IntSet.Lock()
		for e := range IntSet.Val {
			delete(r.Val, e)
		}
		IntSet.Unlock()
	}
	// 将结果转入 s
	s.Lock()
	defer s.Unlock()
	s.Val = map[int]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}

// 差集（函数）
// 获取第 1 个参数与其它参数的差集，并返回
func IntSetMinus(IntSets ...*IntSet) *IntSet {
	// 处理参数数量
	if len(IntSets) == 0 {
		return NewIntSet()
	} else if len(IntSets) == 1 {
		return IntSets[0]
	}
	// 获取差集
	r := IntSets[0].Duplicate()
	for _, IntSet := range IntSets[1:] {
		for e := range IntSet.Val {
			delete(r.Val, e)
		}
	}
	return r
}

// 交集
// 获取 s 与其它参数的交集，结果存入 s
func (s *IntSet) IntSetIntersect(IntSets ...*IntSet) {
	// 为了防止多例程死锁，不能同时锁定两个集合
	// 所以这里没有锁定 s，而是创建了一个临时集合
	r := s.Duplicate()

	for _, IntSet := range IntSets {
		IntSet.Lock()
		for e := range r.Val {
			if _, ok := IntSet.Val[e]; !ok {
				delete(r.Val, e)
			}
		}
		IntSet.Unlock()
	}
	// 将结果转入 s
	s.Lock()
	defer s.Unlock()
	s.Val = map[int]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}

// 交集（函数）
// 获取所有参数的交集，并返回
func IntSetIntersect(IntSets ...*IntSet) *IntSet {
	// 处理参数数量
	if len(IntSets) == 0 {
		return NewIntSet()
	} else if len(IntSets) == 1 {
		return IntSets[0]
	}
	// 获取交集
	r := IntSets[0].Duplicate()
	for _, IntSet := range IntSets[1:] {
		for e := range r.Val {
			if _, ok := IntSet.Val[e]; !ok {
				delete(r.Val, e)
			}
		}
	}
	return r
}

// 补集
// 获取 s 相对于 full 的补集，结果存入 s
func (s *IntSet) IntSetComplement(full *IntSet) {
	r := full.Duplicate()
	s.Lock()
	defer s.Unlock()
	// 获取补集
	for e := range s.Val {
		delete(r.Val, e)
	}
	// 将结果转入 s
	s.Val = map[int]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}
