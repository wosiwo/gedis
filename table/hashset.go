package table

import (
	//"runtime"
	//"sort"
	//"strconv"
	//"sync"
	"sync"
)

//type HashSet Set

type HashSet struct {
	sync.RWMutex
	Val map[string]bool
}

/**
 * 初始化集合
 */
// 新建集合对象
// 可以传入初始元素
func NewHashSet(items ...string) *HashSet {
	s := &HashSet{
		Val: make(map[string]bool, len(items)),
	}
	s.Add(items...)
	return s
}

// 创建副本
func (s *HashSet) Duplicate() *HashSet {
	s.Lock()
	defer s.Unlock()
	r :=&HashSet{
		Val:make(map[string]bool),
	}
	for e := range s.Val {
		r.Val[e] = true
	}
	return r
}


// 添加元素
func (s *HashSet) Add(items ...string) {
	s.Lock()
	defer s.Unlock()
	for _, v := range items {
		s.Val[v] = true
	}
}

// 删除元素
func (s *HashSet) Remove(items ...string) {
	s.Lock()
	defer s.Unlock()
	for _, v := range items {
		delete(s.Val, v)
	}
}

// 判断元素是否存在
func (s *HashSet) Has(items ...string) bool {
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
func (s *HashSet) Count() int {
	s.Lock()
	defer s.Unlock()
	return len(s.Val)
}

// 清空集合
func (s *HashSet) Clear() {
	s.Lock()
	defer s.Unlock()
	s.Val = map[string]bool{}
}

// 空集合判断
func (s *HashSet) Empty() bool {
	s.Lock()
	defer s.Unlock()
	return len(s.Val) == 0
}
//
// 获取元素列表
func (s *HashSet) SMembers() string {
	s.RLock()
	defer s.RUnlock()
	list := ""
	//TODO 每个元素输出一行
	for item := range s.Val {
		list += " "+item
	}
	return list
}


// 并集
// 获取 s 与参数的并集，结果存入 s
func (s *HashSet) Union(HashSets ...*HashSet) {
	// 为了防止多例程死锁，不能同时锁定两个集合
	// 所以这里没有锁定 s，而是创建了一个临时集合
	r := s.Duplicate()
	// 获取并集
	for _, IntHashSet := range HashSets {
		IntHashSet.Lock()
		for e := range IntHashSet.Val {
			r.Val[e] = true
		}
		IntHashSet.Unlock()
	}
	// 将结果转入 s
	s.Lock()
	defer s.Unlock()
	s.Val = map[string]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}

// 并集（函数）
// 获取所有参数的并集，并返回
func Union(HashSets ...*HashSet) *HashSet {
	// 处理参数数量
	if len(HashSets) == 0 {
		return NewHashSet()
	} else if len(HashSets) == 1 {
		return HashSets[0]
	}
	// 获取并集
	r := HashSets[0].Duplicate()
	for _, IntHashSet := range HashSets[1:] {
		IntHashSet.Lock()
		for e := range IntHashSet.Val {
			r.Val[e] = true
		}
		IntHashSet.Unlock()
	}
	return r
}

// 差集
// 获取 s 与所有参数的差集，结果存入 s
func (s *HashSet) Minus(HashSets ...*HashSet) {
	// 为了防止多例程死锁，不能同时锁定两个集合
	// 所以这里没有锁定 s，而是创建了一个临时集合
	r := s.Duplicate()
	// 获取差集
	for _, IntHashSet := range HashSets {
		IntHashSet.Lock()
		for e := range IntHashSet.Val {
			delete(r.Val, e)
		}
		IntHashSet.Unlock()
	}
	// 将结果转入 s
	s.Lock()
	defer s.Unlock()
	s.Val = map[string]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}

// 差集（函数）
// 获取第 1 个参数与其它参数的差集，并返回
func Minus(HashSets ...*HashSet) *HashSet {
	// 处理参数数量
	if len(HashSets) == 0 {
		return NewHashSet()
	} else if len(HashSets) == 1 {
		return HashSets[0]
	}
	// 获取差集
	r := HashSets[0].Duplicate()
	for _, IntHashSet := range HashSets[1:] {
		for e := range IntHashSet.Val {
			delete(r.Val, e)
		}
	}
	return r
}

// 交集
// 获取 s 与其它参数的交集，结果存入 s
func (s *HashSet) Intersect(HashSets ...*HashSet) {
	// 为了防止多例程死锁，不能同时锁定两个集合
	// 所以这里没有锁定 s，而是创建了一个临时集合
	r := s.Duplicate()
	// 获取交集
	for _, IntHashSet := range HashSets {
		IntHashSet.Lock()
		for e := range r.Val {
			if _, ok := IntHashSet.Val[e]; !ok {
				delete(r.Val, e)
			}
		}
		IntHashSet.Unlock()
	}
	// 将结果转入 s
	s.Lock()
	defer s.Unlock()
	s.Val = map[string]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}

// 交集（函数）
// 获取所有参数的交集，并返回
func Intersect(HashSets ...*HashSet) *HashSet {
	// 处理参数数量
	if len(HashSets) == 0 {
		return NewHashSet()
	} else if len(HashSets) == 1 {
		return HashSets[0]
	}
	// 获取交集
	r := HashSets[0].Duplicate()
	for _, IntHashSet := range HashSets[1:] {
		for e := range r.Val {
			if _, ok := IntHashSet.Val[e]; !ok {
				delete(r.Val, e)
			}
		}
	}
	return r
}

// 补集
// 获取 s 相对于 full 的补集，结果存入 s
func (s *HashSet) Complement(full *HashSet) {
	r := full.Duplicate()
	s.Lock()
	defer s.Unlock()
	// 获取补集
	for e := range s.Val {
		delete(r.Val, e)
	}
	// 将结果转入 s
	s.Val = map[string]bool{}
	for e := range r.Val {
		s.Val[e] = true
	}
}
