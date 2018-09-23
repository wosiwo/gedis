package table

import "runtime"

const DEFAULT_TBL_LEN = 16

type Zset struct {
	sl  *SkipList
	tbl map[string]float64
}
type ZSetType Zset


/**
 * 初始化有续集
 */
func New() *ZSetType {
	z := &ZSetType{NewSkipList(), make(map[string]float64, DEFAULT_TBL_LEN)}
	runtime.SetFinalizer(z, func(z *ZSetType) {
		z.sl = nil
		z.tbl = nil
	})
	return z
}

func (z *ZSetType) Add(score float64, member string) {
	if old, ok := z.tbl[member]; ok {
		if old == score {
			return
		}
		z.sl.Remove(int(old))	//将旧的节点删除
	}
	z.sl.Insert(int(score),member)	//跳跃表应该存储member值
	z.tbl[member] = score
}


func (z *ZSetType) Rem(member string) {
	if score, ok := z.tbl[member]; ok {
		z.sl.Remove(int(score))	//将节点删除
		delete(z.tbl, member)
	}
}

func (z *ZSetType) Count() int {
	return len(z.tbl)
}



func (z *ZSetType) Score(member string) (float64, bool) {
	score, ex := z.tbl[member]
	return score, ex
}
