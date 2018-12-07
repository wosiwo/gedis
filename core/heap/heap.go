package heap

import (
	"fmt"
	"sync"
)

type Node struct {
	Value int64
	Key   string
}

type Heap struct {
	List   []*Node
	Length int
	Mut    sync.Mutex
}

//插入堆
func (h *Heap) InsertHeap(one *Node) {
	h.Mut.Lock()
	defer h.Mut.Unlock()
	h.List = append(h.List, one)
	length := len(h.List)
	h.Length = length - 1
	h.AdjustHeap(h.Length)
}

//堆排序
func (h *Heap) SortHeap(heaps []*Node) {
	length := len(heaps)
	length = length - 1
	if length == 1 {
		return
	}
	if length == 2 {
		h.AdjustHeap(length - 1)
	}
	for length > 0 {
		h.SliceNodeSwap(1, length)
		length--
		h.Heapfiy(length, 1)
	}
	//反序
	minPos := 1
	maxPos := h.Length
	for minPos < maxPos {
		h.SliceNodeSwap(minPos, maxPos)
		minPos++
		maxPos--
	}
}

//自下而上调整
func (h *Heap) AdjustHeap(length int) {
	if length < 1 {
		return
	}
	if length == 2 {
		if h.List[length].Value < h.List[length-1].Value {
			h.SliceNodeSwap(length, length-1)
		}
		return
	}
	i := length
	for i/2 > 0 && h.List[i].Value < h.List[i/2].Value {
		h.SliceNodeSwap(i, i/2)
		i = i / 2
	}
	return
}

//输出heap
func heapShow(heaps []*Node) {
	for one, value := range heaps {
		fmt.Println(one, value)
	}
}

//node slice交换
func (h *Heap) SliceNodeSwap(i int, j int) {
	x := h.List[i]
	h.List[i] = h.List[j]
	h.List[j] = x
}

//自上向下堆化
func (h *Heap) Heapfiy(length int, pos int) {
	if length == 2 {
		if h.List[1].Value > h.List[2].Value {
			h.SliceNodeSwap(1, 2)
			return
		}
	}
	for {
		maxPos := pos
		if pos*2 < length && h.List[pos].Value > h.List[pos*2].Value {
			maxPos = pos * 2
		}
		if pos*2+1 < length && h.List[maxPos].Value > h.List[pos*2+1].Value {
			maxPos = pos*2 + 1
		}
		if maxPos == pos {
			break
		}
		fmt.Println(pos, maxPos)
		h.SliceNodeSwap(pos, maxPos)
		pos = maxPos
	}
}

//获取堆顶
func (h *Heap) GetTopHeap() *Node {
	if h.Length == 0 {
		panic("Heap is empty")
	}
	h.Mut.Lock()
	defer h.Mut.Unlock()
	top := h.List[1]
	//堆顶和堆底交换
	h.SliceNodeSwap(1, len(h.List)-1)
	length := len(h.List) - 2
	fmt.Println(length)
	h.Heapfiy(length, 1)
	h.List = append(h.List[:length+1], h.List[length+2:]...)
	heapShow(h.List)
	h.Length--
	return top

}
