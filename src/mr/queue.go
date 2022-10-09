package mr

import (
	"errors"
	"sync"
)

type listNode struct {
	data interface{}
	next *listNode
	prev *listNode
}

func (node *listNode) addBefore(data interface{}) {
	prev := node.prev

	newNode := listNode{}
	newNode.data = data

	newNode.next = node
	node.prev = &newNode
	prev.next = &newNode
	newNode.prev = prev
}

func (node *listNode) addAfter(data interface{}) {
	next := node.next

	newNode := listNode{}
	newNode.data = data

	newNode.next = next
	next.prev = &newNode
	node.next = &newNode
	newNode.prev = node
}

func (node *listNode) removeBefore() {
	prev := node.prev.prev
	node.prev = prev
	prev.next = node
}

func (node *listNode) removeAfter() {
	after := node.next.next
	node.next = after
	after.prev = node
}

type LinkedList struct {
	head  listNode
	count int
}

func (list *LinkedList) pushFront(data interface{}) {
	list.head.addAfter(data)
	list.count++
}

func (list *LinkedList) pushBack(data interface{}) {
	list.head.addBefore(data)
	list.count++
}

func (list *LinkedList) peekFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("peeking empty list")
	}
	return list.head.prev.data, nil
}

func (list *LinkedList) peekBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("peeking empty list")
	}
	return list.head.next.data, nil
}

func (list *LinkedList) popFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("popping empty list")
	}
	data := list.head.prev.data
	list.head.removeBefore()
	list.count--
	return data, nil
}

func (list *LinkedList) popBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("popping empty list")
	}
	data := list.head.next.data
	list.head.removeAfter()
	list.count--
	return data, nil
}

func (list *LinkedList) size() int {
	return list.count
}

// NewLinkedList Implement a circular Linkedlist.
func NewLinkedList() *LinkedList {
	list := LinkedList{}
	list.count = 0
	list.head.next = &list.head
	list.head.next = &list.head
	return &list
}

type BlockQueue struct {
	list *LinkedList
	cond *sync.Cond // provide a locker
}

func NewBlockqueue() *BlockQueue {
	queue := BlockQueue{}
	queue.list = NewLinkedList()
	queue.cond = sync.NewCond(new(sync.Mutex))

	return &queue
}

func (queue *BlockQueue) PutFront(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushFront(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast() // Use Broadcast to awake all goroutine that waits for the Mutex.
}

func (queue *BlockQueue) PutBack(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushBack(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast() // Use Broadcast to awake all goroutine that waits (in GetBack and GetFront) for the Mutex.
}

func (queue *BlockQueue) GetFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popFront()
	return data, err
}

func (queue *BlockQueue) GetBack() (interface{}, error) {
	queue.cond.L.Lock()
	// If the queue is empty, wait for the next item
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popBack()
	return data, err
}

func (queue *BlockQueue) PopFront() (interface{}, error) {
	queue.cond.L.Lock()
	data, err := queue.list.popFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) PopBack() (interface{}, error) {
	queue.cond.L.Lock()
	data, err := queue.list.popBack()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) Size() int {
	queue.cond.L.Lock()
	res := queue.list.size()
	queue.cond.L.Unlock()
	return res
}

func (queue *BlockQueue) PeekFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.peekFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) PeekBack() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.count == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.peekBack()
	queue.cond.L.Unlock()
	return data, err
}
