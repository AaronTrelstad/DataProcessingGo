package main

import (
	"fmt"
	"sync"
	"time"
)

type Heap struct {
	values []*HeapValue
	size   int
}

type HeapValue struct {
	title string
	time  int
}

func NewSchedular(tasks []*HeapValue, threadPoolSize int) {
	heap := &Heap{
		values: []*HeapValue{},
		size:   0,
	}

	for _, task := range tasks {
		heap.push(task)
	}

	var waitGroup sync.WaitGroup
	var heapLock sync.Mutex

	for i := 0; i < threadPoolSize; i++ {
		waitGroup.Add(1)
		go worker(i, &waitGroup, &heapLock, heap)
	}

	waitGroup.Wait()
}

func (heap *Heap) pop() *HeapValue {
	if heap.size == 0 {
		return nil
	}

	root := heap.values[0]
	heap.values[0] = heap.values[heap.size-1]
	heap.values = heap.values[:heap.size-1]
	heap.size--

	heap.heapifyDown(0)
	return root
}

func (heap *Heap) heapifyDown(index int) {
	largestIndex := index
	leftChildIndex := 2*index + 1
	rightChildIndex := 2*index + 2

	if leftChildIndex < heap.size && heap.values[leftChildIndex].time > heap.values[largestIndex].time {
		largestIndex = leftChildIndex
	}

	if rightChildIndex < heap.size && heap.values[rightChildIndex].time > heap.values[largestIndex].time {
		largestIndex = rightChildIndex
	}

	if largestIndex != index {
		heap.swap(index, largestIndex)
		heap.heapifyDown(largestIndex)
	}
}

func (heap *Heap) push(value *HeapValue) {
	heap.values = append(heap.values, value)
	heap.size++
	heap.heapifyUp(heap.size - 1)
}

func (heap *Heap) heapifyUp(index int) {
	parentIndex := (index - 1) / 2
	if parentIndex >= 0 && heap.values[index].time > heap.values[parentIndex].time {
		heap.swap(index, parentIndex)
		heap.heapifyUp(parentIndex)
	}
}

func (heap *Heap) swap(i, j int) {
	heap.values[i], heap.values[j] = heap.values[j], heap.values[i]
}

func worker(id int, waitGroup *sync.WaitGroup, heapLock *sync.Mutex, heap *Heap) {
	defer waitGroup.Done()

	for {
		heapLock.Lock()
		task := heap.pop()
		heapLock.Unlock()

		if task == nil {
			break
		}

		fmt.Printf("Thread %d executing: %s (duration: %d ms)\n", id, task.title, task.time)
		time.Sleep(time.Duration(task.time) * time.Millisecond)
	}
}

func main() {
	tasks := []*HeapValue{
		{"Task 1", 500},
		{"Task 2", 200},
		{"Task 3", 800},
		{"Task 4", 100},
		{"Task 5", 700},
		{"Task 6", 300},
		{"Task 7", 600},
		{"Task 8", 400},
		{"Task 9", 900},
		{"Task 10", 1000},
	}

	threadPoolSize := 3;

	start := time.Now();

	NewSchedular(tasks, threadPoolSize);

	elapsed := time.Since(start);

	fmt.Println("Total time:", elapsed);
}
