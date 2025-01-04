package main

import (
	"fmt"
)

type BTreeNode struct {
	isLeaf bool
	keys []int
	children []*BTreeNode
}

type BTree struct {
	root *BTreeNode
	minDegree int
	pageSize int
}

func NewBTree(pageSize int) *BTree {
	minDegree := pageSize / 8;
	return &BTree {
		root: &BTreeNode {
			isLeaf: true,
			keys: []int{},
			children: nil,
		},
		minDegree: minDegree,
		pageSize: pageSize,
	}
}

func BinarySearch(keys []int, key int) int {
	left, right := 0, len(keys) - 1;
	for left <= right {
		middle := left + (right - left) / 2;

		if keys[middle] == key {
			return middle;
		} else if keys[middle] < key {
			left = middle + 1;
		} else if keys[middle] > key {
			right = middle -1;
		}
	}

	return -1;
}

func (tree *BTree) Get(key int) (string, bool) {
	return tree.search(tree.root, key)
}

func (tree *BTree) search(node *BTreeNode, key int) (string, bool) {
	if node == nil {
		return "", false;
	}

	index := BinarySearch(node.keys, key);

	if index != -1 {
		return "Found", true;
	}

	if node.isLeaf {
		return "", false;
	}

	for index = 0; index < len(node.keys); index++ {
		if key < node.keys[index] {
			break;
		}
	} 

	return tree.search(node.children[index], key);
}

func (tree *BTree) Insert(key int) {
	root := tree.root;
}

func (tree *BTree) insertNonFull(node *BTreeNode, key int) {

}

func (tree *BTree) split(parent *BTreeNode, index int) {

}

func (tree *BTree) Delete(key int) {

}

func (tree *BTree) delete(node *BTreeNode, key int) {

}

func (tree *BTree) rebalance(node *BTreeNode, i int) {

}

func (tree *BTree) borrowFromPrev(node *BTreeNode, i int) {

}

func (tree *BTree) borrowFromNext(node *BTreeNode, i int) {

}

func (tree *BTree) merge(node *BTreeNode, i int) {
	
}

func main() {
	fmt.Println("Hello World")
}
