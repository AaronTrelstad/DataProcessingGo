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
}

func (tree *BTree) Get(key int) (string, bool) {
	return tree.search(tree.root, key)
}

func (tree *BTree) search(node *BTreeNode, key int) (string, bool) {

}

func main() {
	fmt.Println("Hello World")
}
