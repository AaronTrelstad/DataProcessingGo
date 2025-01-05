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

	if len(root.keys) == (2*tree.minDegree - 1) {
		newNode := &BTreeNode{
			isLeaf: false,
			keys: []int{},
			children: []*BTreeNode{root},
		};
		tree.split(newNode, 0);
		tree.root = newNode;
	}

	tree.insertNonFull(tree.root, key);
}

func (tree *BTree) insertNonFull(node *BTreeNode, key int) {
	index := len(node.keys) - 1;

	if node.isLeaf {
		for index >= 0 && node.keys[index] > key {
			index -= 1;
		}

		node.keys = append(node.keys[:index+1], append([]int{key}, node.keys[index+1:]...)...);
	} else {
		for index >= 0 && node.keys[index] > key {
			index -= 1;
		}

		index++;

		if len(node.children[index].keys) == (2*tree.minDegree - 1) {
			tree.split(node, index);

			if node.keys[index] < key {
				index++;
			}
		}

		tree.insertNonFull(node.children[index], key)
	}
}

func (tree *BTree) split(parent *BTreeNode, index int) {
	child := parent.children[index];
	newNode := &BTreeNode{
		isLeaf: child.isLeaf,
		keys: []int{},
		children: nil,
	}

	midKey := child.keys[tree.minDegree - 1];
	parent.keys = append(parent.keys[:index], append([]int{midKey}, parent.keys[index:]...)...)
	parent.children = append(parent.children[:index+1], append([]*BTreeNode{newNode}, parent.children[index+1:]...)...)

	newNode.keys = append(newNode.keys, child.keys[tree.minDegree:]...)
	child.keys = child.keys[:tree.minDegree-1]

	if !child.isLeaf {
		newNode.children = append(newNode.children, child.children[tree.minDegree:]...)
		child.children = child.children[:tree.minDegree]
	}
}

func (tree *BTree) Delete(key int) {
	tree.delete(tree.root, key);
}

func (tree *BTree) delete(node *BTreeNode, key int) {
	index := BinarySearch(node.keys, key);

	if index != -1 {
		if node.isLeaf {
			node.keys = append(node.keys[:index], node.keys[index+1:]...)
		} else {
			node.keys[index] = tree.getPred(node.children[index])
			tree.delete(node.children[index], node.keys[index])
		}
	} else {
		if node.isLeaf {
			return;
		}

		for index = 0; index < len(node.keys); index++ {
			if key < node.keys[index] {
				break
			}
		} 
		index++;

		if len(node.children[index].keys) < tree.minDegree {
			tree.rebalance(node, index);
		}

		tree.delete(node.children[index], key)
	}
}

func (tree *BTree) rebalance(node *BTreeNode, i int) {
	if i > 0 && len(node.children[i-1].keys) >= tree.minDegree {
		tree.borrowFromPrev(node, i)
	} else if i < len(node.children)-1 && len(node.children[i+1].keys) >= tree.minDegree {
		tree.borrowFromNext(node, i)
	} else {
		if i < len(node.children)-1 {
			tree.merge(node, i)
		} else {
			tree.merge(node, i-1)
		}
	}
}

func (tree *BTree) borrowFromPrev(node *BTreeNode, i int) {
	child := node.children[i]
	sibling := node.children[i-1]

	child.keys = append([]int{node.keys[i-1]}, child.keys...)
	node.keys[i-1] = sibling.keys[len(sibling.keys)-1]
	sibling.keys = sibling.keys[:len(sibling.keys)-1]

	if !child.isLeaf {
		child.children = append([]*BTreeNode{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
}

func (tree *BTree) borrowFromNext(node *BTreeNode, i int) {
	child := node.children[i]
	sibling := node.children[i+1]

	child.keys = append(child.keys, node.keys[i])
	node.keys[i] = sibling.keys[0]
	sibling.keys = sibling.keys[1:]

	if !child.isLeaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}
}

func (tree *BTree) merge(node *BTreeNode, i int) {
	child := node.children[i]
	sibling := node.children[i+1]

	child.keys = append(append(child.keys, node.keys[i]), sibling.keys...)
	child.children = append(child.children, sibling.children...)
	node.keys = append(node.keys[:i], node.keys[i+1:]...)
	node.children = append(node.children[:i+1], node.children[i+2:]...)
}

func (tree *BTree) getPred(node *BTreeNode) int {
	for !node.isLeaf {
		node = node.children[len(node.children)-1]
	}
	return node.keys[len(node.keys)-1]
}

func main() {
	tree := NewBTree(32)

	tree.Insert(10)
	tree.Insert(20)
	tree.Insert(5)
	tree.Insert(15)

	fmt.Println(tree.Get(10)) 
	fmt.Println(tree.Get(15)) 
	fmt.Println(tree.Get(30))

	tree.Delete(15)
	fmt.Println(tree.Get(15))
}
