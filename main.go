package main

import (
	"fmt"
	"sort"
	"strings"
)

// Intitialize Globals
const TREE_ORDER uint8 = 4

type Node struct {
	keys     []int
	children []*Node
	values   []string
	isLeaf   bool
	parent   *Node
	next     *Node
}

type BPlusTree struct {
	root  *Node
	order int
}

func main() {
	var tree *BPlusTree = NewBPlusTree(TREE_ORDER)

	var keysToInsert []int = []int{10, 20, 30, 40, 5, 15, 25, 35, 50, 60, 70, 80}
	for _, key := range keysToInsert {
		var value string = fmt.Sprintf("val-%d", key)
		fmt.Printf("--- Inserting %d ---\n", key)
		tree.Insert(key, value)
		tree.PrintTree()
		fmt.Println(strings.Repeat("-", 40))
	}

	fmt.Println("\n--- Final Tree Structure ---")
	tree.PrintTree()
	fmt.Println(strings.Repeat("=", 40))

	fmt.Println("\n--- Searching ---")
	var keyToSearch int = 25
	value, found := tree.Search(keyToSearch)
	if found {
		fmt.Printf("Fund key %d with value: %s\n", keyToSearch, value)
	} else {
		fmt.Printf("Key %d not found. \n", keyToSearch)
	}

	keyToSearch = 99
	value, found = tree.Search(keyToSearch)
	if found {
		fmt.Printf("Found key %d with value: %s\n", keyToSearch, value)
	} else {
		fmt.Printf("Key %d not found.\n", keyToSearch)
	}

	fmt.Println("\n--- Range Scan (keys >= 20) ---")
	var startKey int = 20
	var leaf *Node = tree.findKey(startKey)
	for leaf != nil {
		var i int = sort.Search(len(leaf.keys), func(i int) bool {
			return startKey <= leaf.keys[i]
		})
		for j := i; j < len(leaf.keys); j++ {
			fmt.Printf("Key: %d, Value: %s\n", leaf.keys[j], leaf.values[j])
		}
		leaf = leaf.next
		startKey = -1
	}
}

// BPlusTree methods
func NewBPlusTree(order int) *BPlusTree {
	if order < 3 {
		panic("Order must be at least 3")
	}
	return &BPlusTree{
		order: order,
	}
}

func (t *BPlusTree) findKey(key int) *Node {
	if t.root == nil {
		return nil
	}

	var currentNode *Node = t.root

	for !currentNode.isLeaf {

		var i int = sort.Search(len(currentNode.keys), func(i int) bool {
			return key < currentNode.keys[i]
		})
		currentNode = currentNode.children[i]

	}
	return currentNode
}

func (t *BPlusTree) Search(key int) (string, bool) {
	var leaf *Node = t.findKey(key)
	if leaf == nil {
		return "", false
	}

	var i int = sort.Search(len(leaf.keys), func(i int) bool {
		return key <= leaf.keys[i]
	})

	if i < len(leaf.keys) && leaf.keys[i] == key {
		return leaf.values[i], true
	}

	return "", false
}

func (t *BPlusTree) Insert(key int, value string) {
	if t.root == nil {
		t.root = &Node{
			keys:   []int{key},
			values: []string{value},
			isLeaf: true,
		}
		return
	}

	var leaf *Node = t.findKey(key)

	var i int = sort.Search(len(leaf.keys), func(i int) bool {
		return key < leaf.keys[i]
	})

	leaf.keys = append(leaf.keys, 0)
	copy(leaf.keys[i+1:], leaf.keys[i:])
	leaf.keys[i] = key

	leaf.values = append(leaf.values, "")
	copy(leaf.values[i+1:], leaf.values[i:])
	leaf.values[i] = value

	if len(leaf.keys) == t.order {
		t.split(leaf)
	}
}

func (t *BPlusTree) split(node *Node) {
	var midIndex int = t.order / 2

	var newNode *Node = &Node{
		isLeaf: node.isLeaf,
		parent: node.parent,
	}

	if node.isLeaf {
		newNode.keys = append(newNode.keys, node.keys[midIndex:]...)
		newNode.values = append(newNode.values, node.values[midIndex:]...)

		node.keys = node.keys[:midIndex]
		node.values = node.values[:midIndex]

		newNode.next = node.next
		node.next = newNode
	} else {
		newNode.keys = append(newNode.keys, node.keys[midIndex+1:]...)
		newNode.children = append(newNode.children, node.children[midIndex+1:]...)

		for _, child := range newNode.children {
			child.parent = newNode
		}

		var promotedKey int = node.keys[midIndex]
		node.keys = node.keys[:midIndex]
		node.children = node.children[:midIndex+1]

		t.insertIntoParent(node, promotedKey, newNode)
		return
	}

	if node.parent == nil {
		var newRoot *Node = &Node{
			keys:     []int{newNode.keys[0]},
			children: []*Node{node, newNode},
			isLeaf:   false,
		}
		node.parent = newRoot
		newNode.parent = newRoot
		t.root = newRoot
	} else {
		t.insertIntoParent(node.parent, newNode.keys[0], newNode)
	}
}

func (t *BPlusTree) insertIntoParent(parent *Node, key int, newNode *Node) {
	var i int = sort.Search(len(parent.keys), func(i int) bool {
		return key < parent.keys[i]
	})
	parent.keys = append(parent.keys, 0)
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = key
	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = newNode
	if len(parent.keys) == t.order {
		t.split(parent)
	}
}

func (t *BPlusTree) PrintTree() {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return
	}
	t.printNode(t.root, 0)
}

func (t *BPlusTree) printNode(node *Node, level int) {
	var indent string = strings.Repeat("   ", level)
	if node.isLeaf {
		fmt.Printf("%sLeaf: %v (vals: %v) -> next: %p\n", indent, node.keys, node.values, node.next)
	} else {
		fmt.Printf("%sInternal: %v\n", indent, node.keys)
		for _, child := range node.children {
			t.printNode(child, level+1)
		}
	}
}
