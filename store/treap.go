package store

import "math/rand"

// Treap implements a minimal binary search tree with random distribution
// https://en.wikipedia.org/wiki/Treap
type Treap struct {
	root *Node
}

type Node struct {
	l, r  *Node
	prio  int
	key   string
	value string
}

func (t *Treap) InorderTraversal(f func(n *Node)) {
	inorderTraversal(t.root, f)
}

func inorderTraversal(n *Node, f func(n *Node)) {
	if n == nil {
		return
	}
	inorderTraversal(n.l, f)
	f(n)
	inorderTraversal(n.r, f)
}

func (t *Treap) Get(key string) (string, bool) {
	n := t.root
	for n != nil {
		if key == n.key {
			return n.value, true
		}
		if key < n.key {
			n = n.l
		} else {
			n = n.r
		}
	}
	return "", false
}

func (t *Treap) Upsert(key, value string) {
	node := &Node{
		key:   key,
		value: value,
		prio:  rand.Int(),
	}
	if t.root == nil {
		t.root = node
		return
	}

	upsert(&t.root, t.root, node)
}

func upsert(parent **Node, root *Node, node *Node) {
	if node.key == root.key {
		// update value only
		root.value = node.value
		return
	}

	if node.key < root.key {
		if root.l == nil {
			root.l = node
		} else {
			upsert(&root.l, root.l, node)
		}
		if node.prio > root.prio {
			*parent = rotateRight(root)
		}
	} else {
		if root.r == nil {
			root.r = node
		} else {
			upsert(&root.r, root.r, node)
		}
		if node.prio > root.prio {
			*parent = rotateLeft(root)
		}
	}
}

func rotateLeft(root *Node) *Node {
	pivot := root.r
	root.r = pivot.l
	pivot.l = root
	return pivot
}

func rotateRight(root *Node) *Node {
	pivot := root.l
	root.l = pivot.r
	pivot.r = root
	return pivot
}
