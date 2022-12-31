package avl

import (
	"sync/atomic"
)

type Tree struct {
	root atomic.Pointer[Node]
}

type Node struct {
	left, right *Node
	h           int
	Key         string
	Value       string
}

func (n *Node) height() int {
	if n == nil {
		return 0
	}
	return n.h
}

func (n *Node) updateH() {
	n.h = max(n.left.height(), n.right.height()) + 1
}

func (n *Node) balance() int {
	return n.right.height() - n.left.height()
}

func (t *Tree) Get(key string) (string, bool) {
	n := t.root.Load()
	for n != nil {
		if key == n.Key {
			return n.Value, true
		}
		if key < n.Key {
			n = n.left
		} else {
			n = n.right
		}
	}
	return "", false
}

func (t *Tree) Upsert(key, value string) {
	node := &Node{
		Key:   key,
		Value: value,
		h:     1,
	}

	var changed bool
	for !changed {
		root := t.root.Load()

		changed = t.root.CompareAndSwap(root, upsert(root, node))
	}
}

func upsert(cur *Node, node *Node) *Node {
	if cur == nil {
		return node
	}
	if node.Key == cur.Key { // update
		return &Node{
			Key:   cur.Key,
			Value: node.Value,
			h:     cur.h,
			left:  cur.left,
			right: cur.right,
		}
	}

	if node.Key < cur.Key {
		res := &Node{
			Key:   cur.Key,
			Value: cur.Value,
			left:  upsert(cur.left, node),
			right: cur.right,
		}
		res.updateH()

		if res.balance() < -1 {
			if res.left.balance() <= 0 {
				// left-left
				return rotateRight(res)
			} else {
				// left-right
				return rotateLeftRight(res)
			}
		}

		return res
	} else {
		res := &Node{
			Key:   cur.Key,
			Value: cur.Value,
			left:  cur.left,
			right: upsert(cur.right, node),
		}
		res.updateH()

		if res.balance() > 1 {
			if res.right.balance() >= 0 {
				// right-right
				return rotateLeft(res)
			} else {
				// right-left
				return rotateRightLeft(res)
			}
		}
		return res
	}
}

func (t *Tree) InorderTraversal(f func(n *Node)) {
	root := t.root.Load()
	inorderTraversal(root, f)
}

func inorderTraversal(n *Node, f func(n *Node)) {
	if n == nil {
		return
	}
	inorderTraversal(n.left, f)
	f(n)
	inorderTraversal(n.right, f)
}

func rotateLeft(root *Node) *Node {
	return _rotateLeft(root)
}

func rotateRight(root *Node) *Node {
	return _rotateRight(root)
}

func rotateLeftRight(root *Node) *Node {
	root.left = _rotateLeft(root.left)
	return _rotateRight(root)
}
func rotateRightLeft(root *Node) *Node {
	root.right = _rotateRight(root.right)
	return _rotateLeft(root)
}

func _rotateLeft(root *Node) *Node {
	pivot := root.right
	root.right = pivot.left
	pivot.left = root

	root.updateH()
	pivot.updateH()

	return pivot
}

func _rotateRight(root *Node) *Node {
	pivot := root.left
	root.left = pivot.right
	pivot.right = root

	root.updateH()
	pivot.updateH()

	return pivot
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
