package avl

import (
	"math/rand"
	"testing"
)

func TestTreeGet(t *testing.T) {
	var tree Tree

	tree.Upsert("a", "value_a")
	tree.Upsert("b", "value_b")
	tree.Upsert("c", "value_c")
	tree.Upsert("d", "value_d")
	tree.Upsert("e", "value_e")

	testCases := []struct {
		key string
		val string
		ok  bool
	}{
		{"a", "value_a", true},
		{"c", "value_c", true},
		{"e", "value_e", true},
		{"n", "", false},
		{"", "", false},
	}

	for _, tc := range testCases {
		val, ok := tree.Get(tc.key)
		if ok != tc.ok {
			t.Errorf("invalid ok for %v, expected %v but got %v", tc.key, tc.ok, ok)
		}
		if val != tc.val {
			t.Errorf("invalid val for %v, expected %v but got %v", tc.key, tc.val, val)
		}
	}
}

func TestTreeUpsertBase(t *testing.T) {
	for _, tc := range [][]string{
		{"a", "b", "c"}, // right-right
		{"a", "c", "b"}, // right-left
		{"c", "b", "a"}, // left-left
		{"c", "a", "b"}, // left-right
		{"c", "d", "e", "f", "g", "h"},
		{"c", "d", "e", "g", "f", "h"},
		{"f", "d", "e", "b", "c", "a"},
		{"f", "d", "e", "c", "a", "b"},
	} {
		var tree Tree
		for _, k := range tc {
			tree.Upsert(k, "value-"+k)
		}
		root := tree.root.Load()
		h := computeHeight(root)
		var exp int
		if len(tc) == 3 {
			exp = 2
		} else {
			exp = 3
		}
		if h != exp {
			t.Errorf("unexpected height: %v != %v", h, exp)
		}

		checkInvariants(t, &tree)
	}
}

func TestTreeUpsert1(t *testing.T) {
	var tree Tree

	tree.Upsert("f", "value_f")
	tree.Upsert("b", "value_b")
	tree.Upsert("c", "value_c")
	tree.Upsert("d", "value_d")
	tree.Upsert("a", "value_a")
	tree.Upsert("h", "value_h")
	tree.Upsert("e", "value_e")
	tree.Upsert("f", "value_f2")
	tree.Upsert("g", "value_g")

	checkInvariants(t, &tree)

	var keys []string
	tree.InorderTraversal(func(n *Node) {
		keys = append(keys, n.Key)
	})
	exp := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for i := range keys {
		if keys[i] != exp[i] {
			t.Errorf("invalid key, got %v, exp %v", keys[i], exp[i])
		}
	}
}

func TestRandomUpsert(t *testing.T) {
	var tree Tree
	all := make(map[string]string)

	for i := 0; i < 1e5; i++ {
		rdKey := randString(3)
		rdVal := randString(8)

		tree.Upsert(rdKey, rdVal)
	}
	checkInvariants(t, &tree)

	for expK, expV := range all {
		val, f := tree.Get(expK)
		if !f {
			t.Errorf("Get didn't find key: %v", expK)
		}
		if expV != val {
			t.Errorf("Get didn't return expected value: %v != %v", expV, val)
		}
	}
}

func randString(sz int) string {
	const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	buf := make([]byte, sz)
	for i := 0; i < sz; i++ {
		buf[i] = alpha[rand.Intn(len(alpha))]
	}
	return string(buf)
}

func checkInvariants(t *testing.T, tree *Tree) {
	t.Helper()
	checkKeyOrder(t, tree)
	checkBalance(t, tree)
	checkHeight(t, tree)
}

func checkKeyOrder(t *testing.T, tree *Tree) {
	var prev string
	tree.InorderTraversal(func(n *Node) {
		if n.Key < prev {
			t.Errorf("invalid order: %v < %v", n.Key, prev)
		}
		prev = n.Key
	})
}

func checkBalance(t *testing.T, tree *Tree) {
	tree.InorderTraversal(func(n *Node) {
		if n.balance() < -1 || n.balance() > 1 {
			t.Errorf("invalid balance for %v: %v", n.Key, n.balance())
		}
	})
}

func checkHeight(t *testing.T, tree *Tree) {
	tree.InorderTraversal(func(n *Node) {
		if n.h != computeHeight(n) {
			t.Errorf("height invalid for %v: %v != %v", n.Key, n.h, computeHeight(n))
			t.FailNow()
		}
	})
}

func computeHeight(n *Node) int {
	if n == nil {
		return 0
	}

	return 1 + max(computeHeight(n.left), computeHeight(n.right))
}
