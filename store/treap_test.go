package store

import (
	"testing"
)

func TestTreapGet(t *testing.T) {
	var treap Treap

	treap.Upsert("a", "value_a")
	treap.Upsert("b", "value_b")
	treap.Upsert("c", "value_c")
	treap.Upsert("d", "value_d")
	treap.Upsert("e", "value_e")

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
		val, ok := treap.Get(tc.key)
		if ok != tc.ok {
			t.Errorf("invalid ok for %v, expected %v but got %v", tc.key, tc.ok, ok)
		}
		if val != tc.val {
			t.Errorf("invalid val for %v, expected %v but got %v", tc.key, tc.val, val)
		}
	}
}

func TestTreapUpsert(t *testing.T) {
	var treap Treap

	treap.Upsert("f", "value_f")
	treap.Upsert("b", "value_b")
	treap.Upsert("c", "value_c")
	treap.Upsert("d", "value_d")
	treap.Upsert("a", "value_a")
	treap.Upsert("h", "value_h")
	treap.Upsert("e", "value_e")
	treap.Upsert("f", "value_f2")
	treap.Upsert("g", "value_g")

	var keys []string
	treap.InorderTraversal(func(n *Node) {
		keys = append(keys, n.key)
	})

	exp := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for i := range keys {
		if keys[i] != exp[i] {
			t.Errorf("invalid key, got %v, exp %v", keys[i], exp[i])
		}
	}
}
