package stack

import (
	"go.arcalot.io/assert"
	"testing"
)

func TestStack(t *testing.T) {
	stk := NewStack[int]()

	el, err := stk.Pop()
	assert.Equals(t, err, false)
	assert.NotNil(t, el)

	for i := 1; i < 10; i++ {
		stk.Push(i)
	}

	for i := 0; i < 10; i++ {
		if stk.Empty() {
			t.Fatalf("%d: stack.Empty(): expected %v got %v", i, false, stk.Empty())
		}
		if stk.Size() != 10-i {
			t.Fatalf("%d: stack.Size(): expected %d got %d", i, 10-i, stk.Size())
		}
		if stk.Top() != 9-i {
			t.Fatalf("%d: stack.Top(): expected %d got %d", i, 9-i, stk.Top())
		}
		stk.Pop()
	}
	if !stk.Empty() {
		t.Fatalf("stack.Empty(): should be empty at the end")
	}
}
