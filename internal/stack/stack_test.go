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
	max := 5

	for i := 0; i < max; i++ {
		stk.Push(i)
	}
	assert.Equals(t, stk.Empty(), false)

	assert.Equals(t, stk.Equals([]int{0, 1, 2, 3, 4}), true)
	assert.Equals(t, stk.Equals([]int{0, 1, 2, 3}), false)
	assert.Equals(t, stk.Equals([]int{0, 1, 2, 4, 3}), false)

	for i := 0; i < max; i++ {
		assert.Equals(t, stk.Size(), max-i)
		assert.Equals(t, stk.Top(), max-1-i)
		stk.Pop()
	}

	assert.Equals(t, stk.Empty(), true)
}
