package stack

type Stack[T comparable] interface {
	Empty() bool
	Size() int
	Top() T
	Push(T)
	Pop() (T, bool)
	Values() []T
	Equals(other []T) bool
}

func NewStack[T comparable]() Stack[T] {
	return &stack[T]{}
}

func (s stack[T]) Equals(other []T) bool {
	if len(s) != len(other) {
		return false
	}
	for i, v := range s.Values() {
		if v != other[i] {
			return false
		}
	}
	return true
}

func NewSeededStack[T comparable](el T) Stack[T] {
	return &stack[T]{el}
}

type stack[T comparable] []T

func (s stack[T]) Empty() bool {
	return s.Size() == 0
}

func (s stack[T]) Size() int {
	return len(s)
}

func (s stack[T]) Top() T {
	return s[len(s)-1]
}

func (s *stack[T]) Push(element T) {
	*s = append(*s, element)
}

func (s *stack[T]) Pop() (T, bool) {
	if len(s.Values()) == 0 {
		var zero T
		return zero, false
	}
	top := s.Top()
	*s = (*s)[:len(*s)-1]
	return top, true
}

func (s stack[T]) Values() []T {
	return s
}
