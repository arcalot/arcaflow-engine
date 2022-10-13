package dg

import "fmt"

// ErrNodeDeleted indicates that the current node has already been removed from the DirectedGraph.
type ErrNodeDeleted struct {
	NodeID string
}

func (e ErrNodeDeleted) Error() string {
	return fmt.Sprintf("node with ID %s is deleted", e.NodeID)
}

// ErrCannotConnectToSelf indicates that an attempt was made to connect a node to itself.
type ErrCannotConnectToSelf struct {
	NodeID string
}

func (e ErrCannotConnectToSelf) Error() string {
	return fmt.Sprintf("cannot connect node %s to itself", e.NodeID)
}

// ErrNodeNotFound is an error that is returned if the specified node is not found.
type ErrNodeNotFound struct {
	NodeID string
}

func (e ErrNodeNotFound) Error() string {
	return fmt.Sprintf("node with ID %s not found", e.NodeID)
}

// ErrNodeAlreadyExists signals that a node with the specified ID already exists.
type ErrNodeAlreadyExists struct {
	NodeID string
}

func (e ErrNodeAlreadyExists) Error() string {
	return fmt.Sprintf("node with ID %s already exists", e.NodeID)
}

// ErrConnectionWouldCreateACycle is an error that is returned if the newly created connection would create a cycle.
type ErrConnectionWouldCreateACycle struct {
	SourceNodeID      string
	DestinationNodeID string
}

func (e ErrConnectionWouldCreateACycle) Error() string {
	return fmt.Sprintf(
		"connection from node %s to node %s would create a cycle",
		e.SourceNodeID,
		e.DestinationNodeID,
	)
}

// ErrConnectionAlreadyExists indicates that the connection you are trying to create already exists.
type ErrConnectionAlreadyExists struct {
	SourceNodeID      string
	DestinationNodeID string
}

func (e ErrConnectionAlreadyExists) Error() string {
	return fmt.Sprintf(
		"connection from node %s to node %s already exists",
		e.SourceNodeID,
		e.DestinationNodeID,
	)
}

// ErrConnectionDoesNotExist is returned if the specified connection between the two nodes does not exist.
type ErrConnectionDoesNotExist struct {
	SourceNodeID      string
	DestinationNodeID string
}

func (e ErrConnectionDoesNotExist) Error() string {
	return fmt.Sprintf(
		"connection from node %s to node %s does not exist",
		e.SourceNodeID,
		e.DestinationNodeID,
	)
}
