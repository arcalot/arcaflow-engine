package dg

// DirectedGraph is the representation of a Directed Graph width nodes and directed connections.
type DirectedGraph[NodeType any] interface {
	// AddNode adds a node with the specified ID. If the node already exists, it returns an ErrNodeAlreadyExists.
	AddNode(id string, item NodeType) (Node[NodeType], error)
	// GetNodeByID returns a node with the specified ID. If the specified node does not exist, an ErrNodeNotFound is
	// returned.
	GetNodeByID(id string) (Node[NodeType], error)
	// ListNodesWithoutInboundConnections lists all nodes that do not have an inbound connection. This is useful for
	// performing a topological sort.
	ListNodesWithoutInboundConnections() map[string]Node[NodeType]
	// Clone creates an independent copy of the current directed graph.
	Clone() DirectedGraph[NodeType]
	// HasCycles performs cycle detection and returns true if the DirectedGraph has cycles.
	HasCycles() bool
}

// Node is a single point in a DirectedGraph.
type Node[NodeType any] interface {
	// ID returns the unique identifier of the node in the DG.
	ID() string
	// Item returns the underlying item for the node.
	Item() NodeType
	// Connect creates a new connection from the current node to the specified node. If the specified node does not
	// exist, ErrNodeNotFound is returned. If the connection had created a cycle, ErrConnectionWouldCreateACycle
	// is returned.
	Connect(toNodeID string) error
	// DisconnectInbound removes a connection to the specified node. If the connection does not exist, an
	// ErrConnectionDoesNotExist is returned.
	DisconnectInbound(toNodeID string) error
	// DisconnectOutbound removes a connection to the specified node. If the connection does not exist, an
	// ErrConnectionDoesNotExist is returned.
	DisconnectOutbound(fromNodeID string) error
	// Remove removes the current node and all connections from the DirectedGraph.
	Remove() error
	// ListInboundConnections lists all inbound connections to this node.
	ListInboundConnections() (map[string]Node[NodeType], error)
	// ListOutboundConnections lists all outbound connections from this node.
	ListOutboundConnections() (map[string]Node[NodeType], error)
}
