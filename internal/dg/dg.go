package dg

import "sync"

// New creates a new directed acyclic graph.
func New[NodeType any]() DirectedGraph[NodeType] {
	return &directedGraph[NodeType]{
		&sync.Mutex{},
		map[string]*node[NodeType]{},
		map[string]map[string]struct{}{},
		map[string]map[string]struct{}{},
	}
}

type directedGraph[NodeType any] struct {
	lock                *sync.Mutex
	nodes               map[string]*node[NodeType]
	connectionsFromNode map[string]map[string]struct{}
	connectionsToNode   map[string]map[string]struct{}
}

func (d *directedGraph[NodeType]) Clone() DirectedGraph[NodeType] {
	d.lock.Lock()
	defer d.lock.Unlock()

	newDG := &directedGraph[NodeType]{
		&sync.Mutex{},
		make(map[string]*node[NodeType], len(d.nodes)),
		d.cloneMap(d.connectionsFromNode),
		d.cloneMap(d.connectionsToNode),
	}

	for nodeID, nodeData := range d.nodes {
		newDG.nodes[nodeID] = &node[NodeType]{
			deleted: nodeData.deleted,
			id:      nodeID,
			item:    nodeData.item,
			dg:      newDG,
		}
	}

	return newDG
}

func (d *directedGraph[NodeType]) cloneMap(source map[string]map[string]struct{}) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{}, len(source))
	for nodeID1, tier2 := range source {
		result[nodeID1] = make(map[string]struct{}, len(tier2))
		for nodeID2 := range tier2 {
			result[nodeID1][nodeID2] = struct{}{}
		}
	}
	return result
}

func (d *directedGraph[NodeType]) HasCycles() bool {
	connectionsToNode := d.cloneMap(d.connectionsToNode)
	for {
		removeNodeIDs := []string{}
		// Select all nodes that have no inbound connections
		for nodeID, inboundConnections := range connectionsToNode {
			if len(inboundConnections) == 0 {
				removeNodeIDs = append(removeNodeIDs, nodeID)
			}
		}
		// If no nodes without inbound connections are found...
		if len(removeNodeIDs) == 0 {
			// ...there is a cycle if there are nodes left
			return len(connectionsToNode) != 0
		}
		for _, nodeID := range removeNodeIDs {
			// Remove all previously-selected nodes
			delete(connectionsToNode, nodeID)
			// Remove connections from the selected nodes from the remaining nodes
		}
		for _, nodeID := range removeNodeIDs {
			for targetNodeID := range connectionsToNode {
				delete(connectionsToNode[targetNodeID], nodeID)
			}
		}
	}
}

func (d *directedGraph[NodeType]) AddNode(id string, item NodeType) (Node[NodeType], error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if _, ok := d.nodes[id]; ok {
		return nil, ErrNodeAlreadyExists{
			id,
		}
	}
	d.nodes[id] = &node[NodeType]{
		false,
		id,
		item,
		d,
	}
	d.connectionsToNode[id] = map[string]struct{}{}
	d.connectionsFromNode[id] = map[string]struct{}{}
	return d.nodes[id], nil
}

func (d *directedGraph[NodeType]) GetNodeByID(id string) (Node[NodeType], error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	n, ok := d.nodes[id]
	if !ok {
		return nil, &ErrNodeNotFound{
			id,
		}
	}
	return n, nil
}

func (d *directedGraph[NodeType]) ListNodesWithoutInboundConnections() map[string]Node[NodeType] {
	d.lock.Lock()
	defer d.lock.Unlock()

	result := map[string]Node[NodeType]{}
	for nodeID, n := range d.nodes {
		if len(d.connectionsToNode[nodeID]) == 0 {
			result[nodeID] = n
		}
	}
	return result
}

type node[NodeType any] struct {
	deleted bool
	id      string
	item    NodeType
	dg      *directedGraph[NodeType]
}

func (d *node[NodeType]) ID() string {
	return d.id
}

func (d *node[NodeType]) Item() NodeType {
	return d.item
}

func (d *node[NodeType]) Connect(nodeID string) error {
	d.dg.lock.Lock()
	defer d.dg.lock.Unlock()
	if d.deleted {
		return &ErrNodeDeleted{
			d.id,
		}
	}
	if nodeID == d.id {
		return &ErrCannotConnectToSelf{}
	}
	if _, ok := d.dg.nodes[nodeID]; !ok {
		return &ErrNodeNotFound{
			nodeID,
		}
	}
	if _, ok := d.dg.connectionsFromNode[d.id][nodeID]; ok {
		return &ErrConnectionAlreadyExists{
			d.id,
			nodeID,
		}
	}
	d.dg.connectionsFromNode[d.id][nodeID] = struct{}{}
	d.dg.connectionsToNode[nodeID][d.id] = struct{}{}
	return nil
}

func (d *node[NodeType]) DisconnectInbound(fromNodeID string) error {
	d.dg.lock.Lock()
	defer d.dg.lock.Unlock()
	if d.deleted {
		return &ErrNodeDeleted{
			d.id,
		}
	}
	if _, ok := d.dg.nodes[fromNodeID]; !ok {
		return &ErrNodeNotFound{
			fromNodeID,
		}
	}
	if _, ok := d.dg.connectionsToNode[d.id][fromNodeID]; !ok {
		return &ErrConnectionDoesNotExist{
			d.id,
			fromNodeID,
		}
	}
	delete(d.dg.connectionsToNode[d.id], fromNodeID)
	delete(d.dg.connectionsFromNode[fromNodeID], d.id)
	return nil
}

func (d *node[NodeType]) DisconnectOutbound(toNodeID string) error {
	d.dg.lock.Lock()
	defer d.dg.lock.Unlock()
	if d.deleted {
		return &ErrNodeDeleted{
			d.id,
		}
	}
	if _, ok := d.dg.nodes[toNodeID]; !ok {
		return &ErrNodeNotFound{
			toNodeID,
		}
	}
	if _, ok := d.dg.connectionsFromNode[d.id][toNodeID]; !ok {
		return &ErrConnectionDoesNotExist{
			d.id,
			toNodeID,
		}
	}
	delete(d.dg.connectionsFromNode[d.id], toNodeID)
	delete(d.dg.connectionsToNode[toNodeID], d.id)
	return nil
}

func (d *node[NodeType]) Remove() error {
	d.dg.lock.Lock()
	defer d.dg.lock.Unlock()
	if d.deleted {
		return &ErrNodeDeleted{
			d.id,
		}
	}
	for toNodeID := range d.dg.connectionsFromNode[d.id] {
		delete(d.dg.connectionsToNode[toNodeID], d.id)
	}
	delete(d.dg.connectionsFromNode, d.id)
	for fromNodeID := range d.dg.connectionsToNode[d.id] {
		delete(d.dg.connectionsFromNode[fromNodeID], d.id)
	}
	delete(d.dg.connectionsToNode, d.id)
	delete(d.dg.nodes, d.id)
	d.deleted = true
	return nil
}

func (d *node[NodeType]) ListInboundConnections() (map[string]Node[NodeType], error) {
	d.dg.lock.Lock()
	defer d.dg.lock.Unlock()
	if d.deleted {
		return nil, &ErrNodeDeleted{
			d.id,
		}
	}
	result := make(map[string]Node[NodeType], len(d.dg.connectionsToNode[d.id]))
	for fromNodeID := range d.dg.connectionsToNode[d.id] {
		result[fromNodeID] = d.dg.nodes[fromNodeID]
	}
	return result, nil
}

func (d *node[NodeType]) ListOutboundConnections() (map[string]Node[NodeType], error) {
	d.dg.lock.Lock()
	defer d.dg.lock.Unlock()
	if d.deleted {
		return nil, &ErrNodeDeleted{
			d.id,
		}
	}
	result := make(map[string]Node[NodeType], len(d.dg.connectionsFromNode[d.id]))
	for toNodeID := range d.dg.connectionsFromNode[d.id] {
		result[toNodeID] = d.dg.nodes[toNodeID]
	}
	return result, nil
}
