package dg_test

import (
	"testing"

	"go.arcalot.io/assert"
	"go.flow.arcalot.io/engine/internal/dg"
)

func TestDirectedGraph_BasicNodeAdditionAndRemoval(t *testing.T) {
	d := dg.New[string]()
	n, err := d.AddNode("node-1", "Hello world!")
	assert.NoError(t, err)
	assert.Equals(t, n.ID(), "node-1")
	assert.Equals(t, n.Item(), "Hello world!")

	n2, err := d.GetNodeByID("node-1")
	assert.NoError(t, err)
	assert.Equals(t, n, n2)

	assert.ErrorR[dg.Node[string]](t)(d.GetNodeByID("node-2"))

	nodes := d.ListNodesWithoutInboundConnections()
	assert.Equals(t, len(nodes), 1)

	assert.NoError(t, n.Remove())

	nodes = d.ListNodesWithoutInboundConnections()
	assert.Equals(t, len(nodes), 0)
	assert.ErrorR[dg.Node[string]](t)(d.GetNodeByID("node-1"))
}

func TestDirectedGraph_ConnectSelf(t *testing.T) {
	d := dg.New[string]()
	n, err := d.AddNode("node-1", "Hello world!")
	assert.NoError(t, err)
	assert.Equals(t, n.ID(), "node-1")
	assert.Equals(t, n.Item(), "Hello world!")

	assert.Error(t, n.Connect("node-1"))
}

func TestDirectedGraph_Connect(t *testing.T) {
	d := dg.New[string]()
	n1, err := d.AddNode("node-1", "test1")
	assert.NoError(t, err)

	n2, err := d.AddNode("node-2", "test2")
	assert.NoError(t, err)

	t.Run("connect", func(t *testing.T) {
		assert.NoError(t, n1.Connect(n2.ID()))
		assert.Error(t, n1.Connect(n2.ID()))
		n1In, err := n1.ListInboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n1In), 0)
		n1Out, err := n1.ListOutboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n1Out), 1)
		assert.Equals(t, n1Out["node-2"].ID(), "node-2")
		n2In, err := n2.ListInboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n2In), 1)
		assert.Equals(t, n2In["node-1"].ID(), "node-1")
		n2Out, err := n2.ListOutboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n2Out), 0)
		starterNodes := d.ListNodesWithoutInboundConnections()
		assert.Equals(t, len(starterNodes), 1)
		assert.Equals(t, starterNodes["node-1"].ID(), "node-1")
	})
	t.Run("disconnect", func(t *testing.T) {
		assert.NoError(t, n2.DisconnectInbound(n1.ID()))
		n1In, err := n1.ListInboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n1In), 0)
		n1Out, err := n1.ListOutboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n1Out), 0)

		n2In, err := n2.ListInboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n2In), 0)
		n2Out, err := n2.ListOutboundConnections()
		assert.NoError(t, err)
		assert.Equals(t, len(n2Out), 0)

		starterNodes := d.ListNodesWithoutInboundConnections()
		assert.Equals(t, len(starterNodes), 2)
		assert.Equals(t, starterNodes["node-1"].ID(), "node-1")
		assert.Equals(t, starterNodes["node-2"].ID(), "node-2")
	})
}

func TestDirectedGraph_Clone(t *testing.T) {
	d := dg.New[string]()
	_, err := d.AddNode("node-1", "test1")
	assert.NoError(t, err)

	n2, err := d.AddNode("node-2", "test2")
	assert.NoError(t, err)

	n3, err := d.AddNode("node-3", "test3")
	assert.NoError(t, err)

	assert.NoError(t, n3.Connect(n2.ID()))

	d2 := d.Clone()

	d2n2, err := d2.GetNodeByID("node-2")
	assert.NoError(t, err)
	assert.NoError(t, d2n2.Remove())

	assert.Equals(t, len(d.ListNodesWithoutInboundConnections()), 2)
	assert.Equals(t, len(d2.ListNodesWithoutInboundConnections()), 2)
	n3Out, err := n3.ListOutboundConnections()
	assert.NoError(t, err)
	assert.Equals(t, len(n3Out), 1)
}

func TestDirectedGraph_HasCycles(t *testing.T) {
	d := dg.New[string]()
	n1, err := d.AddNode("node-1", "test1")
	assert.NoError(t, err)
	n2, err := d.AddNode("node-2", "test2")
	assert.NoError(t, err)
	n3, err := d.AddNode("node-3", "test3")
	assert.NoError(t, err)
	assert.NoError(t, n1.Connect(n2.ID()))
	assert.NoError(t, n2.Connect(n3.ID()))
	assert.Equals(t, d.HasCycles(), false)
	assert.NoError(t, n3.Connect(n2.ID()))
	assert.Equals(t, d.HasCycles(), true)
	assert.NoError(t, n2.DisconnectOutbound(n3.ID()))
	assert.Equals(t, d.HasCycles(), false)
	assert.NoError(t, n2.Connect(n1.ID()))
	assert.Equals(t, d.HasCycles(), true)
}
