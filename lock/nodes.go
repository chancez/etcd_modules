package lock

import (
	"path"
	"strconv"

	etcd "github.com/coreos/etcd/client"
)

func FindNodeByValue(nodes []*etcd.Node, value string) (node *etcd.Node) {
	for _, node = range nodes {
		if node.Value == value {
			return
		}
	}
	return nil
}

func NodeIndex(node *etcd.Node) (index uint64) {
	index, _ = strconv.ParseUint(path.Base(node.Key), 10, 64)
	return
}

// Find the node with the largest index in the Nodes that is smaller than the
// given index. Also return the lastModified index of that node. This function
// assumes that the slice of nodes are already in sorted order.
func PrevNodeIndex(nodes []*etcd.Node, index uint64) (uint64, uint64) {
	// Iterate over each node to find the given index. We keep track of the
	// previous index on each iteration so we can return it when we match the
	// index we're looking for.
	var prevIndex uint64
	for _, node := range nodes {
		idx := NodeIndex(node)
		if index == idx {
			return prevIndex, node.ModifiedIndex
		}
		prevIndex = idx
	}
	return 0, 0
}

// Retrieves the first node in the set of lock nodes.
func FirstNode(nodes []*etcd.Node) *etcd.Node {
	if len(nodes) > 0 {
		return nodes[0]
	}
	return nil
}
