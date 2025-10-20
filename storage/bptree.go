package storage

import (
	"fmt"
	"sync"
)

const (
	// B+ Tree configuration
	BPTreeOrder       = 4 // Maximum number of children per node
	BPTreeMaxKeys     = BPTreeOrder - 1
	BPTreeMinKeys     = (BPTreeOrder+1)/2 - 1
	BPTreeLeafOrder   = 4
	BPTreeMaxLeafKeys = BPTreeLeafOrder - 1
)

// BPTreeNodeType represents the type of B+ Tree node
type BPTreeNodeType int

const (
	LeafNode BPTreeNodeType = iota
	InternalNode
)

// BPTreeEntry represents a key-value pair in the B+ Tree
type BPTreeEntry struct {
	Key   int64
	Value int64
}

// BPTreeNode represents a node in the B+ Tree
type BPTreeNode struct {
	nodeType BPTreeNodeType
	keys     []int64
	values   []int64       // For leaf nodes
	children []*BPTreeNode // For internal nodes
	next     *BPTreeNode   // For leaf nodes (linked list)
	parent   *BPTreeNode
	mu       sync.RWMutex // Allows concurrent reads, exclusive writes
}

// NewLeafNode creates a new leaf node
func NewLeafNode() *BPTreeNode {
	return &BPTreeNode{
		nodeType: LeafNode,
		keys:     make([]int64, 0, BPTreeMaxLeafKeys),
		values:   make([]int64, 0, BPTreeMaxLeafKeys),
		children: nil,
		next:     nil,
		parent:   nil,
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode() *BPTreeNode {
	return &BPTreeNode{
		nodeType: InternalNode,
		keys:     make([]int64, 0, BPTreeMaxKeys),
		values:   nil,
		children: make([]*BPTreeNode, 0, BPTreeOrder),
		next:     nil,
		parent:   nil,
	}
}

// BPlusTree represents a B+ Tree index
type BPlusTree struct {
	root   *BPTreeNode
	bpm    *BufferPoolManager
	rootMu sync.RWMutex // Protects root pointer changes
}

// NewBPlusTree creates a new B+ Tree
func NewBPlusTree(bpm *BufferPoolManager) (*BPlusTree, error) {
	if bpm == nil {
		return nil, fmt.Errorf("buffer pool manager cannot be nil")
	}

	return &BPlusTree{
		root: NewLeafNode(),
		bpm:  bpm,
	}, nil
}

// Search searches for a key in the B+ Tree and returns its value
// Uses read locks for high concurrency
func (tree *BPlusTree) Search(key int64) (int64, bool, error) {
	tree.rootMu.RLock()
	if tree.root == nil {
		tree.rootMu.RUnlock()
		return 0, false, nil
	}
	currentRoot := tree.root
	tree.rootMu.RUnlock()

	// Find the leaf node with read locks
	leaf := tree.findLeafConcurrent(currentRoot, key)
	if leaf == nil {
		return 0, false, nil
	}

	// Search in the leaf node with read lock
	leaf.mu.RLock()
	defer leaf.mu.RUnlock()

	for i, k := range leaf.keys {
		if k == key {
			return leaf.values[i], true, nil
		}
	}

	return 0, false, nil
}

// findLeafConcurrent finds the leaf node using read locks for concurrency
func (tree *BPlusTree) findLeafConcurrent(startNode *BPTreeNode, key int64) *BPTreeNode {
	node := startNode

	for {
		node.mu.RLock()
		nodeType := node.nodeType

		if nodeType == LeafNode {
			// We're at a leaf - keep the lock and return
			node.mu.RUnlock()
			return node
		}

		// Internal node - find the appropriate child
		childIndex := 0
		for i, k := range node.keys {
			if key >= k {
				childIndex = i + 1
			} else {
				break
			}
		}
		if childIndex >= len(node.children) {
			childIndex = len(node.children) - 1
		}

		nextNode := node.children[childIndex]
		node.mu.RUnlock()

		node = nextNode
	}
}

// findLeaf finds the leaf node that should contain the key
func (tree *BPlusTree) findLeaf(key int64) *BPTreeNode {
	node := tree.root

	for node.nodeType == InternalNode {
		// Find the appropriate child
		childIndex := 0
		for i, k := range node.keys {
			if key >= k {
				childIndex = i + 1
			} else {
				break
			}
		}
		if childIndex >= len(node.children) {
			childIndex = len(node.children) - 1
		}
		node = node.children[childIndex]
	}

	return node
}

// Insert inserts a key-value pair into the B+ Tree
// Uses write locks for safe concurrent modifications
func (tree *BPlusTree) Insert(key int64, value int64) error {
	tree.rootMu.Lock()
	if tree.root == nil {
		tree.root = NewLeafNode()
	}
	tree.rootMu.Unlock()

	// Find the leaf node
	leaf := tree.findLeaf(key)

	// Lock the leaf for writing
	leaf.mu.Lock()

	// Check if key already exists
	for i, k := range leaf.keys {
		if k == key {
			// Update existing value
			leaf.values[i] = value
			leaf.mu.Unlock()
			return nil
		}
	}

	// Insert into leaf
	leaf.keys = append(leaf.keys, key)
	leaf.values = append(leaf.values, value)

	// Sort keys and values
	tree.sortLeaf(leaf)

	// Check if split is needed
	needsSplit := len(leaf.keys) > BPTreeMaxLeafKeys
	leaf.mu.Unlock()

	if needsSplit {
		// Split requires tree-level coordination
		tree.rootMu.Lock()
		err := tree.splitLeaf(leaf)
		tree.rootMu.Unlock()
		return err
	}

	return nil
}

// sortLeaf sorts the keys and values in a leaf node
func (tree *BPlusTree) sortLeaf(leaf *BPTreeNode) {
	// Simple bubble sort for small arrays
	n := len(leaf.keys)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if leaf.keys[j] > leaf.keys[j+1] {
				// Swap keys
				leaf.keys[j], leaf.keys[j+1] = leaf.keys[j+1], leaf.keys[j]
				// Swap values
				leaf.values[j], leaf.values[j+1] = leaf.values[j+1], leaf.values[j]
			}
		}
	}
}

// splitLeaf splits a leaf node
func (tree *BPlusTree) splitLeaf(leaf *BPTreeNode) error {
	// Create new leaf
	newLeaf := NewLeafNode()

	// Split point
	mid := len(leaf.keys) / 2

	// Move half the keys to new leaf
	newLeaf.keys = append(newLeaf.keys, leaf.keys[mid:]...)
	newLeaf.values = append(newLeaf.values, leaf.values[mid:]...)

	// Truncate original leaf
	leaf.keys = leaf.keys[:mid]
	leaf.values = leaf.values[:mid]

	// Update linked list
	newLeaf.next = leaf.next
	leaf.next = newLeaf

	// Get the key to push up
	keyToInsert := newLeaf.keys[0]

	// If root is a leaf, create new root
	if leaf.parent == nil {
		newRoot := NewInternalNode()
		newRoot.keys = append(newRoot.keys, keyToInsert)
		newRoot.children = append(newRoot.children, leaf, newLeaf)
		leaf.parent = newRoot
		newLeaf.parent = newRoot
		tree.root = newRoot
		return nil
	}

	// Insert into parent
	return tree.insertIntoParent(leaf.parent, keyToInsert, newLeaf)
}

// insertIntoParent inserts a key and child into an internal node
func (tree *BPlusTree) insertIntoParent(parent *BPTreeNode, key int64, newChild *BPTreeNode) error {
	newChild.parent = parent

	// Find insertion position
	insertPos := len(parent.keys)
	for i, k := range parent.keys {
		if key < k {
			insertPos = i
			break
		}
	}

	// Insert key
	parent.keys = append(parent.keys[:insertPos], append([]int64{key}, parent.keys[insertPos:]...)...)

	// Insert child
	parent.children = append(parent.children[:insertPos+1], append([]*BPTreeNode{newChild}, parent.children[insertPos+1:]...)...)

	// Check if split is needed
	if len(parent.keys) > BPTreeMaxKeys {
		return tree.splitInternal(parent)
	}

	return nil
}

// splitInternal splits an internal node
func (tree *BPlusTree) splitInternal(node *BPTreeNode) error {
	// Create new internal node
	newNode := NewInternalNode()

	// Split point
	mid := len(node.keys) / 2

	// Move half the keys and children to new node
	keyToInsert := node.keys[mid]
	newNode.keys = append(newNode.keys, node.keys[mid+1:]...)
	newNode.children = append(newNode.children, node.children[mid+1:]...)

	// Update parent pointers
	for _, child := range newNode.children {
		child.parent = newNode
	}

	// Truncate original node
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]

	// If node is root, create new root
	if node.parent == nil {
		newRoot := NewInternalNode()
		newRoot.keys = append(newRoot.keys, keyToInsert)
		newRoot.children = append(newRoot.children, node, newNode)
		node.parent = newRoot
		newNode.parent = newRoot
		tree.root = newRoot
		return nil
	}

	// Insert into parent
	return tree.insertIntoParent(node.parent, keyToInsert, newNode)
}

// Delete removes a key from the B+ Tree
func (tree *BPlusTree) Delete(key int64) error {
	if tree.root == nil {
		return fmt.Errorf("tree is empty")
	}

	// Find the leaf node
	leaf := tree.findLeaf(key)
	if leaf == nil {
		return fmt.Errorf("key %d not found", key)
	}

	// Find and remove the key
	found := false
	for i, k := range leaf.keys {
		if k == key {
			// Remove key and value
			leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
			leaf.values = append(leaf.values[:i], leaf.values[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("key %d not found", key)
	}

	return nil
}

// BPTreeIterator represents an iterator for B+ Tree
type BPTreeIterator struct {
	currentNode *BPTreeNode
	currentPos  int
}

// Iterator creates a new iterator for the B+ Tree
func (tree *BPlusTree) Iterator() (*BPTreeIterator, error) {
	if tree.root == nil {
		return nil, fmt.Errorf("tree is empty")
	}

	// Find the leftmost leaf
	node := tree.root
	for node.nodeType == InternalNode {
		node = node.children[0]
	}

	return &BPTreeIterator{
		currentNode: node,
		currentPos:  0,
	}, nil
}

// HasNext returns true if there are more elements
func (iter *BPTreeIterator) HasNext() bool {
	if iter.currentNode == nil {
		return false
	}
	if iter.currentPos < len(iter.currentNode.keys) {
		return true
	}
	return iter.currentNode.next != nil
}

// Next returns the next key-value pair
func (iter *BPTreeIterator) Next() (int64, int64, error) {
	if iter.currentNode == nil {
		return 0, 0, fmt.Errorf("no more elements")
	}

	// If we've exhausted current node, move to next
	if iter.currentPos >= len(iter.currentNode.keys) {
		iter.currentNode = iter.currentNode.next
		iter.currentPos = 0
		if iter.currentNode == nil {
			return 0, 0, fmt.Errorf("no more elements")
		}
	}

	// Check if current node has keys (handle empty pages after deletions)
	if len(iter.currentNode.keys) == 0 {
		// Skip empty node and move to next
		iter.currentNode = iter.currentNode.next
		iter.currentPos = 0
		if iter.currentNode == nil {
			return 0, 0, fmt.Errorf("no more elements")
		}
		// Recursively call Next to get value from next non-empty node
		return iter.Next()
	}

	key := iter.currentNode.keys[iter.currentPos]
	value := iter.currentNode.values[iter.currentPos]
	iter.currentPos++

	return key, value, nil
}

// BulkLoad efficiently builds a B+ Tree from sorted key-value pairs.
// Much faster than inserting keys one by one.
// Requirements: entries must be sorted by key in ascending order.
func (tree *BPlusTree) BulkLoad(entries []BPTreeEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Verify entries are sorted
	for i := 1; i < len(entries); i++ {
		if entries[i].Key <= entries[i-1].Key {
			return fmt.Errorf("entries must be sorted and unique, found %d <= %d at index %d",
				entries[i].Key, entries[i-1].Key, i)
		}
	}

	tree.rootMu.Lock()
	defer tree.rootMu.Unlock()

	// Build leaf level first (bottom-up approach)
	leafNodes := tree.buildLeafLevel(entries)
	if len(leafNodes) == 0 {
		return fmt.Errorf("failed to build leaf level")
	}

	// If only one leaf node, it becomes the root
	if len(leafNodes) == 1 {
		tree.root = leafNodes[0]
		return nil
	}

	// Build internal levels until we have a single root
	tree.root = tree.buildInternalLevels(leafNodes)
	return nil
}

// buildLeafLevel creates the leaf level from sorted entries
func (tree *BPlusTree) buildLeafLevel(entries []BPTreeEntry) []*BPTreeNode {
	leafNodes := make([]*BPTreeNode, 0)

	for i := 0; i < len(entries); {
		leaf := NewLeafNode()

		// Fill leaf node to capacity (or remaining entries)
		for j := 0; j < BPTreeMaxLeafKeys && i < len(entries); j++ {
			leaf.keys = append(leaf.keys, entries[i].Key)
			leaf.values = append(leaf.values, entries[i].Value)
			i++
		}

		// Link to previous leaf node if exists
		if len(leafNodes) > 0 {
			leafNodes[len(leafNodes)-1].next = leaf
		}

		leafNodes = append(leafNodes, leaf)
	}

	return leafNodes
}

// buildInternalLevels builds internal nodes from child nodes
func (tree *BPlusTree) buildInternalLevels(childNodes []*BPTreeNode) *BPTreeNode {
	currentLevel := childNodes

	// Keep building levels until we have a single root
	for len(currentLevel) > 1 {
		nextLevel := make([]*BPTreeNode, 0)

		for i := 0; i < len(currentLevel); {
			internal := NewInternalNode()

			// First child doesn't need a separator key
			internal.children = append(internal.children, currentLevel[i])
			currentLevel[i].parent = internal
			i++

			// Add remaining children with separator keys
			for j := 1; j < BPTreeOrder && i < len(currentLevel); j++ {
				// Use the smallest key in the child subtree as separator
				separatorKey := tree.getSmallestKey(currentLevel[i])
				internal.keys = append(internal.keys, separatorKey)
				internal.children = append(internal.children, currentLevel[i])
				currentLevel[i].parent = internal
				i++
			}

			nextLevel = append(nextLevel, internal)
		}

		currentLevel = nextLevel
	}

	// The last remaining node is the root
	return currentLevel[0]
}

// getSmallestKey returns the smallest key in a subtree
func (tree *BPlusTree) getSmallestKey(node *BPTreeNode) int64 {
	// Traverse to the leftmost leaf
	current := node
	for current.nodeType == InternalNode {
		current = current.children[0]
	}
	// Return the first key in the leaf
	return current.keys[0]
}
