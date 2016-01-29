/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
                        Head of the Database Research Group
                        Department of Mathematics and Computer Science
                        University of Marburg
                        Germany

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library;  If not, see <http://www.gnu.org/licenses/>. 

    http://code.google.com/p/xxl/

*/

package xxl.core.binarySearchTrees;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.filters.WhileTaker;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
	BinarySearchTree is a generic implementation of the functionality of
	binary search trees. The implementation uses some ideas that are 
	described in: "Introduction to Algorithms", MIT Electrical Engineering 
	and Computer Science, by Thomas H. Cormen, Charles E. Leiserson, 
	Ronald L. Rivest.
	<br><br>
	Trees constructed using this class are not implicitly balanced. For 
	balanced trees consider the classes AVLTree, BBTree and RedBlackTree.
	@see AVLTree
	@see BBTree
	@see RedBlackTree
*/
public class BinarySearchTree {
	
	/**
	 * Returns a Factory-Method Function (Predicate x Function &rarr; BinarySearchTree) that
	 * constructs new BinarySearchTrees.
	 */
	public static final Function FACTORY_METHOD = new AbstractFunction() {
		public Object invoke (Object fixAggregate, Object fixRotation) {
			return new BinarySearchTree((Predicate)fixAggregate,(Function)fixRotation);
		}
	};

	/**	
	 *	Nodes in a BinarySearchTree are of the type Node.
	 */
	public class Node {

		/**
		 * Object that is contained in the node.
		 */
		protected Object object;
		/**
		 * Parent node of the current node.
		 */
		protected Node parent;
		/**
		 * Nodearray that contains child nodes.
		 */
		protected Node [] children = new Node [] {null, null};

		/**
		 * Creates a new node.
		 * @param object The object that is contained in the node.
		 * @param parent The parent node of the node that becomes created.
		 */
		protected Node (Object object, Node parent) {
			this.object = object;
			this.parent = parent;
		}

		/** 
		 * Returns the object of the node.
		 *
		 * @return Object - object of the node.
		 */
		public Object object () {
			return object;
		}

		/** 
		 * Returns the parent node of the current node.
		 * @return Object - parent node.
		 */
		public Node parent () {
			return parent;
		}

		/** Returns the left or right child.
		 * @param right <code>true</code> for right child, <code>false</code> for left child 
		 * @return Node - returns the right(left) child if true(false)
		 */
		public Node child (boolean right) {
			return children[right? 1: 0];
		}

		/** 
		 * Tests if the node is a leaf.
		 * @return boolean - true if the current node is a leaf
		 */
		public boolean isLeaf () {
			return children[0]==children[1];
		}

		/** 
		 * Returns 0(1) if this node is the left(right) son of its
		 * parent. If the node is the root-node -1 is returned.
		 * @return int - returns the index of the current node from the
		 * 	parent point of view.
		 */
		public int index () {
			return parent==null? -1: parent.index(this);
		}

		/** 
		 * Returns the index of 'child'. If 'child' is not a child
		 * of this node -1 is returned.
		 * @param child the child whose index is searched
		 * @return int - index of a child.
		 */
		public int index (Node child) {
			return child==this.children[1]? 1: child==this.children[0]? 0: -1;
		}

		/** 
		 * Returns the symmetric predecessor (index=0) or
		 * or successor (index=1) for this node. 
		 * @param index 0 for symmetric predecessor or 1 for symmetric successor 
		 * @return Node - returns the symmetric predecessor or successor.
		*/
		public Node next (int index) {
			Node node = this;

			if (children[index]==null) {
				while (node.index()==index)
					node = node.parent;
				node = node.parent;
			}
			else {
				node = node.children[index];
				index ^= 1;
				while (node.children[index]!=null)
					node = node.children[index];
			}
			return node;
		}

		/** 
		 * This method is called when a structural change occures
		 * (e.g. during an insert()- or remove()-operation)
		 * in one of the subtrees. 
		 * In a BinarySearchTree-Object this method has nothing 
		 * to do. It is usually overwritten in subclasses.
		 * @param index
		 * @see AVLTree
		 * @see RedBlackTree
		 * @see BBTree
		 */
		protected void fix (int index) {
		}

		/** 
		 * This method has to be implemented in case that a
		 * rotation invalidates information of the node.
		 * This method should copy the information from this
		 * node to the node <code>next</code>.
		 * In a BinarySearchTree-Object this method has nothing 
		 * to do. It is usually overwritten in subclasses.
		 * @param next destination for the rotation information.
		 * @see AVLTree
		 * @see RedBlackTree
		 */
		protected void designate (Node next) {
		}

		/** 
		 * This method is called for the son of the rotation, i.e.
		 * after the rotation the son is the new father-node.
		 * This method calls <code>fixRotation()</code> (for
		 * the new son) and
		 * <code>fixAggregate()</code> (for father and son).
		 * It is assumed, that the aggregates of the parents of the 
		 * new root of the subtree are not affected.
		 * @return Node - this
		 */
		protected Node rotate () {
			int index = index();

			if ((parent.children[index] = children[index^1])!=null)
				children[index^1].parent = parent;
			parent.parent = (parent = (children[index^1] = parent).parent)==null?
				(root = this):
				(parent.children[parent.index(children[index^1])] = this);
			fixRotation.invoke(children[index^1]);
			fixAggregate.invoke(children[index^1]);
			fixAggregate.invoke(this);
			return this;
		}

		/**
		 * Exactmatch query. Locates an object in the tree in the current node or below.
		 *
	 	 * @param chooseSubtree Comparator that is used to find the requested object.
	 	 * @param object Object[] that contains the object.
	 	 * @param result int[] that contains a return value (see "Returns").
		 * @return Node that contains the object or the node
		 * 	where the search had to be terminated
		 * 	(result[0] is set to 0 if the object was found, &lt;0 if object is left of that
		 * 	node, &gt;0 if greater than the node)
		 */
		public Node get (Comparator chooseSubtree, Object [] object, int [] result) {
			Node node = this, child = node;

			while (child!=null && (result[0] = chooseSubtree.compare(object, node = child))!=0)
				child = node.children[result[0]>0? 1: 0];
			return node;
		}

		/**
		 * Inserts an object into the tree. This method locates the correct
		 * position for the object in the tree and inserts the given object.
		 *
		 * @param chooseSubtree Comparator that is used to locate the place where the object
		 * 	becomes inserted.
		 * @param object Object[] that contains the object.
	 	 * @return null if the tree did not contain the object. 
		 * Otherwise the node that already contains the object (if it is already in the tree)
		 * is returned.
		 *	In the latter case there are no changes in the tree.
		 */
		public Node insert (Comparator chooseSubtree, Object [] object) {
			int [] result = new int [1];
			Node node = get(chooseSubtree, object, result);

			if (result[0]==0)
				return node;
			else {
				node.insert(result[0]>0? 1: 0, object[0]);
				return null;
			}
		}

		/**	
		 * Appends an object to one of the children-nodes.
	 	 * @param index Position where to insert the new node into the children array. 
	 	 * @param object contains the object.		 
		 */
		public void insert (int index, Object object) {
			children[index] = newNode(object, this);
			size++;
			for (Node node = this; node!=null && !fixAggregate.invoke(node);)
				node = node.parent;
			fix(index);
		}

		/**
		 * Locates the object and removes it from the tree.
		 * @param chooseSubtree Comparator that is used to locate the place where the object
		 * 	is located.
		 * @param object Object[] that contains the object.
		 * @param nextIndex index of the object to be removed
		 * @return null if the tree did not contain the object. 
		 * Otherwise the node that has contained the object before the remove-operation
		 * is returned.
		 */
		public Node remove (Comparator chooseSubtree, Object [] object, int nextIndex) {
			int [] result = new int [1];
			Node node = get(chooseSubtree, object, result);

			if (result[0]!=0)
				return null;
			else {
				node.remove(nextIndex);
				return node;
			}
		}

		/**
		 * Removes the child node with number nextIndex from the current node.
		 * This operation is implemented a little bit different compared to the remove
		 * algorithm that is proposed in many books. It looks for the successor-node
		 * and rotates him below the node ...
		 * This method is more generic, because it works with every type of tree that
		 * is subclassed from BinarySearchTree.
		 * @param nextIndex
		 */
		public void remove (int nextIndex) {
			if (isLeaf())
				if (parent==null)
					clear();
				else {
					int index = index();

					parent.children[index] = null;
					size--;
					for (Node node = parent; node!=null && !fixAggregate.invoke(node);)
						node = node.parent;
					parent.fix(index);
				}
			else {
				Node next = next(nextIndex ^= (children[nextIndex]==null? 1: 0));
				int fixIndex = next.index();
				Node toFix = fixIndex==(nextIndex ^= 1)? next.parent: next;

				designate(next);
				while (next.children[nextIndex]!=this)
					next.rotate();
				while (children[nextIndex]!=null)
					children[nextIndex].rotate();
				parent.children[index()] = null;
				size--;
				for (Node node = parent; node!=null && !fixAggregate.invoke(node);)
					node = node.parent;
				toFix.fix(fixIndex);
			}
			parent = null;
		}
	}

	/**
	 * Root of the tree.
	 */
	protected Node root = null;
	/**
	 * Size of the tree (number of nodes).
	 */
	protected int size = 0;

	/** 
	 * Updates aggregate-information of a node, i.e. the invariant for
	 * that node is recomputed. This predicate is called with 1 argument: the node.
	 * Checks if the aggregate-operation is complete or if
	 * the parent has to be updated (only during insertion).
	 * The predicate has to return true, iff the aggregate is fixed
	 * and need not to be (re)calculated for the parent node.
	 */
	protected Predicate fixAggregate;

	/**  
	 * Updates rotation-information of a node.
	 * This function is called with 1 argument: the node
	 */
	protected Function fixRotation;

	/**
	 * Creates a BinarySearchTree. 
	 * 
	 * @param fixAggregate Predicate that is called when an aggregate information in 
	 * 	the tree might be outdated.
	 * @param fixRotation Function that is called when a rotation has occured and rotation 
	 *	information in a node has to become fixed.
	 */
	public BinarySearchTree (Predicate fixAggregate, Function fixRotation) {
		this.fixAggregate = fixAggregate;
		this.fixRotation = fixRotation;
	}

	/** 
	 * Creates and initializes a new node of the tree. The node is 
	 * inserted into the tree only if the parent is set correctly.
	 *
	 * @param object The object that is contained in the node.
	 * @param parent The parent node of the node that becomes created.
	 * @return Node - new node that contains the object object.
	 */
	public Node newNode (Object object, Node parent) {
		return new Node(object, parent);
	}

	/** 
	 * Deletes all elements of the tree. After this operation the tree 
	 * is empty.
	 */
	public void clear () {
		root = null;
		size = 0;
	}

	/**
	 * Initialized the tree with exactly one node that contains the specified object.
	 * Caution: if the tree is not empty, the previously contained nodes are removed.
	 * After performing this operation the number of nodes in the tree is exactly 1.
	 *
	 * @param object The object that is inserted into the only node of the tree (root).
	 */
	public void init (Object object) {
		root = newNode(object, null);
		size = 1;
	}

	/** 
	 * Returns the root of the tree.
	 *
	 * @return Node - the root of the tree.
	 */
	public Node root () {
		return root;
	}

	/** 
	 * Returns the size of the tree (number of nodes)
	 *
	 * @return int - size of the tree
	 */
	public int size () {
		return size;
	}

	/** 
	 * Returns the first node due to the tree comparator.
	 *
	 * @return Node - the first node of the tree.
	 */
	public Node first () {
		Node node = root();

		if (node!=null)
			while (node.children[0]!=null)
				node = node.children[0];
		return node;
	}

	/** 
	 * Returns the last node due to the tree comparator.
	 *
	 * @return Node - the last node of the tree.
	 */
	public Node last () {
		Node node = root();

		if (node!=null)
			while (node.children[1]!=null)
				node = node.children[1];
		return node;
	}

	/** 
	 * Exactmatch query. Locates an object in the tree.
	 *
	 * @param chooseSubtree Comparator that is used to find the requested object.
	 * @param object Object[] that contains the object.
	 * @param result int[] that contains a return value (see "Returns").
	 * @return Node that contains the object or the node where the search had to 
	 * be terminated (result[0] is set to 0 if the object was found, &lt;0 if object 
	 * is left of that node, &gt;0 if greater than the node)
	 */
	public Node get (Comparator chooseSubtree, Object [] object, int [] result) {
		return root()==null? null: root().get(chooseSubtree, object, result);
	}

	/**
	 * Inserts an object into the tree.
	 *
	 * @param chooseSubtree Comparator that is used to locate the place where the object
	 * 	becomes inserted.
	 * @param object Object[] that contains the object.
	 * @return null if the tree did not contain the object, 
	 *	otherwise the node that already contains the object (if it is already in the tree).
	 * 	In the latter case there are no changes in the tree.
	 */
	public Node insert (Comparator chooseSubtree, Object [] object) {
		if (root()==null) {
			init(object[0]);
			return null;
		}
		else
			return root().insert(chooseSubtree, object);
	}

	/** 
	 * Search the object and remove it from the tree.
	 *
	 * @param chooseSubtree Comparator that is used to find the object.
	 * @param object Object[] that contains the object.
	 * @param nextIndex 
	 * @return null if the tree did not contain the object, otherwise the node 
	 * 	that has contained the object before the remove-Operation.
	 */
	public Node remove (Comparator chooseSubtree, Object [] object, int nextIndex) {
		return root()==null? null: root().remove(chooseSubtree, object, nextIndex);
	}

	/**
	 * Returns an inorder forward (left to right) iterator of the objects of this tree.
	 * @return Iterator - inorder forward iterator.
	 */
	public Iterator iterator () {
		return iterator(true);
	}

	/** 
	 * Returns an inorder iterator of the objects of this tree.
	 * @param forwards <code>true</code>: forward (left to right), otherwise
	 * backwards (right to left).
	 * @return Iterator - specified inorder iterator.
	 */
	public Iterator iterator (boolean forwards) {
		return new InOrderIterator(forwards? first(): last(), forwards);
	}

	/** 
	 * Returns a level order iterator of the objects of this tree.
	 * @return Iterator - level order iterator.
	 */
	public Iterator levelOrderIterator () {
		return new LevelOrderIterator(root);
	}
	
	/**
	 * Returns an inorder iterator that contains the objects
	 * that lie between minkey and maxkey (range query).
	 *
	 * @param chooseSubtree The comparator used to choose the right subtree
	 * @param minkey defines the left border
	 * @param maxkey defines the right border
	 * @param forwards specifies the order the elements are returned
	 * @return inorder iterator that contains the objects that lie between minkey and maxkey (range query)
	 */
	public Iterator rangeQuery(final Comparator chooseSubtree, final Object [] minkey, final Object [] maxkey, final boolean forwards) {
		int [] result = new int [1];		
		Node n = get(chooseSubtree,forwards?minkey:maxkey,result);
		
		// object not found
		if (forwards && result[0]>0) 
			n = n.next(1);
		else if (!forwards && result[0]<0) 
			n = n.next(0);
		
		return new WhileTaker(
			new InOrderIterator(n,forwards),
			new AbstractPredicate() {
				public boolean invoke (Object next) {
					return ((forwards?1:-1)*chooseSubtree.compare(forwards?maxkey:minkey,next)>=0);
				}
			},
			true
		);
	}
	
	/** 
	 * Inner class implementing inorder traversal through the tree.
	 * The traversal can start at any node of the tree that has to
	 * be passed to the constructor.
	 */
	public class InOrderIterator implements Iterator {
		
		/** 
		 * needed for remove operation.
		 */
		protected Node node = null;
		/** 
		 * next element of the iteration.
		 */
		protected Node next;
		/** 
		 * needed for remove operation.
		 */
		protected int nextIndex;

		/** 
		 * Constructs an InOrderIterator that starts at the
		 * given node.
		 * @param first the node where the InOrderIterator starts
		 *	its iteration.
		 * @param forwards specifies the order the elements are returned.
		 */
		public InOrderIterator (Node first, boolean forwards) {
			this.next = first;
			this.nextIndex = forwards? 1: 0;
		}

		/**
		 * Returns <tt>true</tt> if the iteration has more elements.
		 * (In other words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
		 * return an element rather than throwing an exception.).
		 * Implements java.util.Iterator.hasNext().
		 *
		 * @return <tt>true</tt> if the traversal has more elements.
		 */
		public boolean hasNext () {
			return next!=null;
		}

		/**
		 * Returns the next element in the iteration. <br>
		 * The tree is not changed.
		 *
	 	 * @return the next element in the iteration.
		 * @throws java.util.NoSuchElementException if the iteration has no more elements.
		 */
		public Object next () throws NoSuchElementException {
			if (!hasNext())
				throw new NoSuchElementException();
			else {
				// return next and
				// calculate the node of the iteration after "next"
				next = (node = next).next(nextIndex);
				return node;
			}
		}

		/** 
		 * Removes an object from the tree. After calling this operation,
		 * the iterator gets invalid and does not return elements
		 * of the traversal.
		 * 
		 * @throws IllegalStateException
		 */
		public void remove () throws IllegalStateException {
			if (node==null)
				throw new IllegalStateException();
			node.remove(nextIndex^1);
			node = null;
		}
	}

	/** 
	 * Inner class implementing level order traversal through the tree.
	 * During a level order traversal, nodes have to be stored in 
	 * memory. This is done in a ListQueue internally. The queue can
	 * contain |leaf nodes| nodes at maximum.<br>
	 * Currently, the remove operation is not supported (throws an 
	 * UnsupportedOperationExcetion).
	 */
	public class LevelOrderIterator implements Iterator {
		/** 
		 * Needed for remove (for future implementation).
		 */
		protected Node node = null;
		/** 
		 * Next element of the iteration.
		 */
		protected Node next;
		/** 
		 * Data structure used to store nodes.
		 */
		protected Queue q;

		/** 
		 * Constructs a LevelOrderIterator that starts at the
		 * given node.
		 * @param first The node where the LevelOrderIterator starts
		 *	its iteration.
		 */
		public LevelOrderIterator (Node first) {
			this.next = first;
			q = new ListQueue();
		}

		/**
		 * Returns <tt>true</tt> if the iteration has more elements.
		 * (In other words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
		 * return an element rather than throwing an exception.).
		 * Implements java.util.Iterator.hasNext().
		 *
		 * @return <tt>true</tt> if the traversal has more elements.
		 */
		public boolean hasNext () {
			return next!=null;
		}

		/** 
		 * Internally used to put the child nodes into the queue.
		 * 
		 * @param cur the node whose child nodes should be put into
		 *        the queue.
		 */
		private void refillQueue(Node cur) {
			Node n;
			if ((n = cur.children[0])!=null)
				q.enqueue(n);
			if ((n = cur.children[1])!=null)
				q.enqueue(n);
		}
		
		/**
		 * Returns the next element in the iteration. <br>
		 * The tree is not changed.
		 *
	 	 * @return the next element in the iteration.
		 * @throws java.util.NoSuchElementException if the iteration has no more elements.
		 */
		public Object next () throws NoSuchElementException {
			if (!hasNext())
				throw new NoSuchElementException();
			else {
				refillQueue(next);
				node = next;
				if (!q.isEmpty())
					next = (Node) q.dequeue();
				else
					next = null;
				return node;
			}
		}

		/** 
		 * Currently not supported. Should remove an object from the tree.
		 * 
		 * @throws IllegalStateException
		 */
		public void remove () throws IllegalStateException {
			throw new UnsupportedOperationException();
		}
	}
	
}
