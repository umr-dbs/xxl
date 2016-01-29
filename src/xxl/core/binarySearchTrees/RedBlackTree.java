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

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;

/**
 *	A binary tree that is equivalent to a 2-3-tree.
 *	The RedBlackTree provides insertion and exactmatch search in O(log(n)) time.
 *	<br><br>
 *	For a detailed discussion see "Introduction to Algorithms", MIT Electrical Engineering 
 *	and Computer Science, by Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest.
 */
public class RedBlackTree extends BinarySearchTree {
	/**
	 * Returns a Factory-Method Function (Function x Function &rarr; BinarySearchTree) that
	 * constructs new RedBlackTrees.
	 */
	public static final Function FACTORY_METHOD = new AbstractFunction() {
		public Object invoke (Object fixRotation, Object fixAggregate) {
			return new RedBlackTree((Predicate)fixRotation, (Function)fixAggregate);
		}
	};
	
	/**	
	 *	Nodes in a RedBlackTree are of the type RedBlackTree.Node.
	 */
	protected class Node extends BinarySearchTree.Node {
		
		/**
		 * The level of the node.
		 */
		protected int level = 1;

		/** 
		 * Creates a new Node.
		 * 
		 * @param object The object to store in the node.
		 * @param parent The parent of the new node.
		 */
		protected Node (Object object, Node parent) {
			super(object, parent);
		}

		/* (non-Javadoc)
		 * @see xxl.core.binarySearchTrees.BinarySearchTree.Node#fix(int)
		 */
		protected void fix (int index) {
			for (Node node = this, child; node!=null && node.level-getLevel(node.children[index])!=1;) {
				if (node.level!=getLevel(node.children[index]) && --node.level!=getLevel(node.children[index ^= 1]))
						node.children[index].rotate();
				if ((child = (Node)node.children[index]).level!=getLevel(node.children[index^1])) {
					if (child.level==getLevel(child.children[index^1]))
						child = (Node)child.children[index^1].rotate();
					if (child.level==getLevel(child.children[index]))
						child = (Node)(node = child).rotate().children[index];
				}
				if (getLevel(child.children[0])!=getLevel(child.children[1]))
					node.level++; 
				index = node.index();
				node = (Node)node.parent;
			}
		}

		/* (non-Javadoc)
		 * @see xxl.core.binarySearchTrees.BinarySearchTree.Node#designate(xxl.core.binarySearchTrees.BinarySearchTree.Node)
		 */
		protected void designate (BinarySearchTree.Node next) {
			((Node)next).level = level;
		}
	}

	/**
	 * Gives the level of a node.
	 * 
	 * @param node Node of the tree.
	 * @return The level of the given node.
	 */
	protected int getLevel (BinarySearchTree.Node node) {
		return node==null? 0: ((Node)node).level;
	}

	/**
	 * Creates a RedBlackTree.
	 * 
	 * @param fixAggregate Predicate that is called when an aggregate information in 
	 * 	the tree might be outdated.
	 * @param fixRotation Function that is called when a rotation has occured and rotation 
	 *	information in a node has to become fixed.
	 */
	public RedBlackTree (Predicate fixAggregate, Function fixRotation) {
		super(fixAggregate, fixRotation);
	}

	/* (non-Javadoc)
	 * @see xxl.core.binarySearchTrees.BinarySearchTree#newNode(java.lang.Object, xxl.core.binarySearchTrees.BinarySearchTree.Node)
	 */
	public BinarySearchTree.Node newNode (Object object, BinarySearchTree.Node parent) {
		return new Node(object, (Node)parent);
	}
}
