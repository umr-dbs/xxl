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
 *	A binary tree where the absolute difference between the height of the subtrees of a Node is 
 *	limited by 1.
 *	The AVLTree provides insertion and exact-match search in O(log(n)) time.
 *	<br><br>
 *	For a detailed discussion see "Introduction to Algorithms", MIT Electrical Engineering 
 *	and Computer Science, by Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest.
 */
public class AVLTree extends BinarySearchTree {
	
	/**
	 * Returns a Factory-Method Function (Function x Function &rarr; BinarySearchTree) that
	 * constructs new AVLTrees.
	 */
	public static final Function FACTORY_METHOD = new AbstractFunction() {
		public Object invoke (Object fixRotation, Object fixAggregate) {
			return new AVLTree((Predicate)fixRotation, (Function)fixAggregate);
		}
	};

	/**	
	 *	Nodes in a AVLTree are of the type AVLTree.Node.
	 */
	protected class Node extends BinarySearchTree.Node {
		
		/** 
		 * The height of the Node
		 */
		protected int height = 1;

		/** 
		 * Creates a new Node.
		 * 
		 * @param object The object to store in the node.
		 * @param parent The parent of the new node.
		 */
		protected Node (Object object, BinarySearchTree.Node parent) {
			super(object, parent);
		}

		/* (non-Javadoc)
		 * @see xxl.core.binarySearchTrees.BinarySearchTree.Node#rotate()
		 */
		protected BinarySearchTree.Node rotate () {
			int index = index();

			super.rotate();
			refreshHeight(children[index^1]);
			refreshHeight(this);
			return this;
		}
		
		/* (non-Javadoc)
		 * @see xxl.core.binarySearchTrees.BinarySearchTree.Node#fix(int)
		 */
		protected void fix (int index) {
			for (BinarySearchTree.Node node = this; node!=null && getHeight(node)!=refreshHeight(node);) {
				int balance = getHeight(node.children[1])-getHeight(node.children[0]);

				if (Math.abs(balance)>maxBalance) {
					if (getHeight((node = node.children[index = balance>0? 1: 0]).children[index^1]) > getHeight(node.children[index]))
						node = node.children[index^1].rotate();
					node.rotate();
				}
				node = node.parent;
			}
		}
	}

	/** 
	 * The maximum allowed balance, i.e. the maximum allowed difference in height
	 * between the children of a node.
	 */
	protected int maxBalance;

	/**
	 * Gives the height of a node.
	 * 
	 * @param node Node of the tree.
	 * @return The height of the given node.
	 */
	protected int getHeight (BinarySearchTree.Node node) {
		return node==null? 0: ((Node)node).height;
	}

	/**
	 * Refreshes the height information of the given node.
	 * 
	 * @param node Node of the tree.
	 * @return The new height of the node after the refresh.
	 */
	protected int refreshHeight (BinarySearchTree.Node node) {
		return ((Node)node).height = 1+Math.max(getHeight(node.children[0]), getHeight(node.children[1]));
	}

	/**
	 * Creates an AVLTree. 
	 * 
	 * @param fixAggregate Predicate that is called when an aggregate information in 
	 * 	the tree might be outdated.
	 * @param fixRotation Function that is called when a rotation has occured and rotation 
	 *	information in a node has to become fixed.
	 * @param maxBalance The maximum allowed balance, i.e. the maximum allowed 
	 * difference in height between the children of a node. 
	 */
	public AVLTree (Predicate fixAggregate, Function fixRotation, int maxBalance) {
		super(fixAggregate, fixRotation);
		this.maxBalance = maxBalance;
	}

	/**
	 * Creates a AVLTree with maximum balance 1.
	 * This constructor is equivalent to the call of
	 * <code>AVLTree(fixAggregate,fixRotation,1)</code>.
	 * 
	 * @param fixAggregate Predicate that is called when an aggregate information in 
	 * 	the tree might be outdated.
	 * @param fixRotation Function that is called when a rotation has occured and rotation 
	 *	information in a node has to become fixed.
	 */
	public AVLTree (Predicate fixAggregate, Function fixRotation) {
		this(fixAggregate, fixRotation, 1);
	}

	/* (non-Javadoc)
	 * @see xxl.core.binarySearchTrees.BinarySearchTree#newNode(java.lang.Object, xxl.core.binarySearchTrees.BinarySearchTree.Node)
	 */
	public BinarySearchTree.Node newNode (Object object, BinarySearchTree.Node parent) {
		return new Node(object, parent);
	}
}
