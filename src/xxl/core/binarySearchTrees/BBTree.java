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
 *	A binary tree where the balance of every subtree is bounded.
 *	The BBTree provides insertion and exactmatch search in O(log(n)) time.
 *	<br><br>
 *	For a detailed discussion see "Introduction to Algorithms", MIT Electrical Engineering 
 *	and Computer Science, by Thomas H. Cormen, Charles E. Leiserson, Ronald L. Rivest.
 */
public class BBTree extends BinarySearchTree {
	
	/**
	 * Returns a Factory-Method Function (Function x Function &rarr; BinarySearchTree) that
	 * constructs new BBTrees.
	 */
	public static final Function FACTORY_METHOD = new AbstractFunction() {
		public Object invoke (Object fixRotation, Object fixAggregate) {
			return new BBTree((Predicate)fixRotation, (Function)fixAggregate);
		}
	};

	/**	
	 *	Nodes in a BBTree are of the type BBTree.Node.
	 */
	protected class Node extends BinarySearchTree.Node {
		
		/**
		 * The weight of the node.
		 */
		protected int weight = 2;

		/** 
		 * Creates a new Node.
		 * 
		 * @param object The object to store in the node.
		 * @param parent The parent of the new node.
		 */
		Node (Object object, BinarySearchTree.Node parent) {
			super(object, parent);
		}

		/* (non-Javadoc)
		 * @see xxl.core.binarySearchTrees.BinarySearchTree.Node#rotate()
		 */
		protected BinarySearchTree.Node rotate () {
			int index = index();
			Node parent = (Node)this.parent;

			super.rotate();
			parent.weight -= getWeight(children[index]);
			weight += getWeight(parent.children[index^1]);
			return this;
		}

		/* (non-Javadoc)
		 * @see xxl.core.binarySearchTrees.BinarySearchTree.Node#fix(int)
		 */
		protected void fix (int index) {
			for (BinarySearchTree.Node node = this; node!=null; node = node.parent) {
				index = getWeight(node.children[0])<getWeight(node.children[1])? 0: 1;
				if (getWeight(node.children[index])<alpha*refreshWeight(node)) {
					node = node.children[index^1];
					if (getWeight(node.children[index])>d*getWeight(node))
						node = node.children[index].rotate();
					node.rotate();
				}
			}
		}
	}

	/**
	 * Refreshs the weight information of the given node.
	 * 
	 * @param node Node of the tree.
	 * @return The new weight of the node after the refresh.
	 */
	protected int refreshWeight (BinarySearchTree.Node node) {
		return ((Node)node).weight = getWeight(node.children[0])+getWeight(node.children[1]);
	}

	/**
	 * Gives the weight of a node.
	 * 
	 * @param node Node of the tree.
	 * @return The weight of the given node.
	 */
	protected int getWeight (BinarySearchTree.Node node) {
		return node==null? 1: ((Node)node).weight;
	}

	/**
	 * Parameter alpha
	 */
	protected final double alpha;
	/**
	 * Parameter d
	 */
	protected final double d;

	/**
	 * Creates a BBTree.
	 * 
	 * @param fixAggregate Predicate that is called when an aggregate information in 
	 * 	the tree might be outdated.
	 * @param fixRotation Function that is called when a rotation has occured and rotation 
	 *	information in a node has to become fixed.
	 * @param alpha Parameter alpha.
	 * @param d Parameter d.
	 */
	public BBTree (Predicate fixAggregate, Function fixRotation, double alpha, double d) {
		super(fixAggregate, fixRotation);
		this.alpha = alpha;
		this.d = d;
	}
	
	/**
	 * Creates a BBTree with default parameters. 
	 * This constructor is equivalent to the call of
	 * <code>BBTree(fixAggregate,fixRotation,3.0/11.0, 6.0/10.0)</code>.
	 * 
	 * @param fixAggregate Predicate that is called when an aggregate information in 
	 * 	the tree might be outdated.
	 * @param fixRotation Function that is called when a rotation has occured and rotation 
	 *	information in a node has to become fixed.
	 *
	 */
	public BBTree (Predicate fixAggregate, Function fixRotation) {
		this(fixAggregate, fixRotation, 3.0/11.0, 6.0/10.0);
	}

	/* (non-Javadoc)
	 * @see xxl.core.binarySearchTrees.BinarySearchTree#newNode(java.lang.Object, xxl.core.binarySearchTrees.BinarySearchTree.Node)
	 */
	public BinarySearchTree.Node newNode (Object object, BinarySearchTree.Node parent) {
		return new Node(object, parent);
	}
}
