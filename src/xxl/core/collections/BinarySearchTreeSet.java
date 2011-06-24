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

package xxl.core.collections;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;

import xxl.core.binarySearchTrees.BinarySearchTree;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.predicates.Predicates;

/**
 * This class provides an implementation of the Set interface that
 * internally uses a binary search tree to store its elements. <p>
 *
 * The performance of the set depends on the performance of the internally
 * used binary search tree (e.g., an avl tree guaratees that insertion,
 * removal and searching requires logarithmic time, but binary search
 * trees can be degenerated).<p>
 *
 * The iterators returned by this class' <tt>iterator</tt> method are
 * <i>fail-fast</i>: if the set is structurally modified at any time after
 * the iterator is created, in any way except through the iterator's own
 * <tt>remove</tt> method, the iterator will throw a
 * ConcurrentModificationException. Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than
 * risking arbitrary, non-deterministic behavior at an undetermined time
 * in the future.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a binary search tree set that is using a binary search tree and the natural
 *     // ordering of the elements
 *
 *     BinarySearchTreeSet set = new BinarySearchTreeSet();
 *
 *     // create a new iterator with 100 random number lower than 1000
 *
 *     Iterator iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(1000), 100);
 *
 *     // insert all elements of the given iterator
 *
 *     while (iterator.hasNext())
 *         set.insert(iterator.next());
 *
 *     // create an iteration of the elements of the set
 *
 *     iterator = set.iterator();
 *
 *     // print all elements of the iteration (set)
 *
 *     while (iterator.hasNext())
 *         System.out.println(iterator.next());
 * </pre>
 *
 * @see AbstractSet
 * @see BinarySearchTree
 * @see ComparableComparator
 * @see Comparator
 * @see Function
 * @see Iterator
 * @see Mapper
 */
public class BinarySearchTreeSet extends AbstractSet {

	/**
	 * The binary search tree is internally used to store the elements of
	 * the set.
	 */
	protected BinarySearchTree tree;

	/**
	 * The comparator to determine the order of the set (and the
	 * internally used binary search tree). More exactly, there can be
	 * three different cases when two elements
	 * <tt>o1</tt> and <tt>o2</tt> are inserted into the set
	 * <ul>
	 * <dl>
	 * <dt><li><tt>comparator.compare(o1, o2) < 0</tt> :</dt>
	 * <dd>the set returns <tt>o1</tt> prior to returning <tt>o2</tt>.</dd>
	 * <dt><li><tt>comparator.compare(o1, o2) == 0</tt> :</dt>
	 * <dd>when inserting equal elements (determined by the used
	 * comparator), there is no guarantee which one will be returned
	 * first.
	 * <dt><li><tt>comparator.compare(o1, o2) > 0</tt> :</dt>
	 * <dd>the set returns <tt>o2</tt> prior to returning <tt>o1</tt>.</dd>
	 * </dl>
	 * </ul>
	 */
	protected Comparator comparator;

	/**
	 * The comparator to determine the subtree of a node of a binary
	 * search tree that would be used to insert a given object. The
	 * compare method of the comparator is called with an array
	 * (<i>parameter list</i>) of objects (to insert) and a node of a
	 * binary search tree.
	 */
	protected Comparator chooseSubtree = new Comparator () {
		public int compare (Object object0, Object object1) {
			return comparator.compare(((Object[]) object0)[0], ((BinarySearchTree.Node)object1).object());
		}
	};

	/**
	 * Constructs a new binary search tree set that initializes the
	 * internally used binary search tree with a new tree and uses the
	 * specified comparator to order elements when inserted. The specified
	 * function is used to create a new binary search tree (it works
	 * like the {@link BinarySearchTree#FACTORY_METHOD FACTORY_METHOD} of
	 * BinarySearchTree).
	 *
	 * @param comparator the comparator to determine the order of the
	 *        set.
	 * @param newBinarySearchTree a function to create a new binary search
	 *        tree.
	 */
	public BinarySearchTreeSet (Comparator comparator, Function newBinarySearchTree) {
		(this.tree = 
			(BinarySearchTree) newBinarySearchTree.invoke(
				Predicates.TRUE, 
				Identity.DEFAULT_INSTANCE)
		).clear();
		this.comparator = comparator;
	}

	/**
	 * Constructs a new binary search tree set that uses the specified
	 * comparator to order elements when inserted. The internally used
	 * binary search tree is initialized by the FACTORY_METHOD of
	 * BinarySearchTree). This constructor is equivalent to the call of
	 * <code>BinarySearchTreeSet(comparator, BinarySearchTree.FACTORY_METHOD)</code>.
	 *
	 * @param comparator the comparator to determine the order of the
	 *        set.
	 * @see BinarySearchTree#FACTORY_METHOD
	 */
	public BinarySearchTreeSet (Comparator comparator) {
		this(comparator, BinarySearchTree.FACTORY_METHOD);
	}

	/**
	 * Constructs a new binary search tree set that initializes the
	 * internally used binary search tree with a new tree and uses the
	 * <i>natural ordering</i> of its element to order them. The specified
	 * function is used to create a new binary search tree (it works
	 * like the {@link BinarySearchTree#FACTORY_METHOD FACTORY_METHOD} of
	 * BinarySearchTree). This constructor is equivalent to the call of
	 * <code>BinarySearchTreeSet(ComparableComparator.DEFAULT_INSTANCE, newBinarySearchTree)</code>.
	 *
	 * @param newBinarySearchTree a function to create a new binary search
	 *        tree.
	 */
	public BinarySearchTreeSet (Function newBinarySearchTree) {
		this(new ComparableComparator(), newBinarySearchTree);
	}

	/**
	 * Constructs a new binary search tree set that uses the <i>natural
	 * ordering</i> of its elements to order them. This constructor is
	 * equivalent to the call of
	 * <code>BinarySearchTreeSet(ComparableComparator.DEFAULT_INSTANCE, BinarySearchTree.FACTORY_METHOD)</code>.
	 *
	 * @see BinarySearchTree#FACTORY_METHOD
	 */
	public BinarySearchTreeSet () {
		this(new ComparableComparator());
	}

	/**
	 * Removes all elements from this set. This set will be empty
	 * after this call returns (unless it throws an exception).
	 */

	public void clear () {
		tree.clear();
	}

	/**
	 * Returns the number of elements in this set (its cardinality). If
	 * this set contains more than <tt>Integer.MAX_VALUE</tt> elements,
	 * <tt>Integer.MAX_VALUE</tt> is  returned.
	 *
	 * @return the number of elements in this set (its cardinality).
	 */
	public int size () {
		return tree.size();
	}

	/**
	 * Returns an iterator over the elements in this set. The elements are
	 * returned in the order determined by the comparator.
	 *
	 * @return an iterator over the elements in this set.
	 */
	public Iterator iterator () {
		return new Mapper(
			new AbstractFunction () {
				public Object invoke (Object node) {
					return ((BinarySearchTree.Node)node).object();
				}
			},
			tree.iterator()
		);
	}

	/**
	 * Returns true if this set contains the specified element. More
	 * formally, returns true if and only if this set contains an element
	 * <tt>e</tt> such that
	 * (<tt>object==null ? e==null : object.equals(e)</tt>).
	 *
	 * @param object the element whose presence in this set is to be tested.
	 * @return true if this set contains the specified element.
	 */
	public boolean contains (Object object) {
		int[] result = new int[1];
		return tree.get(chooseSubtree, new Object [] {object}, result)!=null && result[0]==0;
	}

	/**
	 * Adds the specified element to this set if it is not already
	 * present. More formally, adds the specified element to this set if
	 * this set contains no element <tt>e</tt> such that
	 * (<tt>o==null ? e==null : o.equals(e)</tt>). If this set already
	 * contains the specified element, the call leaves this set unchanged.
	 *
	 * @param object the element to be added to this set.
	 * @return the element that is added to this set.
	 */
	public Object insert (Object object) {
		BinarySearchTree.Node node = tree.insert(chooseSubtree, new Object [] {object});
		return node==null? null: node.object();
	}

	/**
	 * Removes the specified element from this set if it is present. More
	 * formally, removes an element <tt>e</tt> such that
	 * (<tt>object==null ? e==null : object.equals(e)</tt>), if the set
	 * contains such an element. Returns true if the set contained the
	 * specified element.
	 *
	 * @param object the object to be removed from this set, if present.
	 * @return true if the set contained the specified element.
	 * @throws UnsupportedOperationException if the <tt>remove</tt> method
	 *         is not supported by this set.
	 */
	public boolean remove (Object object) throws UnsupportedOperationException {
		BinarySearchTree.Node node = tree.remove(chooseSubtree, new Object [] {object}, size()%2);
		return node!=null;
	}
}
