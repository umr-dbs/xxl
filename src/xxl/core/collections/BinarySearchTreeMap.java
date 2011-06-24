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

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Set;

import xxl.core.binarySearchTrees.BinarySearchTree;
import xxl.core.comparators.ComparableComparator;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.predicates.Predicates;

/**
 * This class provides an implementation of the Map interface that
 * internally uses a binary search tree to store its key-value mappings.
 * <p>
 *
 * The performance of the set depends on the performance of the internally
 * used binary search tree (e.g., an avl tree guaratees that insertion,
 * removal and searching requires logarithmic time, but binary search
 * trees can be degenerated).<p>
 *
 * The iterators returned by the <tt>iterator</tt> method of the set
 * returned by this class' <tt>entrySet</tt> and <tt>keySet</tt> method
 * are <i>fail-fast</i>: if the set is structurally modified at any time
 * after the iterator is created, in any way except through the iterator's
 * own <tt>remove</tt> method, the iterator will throw a
 * ConcurrentModificationException. Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than
 * risking arbitrary, non-deterministic behavior at an undetermined time
 * in the future.<p>
 *
 * Example usage (1).
 * <pre>
		// create a binary search tree map that is using a binary search
		// tree and the natural ordering of the elements
		BinarySearchTreeMap map = new BinarySearchTreeMap();
		// create a new iterator with 100 random number lower than 1000
		xxl.core.cursors.Cursor cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(1000), 100);
		// insert all elements of the given iterator
		while (cursor.hasNext())
			map.put(cursor.peek(), cursor.next());
		// create an iteration of the elements of the set
		java.util.Iterator iterator = map.entrySet().iterator();
		// print all elements of the iteration (set)
		while (iterator.hasNext()) {
			MapEntry entry = (MapEntry)iterator.next();
			System.out.println("key \t= "+entry.getKey()+"\t & value \t= "+entry.getValue());
		}
		System.out.println();
		</pre>
 *
 * Example usage (2).
 * <pre>
		// create a binary search tree map that is using a avl tree and
		// the natural ordering of the elements
		map = new BinarySearchTreeMap(new AbstractFunction() {
			public Object invoke (Object f1, Object f2) {
				return AVLTree.FACTORY_METHOD.invoke(f1, f2);
			}
		});
		// create a new iterator with the numbers from 0 to 100
		cursor = new xxl.core.cursors.sources.Enumerator(101);
		// insert all elements of the given iterator
		while (cursor.hasNext())
			map.put(
				cursor.peek(),
				new Integer(100 * ((Integer)cursor.next()).intValue())
			);
		// create an iteration of the elements of the set
		iterator = map.entrySet().iterator();
		// print all elements of the iteration (set)
		while (iterator.hasNext()) {
			MapEntry entry = (MapEntry)iterator.next();
			System.out.println("key \t= "+entry.getKey()+"\t & value \t= "+entry.getValue());
		}
		System.out.println();
 </pre>
 *
 * @see AbstractMap
 * @see BinarySearchTree
 * @see BinarySearchTreeSet
 * @see ComparableComparator
 * @see Comparator
 * @see Function
 * @see MapEntry
 * @see Set
 */
public class BinarySearchTreeMap extends AbstractMap {

	/**
	 * The binary search tree is internally used to store the elements of
	 * the map.
	 */
	protected BinarySearchTree tree;

	/**
	 * The comparator to determine the order of the map (and the
	 * internally used binary search tree). More exactly, there can be
	 * three different cases when two entries <tt>e1</tt> and <tt>e2</tt>
	 * are put into the map
	 * <ul>
	 * <dl>
	 * <dt><li><tt>comparator.compare(e1.getKey(), e2.getKey()) < 0</tt> :</dt>
	 * <dd>the map returns <tt>e1</tt> prior to returning <tt>e2</tt>.</dd>
	 * <dt><li><tt>comparator.compare(e1.getKey(), e2.getKey()) == 0</tt> :</dt>
	 * <dd>the entry <tt>e1</tt> is replaced by the entry <tt>e2</tt>.</dd>
	 * <dt><li><tt>comparator.compare(e1.getKey(), e2.getKey()) > 0</tt> :</dt>
	 * <dd>the map returns <tt>e2</tt> prior to returning <tt>e1</tt>.</dd>
	 * </dl>
	 * </ul>
	 */
	protected Comparator comparator;

	/**
	 * The comparator to determine the subtree of a node of a binary
	 * search tree that would be used to insert a given entry. The
	 * compare method of the comparator is called with an array
	 * (<i>parameter list</i>) of objects (to insert) and a node of a
	 * binary search tree.
	 */
	protected Comparator chooseSubtree = new Comparator () {
		public int compare (Object object0, Object object1) {
			return comparator.compare(((MapEntry)((Object[]) object0)[0]).getKey(), ((MapEntry)((BinarySearchTree.Node)object1).object()).getKey());
		}
	};

	/**
	 * Constructs a new binary search tree map that initializes the
	 * internally used binary search tree with a new tree and uses the
	 * specified comparator to order entries according to their keys when
	 * inserted. The specified function is used to create a new binary
	 * search tree (it works like the
	 * {@link BinarySearchTree#FACTORY_METHOD FACTORY_METHOD} of
	 * BinarySearchTree).
	 *
	 * @param comparator the comparator to determine the order of the
	 *        map.
	 * @param newBinarySearchTree a function to create a new binary search
	 *        tree.
	 */
	public BinarySearchTreeMap (Comparator comparator, Function newBinarySearchTree) {
		(this.tree = 
			(BinarySearchTree) newBinarySearchTree.invoke(
				Predicates.TRUE,
				Identity.DEFAULT_INSTANCE)
		).clear();
		this.comparator = comparator;
	}

	/**
	 * Constructs a new binary search tree map that uses the specified
	 * comparator to order entries according to their keys when inserted.
	 * The internally used binary search tree is initialized by the
	 * FACTORY_METHOD of BinarySearchTree). This constructor is equivalent
	 * to the call of
	 * <code>BinarySearchTreeMap(comparator, BinarySearchTree.FACTORY_METHOD)</code>.
	 *
	 * @param comparator the comparator to determine the order of the
	 *        map.
	 * @see BinarySearchTree#FACTORY_METHOD
	 */
	public BinarySearchTreeMap (Comparator comparator) {
		this(comparator, BinarySearchTree.FACTORY_METHOD);
	}

	/**
	 * Constructs a new binary search tree map that initializes the
	 * internally used binary search tree with a new tree and uses the
	 * <i>natural ordering</i> of its entries according to their keys to
	 * order them. The specified function is used to create a new binary
	 * search tree (it works like the
	 * {@link BinarySearchTree#FACTORY_METHOD FACTORY_METHOD} of
	 * BinarySearchTree). This constructor is equivalent to the call of
	 * <code>BinarySearchTreeMap(ComparableComparator.DEFAULT_INSTANCE, newBinarySearchTree)</code>.
	 *
	 * @param newBinarySearchTree a function to create a new binary search
	 *        tree.
	 */
	public BinarySearchTreeMap (Function newBinarySearchTree) {
		this(new ComparableComparator(), newBinarySearchTree);
	}

	/**
	 * Constructs a new binary search tree map that uses the <i>natural
	 * ordering</i> of its entries according to their keys to order them.
	 * This constructor is equivalent to the call of
	 * <code>BinarySearchTreeMap(ComparableComparator.DEFAULT_INSTANCE, BinarySearchTree.FACTORY_METHOD)</code>.
	 *
	 * @see BinarySearchTree#FACTORY_METHOD
	 */
	public BinarySearchTreeMap () {
		this(new ComparableComparator());
	}

	/**
	 * Returns the number of key-value mappings in this map. If the map
	 * contains more than <tt>Integer.MAX_VALUE</tt> elements, 
	 * <tt>Integer.MAX_VALUE</tt> is returned.<br>
	 * This implementation returns <code>tree.size()</code>.
	 *
	 * @return the number of key-value mappings in this map.
	 */
	public int size () {
		return tree.size();
	}

	/**
	 * Returns <tt>true</tt> if this map contains a mapping for the
	 * specified key. <br>
	 * This implementation uses the get method of BinarySearchTree to
	 * search efficiently for the key. The standard implementation of
	 * AbstractMap requires linear time in the size of the map but the
	 * performance of this implementation depends on the performance of
	 * the used binary search tree.
	 *
	 * @param key key whose presence in this map is to be tested.
	 * @return <tt>true</tt> if this map contains a mapping for the
	 *         specified key.
	 */
	public boolean containsKey(Object key) {
		int[] result = new int[1];
		return tree.get(chooseSubtree, new Object [] {new MapEntry(key, null)}, result)!=null && result[0]==0;
	}

	/**
	 * Returns the value to which this map maps the specified key. Returns
	 * <tt>null</tt> if the map contains no mapping for this key. A return
	 * value of <tt>null</tt> does not <i>necessarily</i> indicate that
	 * the map contains no mapping for the key; it's also possible that
	 * the map explicitly maps the key to <tt>null</tt>. The containsKey
	 * operation may be used to distinguish these two cases.<br>
	 * This implementation uses the get method of the binary search tree
	 * to search efficiently for the key. The standard implementation of
	 * AbstractMap requires linear time in the size of the map but the
	 * performance of this implementation depends on the performance of
	 * the used binary search tree.
	 *
	 * @param key key whose associated value is to be returned.
	 * @return the value to which this map maps the specified key.
	 * @see #containsKey(Object)
	 */
	public Object get(Object key) {
		int[] result = new int[1];
		BinarySearchTree.Node node = tree.get(chooseSubtree, new Object [] {new MapEntry(key, null)}, result);
		return node!=null && result[0]==0 ? ((MapEntry)node.object()).getValue() : null;
	}

	/**
	 * Associates the specified value with the specified key in this map
	 * (optional operation). If the map previously contained a mapping for
	 * this key, the old value is replaced.<br>
	 * This implementation uses the insert method of the binary search
	 * tree to insert the key-value mapping. The standard implementation
	 * of AbstractMap requires linear time in the size of the map but the
	 * performance of this implementation depends on the performance of
	 * the used binary search tree.
	 *
	 * @param key key with which the specified value is to be associated.
	 * @param value value to be associated with the specified key.
	 * @return previous value associated with specified key, or
	 *         <tt>null</tt> if there was no mapping for key. (A
	 *         <tt>null</tt> return can also indicate that the map
	 *         previously associated <tt>null</tt> with the specified key,
	 *         if the implementation supports <tt>null</tt> values.)
	 * @throws UnsupportedOperationException if the <tt>put</tt> operation
	 *         is not supported by this map.
	 */
	public Object put(Object key, Object value) {
		int[] result = new int[1];
		BinarySearchTree.Node node = tree.get(chooseSubtree, new Object [] {new MapEntry(key, null)}, result);
		if (node!=null && result[0]==0)
			return ((MapEntry)node.object()).setValue(value);
		tree.insert(chooseSubtree, new Object [] {new MapEntry(key, value)});
		return null;
	}

	/**
	 * Removes the mapping for this key from this map if present (optional
	 * operation). <br>
	 * This implementation uses the remove method of the binary search
	 * tree to remove the mapping. The standard implementation of
	 * AbstractMap requires linear time in the size of the map but the
	 * performance of this implementation depends on the performance of
	 * the used binary search tree.
	 *
	 * @param key key whose mapping is to be removed from the map.
	 * @return previous value associated with specified key, or
	 *         <tt>null</tt> if there was no entry for key. (A
	 *         <tt>null</tt> return can also indicate that the map
	 *         previously associated <tt>null</tt> with the specified key,
	 *         if the implementation supports <tt>null</tt> values.)
	 * @throws UnsupportedOperationException if the <tt>remove</tt>
	 *         operation is not supported by this map.
	 */
	public Object remove(Object key) {
		BinarySearchTree.Node node = tree.remove(chooseSubtree, new Object[] {new MapEntry(key, null)}, tree.size()%2);
		return node!=null ? ((MapEntry)node.object()).getValue() : null;
	}

	/**
	 * Removes all mappings from this map (optional operation). <br>
	 * This implementation calls <tt>tree.clear()</tt>.
	 *
	 * @throws UnsupportedOperationException clear is not supported by
	 *         this map.
	 */
	public void clear() {
		tree.clear();
	}

	/**
	 * Returns a set view of the mappings contained in this map. Each
	 * element in this set is a MapEntry. The set is backed by the map, so
	 * changes to the map are reflected in the set, and vice-versa. (If
	 * the map is modified while an iteration over the set is in progress,
	 * the results of the iteration are undefined.) The set supports
	 * element removal, which removes the corresponding entry from the
	 * map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
	 * <tt>removeAll</tt>, <tt>retainAll</tt> and <tt>clear</tt>
	 * operations. It does not support the <tt>add</tt> or <tt>addAll</tt>
	 * operations.<br>
	 * This implementation overrides the class BinarySearchTreeSet and
	 * contains a new constructor that initializes the set's binary search
	 * tree with the field <tt>tree</tt> and the set's comparator with the
	 * the field <tt>comparator</tt>.
	 *
	 * @return a set view of the mappings contained in this map.
	 */
	public Set entrySet() {
		return new BinarySearchTreeSet() {
			{
				this.tree = BinarySearchTreeMap.this.tree;
				this.comparator = BinarySearchTreeMap.this.comparator;
			}
		};
	}
}
