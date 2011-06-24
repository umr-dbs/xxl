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

package xxl.core.collections.bags;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.collections.ReversedList;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.Function;

/**
 * This class provides an implementation of the Bag interface that internally
 * uses a list to store its elements.
 * 
 * <p>The performance of the bag depends on the performance of the internally
 * used list (e.g., an ArrayList guaratees for insertion an amortized constant
 * time, that is, adding n elements requires O(n) time).</p>
 *
 * <p>The cursors returned by this class' <code>cursor</code> and
 * <code>query</code> methods are <i>fail-fast</i>: if the bag is structurally
 * modified at any time after the cursor is created, in any way except through
 * the cursor's own methods, the cursor will throw a
 * ConcurrentModificationException. Thus, in the face of concurrent
 * modification, the cursor fails quickly and cleanly, rather than risking
 * arbitrary, non-deterministic behavior at an undetermined time in the future.
 * </p>
 *
 * <p>Usage example (1).
 * <code><pre>
 *     // create a new list bag (that uses a linked list to store its elements per default)
 *
 *     ListBag&lt;Integer&gt; bag = new ListBag&lt;Integer&gt;();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Cursor&lt;Integer&gt; iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     bag.insertAll(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     Cursor&lt;Integer&gt; cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     iterator.close();
 *     cursor.close();
 *     bag.close();
 * </pre></code>
 *
 * Usage example (2).
 * <code><pre>
 *     // create an iteration over the Integer between 0 and 19
 *
 *     iterator = new Enumerator(20);
 *
 *     // create a new list bag (that uses a linked list to store its elements per default) that
 *     // contains all elements of the given iterator
 *
 *     bag = new ListBag&lt;Integer&gt;(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // remove every even Integer from the cursor (and the underlying list bag)
 *
 *     while (cursor.hasNext()) {
 *         int i = cursor.next();
 *         if (i%2 == 0)
 *             cursor.remove();
 *     }
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     iterator.close();
 *     cursor.close();
 *     bag.close();
 * </pre></code>
 *
 * Usage example (3).
 * <code><pre>
 *     // create a new list bag that uses an array list to store its elements
 *
 *     bag = new ListBag&lt;Integer&gt;(new ArrayList&lt;Integer&gt;());
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     bag.insertAll(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     iterator.close();
 *     cursor.close();
 *     bag.close();
 * </pre></code></p>
 *
 * @param <E> the type of the elements this bag is able to store.
 * @see Cursor
 * @see Iterator
 * @see IteratorCursor
 * @see Function
 * @see LinkedList
 * @see List
 */
public class ListBag<E> extends AbstractBag<E> implements FIFOBag<E>, LIFOBag<E> {
	
	/**
	 * The list is internally used to store the elements of the bag.
	 */
	protected List<E> list;

	/**
	 * Constructs a bag containing the elements of the list. The specified list
	 * is internally used to store the elements of the bag.
	 *
	 * @param list the list that is used to initialize the internally used
	 *        list.
	 */
	public ListBag(List<E> list) {
		this.list = list;
	}

	/**
	 * Constructs an empty bag. This bag instantiates a new LinkedList in order
	 * to store its elements. This constructor is equivalent to the call of
	 * <code>ListBag(new LinkedList&lt;E&gt;())</code>.
	 */
	public ListBag () {
		this(new LinkedList<E>());
	}

	/**
	 * Constructs a bag containing the elements of the list and the specified
	 * iterator. For the most cases the list is used to initialize the
	 * internally used list in order to guartantee a desired performance. This
	 * constructor calls the constructor with the specified list and uses the
	 * insertAll method to insert the elements of the specified iterator.
	 *
	 * @param list the list that is used to initialize the internally used
	 *        list.
	 * @param iterator the iterator whose elements are to be placed into this
	 *        bag.
	 */
	public ListBag(List<E> list, Iterator<? extends E> iterator){
		this(list);
		insertAll(iterator);
	}

	/**
	 * Constructs a bag containing the elements of the specified iterator. This
	 * bag instantiates a new LinkedList in order to store its elements. This
	 * constructor is equivalent to the call of
	 * <code>ListBag(new LinkedList&lt;E&gt;(), iterator)</code>.
	 *
	 * @param iterator the iterator whose elements are to be placed into this
	 *        bag.
	 */
	public ListBag(Iterator<? extends E> iterator){
		this(new LinkedList<E>(), iterator);
	}

	/**
	 * Removes all elements from this bag. The bag will be empty after this
	 * call so that <code>size() == 0</code>.
	 */
	public void clear () {
		list.clear();
	}

	/**
	 * Returns a cursor to iterate over the elements in this bag without any
	 * predefined sequence. The cursor is specifying a <i>view</i> on the
	 * elements of this bag so that closing the cursor takes no effect on the
	 * bag (e.g., not closing the bag). The behavior of the cursor is
	 * unspecified if this bag is modified while the cursor is in progress in
	 * any way other than by calling the methods of the cursor. In this case
	 * every method of the cursor except <code>close()</code> throws a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor to iterate over the elements in this bag without any
	 *         predefined sequence.
	 */
	public Cursor<E> cursor () {
		return new IteratorCursor<E>(list.iterator());
	}

	/**
	 * Returns a cursor representing a FIFO (<i>first in, first out</i>)
	 * iteration over the elements in this bag. The cursor is specifying a
	 * <i>view</i> on the elements of this bag so that closing the cursor takes
	 * no effect on the bag (e.g., not closing the bag). Thebehavior of the
	 * cursor is unspecified if this bag is modified while the cursor is in
	 * progress in any way other than by calling the methods of the cursor. So,
	 * when the implementation of this cursor cannot guarantee that the cursor
	 * is in a valid state after modifing the underlying bag every method of
	 * the cursor except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor representing a FIFO (<i>first in, first out</i>)
	 *         iteration over the elements in this bag.
	 */
	public Cursor<E> fifoCursor () {
		return cursor();
	}

	/**
	 * Returns a cursor representing a LIFO (<i>last in, first out</i>)
	 * iteration over the elements in this bag. The cursor is specifying a
	 * <i>view</i> on the elements of this bag so that closing the cursor takes
	 * no effect on the bag (e.g., not closing the bag). The behavior of the
	 * cursor is unspecified if this bag is modified while the cursor is in
	 * progress in any way other than by calling the methods of the cursor. So,
	 * when the implementation of this cursor cannot guarantee that the cursor
	 * is in a valid state after modifing the underlying bag every method of
	 * the cursor except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor representing a LIFO (<i>last in, first out</i>)
	 *         iteration over the elements in this bag.
	 */
	public Cursor<E> lifoCursor () {
		return new IteratorCursor<E>(new ReversedList<E>(list).iterator());
	}
	
	/**
	 * Adds the specified element to this bag. This method does not perform any
	 * kind of <i>duplicate detection</i>.
	 *
	 * @param object element to be added to this bag.
	 */
	public void insert(E object) {
		list.add(object);
	}

	/**
	 * Returns the number of elements in this bag (its cardinality). If this
	 * bag contains more than <code>Integer.MAX_VALUE</code> elements,
	 * <code>Integer.MAX_VALUE</code> is returned.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	public int size () {
		return list.size();
	}
}
