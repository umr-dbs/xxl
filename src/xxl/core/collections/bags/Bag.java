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
import java.util.List;

import xxl.core.cursors.Cursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;

/**
 * The interface bag represents a data type that is able to contain any kind of
 * object and performs no duplicate detection. More formally, this interface
 * models a kind of mathematical <i>multiset</i> abstraction.
 * 
 * <p>The bag only provides methods for insertion of objetcs and removal of all
 * elements, but for accessing or removing <i>single</i> elements, a cursor
 * iterating over all elements of the bag must be created by calling
 * <code>cursor()</code>. The sequence of elements delivered by this cursor has
 * no predefined order. However, a subclass may define such an order.</p>
 *
 * @param <E> the type of the elements this bag is able to store.
 * @see Cursor
 * @see Function
 * @see Iterator
 */
public interface Bag<E> {

	/**
	 * A factory method to create a default bag. Each concrete implementation
	 * of bag except for ListBag should have a function FACTORY_METHOD that
	 * implements three variants of <code>invoke</code>
	 * <ul>
	 * <dl>
	 * <dt><li><code>Bag invoke()</code>:</dt>
	 * <dd>returns <code>new Bag()</code>.</dd>
	 * <dt><li><code>Bag invoke(Object iterator)</code>:</dt>
	 * <dd>returns <code>new Bag(iterator)</code>.</dd>
 	 * <dt><li><code>Bag invoke(List<? extends Object> internalDataStructure)</code>:</dt>
	 * <dd>returns <code>new Bag((&lt;<i>InternalDataStructure&gt;</i>)internalDataStructure.get(0))</code>.</dd>
	 * </dl>
	 * </ul>
	 * This factory method creates a new ListBag. It may be invoked with a
	 * <i>parameter list</i> (for further details see {@link Function}) of
	 * lists, an iterator or without any parameters. A <i>parameter list</i> of
	 * lists will be used to initialize the internally used list with the list
	 * at index 0 and an iterator will be used to insert the contained elements
	 * into the new ListBag.
	 *
	 * @see Function
	 */
	public static final Function<Object,ListBag<Object>> FACTORY_METHOD = new AbstractFunction<Object,ListBag<Object>> () {
		@Override
		public ListBag<Object> invoke() {
			return new ListBag<Object>();
		}

		@Override
		public ListBag<Object> invoke(Object iterator) {
			return new ListBag<Object>((Iterator<?>)iterator);
		}

		@Override
		public ListBag<Object> invoke(List<? extends Object> list) {
			return new ListBag<Object>((List<Object>)list.get(0));
		}
	};

	/**
	 * Removes all of the elements from this bag. The bag will be empty after
	 * this call so that <code>size() == 0</code>.
	 */
	public abstract void clear();

	/**
	 * Closes this bag and releases any system resources associated with it.
	 * This operation is idempotent, i.e., multiple calls of this method take
	 * the same effect as a single call. When needed, a closed bag can be
	 * implicit reopened by a consecutive call to one of its methods. Because
	 * of having an unspecified behavior when this bag is closed every cursor
	 * iterating over the elements of this bag must be closed.
	 * 
	 * <p><b>Note:</b> This method is very important for bags using external
	 * resources like files or JDBC resources.</p>
	 */
	public abstract void close();

	/**
	 * Returns a cursor to iterate over the elements in this bag without any
	 * predefined sequence. The cursor is specifying a <i>view</i> on the
	 * elements of this bag so that closing the cursor takes no effect on the
	 * bag (e.g., not closing the bag). The behavior of the cursor is
	 * unspecified if this bag is modified while the cursor is in progress in
	 * any way other than by calling the methods of the cursor. So, when the
	 * implementation of this cursor cannot guarantee that the cursor is in a
	 * valid state after modifying the underlying bag every method of the cursor
	 * except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor to iterate over the elements in this bag without any
	 *         predefined sequence.
	 */
	public abstract Cursor<E> cursor();

	/**
	 * Adds the specified element to this bag. This method does not perform any
	 * kind of <i>duplicate detection</i>.
	 *
	 * @param object element to be added to this bag.
	 */
	public abstract void insert(E object);

	/**
	 * Adds all of the elements in the specified iterator to this bag. This
	 * method does not perform any kind of <i>duplicate detection.</i> The
	 * behavior of this operation is unspecified if the specified iterator is
	 * modified while the operation is in progress.
	 *
	 * @param objects iterator whose elements are to be added to this bag.
	 */
	public abstract void insertAll(Iterator<? extends E> objects);

	/**
	 * Returns the number of elements in this bag (its cardinality). If this
	 * bag contains more than <code>Integer.MAX_VALUE</code> elements,
	 * <code>Integer.MAX_VALUE</code> is returned.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	public abstract int size();

	/**
	 * Returns a cursor to iterate over all elements in this bag for which the
	 * given predicate returns <code>true</code>. This method is very similar
	 * to the cursor method except that its result is determined by a
	 * predicate. A possible implementation filters the result of the cursor
	 * method using the following code
	 * <code><pre>
	 * return new Filter<E>(cursor(), predicate);
	 * </pre></code>
	 * Note, that this method is implemented in <i>AbstractBag</i>.
	 * 
	 * <p>The default implementation of this method is not very interesting,
	 * but the method is very import for some bags. When the data structure
	 * that is internally used for storing the elements of this bag is able to
	 * handle with queries, this method can be implemented very efficient by
	 * passing the query to the data structure. For example a range query on a
	 * bag that internally uses a R-tree to store its elements will be more
	 * efficient when it is proceed on the R-tree itself.</p>
	 * 
	 * <p>Like the cursor returned by the cursor method, this cursor's behavior
	 * is unspecified if this bag is modified while the cursor is in progress
	 * in any way other than by calling the methods of the cursor.</p>
	 *
	 * @param predicate a predicate that determines whether an element of this
	 *        bag should be returned or not.
	 * @return a cursor to iterate over all elements in this bag for which the
	 *         given predicate returns <code>true</code>.
	 */
	public abstract Cursor<E> query(Predicate<? super E> predicate);
	
}
