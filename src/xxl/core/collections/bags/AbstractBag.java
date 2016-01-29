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

import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;

/**
 * This class provides a skeletal implementation of the Bag interface, to
 * minimize the effort required to implement this interface.
 * 
 * <p>To implement a bag, the programmer needs only to extend this class and
 * provide implementations for the cursor, insert and size methods. For
 * supporting the removal of elements, the cursor returned by the cursor method
 * must additionally implement its remove method.</p>
 * 
 * <p>The documentation for each non-abstract method in this class describes
 * its implementation in detail. Each of these methods may be overridden if the
 * bag being implemented admits a more efficient implementation.</p>
 *
 * @param <E> the type of the elements this bag is able to store.
 * @see Cursor
 * @see Function
 * @see Iterator
 */
public abstract class AbstractBag<E> implements Bag<E> {

	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public AbstractBag() {}

	/**
	 * Returns a cursor to iterate over the elements in this bag without any
	 * predefined sequence. The cursor is specifying a <i>view</i> on the
	 * elements of this bag so that closing the cursor takes no effect on the
	 * bag (e.g., not closing the bag). The behavior of the cursor is
	 * unspecified if this bag is modified while the cursor is in progress in
	 * any way other than by calling the methods of the cursor. So, when the
	 * implementation of this cursor cannot guarantee that the cursor is in a
	 * valid state after modifing the underlying bag every method of the cursor
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
	 * Returns the number of elements in this bag (its cardinality). If this
	 * bag contains more than <code>Integer.MAX_VALUE</code> elements, returns
	 * <code>Integer.MAX_VALUE</code>.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	public abstract int size();

	/**
	 * Removes all of the elements from this bag. The bag will be empty after
	 * this call so that <code>size()&nbsp;==&nbsp;0</code>.
	 * 
	 * <p>This implementation creates a cursor by calling the cursor method and
	 * iterates over it, removing each element using the
	 * {@link Cursor#remove() Cursor.remove} operation. Most implementations
	 * will probably choose to override this method for efficiency.</p>
	 */
	public void clear() {
		for (Iterator<E> cursor = cursor(); cursor.hasNext(); cursor.remove())
			cursor.next();
	}

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
	 *
	 * <p>This implementation is given by an empty body.</p>
	 */
	public void close() {}

	/**
	 * Adds all of the elements in the specified iterator to this bag. This
	 * method does not perform any kind of <i>duplicate detection.</i> The
	 * behavior of this operation is unspecified if the specified iterator is
	 * modified while the operation is in progress.
	 * 
	 * <p>This implementation iterates over the specified iterator, and inserts
	 * each object, in turn.</p>
	 *
	 * @param objects iterator whose elements are to be added to this bag.
	 */
	public void insertAll(Iterator<? extends E> objects) {
		while (objects.hasNext())
			insert(objects.next());
	}

	/**
	 * Returns a cursor to iterate over all elements in this bag for which the
	 * given predicate returns <code>true</code>. This method is very similar
	 * to the cursor method except that its result is determined by a
	 * predicate. This implementation filters the result of the cursor method
	 * using the following code:
	 * <pre><code>
	 *   return size() &gt; 0 ?
	 *       (Cursor&lt;E&gt;)new Filter&lt;E&gt;(cursor(), predicate) :
	 *       new EmptyCursor&lt;E&gt;();
	 * </code></pre>
	 * The default implementation of this method is not very interesting,
	 * but the method is very import for some bags. When the data
	 * structure that is internally used for storing the elements of this
	 * bag is able to handle with queries, this method can be implemented
	 * very efficient by passing the query to the data structure. For
	 * example a range query on a bag that internally uses a R-tree to
	 * store its elements will be more efficient when it is proceed on the
	 * R-tree itself.<br>
	 * Like the cursor returned by the cursor method, this cursor's
	 * behavior is unspecified if this bag is modified while the cursor is
	 * in progress in any way other than by calling the methods of the
	 * cursor.
	 *
	 * @param predicate a predicate that determines whether an element of
	 *        this bag should be returned or not.
	 * @return a cursor to iterate over all elements in this bag for which
	 *         the given predicate returns <tt>true</tt>.
	 */
	public Cursor<E> query(Predicate<? super E> predicate) {
		return size() > 0 ?
			(Cursor<E>)new Filter<E>(cursor(), predicate) :
			new EmptyCursor<E>();
	}
}
