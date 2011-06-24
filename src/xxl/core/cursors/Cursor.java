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

package xxl.core.cursors;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A cursor extends the interface {@link java.util.Iterator} by adding
 * functionality for opening and closing the cursor, i.e., reserving and
 * releasing of system resources, taking a look at the next element that would
 * be returned by call to <code>next</code>, updating elements of the
 * underlying data structure and traversing it another time without
 * constructing a new cursor.
 * 
 * <p>Cursors <i>differ</i> from iterators in some points:
 * <ul>
 *     <li>
 *         <code>open</code>: before using a cursor the caller must open it in
 *         order to reserve resources, open files, etc.
 *     </li>
 *     <li>
 *         <code>close</code>: after using a cursor it must be closed by the
 *         caller in order to clean up resources, close files, etc.
 *     </li>
 *     <li>
 *         <code>peek</code>: cursors allow the caller to show the next element
 *         in the iteration without proceeding the iteration.
 *     </li>
 *     <li>
 *         <code>supportsPeek</code>: a method that returns <code>true</code>
 *         if an instance of a cursor supports the <code>peek</code> operation,
 *         otherwise it returns <code>false</code>.
 *     </li>
 *     <li>
 *         <code>supportsRemove</code>: a method that returns <code>true</code>
 *         if an instance of a cursor supports the <code>remove</code>
 *         operation, otherwise it returns <code>false</code>.
 *     </li>
 *     <li>
 *         <code>update</code>: cursors allow the caller to update the element
 *         in the iteration returned by the <code>next</code> or
 *         <code>peek</code> method and replacing it in the underlying
 *         collection.
 *     </li>
 *     <li>
 *         <code>supportsUpdate</code>: a method that returns <code>true</code>
 *         if an instance of a cursor supports the <code>update</code>
 *         operation, otherwise it returns <code>false</code>.
 *     </li>
 *     <li>
 *         <code>reset</code>: cursors allow the caller to traverse the
 *         underlying collection again without constructing a new cursor.
 *     </li>
 *     <li>
 *         <code>supportsReset</code>: a method that returns <code>true</code>
 *         if an instance of a cursor supports the <code>reset</code>
 *         operation, otherwise it returns <code>false</code>.
 *     </li>
 * </ul></p>
 * 
 * <p><b>General contract:</b><br />
 * If an exception is thrown by a cursor consecutive calls to methods are not
 * guaranteed to produce correct results.</p>
 * 
 * <p>
 * <b>Important:</b> In order to guarantee a certain semantics, an
 * implementation of a <code>Cursor</code> has to ensure that a call of
 * <code>hasNext</code> always returns <code>false</code> after the first time 
 * <code>false</code> is delivered. Thus, it should not be possible to receive
 * an element by calling <code>hasNext</code> and <code>next</code> at a later 
 * point in time, if <code>hasNext</code> returned <code>false</code> before 
 * (even if the underlying data structure received a new element meanwhile).
 * </p>
 * 
 * <p><b>General order of method invocations:</b>
 * <code><pre>
 *     // creating a new instance of an arbitrary cursor
 * 
 *     Cursor&lt;Object&gt; cursor = ...
 * 
 *     // opening the cursor for first use
 * 
 *     cursor.open();
 * 
 *     // consuming the cursor in a loop; checking if there is a next element
 * 
 *     while(cursor.hasNext()) {
 * 
 *         ...
 * 
 *         // taking a look at the next element, but do not remove it from the
 *         // underlying collection (optional)
 * 
 *         Object peek = cursor.peek();
 * 
 *         ...
 * 
 *         // consuming the next element
 * 
 *         Object next = cursor.next();
 * 
 *         ...
 * 
 *         // removing object 'next' from the underyling collection (optional)
 * 
 *         cursor.remove();
 * 
 *     }
 * 
 *     ...
 * 
 *     // cursor will be used again, so reset it (optional)
 * 
 *     cursor.reset();
 * 
 *     ...
 * 
 *     // cursor is not needed any more; release resources
 * 
 *     cursor.close(); 
 * </pre></code></p>
 * 
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 */
public interface Cursor<E> extends Iterator<E> {

	/**
	 * Opens the cursor, i.e., signals the cursor to reserve resources, open
	 * files, etc. Before a cursor has been opened calls to methods like
	 * <code>next</code> or <code>peek</code> are not guaranteed to yield 
	 * proper results. Therefore <code>open</code> must be called before a 
	 * cursor's data can be processed. Multiple calls to <code>open</code> do 
	 * not have any effect, i.e., if <code>open</code> was called the cursor 
	 * remains in the state <i>opened</i> until its <code>close</code> method 
	 * is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public abstract void open();

	/**
	 * Closes the cursor, i.e., signals the cursor to clean up resources, close
	 * files, etc. When a cursor has been closed calls to methods like
	 * <code>next</code> or <code>peek</code> are not guaranteed to yield 
	 * proper results. Multiple calls to <code>close</code> do not have any 
	 * effect, i.e., if <code>close</code> was called the cursor remains in the
	 * state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public abstract void close();

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>This operation should be implemented idempotent, i.e., consequent
	 * calls to <code>hasNext</code> do not have any effect.</p>
	 * 
	 * <p><b>Important:</b> In order to guarantee a certain semantics, an 
	 * implementation of a <code>Cursor</code> has to ensure that a call of 
	 * <code>hasNext</code> always returns <code>false</code> after the first
	 * time <code>false</code> is delivered. Thus, it should not be possible
	 * to receive an element by calling <code>hasNext</code> and
	 * <code>next</code> at a later point in time, if <code>hasNext</code>
	 * returned <code>false</code> before (even if the underlying data
	 * structure received a new element meanwhile).</p>
	 *
	 * @return <code>true</code> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public abstract boolean hasNext() throws IllegalStateException;

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public abstract E next() throws IllegalStateException, NoSuchElementException;

	/**
	 * Shows the next element in the iteration without proceeding the iteration
	 * (optional operation). After calling <code>peek</code> the returned
	 * element is still the cursor's next one such that a call to
	 * <code>next</code> would be the only way to proceed the iteration. But be
	 * aware that an implementation of this method uses a kind of
	 * buffer-strategy, therefore it is possible that the returned element will
	 * be removed from the <i>underlying</i> iteration, e.g., the caller can
	 * use an instance of a cursor depending on an iterator, so the next
	 * element returned by a call to <code>peek</code> will be removed from the
	 * underlying iterator which does not support the <code>peek</code>
	 * operation and therefore the iterator has to be wrapped and buffered.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors. After calling the <code>peek</code> method a call to
	 * <code>next</code> is strongly recommended.</p> 
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException iteration has no more elements.
	 * @throws UnsupportedOperationException if the <code>peek</code> operation
	 *         is not supported by the cursor.
	 */
	public abstract E peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException;

	/**
	 * Returns <code>true</code> if the <code>peek</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>peek</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public abstract boolean supportsPeek();

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the cursor (optional operation). This method can be called only once per
	 * call to <code>next</code> or <code>peek</code> and removes the element
	 * returned by this method. Note, that between a call to <code>next</code>
	 * and <code>remove</code> the invocation of <code>peek</code> or
	 * <code>hasNext</code> is forbidden. The behaviour of a cursor is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the cursor.
	 */
	public abstract void remove() throws IllegalStateException, UnsupportedOperationException;

	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public abstract boolean supportsRemove();

	/**
	 * Replaces the last element returned by the cursor in the underlying data
	 * structure (optional operation). This method can be called only once per
	 * call to <code>next</code> or <code>peek</code> and updates the element
	 * returned by this method. Note, that between a call to <code>next</code>
	 * and <code>update</code> the invocation of <code>peek</code> or
	 * <code>hasNext</code> is forbidden. The behaviour of a cursor is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        cursor.
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>update</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>update</code>
	 *         operation is not supported by the cursor.
	 */
	public abstract void update(E object) throws IllegalStateException, UnsupportedOperationException;

	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public abstract boolean supportsUpdate();

	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public abstract void reset() throws UnsupportedOperationException;

	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public abstract boolean supportsReset();

}
