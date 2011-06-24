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

package xxl.core.cursors.identities;

import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;

/**
 * This class provides a decorator for a given iteration that prevents updates
 * on the underlying cursor. The methods of this class call the corresponding
 * methods of the internally stored iteration except the methods that update
 * the iteration. The <code>update</code> method throws an
 * <code>UnsupportedOperationException</code>.
 *
 * @param <I> the type of the elements returned by the underlying iteration
 *        (may be a subtype of O).
 * @param <O> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class NotUpdatableCursor<O, I extends O> implements Cursor<O> {

	/**
	 * The decorated cursor that is used for passing method calls to.
	 */
	protected Cursor<I> cursor;

	/**
	 * Creates a new decorator-cursor. The given iterator is wrapped to a
	 * cursor by calling the method
	 * {@link xxl.core.cursors.Cursors#wrap(java.util.Iterator)} and all method
	 * calls are finally passed to it.
	 *
	 * @param iterator the iterator to be decorated.
	 */
	public NotUpdatableCursor(Iterator<I> iterator) {
		this.cursor = Cursors.wrap(iterator);
	}

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
	public void open() {
		cursor.open();
	}

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
	public void close() {
			cursor.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>This operation is implemented idempotent, i.e., consequent calls to
	 * <code>hasNext</code> do not have any effect.</p>
	 *
	 * @return <code>true</code> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public boolean hasNext() throws IllegalStateException {
		return cursor.hasNext();
	}

	/**
	 * Returns the next element in the iteration.
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public O next() throws IllegalStateException {
		return cursor.next();
	}

	/**
	 * Shows the next element in the iteration without proceeding the iteration
	 * (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors. After calling the <code>peek</code> method a call to
	 * <code>next</code> is strongly recommended.</p> 
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public O peek() throws IllegalStateException {
		return cursor.peek();
	}

	/**
	 * Returns <code>true</code> if the <code>peek</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>peek</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsPeek() {
		return cursor.supportsPeek();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the cursor (optional operation). 
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 */
	public void remove()  {
		cursor.remove();
	}

	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return cursor.supportsRemove();
	}

	/**
	 * Replaces the last element returned by the cursor in the underlying data
	 * structure (optional operation). 
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        cursor.
	 */
	public void update(O object) {
		throw new UnsupportedOperationException("this cursor is not updatable.");
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return false;
	}

	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 */
	public void reset() {
		cursor.reset();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return cursor.supportsReset();
	}

}
