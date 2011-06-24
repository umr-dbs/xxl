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
 * This class decorates an object implementing the interface
 * {@link xxl.core.cursors.Cursor} such that all method calls on the decorator
 * are passed to its decorated object. For these purposes it realizes the design
 * pattern, named <i>Decorator</i>. The intent of this design pattern is to
 * attach additional responsibilities to an object dynamically. So decorators
 * provide a flexible alternative to subclassing for extending functionality.
 * For further information see: "Gamma et al.: <i>DesignPatterns. Elements of
 * Reusable Object-Oriented Software.</i> Addision Wesley 1998."
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link java.util.Iterator} it is wrapped to a cursor by a call to the static
 * method {@link xxl.core.cursors.Cursors#wrap(Iterator) wrap}. For guaranteeing
 * the proper functionality of this decorator cursor's
 * <tt>open</tt>/<tt>close</tt> and <tt>hasNext</tt>/<tt>next</tt> mechanisms,
 * it reproduces the mechanisms implemented in the class
 * {@link xxl.core.cursors.AbstractCursor}.</p>
 * 
 * <p>The usage of this class can be seen for example in the classes:<br />
 * <ul>
 *     <li>
 *         {@link xxl.core.cursors.filters.Dropper Dropper}
 *     </li>
 *     <li>
 *         {@link xxl.core.cursors.filters.Filter Filter}
 *     </li>
 *     <li>
 *         {@link xxl.core.cursors.filters.Remover Remover}
 *     </li>
 *     <li>
 *         {@link xxl.core.cursors.filters.Taker Taker}
 *     </li>
 * </ul></p>
 *
 * <p><b>Note:</b> When overwriting a method from SecureDecoratorCursor there
 * must always be a call to the super method.</p>
 * 
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 */
public abstract class SecureDecoratorCursor<E> implements Cursor<E> {

	/**
	 * A flag indicating whether the iteration has more elements.
	 */
	protected boolean hasNext = false;
	
	/**
	 * A flag indicating whether the <tt>hasNext</tt> method has already been
	 * called for the next element in the iteration.
	 */
	protected boolean computedHasNext = false;
		
	/**
	 * A flag indicating whether the element returned by the last call to the
	 * <tt>next</tt> or <tt>peek</tt> method is valid any longer, i.e., it has
	 * not been removed or updated since that time.
	 */
	protected boolean isValid = true;
		
	/**
	 * A flag indicating whether the cursor is already opened.
	 */
	protected boolean isOpened = false;
	
	/**
	 * A flag indicating whether the cursor is already closed.
	 */
	protected boolean isClosed = false;
	
	
	/**
	 * The decorated cursor that is used for passing method calls to.
	 */
	protected Cursor<E> cursor;

	/**
	 * Creates a new decorator-cursor. The given iterator is wrapped to a cursor
	 * by calling the method
	 * {@link xxl.core.cursors.Cursors#wrap(java.util.Iterator)} and all method
	 * calls are finally passed to it.
	 *
	 * @param iterator the iterator to be decorated.
	 */
	public SecureDecoratorCursor(Iterator<E> iterator) {
		this.cursor = Cursors.wrap(iterator);
	}

	/**
	 * Returns the cursor decorated by this decorator cursor, that is used for
	 * passing method calls to. Note that is returned cursor and the iterator
	 * submitted at construction time are usually not the same, because the
	 * submitted iterator will be wrapped by a call to the method
	 * {@link xxl.core.cursors.Cursors#wrap(java.util.Iterator)}. 
	 * 
	 * @return the cursor decorated by this decorator cursor.
	 */
	public Cursor<E> getDecoratedCursor() {
		return cursor;
	}

	/**
	 * Opens the cursor, i.e., signals the cursor to reserve resources, open
	 * files, etc. Before a cursor has been opened calls to methods like
	 * <tt>next</tt> or <tt>peek</tt> are not guaranteed to yield proper
	 * results. Therefore <tt>open</tt> must be called before a cursor's data
	 * can be processed. Multiple calls to <tt>open</tt> do not have any effect,
	 * i.e., if <tt>open</tt> was called the cursor remains in the state
	 * <i>opened</i> until its <tt>close</tt> method is called.
	 * 
	 * <p>Note, that a call to the <tt>open</tt> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if(!isOpened) {
			isOpened = true;
			cursor.open();
		}
	}

	/**
	 * Closes the cursor, i.e., signals the cursor to clean up resources, close
	 * files, etc. When a cursor has been closed calls to methods like
	 * <tt>next</tt> or <tt>peek</tt> are not guaranteed to yield proper
	 * results. Multiple calls to <tt>close</tt> do not have any effect, i.e.,
	 * if <tt>close</tt> was called the cursor remains in the state
	 * <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (!isClosed) {
			hasNext = false;
			computedHasNext = false;
			isValid = false;
			isClosed = true;
			cursor.close();
		}
	}

	/**
	 * Returns <tt>true</tt> if the iteration has more elements. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
	 * return an element rather than throwing an exception.)
	 * 
	 * <p>This operation is implemented idempotent, i.e., consequent calls to
	 * <tt>hasNext</tt> do not have any effect.</p>
	 *
	 * @return <tt>true</tt> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public boolean hasNext() throws IllegalStateException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		if (!computedHasNext) {
			hasNext = cursor.hasNext();
			computedHasNext = true;
			isValid = false;
		}
		return hasNext;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public E next() throws IllegalStateException, NoSuchElementException {
		if (!computedHasNext)
			hasNext();
		if (!hasNext)
			throw new NoSuchElementException();
		hasNext = false;
		computedHasNext = false;
		isValid = true;
		return cursor.next();
	}

	/**
	 * Shows the next element in the iteration without proceeding the iteration
	 * (optional operation). After calling <tt>peek</tt> the returned element is
	 * still the cursor's next one such that a call to <tt>next</tt> would be
	 * the only way to proceed the iteration. But be aware that an
	 * implementation of this method uses a kind of buffer-strategy, therefore
	 * it is possible that the returned element will be removed from the
	 * <i>underlying</i> iteration, e.g., the caller can use an instance of a
	 * cursor depending on an iterator, so the next element returned by a call
	 * to <tt>peek</tt> will be removed from the underlying iterator which does
	 * not support the <tt>peek</tt> operation and therefore the iterator has to
	 * be wrapped and buffered.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors. After calling the <tt>peek</tt> method a call to <tt>next</tt>
	 * is strongly recommended.</p> 
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException iteration has no more elements.
	 * @throws UnsupportedOperationException if the <tt>peek</tt> operation is
	 *         not supported by the cursor.
	 */
	public E peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		if (!supportsPeek())
			throw new UnsupportedOperationException();
		if (!computedHasNext)
			hasNext();
		if (!hasNext)
			throw new NoSuchElementException();
		isValid = true;
		return cursor.peek();
	}

	/**
	 * Returns <tt>true</tt> if the <tt>peek</tt> operation is supported by the
	 * cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>peek</tt> operation is supported by the
	 *         cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsPeek() {
		return cursor.supportsPeek();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the cursor (optional operation). This method can be called only once per
	 * call to <tt>next</tt> or <tt>peek</tt> and removes the element returned
	 * by this method. Note, that between a call to <tt>next</tt> and
	 * <tt>remove</tt> the invocation of <tt>peek</tt> or <tt>hasNext</tt> is
	 * forbidden. The behaviour of a cursor is unspecified if the underlying
	 * data structure is modified while the iteration is in progress in any way
	 * other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>remove</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>remove</tt> operation is
	 *         not supported by the cursor.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		if (!supportsRemove())
			throw new UnsupportedOperationException();
		if (!isValid)
			throw new IllegalStateException();
		hasNext = false;
		computedHasNext = false;
		isValid = false;
		cursor.remove();
	}

	/**
	 * Returns <tt>true</tt> if the <tt>remove</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>remove</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsRemove() {
		return cursor.supportsRemove();
	}

	/**
	 * Replaces the last element returned by the cursor in the underlying data
	 * structure (optional operation). This method can be called only once per
	 * call to <tt>next</tt> or <tt>peek</tt> and updates the element returned
	 * by this method. Note, that between a call to <tt>next</tt> and
	 * <tt>update</tt> the invocation of <tt>peek</tt> or <tt>hasNext</tt> is
	 * forbidden. The behaviour of a cursor is unspecified if the underlying
	 * data structure is modified while the iteration is in progress in any way
	 * other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        cursor.
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>update</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>update</tt> operation is
	 *         not supported by the cursor.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		if (!supportsUpdate())
			throw new UnsupportedOperationException();
		if (!isValid)
			throw new IllegalStateException();
		hasNext = false;
		computedHasNext = false;
		isValid = false;
		cursor.update(object);
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>update</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>update</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsUpdate() {
		return cursor.supportsUpdate();
	}

	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <tt>reset</tt> operation is
	 *         not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		if (!supportsReset())
			throw new UnsupportedOperationException();
		hasNext = false;
		computedHasNext = false;
		isValid = false;
		cursor.reset();
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsReset() {
		return cursor.supportsReset();
	}

}
