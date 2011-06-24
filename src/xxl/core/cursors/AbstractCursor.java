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

import java.util.NoSuchElementException;

/**
 * An abstract cursor implements the methods of the interface
 * {@link xxl.core.cursors.Cursor} and is useful in those cases when you
 * produce elements and want to <i>return</i> them by a cursor. For example
 * consider a query-operation on an index structure that returns an iteration
 * over its results. It is sufficient then to create an abstract cursor and
 * implement the abstract methods <code>hasNextObject</code> and
 * <code>nextObject</code>.<br />
 * Both methods are protected ones that are only called internally by the
 * abstract cursor whenever a new result is requested on that cursor. The
 * contract of these methods is that:
 * <ul>
 *     <li>
 *         <code>hasNextObject</code> determines whether the iteration has more
 *         elements.
 *     </li>
 *     <li>
 *         <code>nextObject</code> computes the next object in the iteration.
 *     </li>
 * </ul>
 * 
 * <b>Important:</b> In order to guarantee a certain semantics, an extension of
 * an <code>AbstractCursor</code> has to ensure that a call of
 * <code>hasNext</code> always returns <code>false</code> after the first time 
 * <code>false</code> is delivered. Thus, it should not be possible to receive
 * an element by calling <code>hasNext</code> and <code>next()</code> at a
 * later point in time, if <code>hasNext</code> returned <code>false</code> 
 * before (even if the underlying data structure received a new element
 * meanwhile). 
 * 
 * <p><b>Example usage:</b>
 * The cursor {@link xxl.core.cursors.sources.DiscreteRandomNumber} produces a
 * certain number (<code>noOfObjects</code>) of random integer objects by
 * calling {@link java.util.Random#nextInt()}. Therefore
 * <tcodet>RandomIntegers</code> extends <code>AbstractCursor</code> and the
 * implementation of its abstract methods is as follows:
 * <code><pre>
 *   protected boolean hasNextObject() {
 *       return noOfObjects == -1 || noOfObjects &gt; 0;
 *   }
 * 
 *   protected Object nextObject() {
 *       if (noOfObjects &gt; 0)
 *           noOfObjects--;
 *       return new Integer(random.nextInt(maxValue));
 *   }
 * </pre></code>
 * This means, as long as <code>noOfObjects&nbsp;&gt;&nbsp;0</code> holds (or
 * <code>noOfObjects&nbsp;==&nbsp;-1</code> for an infinite number of random
 * integer objects), the cursor has more elements and the next element will be
 * set to <code>new Integer(random.nextInt(maxValue))</code>.</p>
 *
 * <p>Note, that the abstract methods of the cursor interface are implemented in
 * this class, i.e., an abstract cursor already provides the mechanisms that are
 * required for its functionality.
 * <ul>
 *     <li>
 *         An abstract cursor provides a <code>hasNext</code>/<code>next</code>
 *         mechanism guaranteeing a call to the method <code>hasNext</code>
 *         before the next element in the iteration will be accessed, i.e.,
 *         there will be at least on call to the abstract method
 *         <code>hasNextObject</code> before the second abstract method
 *         <code>nextObject</code> is called.
 *     </li>
 *     <li>
 *         It also provides an <code>open</code>/<code>close</code> mechanism.
 *         Due to the decoupling of a cursor's construction and its open phase,
 *         it must be guaranteed that a cursor is opened before it is accessed.
 *         For this reason the <code>hasNext</code> method checks whether the
 *         cursor has already been opened and opens it explicitly if not. When
 *         a cursor has been closed, its elements cannot be accessed any more,
 *         i.e., a call to the methods <code>peek</code> and <code>next</code>
 *         will throw an exception rather than returning an element. Also a
 *         closed cursor cannot be re-opened by a call to its <code>open</code>
 *         method. Both methods (<code>open</code> and <code>close</code> are
 *         implemented idempotent, i.e., consecutive calls to this methods will
 *         have the same effect as a single call.
 *     </li>
 * </ul>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see xxl.core.cursors.Cursor
 */
public abstract class AbstractCursor<E> implements Cursor<E> {

	/**
	 * A flag indicating whether the iteration has more elements.
	 */
	protected boolean hasNext = false;
	
	/**
	 * A flag indicating whether the <code>hasNext</code> method has already
	 * been called for the next element in the iteration.
	 */
	protected boolean computedHasNext = false;
		
	/**
	 * A flag indicating whether the element returned by the last call to the
	 * <code>next</code> or <code>peek</code> method is valid any longer, i.e.,
	 * it has not been removed or updated since that time.
	 */
	protected boolean isValid = true;
	
	/**
	 * The next element in the iteration. This object will be determined by the
	 * first call to the <code>next</code> or <code>peek</code> method and
	 * simply returned by consecutive calls.
	 */
	protected E next = null;
	
	/**
	 * A flag indicating whether the element stored by the field {@link #next}
	 * is already returned by a call to the <code>next</code> method.
	 */
	protected boolean assignedNext = false;
		
	/**
	 * A flag indicating whether the cursor is already opened.
	 */
	protected boolean isOpened = false;
	
	/**
	 * A flag indicating whether the cursor is already closed.
	 */
	protected boolean isClosed = false;
	
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
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public void open() {
	 *       if (!isOpened)
	 *           isOpened = true;
	 *   }
	 * </pre></code></p>
	 */
	public void open() {
		if (!isOpened)
			isOpened = true;
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
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public void close() {
	 *       if (!isClosed) {
	 *           hasNext = false;
	 *           computedHasNext = false;
	 *           isValid = false;
	 *           isClosed = true;
	 *       }
	 *   }
	 * </pre></code></p>
	 */
	public void close() {
		if (!isClosed) {
			hasNext = false;
			computedHasNext = false;
			isValid = false;
			isClosed = true;
		}
	}

	/**
	 * Returns <code>true</code> if the cursor has been closed.
	 * 
	 * @return <code>true</code> if the cursor has been closed.
	 */
	public boolean isClosed() {
		return isClosed;
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
	 * <b>Important:</b> In order to guarantee a certain semantics, an
	 * extension of an <code>AbstractCursor</code> has to ensure that a call of
	 * <code>hasNext</code> always returns <code>false</code> after the first
	 * time <code>false</code> is delivered. Thus, it should not be possible to
	 * receive an element by calling <code>hasNext()</code> and
	 * <code>next()</code> at a later point in time, if <code>hasNext()</code>
	 * returned <tcodet>false</code> before (even if the underlying data
	 * structure received a new element meanwhile). 
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public final boolean hasNext() throws IllegalStateException {
	 *       if (isClosed)
	 *           throw new IllegalStateException();
	 *       if (!isOpened)
	 *           open();
	 *       if (!computedHasNext) {
	 *           hasNext = hasNextObject();
	 *           computedHasNext = true;
	 *           isValid = false;
	 *           assignedNext = false;
	 *       }
	 *       return hasNext;
	 *   }
	 * </pre></code></p>
	 *
	 * @return <code>true</code> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public final boolean hasNext() throws IllegalStateException {
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		if (!computedHasNext) {
			hasNext = hasNextObject();
			computedHasNext = true;
			isValid = false;
			assignedNext = false;
		}
		return hasNext;
	}
	
	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>This abstract operation should implement the core functionality of
	 * the <code>hasNext</code> method which secures that the cursor is in a
	 * proper state when this method is called. Due to this the
	 * <code>hasNextObject</code> method need not to deal with exception
	 * handling.</p>
	 * 
	 * @return <code>true</code> if the cursor has more elements.
	 */
	protected abstract boolean hasNextObject();

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public final E next() throws IllegalStateException,
	 *                                NoSuchElementException {
	 *       if (!computedHasNext)
	 *           hasNext();
	 *       if (!hasNext)
	 *           throw new NoSuchElementException();
	 *       if (!assignedNext) {
	 *           next = nextObject();
	 *           assignedNext = true;
	 *       }
	 *       hasNext = false;
	 *       computedHasNext = false;
	 *       isValid = true;
	 *       return next;
	 *   }
	 * </pre></code></p>
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public final E next() throws IllegalStateException, NoSuchElementException {
		if (!computedHasNext)
			hasNext();
		if (!hasNext)
			throw new NoSuchElementException();
		if (!assignedNext) {
			next = nextObject();
			assignedNext = true;
		} 
		hasNext = false;
		computedHasNext = false;
		isValid = true;
		return next;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * <p>This abstract operation should implement the core functionality of
	 * the <code>next</code> method which secures that the cursor is in a
	 * proper state when this method is called. Due to this the
	 * <code>nextObject</code> method need not to deal with exception
	 * handling.</p>
	 *
	 * @return the next element in the iteration.
	 */
	protected abstract E nextObject();

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
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public final E peek() throws IllegalStateException,
	 *                                NoSuchElementException,
	 *                                UnsupportedOperationException {
	 *       if (!supportsPeek())
	 *           throw new UnsupportedOperationException();
	 *       if (!computedHasNext)
	 *           hasNext();
	 *       if (!hasNext)
	 *           throw new NoSuchElementException();
	 *       if (!assignedNext) {
	 *           next = nextObject();
	 *           assignedNext = true;
	 *       }
	 *       isValid = true;
	 *       return next;
	 *   }
	 * </pre></code></p>
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException iteration has no more elements.
	 * @throws UnsupportedOperationException if the <code>peek</code> operation
	 *         is not supported by the cursor.
	 */
	public final E peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		if (!supportsPeek())
			throw new UnsupportedOperationException();
		if (!computedHasNext)
			hasNext();
		if (!hasNext)
			throw new NoSuchElementException();
		if (!assignedNext) {
			next = nextObject(); 
			assignedNext = true;
		}
		isValid = true;
		return next;
	}

	/**
	 * Returns <code>true</code> if the <code>peek</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *     public final boolean supportsPeek() {
	 *         return true;
	 *     }
	 * </pre></code></p>
	 *
	 * @return <code>true</code> if the <code>peek</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public final boolean supportsPeek() {
		return true;
	}

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
	 * <p>Note, that this operation is optional and does not work for this
	 * cursor.</p>
	 *
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public void remove() throws IllegalStateException,
	 *                               UnsupportedOperationException {
	 *       if (!supportsRemove())
	 *           throw new UnsupportedOperationException();
	 *       if (!isValid)
	 *           throw new IllegalStateException();
	 *       hasNext = false;
	 *       computedHasNext = false;
	 *       isValid = false;
	 *       assignedNext = false;
	 *   }
	 * </pre></code></p>
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the cursor.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		if (!supportsRemove())
			throw new UnsupportedOperationException();
		if (!isValid)
			throw new IllegalStateException();
		hasNext = false;
		computedHasNext = false;
		isValid = false;
		assignedNext = false;
	}

	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public boolean supportsRemove() {
	 *       return false;
	 *   }
	 * </pre></code></p>
	 * 
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return false;
	}
	
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
	 * <p>Note, that this operation is optional and does not work for this
	 * cursor.</p>
	 *
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public void update(E object) throws IllegalStateException,
	 *                                       UnsupportedOperationException {
	 *       if (!supportsUpdate())
	 *           throw new UnsupportedOperationException();
	 *       if (!isValid)
	 *           throw new IllegalStateException();
	 *       hasNext = false;
	 *       computedHasNext = false;
	 *       isValid = false;
	 *       assignedNext = false;
	 *   }
	 * </pre></code></p>
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
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		if (!supportsUpdate())
			throw new UnsupportedOperationException();
		if (!isValid)
			throw new IllegalStateException();
		hasNext = false;
		computedHasNext = false;
		isValid = false;
		assignedNext = false;
	}

	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 * 
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public boolean supportsUpdate() {
	 *       return false;
	 *   }
	 * </pre></code></p>
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
	 * <p>Note, that this operation is optional and does not work for this
	 * cursor.</p>
	 *
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public void reset() throws IllegalStateException,
	 *                              UnsupportedOperationException {
	 *       if (!supportsReset())
	 *           throw new UnsupportedOperationException();
	 *       hasNext = false;
	 *       computedHasNext = false;
	 *       isValid = false;
	 *       assignedNext = false;
	 *   }
	 * </pre></code></p>
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		if (!supportsReset())
			throw new UnsupportedOperationException();
		hasNext = false;
		computedHasNext = false;
		isValid = false;
		assignedNext = false;
	}

	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public boolean supportsReset() {
	 *       return false;
	 *   }
	 * </pre></code></p>
	 * 
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return false;
	}
}
