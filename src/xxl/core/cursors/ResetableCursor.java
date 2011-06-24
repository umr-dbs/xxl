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

import java.util.ArrayList;
import java.util.Iterator;

import xxl.core.cursors.identities.TeeCursor;
import xxl.core.cursors.identities.TeeCursor.ListStorageArea;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides a wrapper that enhance a
 * {@link xxl.core.cursors.Cursor cursor} (or
 * {@link java.util.Iterator iterator}) that is not resetable by a reset
 * functionality. For this reason, the cursor is wrapped by a
 * {@link xxl.core.cursors.identities.TeeCursor tee-cursor} that stores the
 * elements returned by it. When the <code>reset</code> method is called on the
 * resetable cursor, it iterates over the stored elements at first and
 * continues the iteration with the elements of the wrapped cursor afterwards.
 * 
 * <p><b>Note</b> that the elements returned by a resetable cursor are stored
 * in the storage area of a tee cursor completely. So be aware of the potential
 * size of the wrapped cursor in order to estimate memory usage.</p>
 * 
 * <p>When a cursor has been wrapped to a resetable cursor, the working of the
 * <code>reset</code> method can be guaranteed and the cursor can be used as in
 * the example shown below.
 * <code><pre>
 *     // Create a cursor with ten random numbers.
 * 
 *     Cursor randomNumbers = new DiscreteRandomNumber(
 *         new JavaDiscreteRandomWrapper(),
 *         10
 *     );
 * 
 *     // Wrap the cursor to a resetable cursor.
 * 
 *     ResetableCursor bufferedCursor = new ResetableCursor(randomNumbers);
 * 
 *     // Process five elements of the cursor (they will be stored internally).
 * 
 *     bufferedCursor.open();
 *     System.out.println("get 5 elements:");
 *     for (int i = 1; i&lt;5 && bufferedCursor.hasNext(); i++)
 *         System.out.println(bufferedCursor.next());
 * 
 *     // Now reset the cursor and process all elements.
 * 
 *     System.out.println("reset buffered cursor, and get all elements:");
 *     bufferedCursor.reset();
 *     Cursors.println(bufferedCursor);
 *     
 *     // Another time.
 * 
 *     System.out.println("reset buffered cursor, and get all elements:");
 *     bufferedCursor.reset();
 *     Cursors.println(bufferedCursor);
 * 
 *     // Finally close the cursor!
 * 
 *     bufferedCursor.close();
 * </pre></code></p>
 * 
 * @param <E> the type of the elements returned by this iteration.
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.identities.TeeCursor
 */
public class ResetableCursor<E> extends AbstractCursor<E> {

	/**
	 * The tee cursor that is internally used to store the elments of the
	 * (usually) not resetable cursor.
	 */
	protected TeeCursor<E> teeCursor;
	
	/**
	 * A cursor iterating over the elements internally stored in the storage
	 * area of the tee cursor.
	 */
	protected Cursor<E> bufferedCursor;
	
	/**
	 * Creates a new resetable cursor that enhances the given iterator by a
	 * reset functionality and uses the specified factory method to get a tee
	 * cursor that stores the wrapped cursor's elements. The factory must
	 * implements an {@link xxl.core.functions.Function#invoke(Object) invoke
	 * method} that expects the iterator to be wrapped as argument.
	 *
	 * @param iterator the iterator that should be enhance by a reset
	 *        functionality.
	 * @param teeCursorFactory a factory method which returns a tee cursor. The
	 *        factory must handle with an iterator to be wrapped as argument.
	 */
	public ResetableCursor(Iterator<? extends E> iterator, Function<Iterator<? extends E>, TeeCursor<E>> teeCursorFactory) {
		teeCursor = teeCursorFactory.invoke(iterator);
	}

	/**
	 * Creates a new resetable cursor that enhances the given iterator by a
	 * reset functionality. For getting a tee cursor to store the wrapped
	 * iterator's elements the default constructo of {@link TeeCursor} is used.
	 *
	 * @param iterator the iterator that should be enhance by a reset
	 *        functionality.
	 */
	public ResetableCursor(Iterator<? extends E> iterator) {
		this(
			iterator,
			new AbstractFunction<Iterator<? extends E>, TeeCursor<E>>() {
				@Override
				public TeeCursor<E> invoke(Iterator<? extends E> iterator) {
					return new TeeCursor<E>(iterator, new ListStorageArea<E>(new ArrayList<E>()), true);
				}
			}
		);
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
	@Override
	public void open() {
		if (isOpened)
			return;
		super.open();
		teeCursor.open();
		bufferedCursor = null;
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
	@Override
	public void close() {
		if (isClosed)
			return;
		super.close();
		teeCursor.close();
		if (bufferedCursor != null) {
			bufferedCursor.close();
			bufferedCursor = null;
		}
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the cursor has more elements.
	 */
	@Override
	protected boolean hasNextObject() {
		if (bufferedCursor != null) {
			if (bufferedCursor.hasNext())
				return true;
			bufferedCursor.close();
			bufferedCursor = null;
		}
		return teeCursor.hasNext();
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	@Override
	protected E nextObject() {
		return bufferedCursor != null ?
			bufferedCursor.next() :
			teeCursor.next();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the cursor (optional operation). This method can be called only once per
	 * call to <code>next</code> or <code>peek</code> and removes the element
	 * returned by this method. Note, that between a call to <code>next</code>
	 * and <code>remove</code> the invocation of <code>peek</code> or
	 * <code>hasNext</code> is forbidden. The behavior of a cursor is
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
	@Override
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		if (bufferedCursor != null)
			throw new IllegalStateException("remove cannot be performed on an element that is already buffered");
		super.remove();
		teeCursor.remove();
	}
	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	@Override
	public boolean supportsRemove() {
		return teeCursor.supportsRemove();
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
	@Override
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		if (bufferedCursor != null)
			throw new IllegalStateException("update cannot be performed on an element that is already buffered");
		super.update(object);
		teeCursor.update(object);
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	@Override
	public boolean supportsUpdate() {
		return teeCursor.supportsReset();
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
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	@Override
	public void reset() throws UnsupportedOperationException {
		super.reset();
		if (bufferedCursor != null)
			bufferedCursor.close();
		bufferedCursor = teeCursor.cursor();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	@Override
	public boolean supportsReset() {
		return true;
	}

}
