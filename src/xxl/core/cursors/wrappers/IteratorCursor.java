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

package xxl.core.cursors.wrappers;

import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;

/**
 * The iterator-cursor wraps an {@link java.util.Iterator iterator} to a
 * cursor, i.e., the wrapped iterator can be accessed via the
 * {@link xxl.core.cursors.Cursor cursor} interface. An iterator-cursor tries
 * to pass all method calls to the underlying iterator. If an operation is not
 * supported by the underlying iterator (<code>update</code> and
 * <code>reset</code>) an {@link java.lang.UnsupportedOperationException} is
 * thrown.
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class IteratorCursor<E> extends AbstractCursor<E> {

	/**
	 * The internally used iterator that is wrapped to a cursor.
	 */
	protected Iterator<? extends E> iterator;

	/**
	 * Creates a new iterator-cursor.
	 *
	 * @param iterator the iterator to be wrapped to a cursor.
	 */
	public IteratorCursor(Iterator<? extends E> iterator) {
		this.iterator = iterator;
	}
	
	/**
	 * Creates a new iterator-cursor.
	 *
	 * @param iterable the object providing iterators
	 */
	public IteratorCursor(Iterable<? extends E> iterable) {
		this.iterator = iterable.iterator();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the iterator-cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return iterator.hasNext();
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the iterator-cursor's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return iterator.next();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the iterator-cursor (optional operation). This method can be called only
	 * once per call to <code>next</code> or <code>peek</code> and removes the
	 * element returned by this method. Note, that between a call to
	 * <code>next</code> and <code>remove</code> the invocation of
	 * <code>peek</code> or <code>hasNext</code> is forbidden. The behaviour of
	 * an iterator-cursor is unspecified if the underlying data structure is
	 * modified while the iteration is in progress in any way other than by
	 * calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the iterator-cursor.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		iterator.remove();
	}
	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the iterator-cursor. Otherwise it returns
	 * <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the iterator-cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return true;
	}

}
