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

package xxl.core.cursors.sources;

import java.util.Arrays;
import java.util.Collection;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.wrappers.IteratorCursor;

/**
 * An collection-cursor constructs a cursor backed on an arbitrary collection.
 * All elements from the given collection will be returned by the
 * collection-cursor.
 * 
 * @param <E>
 *            the type of the specified collection's components.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class CollectionCursor<E> extends AbstractCursor<E> {

	/**
	 * The collection holding the data to be returned by the collection-cursor.
	 */
	protected Collection<E> collection;

	/**
	 * The inner cursor providing the data
	 */
	protected Cursor<E> iteratorCursor;

	/**
	 * Creates a new collection-cursor. The elements stored in the given oject
	 * array will be returned according to their order.
	 * 
	 * @param array
	 *            the object array delivering the elements of the cursor.
	 */
	public CollectionCursor(E... array) {
		this.collection = Arrays.asList(array);
	}

	/**
	 * Creates a new collection-cursor. The elements are returned in the same
	 * order as they are contained in the given collection.
	 * 
	 * @param array
	 *            the object array delivering the elements.
	 */
	public CollectionCursor(Collection<E> collection) {
		this.collection = collection;
		this.iteratorCursor = new IteratorCursor<E>(this.collection);
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the array-cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return this.iteratorCursor.hasNext();
	}

	/**
	 * Returns the next element in the iteration.
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return this.iteratorCursor.next();
	}

	/**
	 * Resets the collection-cursor to its initial state such that the caller is
	 * able to traverse the underlying object array again without constructing a
	 * new collection-cursor (optional operation).
	 * 
	 */
	public void reset() {
		super.reset();
		this.iteratorCursor = new IteratorCursor<E>(this.collection);
	}

	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the collection-cursor. Otherwise it returns
	 * <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the collection-cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return true;
	}
}
