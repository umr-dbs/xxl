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

import xxl.core.cursors.SecureDecoratorCursor;

/**
 * This class provides a decorator for a given iteration that cannot be modified.
 * The methods of this class call the corresponding methods of the internally
 * stored iteration except the methods that modify the iteration. These methods
 * (<tt>remove</tt> and <tt>update</tt>) throws an
 * <tt>UnsupportedOperationException</tt>.
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class UnmodifiableCursor<E> extends SecureDecoratorCursor<E> {

	/**
	 * Creates a new unmodifiable cursor that wraps the given iteration.
	 * 
	 * @param iterator the interation that should be made unmodifiable.
	 */
	public UnmodifiableCursor(Iterator<E> iterator) {
		super(iterator);
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>remove</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>remove</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public final boolean supportsRemove() {
		return false;
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>update</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>update</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public final boolean supportsUpdate() {
		return false;
	}

}
