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

import xxl.core.cursors.Cursor;
import xxl.core.functions.Function;

/**
 * The interface FIFO bag represents a FIFO (<i>first in, first out</i>)
 * iteration over the elements of a bag. This interface predefines a
 * <i>FIFO strategy</i> for addition of elements.
 *
 * @param <E> the type of the elements this bag is able to store.
 * @see Function
 */
public interface FIFOBag<E> extends Bag<E> {

	/**
	 * Returns a cursor representing a FIFO (<i>first in, first out</i>)
	 * iteration over the elements in this bag. The cursor is specifying a
	 * <i>view</i> on the elements of this bag so that closing the cursor takes
	 * no effect on the bag (e.g., not closing the bag). The behavior of the
	 * cursor is unspecified if this bag is modified while the cursor is in
	 * progress in any way other than by calling the methods of the cursor. So,
	 * when the implementation of this cursor cannot guarantee that the cursor
	 * is in a valid state after modifing the underlying bag every method of
	 * the cursor except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor representing a FIFO (<i>first in, first out</i>)
	 *         iteration over the elements in this bag.
	 */
	public abstract Cursor<E> fifoCursor();
	
}
