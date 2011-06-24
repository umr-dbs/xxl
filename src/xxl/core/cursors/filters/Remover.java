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

package xxl.core.cursors.filters;

import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.cursors.SecureDecoratorCursor;

/**
 * The remover removes all elements of a given input iteration in a lazy way,
 * i.e., the <code>remove</code> method is called for all elements of the input
 * iterator that are returned by a call to the <code>next</code> method. Each
 * call to the input iteration's <code>next</code> method implies that its
 * <code>remove</code> is called thereafter. So the element returned by
 * <code>next</code> is automatically removed from the underlying data
 * structure. Therefore the caller is not able to traverse the underlying
 * collection again by invoking the <code>reset</code> method.
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor.</p>
 * 
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class Remover<E> extends SecureDecoratorCursor<E> {

	/**
	 * Creates a new remover which removes every element of the input iteration
	 * that is returned by its <code>next</code> method.
	 * 
	 * @param iterator the input iteration which elements should be removed.
	 */
	public Remover(Iterator<E> iterator) {
		super(iterator);
	}

	/**
	 * Returns the next element in the iteration. This element will <b>not</b>
	 * be accessible by some of the cursor's methods, e.g., <code>update</code>
	 * or <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs, because it is removed from the underlying data
	 * structure at the same time.
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public E next() throws IllegalStateException, NoSuchElementException {
		E next = super.next();
		super.remove();
		return next;
	}
	
}
