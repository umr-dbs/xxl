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
 * A taker is a {@link SecureDecoratorCursor decorator-cursor} that returns the
 * next <code>number</code> elements of a given iteration. The
 * <code>number</code> of elements that should be delivered from the given
 * iteration is defined by the user as well as the input iteration the elements
 * should be taken from.
 * 
 * <p><b>Implementation details:</b> The taker only contains further elements,
 * i.e., its <code>hasNext</code> method returns <code>true</code>, if the
 * underlying iteration has more elements and the number of elements returned
 * by the taker is lower than the given <code>number</code>. If
 * <code>next</code> is called the next element in the iteration is returned
 * and the <code>number</code> of elements to be delivered from the underlying
 * input iteration is decremented.</p>
 * 
 * <p><b>Note:</b> If the input iteration is given by an object of the class
 * {@link java.util.Iterator}, i.e., it does not support the <code>peek</code>
 * operation, it is internally wrapped to a cursor.</p>
 *
 * <p><b>Example usage:</b>
 * <code><pre>
 *   Taker&lt;Integer&gt; taker = new Taker&lt;Integer&gt;(new Enumerator(11), 5);
 *   
 *   taker.open();
 *   
 *   while (taker.hasNext())
 *       System.out.print(taker.next() +"; ");
 *   System.out.flush();
 *   
 *   taker.close();
 * </pre></code>
 * This instance of a taker delivers the first five elements of the given
 * {@link xxl.core.cursors.sources.Enumerator enumerator} which contains the
 * elements 0,...,10. So the output that results after consuming the entire
 * taker is:
 * <pre>
 *   0; 1; 2; 3; 4;
 * </pre></p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class Taker<E> extends SecureDecoratorCursor<E> {

	/**
	 * The number of elements that still have to be delivered.
	 */
	protected long number;

	/**
	 * The initial number of elements to be delivered.
	 */
	protected long initialNumber;

	/**
	 * Creates a new taker.
	 *
	 * @param iterator the iterator which contains the elements.
	 * @param number the number of elements which should be delivered from the
	 *        given iterator.
	 */
	public Taker(Iterator<E> iterator, long number) {
		super(iterator);
		this.number = initialNumber = number;
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
	 * <p>In this case <code>hasNext</code> returns <code>true</code> if the
	 * underlying iteration has more elements and the number of elements
	 * returned by the taker is lower than the given <code>number</code>.</p>
	 *
	 * @return <code>true</code> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public boolean hasNext() throws IllegalStateException {
		return super.hasNext() && number>0;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * <p>In this case the number of elements that still have to be returned is
	 * decremented.</p>
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public E next() throws IllegalStateException, NoSuchElementException {
		number--;
		return super.next();
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
	 * <p>In this case the input iteration has to be reset and the number of
	 * elements that have to be returned by this taker has to be set to its
	 * initial value.</p>
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public void reset () throws UnsupportedOperationException {
		super.reset();
		number = initialNumber;
	}
}
