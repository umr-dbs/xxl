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

import xxl.core.cursors.SecureDecoratorCursor;

/**
 * A dropper drops a user defined number of the given iterator's elements. The
 * implementation of the method <code>drop</code> is as follows:
 * <code><pre>
 *   while (number--&gt;0 && super.hasNext());
 *       super.next();
 * </pre></code>
 * The first <code>number</code> elements are removed from the wrapped
 * iteration, because the <code>next</code> method is called<code>number</code>
 * times.
 * 
 * <p><b>Example usage:</b>
 * <code><pre>
 *   Dropper&lt;Integer&gt; dropper = new Dropper&lt;Integer&gt;(new Enumerator(11), 5);
 *   
 *   dropper.open();
 *   
 *   while(dropper.hasNext())
 *       System.out.println(dropper.next());
 *       
 *   dropper.close();
 * </pre></code>
 * This example creates a new dropper by using an
 * {@link xxl.core.cursors.sources.Enumerator enumerator} delivering integer
 * elements from 0 to 10, where the first five elements are dropped. Therefore
 * the generated output is:
 * <pre>
 *     5
 *     6
 *     7
 *     8
 *     9
 *     10
 * </pre></p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class Dropper<E> extends SecureDecoratorCursor<E> {

	/**
	 * The initial number of elements to be dropped.
	 */
	protected int initialNumber;

	/**
	 * The number of elements that still have to be dropped.
	 */
	protected int number;

	/**
	 * Creates a new dropper that drops the first <code>number</code> elements
	 * of the given iteration.
	 *
	 * @param iterator the input iterator the elements are dropped of.
	 * @param number the number of elements to be dropped.
	 */
	public Dropper(Iterator<E> iterator, int number) {
		super(iterator);
		this.number = initialNumber = number;
	}

	/**
	 * Dropping the first <code>number</code> elements of the wrapped
	 * iteration.
	 */
	public void drop() {
		while (number-->0 && super.hasNext())
			super.next();
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
		if (isOpened) return;
		super.open();
		drop();
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
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		number = initialNumber;
		drop();
	}
}
