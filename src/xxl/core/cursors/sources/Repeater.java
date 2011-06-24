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

import xxl.core.cursors.AbstractCursor;

/**
 * A repeater reiterates a given object <tt>n</tt> times. The argument
 * <tt>times</tt> may be specified by the caller, so the given object will be
 * delivered by calls to the <tt>next</tt> method <tt>times</tt> times. Otherwise
 * the same object will infinitely be returned to the caller, when invoking the
 * <tt>next</tt> method. In that case the <tt>hasNext</tt> method never returns
 * <tt>false</tt>.
 * 
 * <p><b>Example usage (1):</b>
 * <pre>
 *     Repeater repeater = new Repeater(new Integer(1));
 * 
 *     repeater.open();
 * 
 *     for (int i = 0; i &lt; 10; i++)
 *         System.out.print(repeater.next() + "; ");
 *     System.out.flush();
 *     System.out.println();
 * 
 *     repeater.close();
 * </pre>
 * This instance of a repeater returns an integer instance with value '1'
 * infinitely often. With the intention to abort this operator, a for-loop is
 * used returning only the first ten elements. In consequence the integer value
 * is printed to the output stream for ten times.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <pre>
 *     repeater = new Repeater(new Integer(1), 10);
 * 
 *     repeater.open();
 * 
 *     while (repeater.hasNext())
 *         System.out.print(repeater.next() + "; ");
 *     System.out.flush();
 *     System.out.println();
 * 
 *     repeater.close();
 * </pre>
 * This example shows a repeater with a given integer instance with value '1',
 * which is returned for ten times, because in this case the user specified the
 * parameter <tt>times</tt> in the constructor. The output is equal to that of
 * example(1).</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class Repeater<E> extends AbstractCursor<E> {

	/**
	 * The object repeatedly returned to the caller.
	 */
	protected E object;

	/**
	 * A flag to signal if the object was exchanged.
	 */
	protected boolean replaced;

	/**
	 * The number of times the object should be repeated.
	 */
	protected int times;
	
	/**
	 * The number of times the object is still to be repeated.
	 */
	protected int left;

	/**
	 * A flag to signal if the object should be repeated infinitely often.
	 */
	protected boolean unlimited;

	/**
	 * Creates a new repeater. This repeater reiterates the input element only
	 * for a finite number of times.
	 *
	 * @param object the element that should repeatedly be returned to the
	 *        caller.
	 * @param times the number of times the element should be repeated.
	 */
	public Repeater(E object, int times) {
		this.object = object;
		this.left = this.times = times;
		this.unlimited = false;
		this.replaced = true;
	}

	/**
	 * Creates a new repeater. This repeater reiterates the input element
	 * infinitely often.
	 *
	 * @param object the element that should repeatedly be returned to the
	 *        caller.
	 */
	public Repeater(E object) {
		this.object = object;
		this.unlimited = true;
		this.replaced = true;
	}

	/**
	 * Returns <tt>true</tt> if the iteration has more elements. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
	 * return an element rather than throwing an exception.)
	 * 
	 * @return <tt>true</tt> if the repeater has more elements.
	 */
	protected boolean hasNextObject() {
		return unlimited ? true : left > 0;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the repeater's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		if (!unlimited)
			left--;
		if (replaced) 
			replaced = false;
		return object;
	}

	/**
	 * Replaces the last element returned by the repeater in the underlying data
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
	 *        repeater.
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>update</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>update</tt> operation is
	 *         not supported by the repeater.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		super.update(object);
		this.object = object;
		replaced = true;
	}

	/**
	 * Returns <tt>true</tt> if the <tt>update</tt> operation is supported by
	 * the repeater. Otherwise it returns <tt>false</tt>.
	 * 
	 * @return <tt>true</tt> if the <tt>update</tt> operation is supported by
	 *         the repeater, otherwise <tt>false</tt>.
	 */
	public boolean supportsUpdate() {
		return true;
	}

	/**
	 * Resets the repeater to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * repeater (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <tt>reset</tt> operation is
	 *         not supported by the repeater.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		if (!unlimited)
			left = times;
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 * the repeater. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 *         the repeater, otherwise <tt>false</tt>.
	 */
	public boolean supportsReset() {
		return true;
	}
}
