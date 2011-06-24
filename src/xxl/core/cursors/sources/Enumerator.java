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
 * An enumerator returns an ascending or descending sequence of integer objects
 * within an optional given range. There are three different ways to generate
 * integer objects with an enumerator:
 * <ul>
 *     <li>
 *         Specifying a range, i.e., the start- and end-position are user
 *         defined.
 *     </li>
 *     <li>
 *         Specifying only the end-position; <code>start&nbsp;=Bnbsp;0</code>.
 *     </li>
 *     <li>
 *         Specifying no range; <code>start&nbsp;=&nbsp;0</code>,
 *         <code>end&nbsp;=&nbsp;{@link java.lang.Integer#MAX_VALUE}</code>.
 *     </li>
 * </ul>
 * In the first case an ascending or a descending sequence can be generated
 * depending on the given start- and end-position.<br />
 * <b>Note:</b> The start-position of the integer sequence will be returned by
 * the enumerator, but not the end-position! So only the integer elements of
 * the interval [<code>start</code>, <code>end</code>) will be returned by the
 * enumerator.
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *     Enumerator enumerator = new Enumerator(0, 11);
 * 
 *     enumerator.open();
 * 
 *     while (enumerator.hasNext())
 *         System.out.println(enumerator.next());
 * 
 *     enumerator.close();
 * </pre></code>
 * This example prints the numbers 0,...,10 to the output stream.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *     enumerator = new Enumerator(10, -1);
 * 
 *     enumerator.open();
 * 
 *     while (enumerator.hasNext())
 *         System.out.println(enumerator.next());
 * 
 *     enumerator.close();
 * </pre></code>
 * This example prints the numbers 10,...,0 to the output stream.</p>
 * 
 * <p><b>Example usage (3):</b>
 * <code><pre>
 *     enumerator = new Enumerator(11);
 * 
 *     enumerator.open();
 * 
 *     while (enumerator.hasNext())
 *         System.out.println(enumerator.next());
 * 
 *     enumerator.close();
 * </pre></code>
 * This example prints the numbers 0,...,10 to the output stream using only a
 * specified end position.</p>
 *
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 */
public class Enumerator extends AbstractCursor<Integer> {

	/**
	 * The start of the returned integer sequence (inclusive).
	 */
	protected int from;

	/**
	 * The end of the returned integer sequence (exclusive).
	 */
	protected int to;

	/**
	 * The int value returned by the next call to <code>next</code> or
	 * <code>peek</code>.
	 */
	protected int nextInt;

	/**
	 * If <code>true</code> the sequence is ascending, else the sequence is
	 * descending.
	 */
	protected boolean up;

	/**
	 * Creates a new enumerator instance with a specified range, i.e., the
	 * start- and an end-position must be defined.
	 *
	 * @param from start of the returned integer sequence (inclusive).
	 * @param to end of the returned integer sequence (exclusive).
	 */
	public Enumerator(int from, int to) {
		this.from = from;
		this.to = to;
		this.up = from <= to;
		this.nextInt = from;
	}

	/**
	 * Creates a new enumerator instance with a user defined end position,
	 * i.e., the returned integer sequence starts with <code>0</code> and ends
	 * with <code>number-1</code>.
	 *
	 * @param number the end of the returned integer sequence (exclusive).
	 */
	public Enumerator(int number) {
		this(0, number);
	}

	/**
	 * Creates an enumerator instance. The returned integer sequence starts
	 * with <code>0</code> and ends with
	 * <code>{@link java.lang.Integer#MAX_VALUE}-1</code>.
	 */
	public Enumerator() {
		this(0, Integer.MAX_VALUE);
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the enumerator has more elements.
	 */
	protected boolean hasNextObject() {
		return up ? nextInt < to : nextInt > to;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the enumerator's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected Integer nextObject() {
		return up ? nextInt++ : nextInt--;
	}

	/**
	 * Resets the enumerator to its initial state such that the caller is able
	 * to traverse the iteration again without constructing a new enumerator
	 * (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the enumerator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		nextInt = from;
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the enumerator. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the enumerator, otrwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return true;
	}
}
