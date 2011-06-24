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
import xxl.core.util.random.DiscreteRandomWrapper;
import xxl.core.util.random.JavaDiscreteRandomWrapper;

/**
 * A cursor providing a finite or finite stream of randomly distributed integer
 * values given by the wrapped <i>pseudo random number generator</i>
 * {@link xxl.core.util.random.DiscreteRandomWrapper} for discrete numbers. XXL
 * provides two kinds of PRNG, namley, the first based on
 * {@link java.util.Random Java}'s random number generator and the second based
 * on Colt's random number generator
 * (<code>cern.jet.random.AbstractDiscreteDistribution</code>).
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *     DiscreteRandomNumber drn = new DiscreteRandomNumber(
 *         new JavaDiscreteRandomWrapper(),
 *         200
 *     );
 * 
 *     drn.open();
 * 
 *     long i = 0;
 *     while (drn .hasNext())
 *         System.out.println((i++) + "\t:\t" + drn.next());
 *     System.out.println();
 * 
 *     drn.close();
 * </pre></code>
 * This example produces a finite stream of randomly distributed integer values
 * delivered by a {@link xxl.core.util.random.JavaDiscreteRandomWrapper PRNG},
 * i.e., Java's random number generator is used for the provision of randomly
 * distributed integer values. The returned stream of integer objects contains
 * 200 elements as specified in the constructor.</p>
 * 
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see java.util.Random
 */
public class DiscreteRandomNumber extends AbstractCursor<Integer> {

	/**
	 * The random number generator wrapper delivering integer values according
	 * to the wrapped PRNG.
	 */
	protected DiscreteRandomWrapper discreteRandomWrapper;

	/**
	 * A flag signaling if the returned stream of integer values should be
	 * infinite.
	 */
	protected boolean infiniteStream;

	/**
	 * If the returned stream of integer elements is finite, this long value
	 * defines the number of elements to be returned.
	 */
	protected long numberOfElements;

	/**
	 * An internal used counter counting the elements that are yet returned by
	 * this iteration.
	 */
	protected long count;

	/**
	 * Constructs a new discrete random number cursor delivering randomly
	 * distributed discrete numbers (integer values). If the given number of
	 * elements is greater than or equal to <code>0</code> the cursor provides
	 * a <b>finite</b> number of elements otherwise an <b>infinite</b> number
	 * of elements is provided.
	 *
	 * @param discreteRandomWrapper the random number generator wrapper to be
	 *        used for the provision of random integer values.
	 * @param numberOfElements the number of elements to be returned. If the
	 *        given number is greater than or equal to <code>0</code> the
	 *        cursor provides a <b>finite</b> number of elements otherwise an
	 *        <b>infinite</b> number of elements is provided.
	 */
	public DiscreteRandomNumber(DiscreteRandomWrapper discreteRandomWrapper, long numberOfElements) {
		this.discreteRandomWrapper = discreteRandomWrapper;
		this.infiniteStream = numberOfElements < 0;
		this.numberOfElements = numberOfElements;
		this.count = 0;
	}

	/**
	 * Constructs a new discrete random number cursor delivering an
	 * <b>infinite</b> number of randomly distributed discrete numbers (integer
	 * values).
	 *
	 * @param discreteRandomWrapper the random number generator wrapper to be
	 *        used for the provision of random integer values.
	 */
	public DiscreteRandomNumber(DiscreteRandomWrapper discreteRandomWrapper) {
		this(discreteRandomWrapper, -1);
	}

	/**
	 * Constructs a new discrete random number cursor delivering randomly
	 * distributed discrete numbers (integer values). If the given number of
	 * elements is greater than or equal to <code>0</code> the cursor provides
	 * a <b>finite</b> number of elements otherwise an <b>infinite</b> number
	 * of elements is provided. Java's random number generator is is used for
	 * the provision of random integer values.
	 *
	 * @param numberOfElements the number of elements to be returned. If the
	 *        given number is greater than or equal to <code>0</code> the
	 *        cursor provides a <b>finite</b> number of elements otherwise an
	 *        <b>infinite</b> number of elements is provided.
	 */
	public DiscreteRandomNumber(long numberOfElements) {
		this(new JavaDiscreteRandomWrapper(), numberOfElements);
	}

	/**
	 * Constructs a new discrete random number cursor delivering an
	 * <b>infinite</b> number of randomly distributed discrete numbers (integer
	 * values). Java's random number generator is is used for the provision of
	 * random integer values.
	 */
	public DiscreteRandomNumber() {
		this(new JavaDiscreteRandomWrapper());
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the discrete random number cursor has more
	 *         elements.
	 */
	protected boolean hasNextObject() {
		return infiniteStream || ++count <= numberOfElements;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the discrete random number cursor's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected Integer nextObject() {
		return discreteRandomWrapper.nextInt();
	}

	/**
	 * Resets the discrete random number cursor to its initial state such that
	 * the caller is able to traverse the iteration again without constructing
	 * a new discrete random number cursor (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the discrete random number cursor.
	 */
	public void reset() {
		super.reset();
		this.count = 0;
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the discrete random number cursor. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the discrete random number cursor, otherwise
	 *         <code>false</code>.
	 */
	public boolean supportsReset() {
		return true;
	}
}
