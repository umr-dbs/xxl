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

package xxl.core.util.random;

import java.util.Random;

/**
 *	Default implementation of RandomWrapper-interface. This class
 *	uses the Random-generator provided by Java.
 *	@see java.util.Random
 */

public class JavaDiscreteRandomWrapper implements DiscreteRandomWrapper {

	/** internally used pseudo random number generator */
	protected Random random;
	
	/** the upper bound for the randomly generated discrete numbers */
	protected int maxValue;

	/** Constructs a new wrapper for the given Random object returning int
	 * values between <code>0</code> (inclusive) and <code>maxValue</code>
	 * (exclusive). If the given int is negative or zero, it is ignored.
	 * @param random pseudo random number generator.
	 * @param maxValue the upper bound for the randomly generated discrete
	 * numbers.
	 */
	public JavaDiscreteRandomWrapper(Random random, int maxValue) {
		this.random = random;
		this.maxValue = maxValue;
	}

	/** Constructs a new wrapper for the given Random object.
	 * @param random pseudo random number generator.
	 */
	public JavaDiscreteRandomWrapper(Random random) {
		this(random, -1);
	}

	/** Constructs a new wrapper for the pseudo
	 * random number generator provided by java using the given seed returning
	 * int values between <code>0</code> (inclusive) and <code>maxValue</code>
	 * (exclusive). If the given int is negative or zero, it is ignored.
	 * @param seed parameter for random number generator 
	 * @param maxValue the upper bound for the randomly generated discrete
	 * numbers.
	 */
	public JavaDiscreteRandomWrapper(long seed, int maxValue) {
		this(new Random(seed), maxValue);
	}

	/** Constructs a new wrapper for the pseudo
	 * random number generator provided by java using the given seed.
	 * @param seed parameter for random number generator 
	 */
	public JavaDiscreteRandomWrapper(long seed) {
		this(seed, -1);
	}

	/** Constructs a new wrapper for the pseudo
	 * random number generator provided by java returning int values between
	 * <code>0</code> (inclusive) and <code>maxValue</code> (exclusive). If the
	 * given int is negative or zero, it is ignored.
	 * @param maxValue the upper bound for the randomly generated discrete
	 * numbers.
	 */
	public JavaDiscreteRandomWrapper(int maxValue) {
		this(new Random(), maxValue);
	}

	/** Constructs a new wrapper for the pseudo
	 * random number generator provided by java.
	 */
	public JavaDiscreteRandomWrapper() {
		this(-1);
	}

	/** Returns the next pseudo-random, uniformly distributed integer value
	 * between 0.0 and Integer.MAX_VALUE from the java
	 * random number generator's sequence.
	 * The method nextInt is implemented as follows: 
	 * <br><br>
	 * <code><pre>
	public int nextInt(){
		return Math.abs(random.nextInt());
	}
	 * </code></pre><br>
	 * @return the next pseudorandom, uniformly distributed
	 * integer value between 0 and {@link java.lang.Integer#MAX_VALUE Integer.MAX_VALUE}
	 * from the java random number generator's sequence.
	 * @see java.util.Random#nextInt() nextInt()
	 * @see xxl.core.util.random.DiscreteRandomWrapper
	 */
	public int nextInt(){
		return Math.abs(maxValue > 0 ? random.nextInt(maxValue) : random.nextInt());
	}
}
