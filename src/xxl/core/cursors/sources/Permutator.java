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

//must be imported in order to link this class in javadoc ???
import java.util.Random;

import xxl.core.cursors.AbstractCursor;

/**
 * The permutator 'returns' a permutation of a given input array or creates a
 * permutation of integer values from 0 to <code>n</code> by lazy evaluation.
 * With the help of this class the caller is able to get a permutation of every
 * kind of 1-dimensional arrays, especially integer arrays. Furthermore the
 * caller is able to specify his own pseudo random number generator, e.g., with
 * the intention to get a better permutation.
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *     Permutator permutator = new Permutator(10);
 * 
 *     permutator.open();
 * 
 *     while (permutator.hasNext())
 *         System.out.print(permutator.next() + "; ");
 *     System.out.flush();
 *     System.out.println();
 * 
 *     permutator.close();
 * </pre></code>
 * This example permutes the integer numbers of the interval [0,10[. Therefore
 * a new integer array is created. In this case {@link java.util.Random Java}'s
 * random number generator generates a stream of pseudo random numbers. The
 * class uses a 48-bit seed, which is modified using a linear congruential
 * formula. (See Donald Knuth, <i>The Art of Computer Programming,
 * Volume&nbsp;2</i>, Section&nbsp;3.2.1.) If two instances of the class
 * {@link java.util.Random} are created with the same seed, and the same
 * sequence of method calls is made for each, they will generate and return
 * identical sequences of numbers. In order to guarantee this property,
 * particular algorithms are specified for the class <code>Random</code>. Each
 * call to <code>next</code> returns one random element of the array. After
 * completely consuming the permutator the result is a complete permutation of
 * the given array.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *     permutator = new Permutator(new int[10], new SecureRandom());
 * 
 *     permutator.open();
 * 
 *     while (permutator.hasNext())
 *         System.out.print(permutator.next() + "; ");
 *     System.out.flush();
 *     System.out.println();
 * 
 *     permutator.close();
 * </pre></code>
 * A new permutator is created with the help of a given array containing the
 * integers 0,...,9 and a {@link java.security.SecureRandom secure} random
 * number generator. In this case the <code>next</code> method calls the
 * <code>nextInt</code> method of the class {@link java.security.SecureRandom}.
 * This class provides a cryptographically strong pseudo-random number
 * generator (PRNG). After consuming the whole permutator the result is a
 * complete permutation of the given array.</p>
 *
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see java.util.Random
 * @see java.security.SecureRandom
 */
public class Permutator extends AbstractCursor<Integer> {

	/**
	 * The array the permutation should be fulfilled with.
	 */
	protected int[] array;

	/**
	 * An index used to save an array's position.
	 */
	protected int index = 0;

	/**
	 * The random number generator wrapper delivering integer values according
	 * to the wrapped PRNG.
	 */
	protected Random random;
	
	private boolean newArray = false;

	/**
	 * Creates a new permutator backed on the given array using the given
	 * pseudo random number generator (PRNG).
	 *
	 * @param array the array the permutation should be fulfilled with.
	 * @param random the pseudo random number generator (PRNG).
	 */
	public Permutator(int[] array, Random random) {
		this.array = array;
		this.random = random;
		this.newArray = false; 
	}

	/**
	 * Creates a new permutator backed on the given array. As default pseudo
	 * random number generator
	 * {@link xxl.core.util.random.JavaDiscreteRandomWrapper} is used.
	 *
	 * @param array the array the permutation should be fulfilled with.
	 */
	public Permutator(int[] array) {
		this(array, new Random());
	}

	/**
	 * Creates a new permutator by creating an int array using the given
	 * parameter <code>n</code> as the array's length. Therefore the
	 * permutator's range is [0,n[.
	 *
	 * @param n the integer constant defining the length of an integer array,
	 *        the permutation should be fulfilled with.
	 * @param random the pseudo random number generator (PRNG).
	 */
	public Permutator(int n, Random random) {
		this(new int[n], random);
		this.newArray = true; 
	}

	/**
	 * Creates a new permutator by creating an integer array using the given
	 * parameter <code>n</code> as the array's length.
	 * {@link xxl.core.util.random.JavaDiscreteRandomWrapper} is used as pseudo
	 * random number generator.
	 *
	 * @param n the integer constant defining the length of an integer array,
	 *        the permutation should be fulfilled with.
	 */
	public Permutator(int n) {
		this(n, new Random());
	}
	
	/**
	 * Opens the permutator, i.e., signals it to reserve resources, initialize
	 * the intern array, etc. Before a cursor has been opened calls to methods
	 * like <code>next</code> or <code>peek</code> are not guaranteed to yield
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
		if (!isOpened && newArray)
			for (int i = 0; i < array.length; i++)
				array[i] = i;
		super.open();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the permutator has more elements.
	 */
	protected boolean hasNextObject() {
		return index < array.length;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the permutator's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * <p>Each element of the array will be returned only for one time. Every
	 * call to this method swaps two elements in the underlying array: One
	 * position to be swapped depends on the given random number generator and
	 * is computed with
	 * <code><pre>
	 *     index + random.nextInt(array.length - index)
	 * </pre></code>
	 * The uniformly distributed <code>int</code> value delivered by random's
	 * <code>nextInt</code> method lies between 0 (inclusive) and the specified
	 * <code>array.length-index</code> (exclusive). The position to be swapped
	 * is determined by adding <code>index</code> to this random
	 * <code>int</code> value. Thereafter the element located at this position
	 * is swapped with <code>array[index]</code>.</p>
	 *
	 * @return the next element in the iteration.
	 */
	protected Integer nextObject() {
		int randomIndex = index + random.nextInt(array.length - index);
		int next = array[randomIndex];
		array[randomIndex] = array[index];
		return array[index++] = next;
	}
}
