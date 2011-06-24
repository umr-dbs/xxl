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

import java.util.Arrays;

import xxl.core.functions.AbstractFunction;
import xxl.core.predicates.AbstractPredicate;

/**
 * This class provides ready to use
 * {@link xxl.core.cursors.sources.Inductor inductors}. The static methods
 * implemented in this class return an inductor, each realizing a particular
 * kind of an inductive algorithm. The following inductors are supported:
 * <ul>
 *     <li>
 *         natural numbers
 *     </li>
 *     <li>
 *         factorial numbers
 *     </li>
 *     <li>
 *         fibonacci numbers
 *     </li>
 *     <li>
 *         n-dimensional counters
 *     </li>
 * </ul>
 * 
 * <p><b>Example usage (1): n-dimensional counter</b>
 * <code><pre>
 *   Cursor&lt;int[]&gt; counter = nDimCounter(new int[] {6, 7, 2});
 *   
 *   counter.open();
 *   
 *   while (counter.hasNext())
 *       System.out.println(Strings.toString(counter.next()));
 *   
 *   counter.close();
 * </pre></code>
 * An extract of the output demonstrates the kind this counter works:
 * <pre>
 *   0 0 0
 *   1 0 0
 *   2 0 0
 *   3 0 0
 *   4 0 0
 *   5 0 0
 *   0 1 0
 *   1 1 0
 *   2 1 0
 *   3 1 0
 *   4 1 0
 *   5 1 0
 *   0 2 0
 *   1 2 0
 *   2 2 0
 *   3 2 0
 *   4 2 0
 *   5 2 0
 *   0 3 0
 *   1 3 0
 *   2 3 0
 *   3 3 0
 *   4 3 0
 *   5 3 0
 *   0 4 0
 *   ...
 * </pre></p>
 * 
 * <p><b>Example usage (2): natural numbers</b>
 * <code><pre>
 *   int f = 3;
 *   int t = 78;
 *   
 *   Cursor&lt;Long&gt; naturalNumbers = naturalNumbers(f, t);
 *   
 *   naturalNumbers.open();
 *   
 *   while (naturalNumbers.hasNext())
 *       System.out.println(naturalNumbers.next());
 *   
 *   naturalNumbers.close();
 * </pre></code>
 * This example prints the natural numbers of the interval [3, 78] to the
 * standard output stream.
 *
 * @see xxl.core.cursors.sources.Inductor
 */
public class Inductors {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Inductors() {}

	/**
	 * This methods returns an
	 * {@link xxl.core.cursors.sources.Inductor inductor} delivering objects of
	 * type <code>int[]</code>. This int-arrays could be seen as a
	 * n-dimensional counter consisting of n counters, each for every
	 * dimension.
	 *
	 * @param cardinal cardinality for each dimension. That means every
	 *        returned counter object <code>c</code> holds:
	 *        <pre>
	 *          0 &le; c[i] &lt; cardinal[i], i = 0,...,cardinal.length
	 *        </pre>
	 * @return an inductor delivering counting objects.
	 */
	public static Inductor<int[]> nDimCounter(final int[] cardinal) {
		final int[] start = new int[cardinal.length];
		Arrays.fill(start, 0);
		return new Inductor<int[]>(
			new AbstractPredicate<int[]>() {
				@Override
				public boolean invoke(int[] count) {
					boolean r = true;
					int numberOfEquals = 0;
					for (int i = 0; i < count.length; i++) {
						if (count[i] >= cardinal[i])
							r = false;
						if (count[i] == (cardinal[i]-1))
							numberOfEquals++;
					}
					if (numberOfEquals == count.length)
						r = false;
					return r;
				}
			},
			new AbstractFunction<int[], int[]>() {
				@Override
				public int[] invoke(int[] old) {
					int[] next = new int[cardinal.length];
					for (int i = 0 ; i < next.length; i++)
						next[i] = old[i];
					//
					int i=0;
					boolean carry = true;
					while (carry) {
						next[i]++;
						carry = false;
						if ((next[i] % cardinal[i]) == 0) { // ueberlauf
							carry = true;
							next[i] = 0;
							i++;
						}
					} // end of while
					return next;
				}
			},
			start
		);
	}

	/**
	 * This methods returns an
	 * {@link xxl.core.cursors.sources.Inductor inductor} delivering objects of
	 * type <code>Long</code> representing <tt>natural numbers</tt> in
	 * ascending order.
	 *
	 * @param from starting with this natural number.
	 * @param to ending with this natural number.
	 * @return an inductor delivering natural numbers of the given range.
	 */
	public static Inductor<Long> naturalNumbers(final long from, final long to) {
		return new Inductor<Long>(
			new AbstractPredicate<Long>() {
				long c = from;
				@Override
				public boolean invoke(Long o) {
					return c++ < to;
				}
			},
			new AbstractFunction<Long, Long>() {
				@Override
				public Long invoke(Long n) {
					return n+1;
				}
			},
			from
		);
	}

	/**
	 * This methods returns an
	 * {@link xxl.core.cursors.sources.Inductor inductor} delivering objects of
	 * type <code>Long</code> representing <tt>natural numbers</tt> in
	 * ascending order starting with 0.
	 *
	 * @param to ending with this natural number.
	 * @return an inductor delivering natural numbers of the given range.
	 */
	public static Inductor<Long> naturalNumbers(long to) {
		return naturalNumbers(0, to);
	}

	/**
	 * This methods returns an
	 * {@link xxl.core.cursors.sources.Inductor inductor} delivering objects of
	 * type <code>Long</code> representing <tt>natural numbers</tt> in
	 * ascending order starting with 0 and ending with
	 * {@link java.lang.Long#MAX_VALUE}.
	 *
	 * @return an inductor delivering natural numbers.
	 */
	public static Inductor<Long> naturalNumbers() {
		return naturalNumbers(0, Long.MAX_VALUE);
	}

	/**
	 * This methods returns an
	 * {@link xxl.core.cursors.sources.Inductor inductor} delivering objects of
	 * type <code>Integer</code> representing <tt>fibonacci numbers</tt>. The
	 * calculation rule is
	 * <pre>
	 *   fib(i) = fib(i-1) + fib(i-2)
	 * </pre>
	 *
	 * @return an inductor delivering <tt>fibonacci numbers</tt>.
	 */
	public static Inductor<Integer> fibonacci() {
		return new Inductor<Integer>(
			new AbstractFunction<Integer, Integer>() {
				@Override
				public Integer invoke(Integer fib_1, Integer fib_2) {
					return fib_1 + fib_2;
				}
			},
			1,
			1
		);
	}

	/**
	 * This methods returns an
	 * {@link xxl.core.cursors.sources.Inductor inductor} delivering objects of
	 * type <code>Integer</code> representing a sequence of factorial numbers.
	 *
	 * @return an inductor delivering factorial numbers.
	 */
	public static Inductor<Integer> factorial() {
		return new Inductor<Integer>(
			new AbstractFunction<Integer, Integer>() {
				int factor = 1;
				
				@Override
				public Integer invoke(Integer n) {
					return n * factor++;
				}
			},
			1
		);
	}
	
}
