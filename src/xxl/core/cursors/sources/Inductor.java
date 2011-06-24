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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.FunctionPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.Predicates;

/**
 * Given a tuple of start values (<tt>a<sub>0</sub></tt> to
 * <tt>a<sub>n</sub></tt>) the inductor incrementally computes the value
 * <tt>a<sub>(n+1+i)</sub></tt> (<tt>i</tt> &ge; 0) by calling the function
 * <code>next</code> with the parameters <tt>a<sub>i</sub></tt> to
 * <tt>a<sub>(n+i)</sub></tt>. Optionally, the user can specify the predicate
 * <code>hasNext</code> (called with parameters <tt>a<sub>i</sub></tt> to
 * <tt>a<sub>(n+i)</sub></tt>) which determines whether a next value exists.
 * With the help of an inductor a sequence of elements can be computed
 * represented as an iteration.
 * 
 * <p><b>Example usage (1): the <i>Fibonacci</i> sequence</b>
 * <code><pre>
 *   Inductor&lt;Integer&gt; fibonacci = new Inductor&lt;Integer&gt;(
 *       new Function&lt;Integer, Integer&gt;() {
 *           public Integer invoke(Integer fib_1, Integer fib_2) {
 *               return fib_1 + fib_2;
 *           }
 *       },
 *       1,
 *       1
 *   );
 *   
 *   fibonacci.open();
 *   
 *   System.out.println(Cursors.nth(fibonacci, 7));
 *   
 *   fibonacci.close();
 * </pre></code>
 * This example computes the 7th number of the fibonacci sequence by
 * initializing the inductor with two integer objects with value 1 (beginning
 * of the induction sequence; <tt>n</tt> = 1, 2). All following elements are
 * computed with the following function:
 * <pre>
 *   fib(n) = fib(n-1) + fib(n-2);
 * </pre>
 * <b>Implemenation details:</b> If the next element is available, i.e., the
 * flag <code>nextAvailable</code> is <code>true</code>, the function
 * <code>next</code> is invoked on the list <code>objects</code>. This list is
 * shifted left for one position by removing the first value. Then the returned
 * object is added to this list as the last element with regard to the further
 * computation of the inductor sequence. The first element of the list
 * <code>objects</code> is returned, because this is the next element that has
 * be returned in the inductor sequence. So this inductor would return the next
 * number of the fibonacci sequence as long as the <code>next</code> method is
 * called. In this case the static method <code>nth()</code> is used to return
 * only the 7th number of the sequence, but also an other function
 * <code>hasNext</code> can be specified in a constructor with the intention to
 * terminate such an inductor sequence.</p>
 * 
 * <p><b>Example usage (2): the factorial method</b>
 * <code><pre>
 *   Inductor&lt;Integer&gt; factorial = new Inductor&lt;Integer&gt;(
 *       new Function&lt;Integer, Integer&gt;() {
 *           int factor = 1;
 *           
 *           public Integer invoke(Integer n) {
 *               return n * factor++;
 *           }
 *       },
 *       1
 *   );
 *   
 *   factorial.open();
 *   
 *   System.out.println(Cursors.nth(factorial, 3));
 *   
 *   factorial.close();
 * </pre></code>
 * This example computes the 3rd number of the factorial method (3! = 6). The
 * induction is initialized by one integer object with value 1
 * (<tt>n</tt> = 0). Further elements are iteratively computed by invoking the
 * above defined function:
 * <pre>
 *   fac(n) = n * fac(n-1);
 * </pre>
 * But the factor, i.e., the next integer to multiply the next element
 * <tt>fac(n-1)</tt> by, which is written back to the array, has to be stored
 * (in the anonymous class). For further examples see
 * {@link xxl.core.cursors.sources.Inductors}.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see xxl.core.cursors.sources.Inductors
 * @see xxl.core.predicates.Predicate
 * @see xxl.core.functions.Function
 */
public class Inductor<E> extends AbstractCursor<E> {

	/**
	 * The predicate that is used to determine an end of the inductor sequence.
	 */
	protected Predicate<? super E> hasNext;

	/**
	 * The function that is used to compute the next element of the induction
	 * sequence.
	 */
	protected Function<? super E, ? extends E> next;

	/**
	 * A list storing the parameters <tt>a<sub>i</sub></tt> to
	 * <tt>a<sub>(n+i)</sub></tt>] required for the computation of the value
	 * <tt>a<sub>(n+1+i)</sub></tt>.
	 */
	protected List<E> objects;

	/**
	 * The number of values of the induction sequence that are available from
	 * the list <code>objects</code>.
	 */
	protected int available;

	/**
	 * A flag to signal if the next element in the sequence is available. The
	 * implementation used to set this flag is as follows:
	 * <code><pre>
	 *   nextAvailable = objects.size()==available-- && hasNext.invoke(objects);
	 * </pre></code>
	 * So the length of the <code>objects</code> list has to be the
	 * start-length, namely <code>available</code>, and the predicate
	 * <code>hasNext</code> must be <code>true</code>.
	 */
	protected boolean nextAvailable = false;

	/**
	 * Creates a new inductor with a user defined predicate delivering the end
	 * of the induction sequence.
	 *
	 * @param hasNext the predicate which determines whether a next value
	 *        exists.
	 * @param next the function using parameters <tt>a<sub>i</sub></tt> to
	 *        <tt>a<sub>(n+i)</sub></tt> (<tt>i</tt> &ge; 0) to compute the
	 *        result-value <tt>a<sub>(n+1+i)</sub></tt>.
	 * @param objects the start-values <tt>a<sub>0</sub></tt> to
	 *        <tt>a<sub>n</sub></tt> of the induction sequence.
	 */
	public Inductor(Predicate<? super E> hasNext, Function<? super E, ? extends E> next, E... objects) {
		this.hasNext = hasNext;
		this.next = next;
		this.objects = new LinkedList<E>();
		this.available = objects.length;
		Collections.addAll(this.objects, objects);
	}

	/**
	 * Creates a new inductor with a user defined boolean function delivering
	 * the end of the induction sequence. The boolean function
	 * <code>hasNext</code> is internally wrapped to a predicate.
	 *
	 * @param hasNext the boolean function which determines whether a next
	 *        value exists.
	 * @param next the function using parameters <tt>a<sub>i</sub></tt> to
	 *        <tt>a<sub>(n+i)</sub></tt> (<tt>i</tt> &ge; 0) to compute the
	 *        result-value <tt>a<sub>(n+1+i)</sub></tt>.
	 * @param objects the start-values <tt>a<sub>0</sub></tt> to
	 *        <tt>a<sub>n</sub></tt> of the induction sequence.
	 */
	public Inductor(Function<? super E, Boolean> hasNext, Function<? super E, ? extends E> next, E... objects) {
		this(new FunctionPredicate<E>(hasNext), next, objects);
	}

	/**
	 * Creates a new inductor delivering an infinite induction sequence, i.e.,
	 * the predicate which determines whether a next value exists is given by
	 * {@link xxl.core.predicates.Predicates#TRUE}.
	 *
	 * @param next the function using parameters <tt>a<sub>i</sub></tt> to
	 *        <tt>a<sub>(n+i)</sub></tt> (<tt>i</tt> &ge; 0) to compute the
	 *        result-value <tt>a<sub>(n+1+i)</sub></tt>.
	 * @param objects the start-values <tt>a<sub>0</sub></tt> to
	 *        <tt>a<sub>n</sub></tt> of the induction sequence.
	 */
	public Inductor(Function<? super E, ? extends E> next, E... objects) {
		this(Predicates.TRUE, next, objects);
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the inductor has more elements.
	 */
	@Override
	protected boolean hasNextObject() {
		return nextAvailable || available > 0;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the inductor's methods, e.g., <code>update</code>
	 * or <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	@Override
	protected E nextObject() {
		if (available < objects.size()) {
			E nextObject = null;
			if (nextAvailable)
				nextObject = next.invoke(objects);
			objects.remove(0);
			if (nextAvailable) {
				objects.add(nextObject);
				available++;
			}
		}
		nextAvailable = objects.size()==available-- && hasNext.invoke(objects);
		return objects.get(0);
	}
}
