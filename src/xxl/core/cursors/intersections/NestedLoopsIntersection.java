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

package xxl.core.cursors.intersections;

import java.util.BitSet;
import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.Function;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;

/**
 * A nested-loops implementation of the intersection operator. The nested-loops
 * intersection operator is based on a loop iteration and therefore it has no
 * special conditions with regard to the order of the elements contained in the
 * two input iterations. The input iteration <code>input0</code> is traversed
 * in the "outer" loop (only for one time) and the input iteration
 * <code>input1</code> is repeatedly consumed in the "inner" loop (for a
 * maximum of times determined by the elements of input iteration
 * <code>input0</code>). An user defined predicate is used to decide whether
 * two elements of the input iterations are equal concerning their values.
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link Iterator} it is wrapped to a cursor by a call to the static method
 * {@link Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   NestedLoopsIntersection&lt;Integer&gt; intersection = new NestedLoopsIntersection&lt;Integer&gt;(
 *       Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).iterator(),
 *       Arrays.asList(0, 2, 4, 6, 8, 10).iterator(),
 *       new Function&lt;Object, Iterator&lt;Integer&gt;&gt;() {
 *           public Iterator&lt;Integer&gt; invoke() {
 *               return Arrays.asList(0, 2, 4, 6, 8, 10).iterator();
 *           }
 *       }
 *   );
 *   
 *   intersection.open();
 *   
 *   Cursors.println(intersection);
 *   
 *   intersection.close();
 * </pre></code>
 * The input iteration for the intersection operation are given by two
 * list-iterators. The the first one contains all integer elements of the
 * interval [0, 10]. The second one delivers only the even elements of the same
 * interval. So the intersection operator should return the same integer values
 * as contained in the second input cursor. Because an iterator is not
 * resetable, a function reseting the second input cursor has to be provided,
 * because the second input cursor is traversed in the inner loop and has to be
 * reseted as often as the first input iterator contains elements. The reseting
 * function has to implement the parameterless <code>invoke</code> method and
 * in this case it delivers a newlist iterator iterating over the second input
 * list.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *   intersection = new NestedLoopsIntersection&lt;Integer&gt;(
 *       Arrays.asList(2, 2).iterator(),
 *       Arrays.asList(2, 2, 2).iterator(),
 *       new Function&lt;Object, Iterator&lt;Integer&gt;&gt;() {
 *           public Iterator&lt;Integer&gt; invoke() {
 *               return Arrays.asList(2, 2, 2).iterator();
 *           }
 *       }
 *   );
 *   
 *   intersection.open();
 *   
 *   Cursors.println(intersection);
 *   
 *   intersection.close();
 * </pre></code>
 * The second example usage computes the intersection of the input iterations
 * {2,2} and {2,2,2}. The intersection is created in the same way as in the
 * previous example. Conforming with the correct duplicate handling the result
 * will be {2,2}.</p>
 *
 * @param <E> the type of the elements returned by this intersection.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see xxl.core.cursors.intersections.SortBasedIntersection
 * @see xxl.core.relational.cursors.NestedLoopsIntersection
 */
public class NestedLoopsIntersection<E> extends AbstractCursor<E> {

	/**
	 * The first (or "outer") input iteration of the intersection operator.
	 */
	protected Cursor<? extends E> input0;
	
	/**
	 * The second (or "inner") input iteration of the intersection operator.
	 */
	protected Cursor<? extends E> input1;
	
	/**
	 * A parameterless that resets the "inner" loop (the iteration
	 * <code>input1</code>). Such a function must be specified, if the "inner"
	 * iteration is not resetable, i.e., the <code>reset</code> method of
	 * <code>input1</code> will cause an
	 * {@link java.lang.UnsupportedOperationException}. A call to the
	 * <code>invoke</code> method of this function must deliver the "inner"
	 * iteration again, if it has to be traversed for an other time.
	 */
	protected Function<?, ? extends Iterator<? extends E>> resetInput1;

	/**
	 * A binary predicate that selects the matching tuples, i.e., the tuples
	 * that will be returned when the <code>next</code> method is called.
	 * Therfore the predicate is evaluated for an element of each input
	 * iteration. Only the tuples, where the predicate's evaluation result is
	 * <code>true</code>, have been qualified to be a result of the
	 * intersection operation.
	 */
	protected Predicate<? super E> equals;
	
	/**
	 * A boolean flag indicating whether the second input iteration should be
	 * reseted next time.
	 */
	protected boolean reset = false;
	
	/**
	 * A bit set storing for every element in the "inner" loop (the iteration
	 * <code>input1</code>) a single bit. The <code>n</code>-th bit of the bit
	 * set is set to <code>1</code> if the <code>n</code>-th element of the
	 * "inner" loop has found a matching element in the "outer" loop.
	 */
	protected BitSet bitVector = null;
	
	/**
	 * The position of the "inner" loop, i.e., if <code>position</code> is set
	 * to <code>n</code>, the <code>n</code>-th element of the iteration
	 * <code>input1</code> is actually tested for a matching.
	 */
	protected int position = 0;
	
	/**
	 * Creates a new nested-loops intersection backed on two iterations using a
	 * user defined predicate to decide whether two tuples match. This
	 * constructor also supports the handling of a non-resetable input
	 * iteration <code>input1</code>, because a parameterless function can be
	 * defined that returns this input iteration again.
	 *
	 * @param input0 the first input iteration that is traversed in the "outer"
	 *        loop.
	 * @param input1 the second input iteration that is traversed in the
	 *        "inner" loop.
	 * @param resetInput1 a parameterless function that delivers the second
	 *        input iteration again, when it cannot be reseted, i.e., the
	 *        <code>reset</code> method of <code>input1</code> will cause a
	 *        {@link java.lang.UnsupportedOperationException}. If the second
	 *        input iteration supports the <code>reset</code> operation this
	 *        argument can be set to <code>null</code>.
	 * @param equals the binary predicate evaluated for each tuple of elements
	 *        backed on one element of each input iteration in order to select
	 *        them. Only these tuples where the predicate's evaluation result
	 *        is <code>true</code> have been qualified to be a result of the
	 *        intersection operation.
	 */
	public NestedLoopsIntersection(Iterator<? extends E> input0, Iterator<? extends E> input1, Function<?, ? extends Iterator<? extends E>> resetInput1, Predicate<? super E> equals) {
		this.input0 = Cursors.wrap(input0);
		this.input1 = Cursors.wrap(input1);
		this.resetInput1 = resetInput1;
		this.equals = equals;
		
		int counter = 0;
		for (; input1.hasNext(); counter++)
			input1.next();
		bitVector = new BitSet(counter);
		resetInput1();
	}
	
	/**
	 * Creates a new nested-loops intersection backed on two iterations using a
	 * default {@link Equal equality} predicate to decide whether two tuples
	 * match. This constructor also supports the handling of a non-resetable
	 * input iteration <code>input1</code>, because a parameterless function
	 * can be defined that returns this input iteration again.
	 *
	 * @param input0 the first input iteration that is traversed in the "outer"
	 *        loop.
	 * @param input1 the second input iteration that is traversed in the
	 *        "inner" loop.
	 * @param resetInput1 a parameterless function that delivers the second
	 *        input iteration again, when it cannot be reseted, i.e., the
	 *        <code>reset</code> method of <code>input1</code> will cause a
	 *        {@link java.lang.UnsupportedOperationException}. If the second
	 *        input iteration supports the <code>reset</code> operation this
	 *        argument can be set to <code>null</code>.
	 */
	public NestedLoopsIntersection(Iterator<? extends E> input0, Iterator<? extends E> input1, Function<?, ? extends Iterator<? extends E>> resetInput1) {
		this(input0, input1, resetInput1, Equal.DEFAULT_INSTANCE);
	}
	
	/**
	 * Creates a new nested-loops intersection backed on two iterations using a
	 * user defined predicate to decide whether two tuples match. This
	 * constructor does not support the handling of a non-resetable input
	 * iteration <code>input1</code>, so the <code>reset</code> operation must
	 * be guaranteed.
	 *
	 * @param input0 the first input iteration that is traversed in the "outer"
	 *        loop.
	 * @param input1 the second input iteration that is traversed in the
	 *        "inner" loop.
	 * @param equals the binary predicate evaluated for each tuple of elements
	 *        backed on one element of each input iteration in order to select
	 *        them. Only these tuples where the predicate's evaluation result
	 *        is <code>true</code> have been qualified to be a result of the
	 *        intersection operation.
	 */
	public NestedLoopsIntersection(Iterator<? extends E> input0, Cursor<? extends E> input1, Predicate<? super E> equals) {
		this(input0, input1, null, equals);
	}
	
	/**
	 * Creates a new nested-loops intersection backed on two iterations using a
	 * default {@link Equal equality} predicate to decide whether two tuples
	 * match. This constructor does not support the handling of a non-resetable
	 * input iteration <code>input1</code>, so the <code>reset</code> operation
	 * must be guaranteed.
	 *
	 * @param input0 the first input iteration that is traversed in the "outer"
	 *        loop.
	 * @param input1 the second input iteration that is traversed in the
	 *        "inner" loop.
	 */
	public NestedLoopsIntersection(Iterator<? extends E> input0, Cursor<? extends E> input1) {
		this(input0, input1, null, Equal.DEFAULT_INSTANCE);
	}
	
	/**
	 * Resets the "inner" loop (the iteration <code>input1</code>) of the
	 * nested-loops intersection operator. If the function
	 * <code>resetInput1</code> is specified, it is invoked, else the
	 * <code>reset</code> method of <code>input1</code> is called.
	 */
	private void resetInput1() {
		if (resetInput1 != null)
			input1 = Cursors.wrap(resetInput1.invoke());
		else
			input1.reset();
		position = 0;
	}

	/**
	 * Opens the intersection operator, i.e., signals the cursor to reserve
	 * resources, open the input iteration, etc. Before a cursor has been
	 * opened calls to methods like <code>next</code> or <code>peek</code> are
	 * not guaranteed to yield proper results. Therefore <code>open</code> must
	 * be called before a cursor's data can be processed. Multiple calls to
	 * <code>open</code> do not have any effect, i.e., if <code>open</code> was
	 * called the cursor remains in the state <i>opened</i> until its
	 * <code>close</code> method is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (isOpened)
			return;
		super.open();
		input0.open();
		input1.open();
	}
	
	/**
	 * Closes the intersection operator, i.e., signals the cursor to clean up
	 * resources, close the input iterations, etc. When a cursor has been
	 * closed calls to methods like <code>next</code> or <code>peek</code> are
	 * not guaranteed to yield proper results. Multiple calls to
	 * <code>close</code> do not have any effect, i.e., if <code>close</code>
	 * was called the cursor remains in the state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		input0.close();
		input1.close();
	}
	
	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>The implementation of this method is as follows:
	 * <pre>
	 *     E next0;
	 *     while (input0.hasNext()) {
	 *         next0 = input0.next();
	 *         if (reset)
	 *             resetInput1();
	 *         while (input1.hasNext()) {
	 *             if (equals.invoke(next0, input1.next()) && !bitVector.get(position)) {
	 *                 next = next0;
	 *                 return true;
	 *             }
	 *             position++;
	 *         }
	 *         reset = true;
	 *     }
	 *     return false;
	 * </pre>
	 * The complete second input iteration is checked in the inner loop against
	 * each object of the first input iteration for matches.</p>
	 * 
	 * @return <code>true</code> if the intersection operator has more
	 *         elements.
	 */
	protected boolean hasNextObject() {
		E next0;
		while (input0.hasNext()) {
			next0 = input0.next();
			if (reset)
				resetInput1();
			while (input1.hasNext()) {
				if (equals.invoke(next0, input1.next()) && !bitVector.get(position)) {
					next = next0;
					return true;
				}
				position++;
			}
			reset = true;
		}
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the intersection operator's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		bitVector.set(position++);
		return next;
	}
	
	/**
	 * Resets the intersection operator to its initial state such that the
	 * caller is able to traverse the join again without constructing a new
	 * intersection operator (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the intersection operator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input0.reset();
		resetInput1();
		reset = false;
		bitVector.clear();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the intersection operator. Otherwise it returns
	 * <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the intersection operator, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return input0.supportsReset();
	}
}
