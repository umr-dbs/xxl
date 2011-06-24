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

package xxl.core.cursors.joins;

import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.Predicates;
import xxl.core.util.BitSet;

/**
 * A nested-loops implementation of the join operator. This class provides a
 * generic, untyped nested-loops join algorithm. Furthermore it provides
 * support for left, right and outer joins. A nested-loops join is based on a
 * loop iteration and therefore it has no special conditions with regard to the
 * order of the elements contained in the two input iterations. The input
 * iteration <code>input0</code> is traversed in the "outer" loop (only for one
 * time) and the input iteration <code>input1</code> is repeatedly consumed in
 * the "inner" loop (for a maximum of times determined by the elements of input
 * iteration <code>input0</code>).
 * 
 * <p><i>The algorithm works as follows:</i> For each element of the "outer"
 * iteration all elements of the "inner" iteration are checked to fulfill the
 * user defined predicate evaluated for each tuple of elements backed on the
 * element of "outer" iteration and the current element of the "inner"
 * iteration. Only these tuples where the predicate's evaluation result is
 * <code>true</code> have been qualified to be a result of the join operation.
 * Because internally an {@link AbstractCursor abstract cursor} is used to
 * implement this join, the next element that will be returned to the caller is
 * stored in the field <code>next</code>. But be aware, that the function
 * <code>newResult</code> is invoked on the qualifying tuple before it is
 * stored in the field.<br />
 * An incomplete extract of the implementation:
 * <code><pre>
 *   while (input0.hasNext()) {
 *       ...
 *       while (input1.hasNext()) {
 *           ...
 *           if (predicate.invoke(input0.peek(), input1.peek())) {
 *               ...
 *               next = newResult.invoke(input0.peek(), input1.next());
 *               return true;
 *           }
 *           ...
 *           input1.next();
 *           ...
 *       }
 *       ...
 *       input0.next();
 *       resetInput1();
 *       ...
 *   }
 * </pre></code>
 * The implementation is a bit more complex due to additional checks of
 * join-types and the generation of result-tuples where the evaluated join
 * predicate returned <code>false</code>.</p>
 * 
 * <p>Some information about the parameters used in the constructors:
 * <ul>
 *     <li>
 *         A binary predicate can be specified in some constructors with the
 *         intention to select the qualifying tuples, i.e., the tuples that
 *         will be returned when the <code>next</code> method is called.
 *         Therefore the predicate is evaluated for a tuple built of one element
 *         of each input iteration. Only the tuples, where the predicate's
 *         evaluation result is <code>true</code>, have been qualified to be a
 *         result of the join operation.<br />
 *         If the <i>Cartesian</i> product should be computed with the help of
 *         this join, the predicate always returns <code>true</code>.
 *     </li>
 *     <li>
 *         If the "inner" iteration is not resetable, i.e., the
 *         <code>reset</code> method of <code>input1</code> will cause an
 *         {@link java.lang.UnsupportedOperationException}, a parameterless
 *         function, <code>resetInput1</code>, can be specified in special
 *         constructors to deliver this iteration again, if it has to be
 *         traversed for another time.
 *     </li>
 *     <li>
 *         Another function, named <code>newResult</code>, that is invoked on
 *         each qualifying tuple before it is returned to the caller concerning
 *         a call to the <code>next</code> method, has to be specified in each
 *         constructor. This binary function works like a kind of factory
 *         method modeling the resulting object (tuple). Be aware that this
 *         function possibly has to handle <code>null</code> values in cases of
 *         outer joins.
 *     </li>
 * </ul></p>
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link Iterator} it is wrapped to a cursor by a call to the static method
 * {@link Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   NestedLoopsJoin&lt;Integer, Integer[]&gt; join = new NestedLoopsJoin&lt;Integer, Integer[]&gt;(
 *       Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).iterator(),
 *       new Function&lt;Object, Iterator&lt;Integer&gt;&gt;() {
 *           public Iterator&lt;Integer&gt; invoke() {
 *               return Arrays.asList(2, 4, 6, 8, 10).iterator();
 *           }
 *       },
 *       new ComparatorBasedEqual&lt;Integer&gt;(ComparableComparator.INTEGER_COMPARATOR),
 *       new Tuplify(),
 *       OUTER_JOIN
 *   );
 *   
 *   join.open();
 *   
 *   while (join.hasNext())
 *       System.out.println(Arrays.toString(join.next()));
 *   
 *   join.close();
 * </pre></code>
 * This example illustrates the constructor call for a full outer join. At
 * first, an iteration containing all elements from 0 to 10 and an factory
 * method returning an iteration containing the even elements are created. The
 * predicate that determines the tuples qualifying for join results compares
 * the numeric elements for equality. A new result-tuple is build by inserting
 * both qualifying elements in an object array. At last the type is set to
 * OUTER_JOIN, so all tuples for which the predicate is <code>true</code> will
 * be returned as well as all elements that not refer to a join result will be
 * returned by filling the missing element of the result-tuple with a
 * <code>null</code> value. The output of this example is:
 * <pre>
 *   [1, null]
 *   [2, 2]
 *   [3, null]
 *   [4, 4]
 *   [5, null]
 *   [6, 6]
 *   [7, null]
 *   [8, 8]
 *   [9, null]
 *   [10, 10]
 * </pre></p>
 *
 * @param <I> the type of the elements consumed by this iteration.
 * @param <E> the type of the elements returned by this join operation.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see xxl.core.cursors.joins.SortMergeJoin
 * @see xxl.core.relational.cursors.NestedLoopsJoin
 * @see xxl.core.spatial.cursors.NestedLoopsJoin
 */
public class NestedLoopsJoin<I, E> extends AbstractCursor<E> {

	/**
	 * An enumeration of constants specifying the join types supported by this
	 * class.
	 */
	public static enum Type {
		
		/**
		 * A constant specifying a theta-join. Only the tuples for which the
		 * specified predicate is <code>true</code> will be returned.
		 */
		THETA_JOIN,
		
		/**
		 * A constant specifying a left outer-join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * <code>input0</code> not qualifying concerning the predicate will be
		 * returned. The function <code>newResult</code> is called with an
		 * element of <code>input0</code> and the <code>null</code> value.
		 */
		LEFT_OUTER_JOIN,
		
		/**
		 * A constant specifying a right outer-join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * <code>input1</code> not qualifying concerning the predicate will be
		 * returned. The function <code>newResult</code> is called with an
		 * element of <code>input1</code> and the <code>null</code> value.
		 */
		RIGHT_OUTER_JOIN,
		
		/**
		 * A constant specifying a full outer-join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all tuples
		 * additionally returned by the left and right outer-join will be
		 * returned.
		 */
		OUTER_JOIN
	};

	/**
	 * The first (or "outer") input iteration of the join operator.
	 */
	protected Cursor<? extends I> input0;
	
	/**
	 * The second (or "inner") input iteration of the join operator.
	 */
	protected Cursor<? extends I> input1;
	
	/**
	 * A boolean flag determining whether an "inner" loop (the iteration
	 * <code>input1</code>) has more elements or if it is finished and must be
	 * reseted.
	 */
	protected boolean flag = false;
	
	/**
	 * A boolean flag determining whether the actual element of the "outer"
	 * loop (the iteration <code>input0</code>) has already found a matching
	 * element in the "inner" loop (the iteration <code>input1</code>). This
	 * information is necessary for providing (left) outer joins.
	 */
	protected boolean match = false;
	
	/**
	 * A bit set storing for every element in the "inner" loop (the iteration
	 * <code>input1</code>) a single bit. The <tt>n</tt>-th bit of the bit set
	 * is set to <tt>1</tt> if the <tt>n</tt>-th element of the "inner" loop
	 * has found a matching element in the "outer" loop. This information is
	 * necessary for providing (right) outer joins.
	 */
	protected BitSet bitVector = null;
	
	/**
	 * The position of the "inner" loop, i.e., if <code>position</code> is set
	 * to <tt>n</tt>, the <tt>n</tt>-th element of the iteration
	 * <code>input1</code> is actually tested for a matching.
	 */
	protected int position = 0;
	
	/**
	 * A parameterless that resets the "inner" loop (the iteration
	 * <code>input1</code>). Such a function must be specified, if the "inner"
	 * iteration is not resetable, i.e., the <code>reset</code> method of
	 * <code>input1</code> will cause an
	 * {@link java.lang.UnsupportedOperationException}. A call to the
	 * <code>invoke</code> method of this function must deliver the "inner"
	 * iteration again, if it has to be traversed for an other time.
	 */
	protected Function<?, ? extends Iterator<? extends I>> resetInput1 = null;
	
	/**
	 * A binary predicate that selects the qualifying tuples, i.e., the tuples
	 * that will be returned when the <code>next</code> method is called.
	 * Therefore the predicate is evaluated for a tuple built of one element of
	 * each input iteration. Only the tuples, where the predicate's evaluation
	 * result is <code>true</code>, have been qualified to be a result of the
	 * join operation.<br />
	 * If the <i>Cartesian</i> product should be computed with the help of this
	 * join, the predicate always returns <code>true</code>.
	 */
	protected Predicate<? super I> predicate = null;
	
	/**
	 * A function that is invoked on each qualifying tuple before it is
	 * returned to the caller concerning a call to the <code>next</code>
	 * method. This binary function works like a kind of factory method
	 * modeling the resulting object (tuple). Be aware that this function
	 * possibly has to handle <code>null</code> values in cases of outer joins.
	 */
	protected Function<? super I, ? extends E> newResult = null;
	
	/**
	 * The type of this nested-loops join operator. Determines whether it
	 * calculates a theta- or an outer-join.
	 */
	protected Type type;	

	/**
	 * Creates a new nested-loops join backed on two iterations using a user
	 * defined predicate to select the resulting tuples. This constructor also
	 * supports the handling of a non-resetable input iteration
	 * <code>input1</code>, because a parameterless function can be defined
	 * that returns this input iteration again. Furthermore a function named
	 * <code>newResult</code> can be specified that is invoked on each
	 * qualifying tuple before it is returned to the caller concerning a call
	 * to the <code>next</code> method. This function is a kind of factory
	 * method to model the resulting object.
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
	 * @param predicate the binary predicate evaluated for each tuple of
	 *        elements backed on one element of each input iteration in order
	 *        to select them. Only these tuples where the predicate's
	 *        evaluation result is <code>true</code> have been qualified to be
	 *        a result of the join operation.
	 * @param newResult a factory method (function) that takes two parameters
	 *        as argument and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e., on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        the <code>next</code> method.
	 * @param type the type of this join; use one of the public constants
	 *        defined in this class.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 */
	public NestedLoopsJoin(Iterator<? extends I> input0, Iterator<? extends I> input1, Function<?, ? extends Iterator<? extends I>> resetInput1, Predicate<? super I> predicate, Function<? super I, ? extends E> newResult, Type type) throws IllegalArgumentException {
		this.input0 = Cursors.wrap(input0);
		this.input1 = Cursors.wrap(input1);
		this.resetInput1 = resetInput1;
		this.predicate = predicate;
		this.newResult = newResult;
		this.type = type;	
	}

	/**
	 * Creates a new nested-loops join realizing a theta-join backed on two
	 * iterations using a user defined predicate to select the resulting
	 * tuples. This constructor also supports the handling of a non-resetable
	 * input iteration <code>input1</code>, because a parameterless function
	 * can be defined that returns this input iteration again. Furthermore a
	 * function named <code>newResult</code> can be specified that is invoked
	 * on each qualifying tuple before it is returned to the caller concerning
	 * a call to the <code>next</code> method. This function is a kind of
	 * factory method to model the resulting object.
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
	 * @param predicate the binary predicate evaluated for each tuple of
	 *        elements backed on one element of each input iteration in order
	 *        to select them. Only these tuples where the predicate's
	 *        evaluation result is <code>true</code> have been qualified to be
	 *        a result of the join operation.
	 * @param newResult a factory method (function) that takes two parameters
	 *        as argument and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e., on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        the <code>next</code> method.
	 */
	public NestedLoopsJoin(Iterator<? extends I> input0, Iterator<? extends I> input1, Function<?, ? extends Iterator<? extends I>> resetInput1, Predicate<? super I> predicate, Function<? super I, ? extends E> newResult) {
		this(input0, input1, resetInput1, predicate, newResult, Type.THETA_JOIN);
	}

	/**
	 * Creates a new nested-loops join backed on two iterations using a user
	 * defined predicate to select the resulting tuples. This constructor does
	 * not support the handling of a non-resetable input iteration
	 * <code>input1</code>, so the <code>reset</code> operation must be
	 * guaranteed. Furthermore a function named <code>newResult</code> can be
	 * specified that is invoked on each qualifying tuple before it is returned
	 * to the caller concerning a call to the <code>next</code> method. This
	 * function is a kind of factory method to model the resulting object.
	 *
	 * @param input0 the first input iteration that is traversed in the "outer"
	 *        loop.
	 * @param input1 the second input iteration that is traversed in the
	 *        "inner" loop.
	 * @param predicate the binary predicate evaluated for each tuple of
	 *        elements backed on one element of each input iteration in order
	 *        to select them. Only these tuples where the predicate's
	 *        evaluation result is <code>true</code> have been qualified to be
	 *        a result of the join operation.
	 * @param newResult a factory method (function) that takes two parameters
	 *        as argument and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e., on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        the <code>next</code> method.
	 * @param type the type of this join; use one of the public constants
	 *        defined in this class.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 */
	public NestedLoopsJoin(Iterator<? extends I> input0, Cursor<? extends I> input1, Predicate<? super I> predicate, Function<? super I, ? extends E> newResult, Type type) throws IllegalArgumentException {
		this(input0, input1, null, predicate, newResult, type);
	}

	/**
	 * Creates a new nested-loops join realizing a self-join backed on one
	 * iteration using a user defined predicate to select the resulting tuples.
	 * The second input iteration for the join is retrieved by calling the
	 * function <code>resetInput</code>, which is also used to substitute the
	 * <code>reset</code> functionality. Furthermore a function named
	 * <code>newResult</code> can be specified that is invoked on each
	 * qualifying tuple before it is returned to the caller concerning a call
	 * to the <code>next</code> method. This function is a kind of factory
	 * method to model the resulting object.
	 *
	 * @param input the input iteration for the self join.
	 * @param resetInput a parameterless function that delivers the input
	 *        iteration again.
	 * @param predicate the binary predicate evaluated for each tuple of
	 *        elements backed on one element of each input iteration in order
	 *        to select them. Only these tuples where the predicate's
	 *        evaluation result is <code>true</code> have been qualified to be
	 *        a result of the join operation.
	 * @param newResult a factory method (function) that takes two parameters
	 *        as argument and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e., on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        the <code>next</code> method.
	 * @param type the type of this join; use one of the public constants
	 *        defined in this class.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 */
	public NestedLoopsJoin(Iterator<? extends I> input, Function<?, ? extends Iterator<? extends I>> resetInput, Predicate<? super I> predicate, Function<? super I, ? extends E> newResult, Type type) throws IllegalArgumentException {
		this(input, resetInput.invoke(), resetInput, predicate, newResult, type);
	}

	/**
	 * Creates a new nested-loops join realizing a theta-join backed on two
	 * iterations using a {@link Predicates#TRUE true} predicate which leads
	 * the computation of the <i>Cartesian</i> product. This constructor does
	 * not support the handling of a non-resetable input iteration
	 * <code>input1</code>, so the <code>reset</code> operation must be
	 * guaranteed. Furthermore a function named <code>newResult</code> can be
	 * specified that is invoked on each qualifying tuple before it is returned
	 * to the caller concerning a call to the <code>next</code> method. This
	 * function is a kind of factory method to model the resulting object.
	 *
	 * @param input0 the first input iteration that is traversed in the "outer"
	 *        loop.
	 * @param input1 the second input iteration that is traversed in the
	 *        "inner" loop.
	 * @param newResult a factory method (function) that takes two parameters
	 *        as argument and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e., on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        the <code>next</code> method.
	 */
	public NestedLoopsJoin(Iterator<? extends I> input0, Cursor<? extends I> input1, Function<? super I, ? extends E> newResult) {
		this(input0, input1, Predicates.TRUE, newResult, Type.THETA_JOIN);
	}

	/**
	 * Creates a new nested-loops join realizing a theta-self-join backed on
	 * one iteration using a {@link Predicates#TRUE true} predicate which leads
	 * the computation of the <i>Cartesian</i> product. The second input
	 * iteration for the join is retrieved by calling the function
	 * <code>resetInput</code>, which is also used to substitute the
	 * <code>reset</code> functionality. Furthermore a function named
	 * <code>newResult</code> can be specified that is invoked on each
	 * qualifying tuple before it is returned to the caller concerning a call
	 * to the <code>next</code> method. This function is a kind of factory
	 * method to model the resulting object.
	 *
	 * @param input the input iteration for the self join.
	 * @param resetInput a parameterless function that delivers the input
	 *        iteration again.
	 * @param newResult a factory method (function) that takes two parameters
	 *        as argument and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e., on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        the <code>next</code> method.
	 */
	public NestedLoopsJoin(Cursor<? extends I> input, Function<?, ? extends Iterator<? extends I>> resetInput, Function<? super I, ? extends E> newResult) {
		this(input, resetInput, Predicates.TRUE, newResult, Type.THETA_JOIN);
	}
	
	/**
	 * Resets the "inner" loop (the iteration <code>input1</code>) of the
	 * nested-loops join operator. If the function <code>resetInput1</code> is
	 * specified, it is invoked, else the <code>reset</code> method of
	 * <code>input1</code> is called.
	 */
	private void resetInput1() {
		if (resetInput1 != null)
			input1 = Cursors.wrap(resetInput1.invoke());
		else
			input1.reset();
		position = 0;
	}

	/**
	 * Opens the join operator, i.e., signals the cursor to reserve resources,
	 * open the input iteration, etc. Before a cursor has been opened calls to
	 * methods like <code>next</code> or <code>peek</code> are not guaranteed
	 * to yield proper results. Therefore <code>open</code> must be called
	 * before a cursor's data can be processed. Multiple calls to
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
	 * Closes the join operator, i.e., signals the cursor to clean up
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
	 * <p>An incomplete extract of the implementation:
	 * <code><pre>
	 *   while (input0.hasNext()) {
	 *       ...
	 *       while (input1.hasNext()) {
	 *           ...
	 *           if (predicate.invoke(input0.peek(), input1.peek())) {
	 *               ...
	 *               next = newResult.invoke(input0.peek(), input1.next());
	 *               return true;
	 *           }
	 *           ...
	 *           input1.next();
	 *           ...
	 *       }
	 *       ...
	 *       input0.next();
	 *       resetInput1();
	 *       ...
	 *   }
	 * </pre></code>
	 * The implementation is a bit more complex due to additional checks of
	 * join-types and the generation of result-tuples where the evaluated join
	 * predicate returned <code>false</code>.</p>
	 * 
	 * @return <code>true</code> if the join operator has more elements.
	 */
	protected boolean hasNextObject() {
		if ((type == Type.RIGHT_OUTER_JOIN || type == Type.OUTER_JOIN) && bitVector == null) {
			bitVector = new BitSet(Cursors.count(input1));
			resetInput1();
		}
		while (flag || input0.hasNext()) {
			flag = true;
			while (input1.hasNext()) {
				if (predicate.invoke(input0.peek(), input1.peek())) {
					match = true;
					if (type == Type.RIGHT_OUTER_JOIN || type == Type.OUTER_JOIN)
						bitVector.set(position);
					position++;
					next = newResult.invoke(input0.peek(), input1.next());
					return true;
				}
				position++;
				input1.next();
			}
			if ((type == Type.LEFT_OUTER_JOIN || type == Type.OUTER_JOIN) && !match)
				next = newResult.invoke(input0.peek(), null);
			flag = false;
			input0.next();
			resetInput1();
			if ((type == Type.LEFT_OUTER_JOIN || type == Type.OUTER_JOIN) && !match)
				return true;
			match = false;
		}
		if (type == Type.RIGHT_OUTER_JOIN || type == Type.OUTER_JOIN)
			while (input1.hasNext()) {
				if (!bitVector.get(position++)) {
					next = newResult.invoke(null, input1.next());
					return true;
				}
				input1.next();
			}
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the join operator's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return next;
	}
	
	/**
	 * Resets the join operator to its initial state such that the caller is
	 * able to traverse the join again without constructing a new join operator
	 * (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input0.reset();
		resetInput1();
		if (type == Type.LEFT_OUTER_JOIN || type == Type.RIGHT_OUTER_JOIN || type == Type.OUTER_JOIN)
			bitVector = null;
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the join operator. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the join operator, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return input0.supportsReset();
	}
}
