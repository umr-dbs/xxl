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

package xxl.core.cursors.mappers;

import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.math.functions.AggregationFunction;

/**
 * The aggregator incrementally computes one or even more aggregates for an
 * input iteration. Due to the fact that an aggregator has to be initialized,
 * the user defined aggregation-function has to handle the case that the
 * aggregate is <code>null</code>. If the initialization of the aggregate is
 * finished, in the next step the aggregate-function is applied on the
 * aggregate and the input iteration's (a given input iterator is internally
 * wrapped to a cursor} <i>peek</i>-element, i.e., the element returned by the
 * iteration's <code>peek</code> method. In order to indicate that the
 * aggregation-function has not yet become initialized, <code>null</code> is
 * returned. The following code fragment shows this behaviour:
 * <code><pre>
 *     aggregate = function.invoke(aggregate, input.peek());
 *     if (aggregate != null)
 *         initialized = true;
 *     return aggregate;
 * </pre></code>
 * If the aggregate has been initialized, the further computation is
 * demand-driven, so a call to the <code>next</code> method will set the
 * aggregate as follows:
 * <code><pre>
 *     aggregate = function.invoke(aggregate, input.next());
 * </pre></code>
 * This incremental computation with the help of a binary aggregation-function
 * implies that the absolute aggregate's value is first being determined when
 * the last element of the underlying iteration has been consumed and the
 * aggregation function has been applied. If the user is not interested in the
 * incremental computation of the aggregate during the demand-driven
 * computation, the final aggregation value can be delivered directly calling
 * the method <code>last</code>.
 * 
 * <p>Futhermore the aggregator offers the possibility to define more than one
 * binary aggregation function, i.e., the user is able to compute a sum and an
 * average of the same data set in only one iteration process. This kind of
 * usage is often needed for SQL queries on relations. For further information
 * concerning the usage of multi-aggregate functions see
 * {@link xxl.core.relational.cursors.Aggregator}. Another very impressive use
 * of this class is given by the
 * {@link xxl.core.cursors.mappers.ReservoirSampler reservoir-sampler} which
 * uses a reservoir sampling function with a given strategy for <i>on-line
 * sampling</i>.</p>
 *
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link java.util.Iterator} it is wrapped to a cursor by a call to the static
 * method {@link xxl.core.cursors.Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p><b>Example usage:</b>
 * <code><pre>
 *     Aggregator&lt;Integer, Integer&gt; aggregator = new Aggregator&lt;Integer, Integer&gt;(
 *         new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50),
 *         new AggregationFunction&lt;Integer, Integer&gt;() {
 *             public Integer invoke(Integer aggregate, Integer next) {
 *                 if (aggregate == null)
 *                     return next;
 *                 return aggregate = Math.max(aggregate, next);
 *             }
 *         }
 *     );
 * 
 *     aggregator.open();
 * 
 *     System.out.print("The result of the maximum aggregation is: " + aggregator.last());
 * 
 *     aggregator.close();
 * </pre></code>
 * This example determines the maximum of 50 random numbers with the
 * restriction that the value of a random number is not greater than 99. A new
 * function for the aggregation is defined that compares the value of the
 * current aggregate with the value of the next object and returns the object
 * with the maximum value. Furthermore the first two lines of the
 * <code>invoke</code> method show the initialization of this instance of an
 * aggregator. Because the aggregator works demand-driven the absolute maximum
 * is definitively detected if all elements were consumed. Therefore the method
 * <code>last</code> is used generating the final output. At last the
 * aggregator is closed with the intention to release resources.</p>
 * 
 * <p>In order to compute various aggregation-functions simultanously, they
 * must be wrapped by a special
 * {@link xxl.core.math.Maths#multiDimAggregateFunction(xxl.core.math.functions.AggregationFunction[]) aggregation-function}
 * that calls successively all given functions. The wrapping
 * aggregation-function is initialized if and only if all specified functions
 * are initialized, meaning <code>null</code> will be returned by calling the
 * <code>next</code> method as long as all functions will return objects that
 * are not <code>null</code>! After initialization a list containing the
 * aggregation values given by the corresponding functions will be returned
 * every time calling the <code>next</code> method!
 * <code><pre>
 *   Aggregator&lt;Integer, List&lt;Integer&gt;&gt; aggregator2 = new Aggregator&lt;Integer, List&lt;Integer&gt;&gt;(
 *       new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50), // the input cursor
 *       Maths.multiDimAggregateFunction(
 *           new AggregationFunction&lt;Integer, Integer&gt;() { // the aggregation function
 *               public Integer invoke(Integer aggregate, Integer next) {
 *                   if (aggregate == null)
 *                       return next;
 *                   return Maths.max(aggregate, next);
 *               }
 *           },
 *           new AggregationFunction&lt;Integer, Integer&gt;() { // the second aggregation function
 *               public Integer invoke(Integer aggregate, Integer next) {
 *                   if (aggregate == null)
 *                       return next;
 *                   return aggregate + next;
 *               }
 *           }
 *       )
 *   );
 * </pre></code>
 * </p>
 *
 * @param <E> the type of the elements returned by the iteration to be
 *        aggregated.
 * @param <A> the type of the elements returned by the aggregated iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 * @see xxl.core.relational.cursors.Aggregator
 */
public class Aggregator<E, A> extends AbstractCursor<A> {

	/**
	 * The input iteration holding the data to be aggregated.
	 */
	protected Cursor<? extends E> input;
	
	/**
	 * The function used for the aggregation. This binary function is invoked
	 * with the prior aggregate and the next element of the input iteration.
	 * When the aggregate is not yet initialized, a <code>null</code> value is
	 * given to the aggregation-function.
	 */
	protected AggregationFunction<? super E, A> function;
	
	/**
	 * The current aggregate of the processed input iteration.
	 */
	protected A aggregate;

	/**
	 * A boolean flag to detect if an result-object for the aggregation has
	 * been specified, i.e., the aggregate is already initialized.
	 */
	protected boolean initialized;
	
	/**
	 * Creates a new aggregator backed on an input iteration and an aggregation
	 * function. Every iterator given to this constructor is wrapped to a
	 * cursor.
	 *
	 * @param iterator the input iteration holding the data to be aggragated.
	 * @param function an aggregation-function.
	 */
	public Aggregator(Iterator<? extends E> iterator, final AggregationFunction<? super E, A> function) {
		this.input = Cursors.wrap(iterator);
		this.function = function;
		this.aggregate = null;
		this.initialized = false;
	}

	/**
	 * Opens the aggregator, i.e., signals the cursor to reserve resources,
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
		input.open();
	}
	
	/**
	 * Closes the aggregator, i.e., signals the cursor to clean up resources,
	 * close the input iterations, etc. When a cursor has been closed calls to
	 * methods like <code>next</code> or <code>peek</code> are not guaranteed
	 * to yield proper results. Multiple calls to <code>close</code> do not
	 * have any effect, i.e., if <code>close</code> was called the cursor
	 * remains in the state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close () {
		if (isClosed)
			return;
		super.close();
		input.close();
	}
	
	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the aggregator has more elements, otherwise
	 *         <code>false</code>.
	 */
	protected boolean hasNextObject() {
		if (input.hasNext()) {
			aggregate = function.invoke(aggregate, input.next());
			if (!initialized && aggregate != null)
				initialized = true;
			return true;
		}
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the aggregator's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected A nextObject() {
		return aggregate;
	}

	/**
	 * Returns the last element of this aggregator. This element represents the
	 * final aggregation value.
	 *
	 * @return the last element of the aggregator.
	 * @throws NoSuchElementException if a last element does not exist, i.e.,
	 *         the input iteration does not hold enough elements to initialize
	 *         the aggregate.
	 */
	public A last() throws NoSuchElementException {
		try {
			return Cursors.last(this);
		}
		catch (NoSuchElementException nsee) {
			if (!initialized)
				throw nsee;
			return aggregate;
		}
	}

	/**
	 * Resets the aggregator to its initial state such that the caller is able
	 * to traverse the aggregation again without constructing a new aggregator
	 * (optional operation).
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the aggregator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input.reset();
		aggregate = null;
		initialized = false;
	}

	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the aggregator. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the aggregator, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return input.supportsReset();
	}
}
