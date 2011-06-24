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

package xxl.core.math.statistics.parametric.aggregates;

import xxl.core.math.functions.AggregationFunction;

/** This class provides an estimator for the aggregation function <TT>SUM</TT>.
 * The expected sum is computed by S_e = size_of_entirety * running_average, i.e.,
 * the totally number of elements must be given in advance.<BR>
 * If the already processed data exceeds the given size of the entirety there are three
 * different strategies available to solve this: <BR>
 * 1. RESTRICTIVE: Throw an IllegalStateException <BR>
 * 3. ADAPTIVE: Adapt the given size of the entirety to the number of already processed data <BR>
 * 3. QUIET: Do nothing <BR>
 * The used strategy is passed to the estimator by an int.
 *
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * <br>
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers only <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized.
 * <br>
 * Objects of this class don't use any internally stored information to obtain the estimated sum, 
 * so one could say objects of this type or 'status-less'.
 * See {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation} for further details about 
 * aggregation function using internally stored information.
 *
 * Consider the following example:
 * <code><pre>
 * Aggregator aggregator = new Aggregator(
		new RandomIntegers(100, 50), // input-Cursor
		new SumEstimator()			// aggregate function
	);
 * <\code><\pre>
 * <br>
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 */

public class SumEstimator extends AggregationFunction<Number,Number> {

	/** Some constant indicating strategy number 1 will be used */
	public static final int RESTRICTIVE = 1;

	/** Some constant indicating strategy number 2 will be used */
	public static final int ADAPTIVE = 2;

	/** Some constant indicating strategy number 3 will be used */
	public static final int QUIET = 3;

	// ----

	/** the number of data elements in the processed stream */
	protected long N;

	/** number of already processed data */
	protected long n;

	/** internally used function for computing the average of the already processed data */
	protected StatefulAverage avg;

	/** actual computed average */
	protected Number a;

	/** used strategy for error handling */
	protected int strategy;

	/** Constructs a new Object of this type.
	 *
	 * @param size number of data elements in the processed stream
	 * @param strategy used strategy for error handling
	 */
	public SumEstimator(long size, int strategy) {
		N = size;
		avg = new StatefulAverage();
		a = null;
		n = 0;
		this.strategy = strategy;
	}

	/** Constructs a new Object of this type using the restrictive-strategy
	 * for error handling by default.
	 *
	 * @param size number of data elements in the processed stream
	 */
	public SumEstimator(long size) {
		this(size, RESTRICTIVE);
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param expectedSum result of the aggregation function in the previous computation step
	 * @param next next number used for computation
	 * @return aggregation value after n steps
	 * @throws IllegalStateException if the number or already processed data exceeds the given size of entirety
	 */
	public Number invoke(Number expectedSum, Number next) throws IllegalStateException {
		n++;
		if (n > N) {
			switch (strategy) {
				case RESTRICTIVE :
					throw new IllegalStateException("Maximum number of data resp. given size of entirety exceeded!");
				case ADAPTIVE :
					N = n;
					break;
			}
		}
		a = avg.invoke(a, next);
		return new Double(N * ((Number) a).doubleValue());
	}
}
