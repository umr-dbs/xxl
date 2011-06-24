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

/**
 * Computes the average of the n last seen
 * {@link java.lang.Number numbers} without any error control.
 * <br>
 * <p><b>Objects of this type are recommended for use with aggregator cursors!</b></p>
 * <br>
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers only <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized, meaning for this class the first (n-1)-th delivered
 * objects are <tt>null</tt>!
 *
 * Consider the following example:
 * <code><pre>
 * Aggregator agg = new Aggregator(
		new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50), // input-Cursor
		new LastNthAverage(10)	// aggregate function
	);
 * <\code><\pre>
 * <br>
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.parametric.aggregates.StatefulAverage
 */

public class LastNthAverage extends AggregationFunction<Number,Number> {

//	/** stores the number of already processed objects */
//	protected int count;

	/** number of objects to compute the average */
	protected int n;

	/** position of the number in the internally stored array to substitute next */
	protected int pos;

	/** internally stored numbers */
	protected double[] store;

	/** internally stored sum of the n last seen numbers */
	protected double sum;

	/** indicates whether the average has been initialized or not */
	protected boolean init;

	/** Constructs a new object of this type.
	 * 
	 * @param n number of objects to compute the average from
	 * @throws IllegalArgumentException if a number less or equal 0 is given
	 */
	public LastNthAverage(int n) throws IllegalArgumentException {
		if (n < 1)
			throw new IllegalArgumentException(
				"Can't compute the average of the last " + n + " numbers! There has to be given a number n >= 1!");
		this.n = n;
		pos = 0;
//		count = 0;
		sum = 0.0;
		init = false;
		store = new double[n];
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param average result of the aggregation function of the last n seen elements
	 * numbers in the previous computation step
	 * @param next next number used for computation
	 * @return actual valid aggregation value
	 */
	public Number invoke(Number average, Number next) {
//		// reinit if a previous aggregation value == null is given
//		// and the aggregation function has already been initialized
//		if ((average == null) && (init)) {
//			pos = 0;
//			count = -1;
//			sum = 0.0;
//			init = false;
//			store = new double[n];
//		}
//		count++;
//		if (!init) {
//			store[count] = ((Number) next).doubleValue();
//			if (count == n - 1) {
//				for (int i = 0; i < store.length; i++)
//					sum += store[i];
//				init = true;
//			} else {
//				return null;
//			}
//		} else {
//			sum = sum - store[pos] + ((Number) next).doubleValue();
//			store[pos] = ((Number) next).doubleValue();
//			pos++;
//			if (pos == n)
//				pos = 0;
//		}
//		return new Double(sum / n);
		if (next == null)
			return average;
		else {
			// reinit if a previous aggregation value == null is given
			// and the aggregation function has already been initialized
			if ((average == null) && (init)) {
				pos = 0;
				sum = 0.0;
				init = false;
				store = new double[n];
			}
			if (init)
				sum -= store[pos];
			sum += (store[pos++] = next.doubleValue());
			if (pos == n) {
				init = true;
				pos = 0;
			}
			return init ? new Double(sum / n) : null;
		}

	}
}
