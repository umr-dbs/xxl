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
 * Estimates iteratively the average of given
 * {@link java.lang.Number numerical data} without any error control. In
 * contrast to aggregation functions provided by
 * {@link xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction}
 * objects of this class don't compute confidence intervals for processed data.
 * The average itself is computed iteratively, i.e. the data is successively
 * consumed and the current average is computed with the 'old' average and the
 * new data element.
 * 
 * <p><b>Objects of this type are recommended for the usage with aggregator
 * cursors!</b></p>
 * 
 * <p>Generally, each aggregation function must support a function call of the
 * following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt>
 * steps, <tt>f</tt> the aggregation function, <tt>agg_n-1</tt> the computed
 * aggregation value after <tt>n-1</tt> steps and <tt>next</tt> the next object
 * to use for computation. An aggregation function delivers only <tt>null</tt>
 * as aggregation result as long as the aggregation function has not yet fully
 * initialized.<br>
 * Consider the following example:
 * <code><pre>
	Aggregator aggregator = new Aggregator(
		new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50), // input cuursor
		new StatefulAverage()			// aggregate function
	);
 * </pre></code>
 * 
 * A more detailed coverage of online aggregation is given in [HHW97]: P. Haas,
 * J. Hellerstein, H. Wang. Online Aggregation. 1997.
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 */

public class StatefulAverage extends AggregationFunction<Number,Number> {

	/** stores the number of already processed objects */
	protected long count;

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param average result of the aggregation function in the previous computation step
	 * @param next next number used for computation
	 * @return aggregation value after n steps
	 */
	@Override
	public Number invoke(Number average, Number next) {
		if (next == null)
			return average;
		if (average == null) {
			count = 1;
			return next;
		}
		count++;
		return ((count - 1d) * average.doubleValue() + next.doubleValue()) / count;
	}
}
