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
 * Computes an estimation of the variance (in this case the sample variance) in a recursive manner
 * using an algorithm based upon [Wes79]:
 * D.H.D. West, Updating mean and StatefulVariance Estimates: An improved Method, 
 * Comm. Assoc. Comput. Mach., 22:532-535, 1979 <BR> and <BR>
 * [CGL83]: Chan, T.F., G.H. Golub, & R.J. LeVeque,
 * Algorithms for Computing the Sample StatefulVariance: Analysis and Recommendations,
 * The American Statistician Vol 37 1983: 242-247. <BR>
 * of
 * {@link java.lang.Number numerical data} without any error control.
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * <br>
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * A aggregation function delivers only <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized.
 * Objects of this type have a two-step phase for initialization.
 * <br>
 * As result of the aggregation, the sample variance is returned!
 * Also objects of this class are using internally stored information to obtain the standard deviation, 
 * objects of this type don't support on-line features.
 * See {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation} for further details about 
 * aggregation function using internally stored information supporting <tt>on-line aggregation</tt>.
 * 
 * Consider the following example:
 * <code><pre>
 * Aggregator aggregator1 = new Aggregator(
		new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50), // the input-Cursor
		new StatefulVarianceEstimator()		// aggregate functions
	);
 * <\code><\pre>
 * <br>
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.parametric.aggregates.StatefulStandardDeviation
 */

public class StatefulVarianceEstimator extends AggregationFunction<Number,Number> {

	/**
	 * 
	 */
	private double sk;

	/**
	 * variance
	 */
	private double vk;

	/**
	 * Number of steps
	 */
	protected long n;

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * Objects of this type have a two-step phase for initialization.
	 * 
	 * @param variance result of the aggregation function in the previous computation step
	 * @param next next number used for computation
	 * @return aggregation value after n steps
	 */
	@Override
	public Number invoke(Number variance, Number next) {
		if (next == null)
			return variance;
		if (variance == null) {
			n = 1;
			sk = next.doubleValue();
			vk = 0d;
			return 0d;
		}
		n++;
		vk += (Math.pow((sk - (n - 1) * next.doubleValue()), 2.0) / n) / (n - 1);
		sk += next.doubleValue();
		return vk / (n - 1); // returning sample variance
	}
}
