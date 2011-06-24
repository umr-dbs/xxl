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

import xxl.core.functions.Function;
import xxl.core.functions.Functions;
import xxl.core.math.functions.AggregationFunction;

/**
 * Estimates iteratively the third central moment of given {@link java.lang.Number numerical data} without any error control.
 * In contrast to aggregation functions provided by {@link xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction}
 * objects of this class don't compute confidence intervals for processed data.
 * The third central moment itself is computed iteratively, i.e. the data is successively consumed and 
 * the current fourth central moment
 * is computed with the 'old' average and the new data element.
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
 * Objects of this type have a two-step phase for initialization.
 * <br>
 * Also objects of this class are using internally stored information to obtain the third central moment, 
 * objects of this type don't support online features.
 * See {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation} for further details about 
 * aggregation function using internally stored information supporting <tt>on-line aggregation</tt>.
 * 
 * * <br>
 * Consider the following example:
 * <code><pre>
 * Aggregator aggregator = new Aggregator(
		new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50), // input-Cursor
		new ThirdCentralMomentEstimator() // aggregate function
	);
 * <\code><\pre>
 * <br>
 * A more detailed coverage of online aggregation is given in [HHW97]: P. Haas, J. Hellerstein, 
 * H. Wang. Online Aggregation. 1997.
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 */

public class ThirdCentralMomentEstimator extends AggregationFunction<Number,Number> {

	/** internal average estimator */
	protected Function<Number, Number> avg;

	/** internal third central moment estimator */
	protected Function<Number, Number> mom3;

	/** internal counter */
	protected long n;

	/** internally stored value of the third central moment */
	private double m3;

	/** internally stored value of the average */
	private double a;

	/** internal variable storing the next value */
	private double xn;

	/** internal variance estimator */
	protected Function<Number, Number> var;

	/** internally stored value of the variance */
	private double v;

	/** Constructs a new Object of type ThirdCentralMoment */
	public ThirdCentralMomentEstimator() {}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * Objects of this type have a two-step phase for initialization.
	 * 
	 * @param old result of the aggregation function in the previous computation step
	 * @param next next number used for computation
	 * @return aggregation value after n steps
	 */
	public Number invoke(Number old, Number next) {
		if (next == null)
			return old;
		else {
			if (old == null) {
				avg = Functions.aggregateUnaryFunction(new StatefulAverage());
				a = avg.invoke(next).doubleValue();
				var = Functions.aggregateUnaryFunction(new StatefulVarianceEstimator());
				v = var.invoke(next).doubleValue();
				n = 1;
				m3 = 0.0;
			} else {
				xn = next.doubleValue();
				n++;
				a = avg.invoke(next).doubleValue();
				m3 = (n - 2) * m3 + (3.0 * v * (a - xn)) / n;
				m3 = m3 - ((double) (n - 2) * (double) (n - 1) / (n)) * (Math.pow((a - xn), 3.0) / (n));
				m3 = m3 / (n - 1);
				v = var.invoke(next).doubleValue();
			}
			return new Double(m3);
		}
	}
}
