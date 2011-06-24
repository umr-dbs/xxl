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
 * Computes the standard deviation step-by-step on 
 * {@link java.lang.Number numerical data} without any error control
 * using {@link xxl.core.math.statistics.parametric.aggregates.StatefulVariance}.
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
 * Also objects of this class are using internally stored information to obtain the standard deviation, 
 * objects of this type don't support online features.
 * See {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation} for further details about 
 * aggregation function using internally stored information supporting <tt>online aggregation</tt>.
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 */

public class StatefulStandardDeviation extends AggregationFunction<Number,Number> {

	/** internally used Function for recursive computing of internally used variance */
	protected StatefulVariance var;

	/** internally used variable storing the variance of the processed data */
	protected Number v;
	
	/**
	 * Creates a new stateful standard deviation aggregation function.
	 */
	public StatefulStandardDeviation() {
		var = new StatefulVariance();
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * Objects of this type have a two-step phase for initialization.
	 * @param stddev result of the aggregation function in the previous computation step
	 * @param next next object used for computation
	 * @return aggregation value after n steps
	 * (the new computed standard deviation based upon the internally stored
	 * status and the next given object as an object of type <tt>Double</tt>).
	 */
	@Override
	public Number invoke(Number stddev, Number next) {
		if (next == null)
			return stddev;
		// reinit if a previous aggregation value == null is given
		if (stddev == null) {
			v = var.invoke(null, next);
			return 0d;
		}
		v = var.invoke(v, next);
		return Math.sqrt(v.doubleValue());
	}
}
