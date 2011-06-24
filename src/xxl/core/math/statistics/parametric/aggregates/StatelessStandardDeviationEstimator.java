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

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.functions.StatelessAggregationFunction;

/**
 * Provides the same functionality as {@link StatefulStandardDeviationEstimator} but
 * keeps the state information. Hence, the incrementally
 * computed aggregate consists of an Object array whose
 * first component is the current standard deviation and the following
 * components are the state.
 * 
 * @see StatefulStandardDeviationEstimator
 */
public class StatelessStandardDeviationEstimator extends StatelessAggregationFunction<Number, Object[], Number> {

    /**
     * The aggregate mapping function.
     */
	public static final Function<Object[], Number> AGGREGATE_MAPPING = new AbstractFunction<Object[], Number>() {
    	@Override
    	public Number invoke(Object[] state) {
    		return (Number)state[0];
    	}
    };
	
	/** internally used Function for recursive computing of internally used variance */
	protected StatelessVarianceEstimator var;

	/**
	 * Creates a new stateless standard deviation aggregation function.
	 */
	public StatelessStandardDeviationEstimator() {
		var = new StatelessVarianceEstimator();
	}

	   /** 
     * Function call for incremental aggregation.
     * The first argument corresponds to the old aggregate,
     * whereas the second argument corresponds to the new
     * incoming value. <br>
     * Depending on these two arguments the new aggregate, i.e. 
     * average, has to be computed and returned.
	 * 
	 * @param state result of the aggregation function in the previous computation step
	 * @param next next object used for computation
	 * @return an Object array that contains the new aggregation value of type Double,
	 * and a counter of type Integer that reveals how often this function has
	 * been invoked.
	 */
	@Override
	public Object[] invoke(Object[] state, Number next) {
		if (next == null)
			return state;
		// reinit if a previous aggregation value == null is given
		if (state == null)
			return new Object[] {
				0d,
				var.invoke(null, next)
			};
		Number[] v = var.invoke((Number[])state[1], next);
		return new Object[] {
			Math.sqrt(var.getAggregateMapping().invoke(v).doubleValue()),
			v
		};
	}
	
	@Override
	public Function<Object[], Number> getAggregateMapping() {
		return AGGREGATE_MAPPING;
	}

}
