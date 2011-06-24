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
 * Provides the same functionality as {@link StatefulVarianceEstimator} but
 * keeps the state information. Hence, the incrementally
 * computed aggregate consists of an Object array whose
 * first component is the current variance and the following
 * components are the state.
 * 
 * @see StatefulVarianceEstimator
 */
public class StatelessVarianceEstimator extends StatelessAggregationFunction<Number, Number[], Number> {

    /**
     * The aggregate mapping function.
     */
	public static final Function<Number[], Number> AGGREGATE_MAPPING = new AbstractFunction<Number[], Number>() {
    	@Override
    	public Number invoke(Number[] state) {
    		return state[0];
    	}
    };
	
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
	public Number[] invoke(Number[] state, Number next) {
		if (next == null)
			return state;
		if (state == null)
			return new Number[] {
				0d,
				1l,
				next.doubleValue(),
				0d
			};
		long n = state[1].longValue() + 1;
		double sk = state[2].doubleValue();
		double vk = state[3].doubleValue() + (Math.pow((sk - (n - 1) * next.doubleValue()), 2.0) / n) / (n - 1);
		sk += next.doubleValue();
		return new Number[] {
			vk / (n - 1), // returning sample variance
			n,
			sk,
			vk
		};
	}
	
	@Override
	public Function<Number[], Number> getAggregateMapping() {
		return AGGREGATE_MAPPING;
	}
}
