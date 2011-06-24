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

package xxl.core.math.functions;

import xxl.core.util.Decorator;

/**
 * A decorator-aggregation-function decorates (wraps) an aggregation-function
 * by passing given arguments to the invoke method of the wrapped
 * aggregation-function. To enhance the functionality of an
 * aggregation-function just inherit from this class and override the invoke
 * method.
 * 
 * @param <P> the type of the aggregated values.
 * @param <A> the return type of the function, i.e., the type of the aggregate.
 */
public class DecoratorAggregationFunction<P, A> extends AggregationFunction<P, A> implements Decorator<AggregationFunction<? super P, A>> {

	/**
	 * The aggregation-function to be decorated.
	 */
	protected AggregationFunction<? super P, A> aggregationFunction;

	/**
	 * Constructs a new decorator-aggregation-function wrapping the given
	 * aggregation-function.
	 * 
	 * @param aggregationFunction the aggregation-function to be decorated or
	 *        wrapped.
	 */
	public DecoratorAggregationFunction(AggregationFunction<? super P, A> aggregationFunction){
		this.aggregationFunction = aggregationFunction;
	}

	/**
	 * Passes the arguments to the decorated resp. wrapped aggregation-function
	 * by calling the wrapped aggregation-function's invoke method.
	 *
	 * @param aggregate the aggregation value passed to the wrapped
	 *        aggregation-function.
	 * @param value second argument passed to the wrapped aggregation-function.
	 * @return the returned object of the wrapped aggregation-function.
	 */
	@Override
	public A invoke(A aggregate, P value) {
		return aggregationFunction.invoke(aggregate, value);
	}

	@Override
	public AggregationFunction<? super P, A> getDecoree() {
		return aggregationFunction;
	}
	
}
