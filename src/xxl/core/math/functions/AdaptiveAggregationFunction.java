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

import java.util.Arrays;
import java.util.List;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/** 
 * This class provides an adaptive online aggregation strategy by
 * performing a convex merging of two objects. This is typically 
 * realized with a
 * {@link xxl.core.math.functions.LinearCombination convex-linear combination}
 * of the last returned and every new
 * function delivered by the wrapped aggregation function, i.e. in each step
 * functions are provided and merged.
 * But the user can also realize his own merge procedure based on the current objects
 * and weights.
 * <BR>  
 * For the weighting of the functions different strategies exist that allow a time-dependent emphasis
 * of the functions. The strategies in turn are realized with 
 * {@link xxl.core.math.functions.AdaptiveWeightFunctions adaptive weight functions}.
 * <BR>
 * This function has two phases during runtime (exemplified with functions):<BR>
 * 1) Initialize: As far as the internal used (wrapped) aggregation function
 *    returns null-objects, the adaptive aggregation function will do this as well.
 *    The first non-null object returned by the wrapped function will be stored internally
 *    and passed directly to the consumer, i.e., it will be directly returned.
 * 2) Working: For every NEW function returned by the wrapped function, a counter
 *    is incremented, the corresponding weight is computed and a linear 
 *    combination of the old function and the new return function 
 *    is returned. The code for determining a NEW function looks like <BR>
 *    <PRE>
 *    <CODE>
			if( lastF == newF){
				return lastF;
			}
			else{
				// COMPUTE new function
			}
 *    </CODE>
 *    </PRE>
 *
 * Generally, each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>. <br>
 * There, <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> represents the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet been fully initialized. 
 * <br>
 * 
 * For a more detailed discussion concerning the idea of online aggregation
 * we refer to [HHW97]:P. Haas, J. Hellerstein, H. Wang, Online Aggregation, 1997.
 * 
 * @see xxl.core.math.functions.RealFunction
 * @see xxl.core.math.functions.LinearCombination
 * @see xxl.core.math.functions.AdaptiveWeightFunctions
 * @see xxl.core.functions.Function
 */

public class AdaptiveAggregationFunction extends AggregationFunction<Object,Object> {

	/** internally used (wrapped) aggregation function providing every k-th step a
	 *  NEW estimator 
	 */
	protected AggregationFunction function;

	/** RealFunction that consumes the current step and returns the corresponding 
	 * weight
	 */
	protected RealFunction weights;

	/** current step, i.e., current number of already combined estimators */
	protected int step;

	/** internally stored aggregation value of the previous aggregation step performed
	 * by the function 
	 */
	private Object internalOld;

	/** Indicates whether the adaptive estimator is in 'RealFunction' mode, i.e.,
	 * the functions returned from the wrapped aggregation function will be treated as
	 * objects of type {@link xxl.core.math.functions.RealFunction}. Otherwise objects of type
	 * {@link xxl.core.functions.Function Function}
	 * consuming objects of type <TT>Number</TT> will be assumed.
	 */
	protected boolean realMode;

	/**
	 * Internal factory for the convex-linear merge of two objects.
	 * Typically, this is realized by building a convex linear combination of two 
	 * RealFunctions.
	 */
	protected Function<Object,? extends Object> convexMergeFunction;

	/** Constructs a new object of this type. The estimator is assumed not to be in the real mode.
	 *
	 * @param function wrapped aggregation function
	 * @param weights delivering weights for every adaptive step
	 * @param convexMergeFunction internal factory for the convex-linear merge of two objects
	 */
	public AdaptiveAggregationFunction(AggregationFunction function, RealFunction weights, Function<Object,? extends Object> convexMergeFunction) {
		this.function = function;
		this.weights = weights;
		this.convexMergeFunction = convexMergeFunction;
		step = 0;
		internalOld = null;
	}

	/** Constructs a new object of this type. 
	 *
	 * @param function wrapped aggregation function
	 * @param weights delivering weights for every adaptive step
	 * @param realMode indicates whether the estimator is in real mode,i.e.,
	 * the wrapped aggregations functions will return objects of
	 * type {@link RealFunction}
	 * @param convexMergeFunction internal factory for the convex merge of two objects
	 */
	public AdaptiveAggregationFunction(
			AggregationFunction function,
		RealFunction weights,
		boolean realMode,
		Function<Object,? extends Object> convexMergeFunction) {
		this.function = function;
		this.weights = weights;
		this.convexMergeFunction = convexMergeFunction;
		step = 0;
		internalOld = null;
		this.realMode = realMode;
	}

	/** Constructs a new object of this type. Initially, convex linear combinations of two Functions 
	 * respectively RealFunctions are built.
	 *
	 * @param function wrapped aggregation function
	 * @param weights delivering weights for every adaptive step
	 * @param realMode indicates whether the estimator is in real mode,i.e.,
	 * the wrapped aggregations functions will return objects of
	 * type {@link RealFunction}
	 */
	public AdaptiveAggregationFunction(AggregationFunction function, RealFunction weights, final boolean realMode) {
		this(function, weights, false, new AbstractFunction<Object,LinearCombination>() {
			public LinearCombination invoke(List<? extends Object> list) {
				if (realMode) {
					return new LinearCombination(
						(RealFunction) list.get(0),
						((Double) list.get(1)).doubleValue(),
						(RealFunction) list.get(2),
						((Double) list.get(3)).doubleValue());
				}
				return new LinearCombination(
					(Function) list.get(0),
					((Double) list.get(1)).doubleValue(),
					(Function) list.get(2),
					((Double) list.get(3)).doubleValue());
			}
		});
	}

	/** Constructs a new object of this type. The estimator is assumed not to be in the real mode.
	 * Initially, convex linear combinations of two functions 
	 * are built.
	 *
	 * @param function wrapped aggregation function
	 * @param weights delivering weights for every adaptive step
	 */
	public AdaptiveAggregationFunction(AggregationFunction function, RealFunction weights) {
		this(function, weights, false, new AbstractFunction<Object,LinearCombination>() {
			public LinearCombination invoke(List<? extends Object> list) {
				return new LinearCombination(
					(Function) list.get(0),
					((Double) list.get(1)).doubleValue(),
					(Function) list.get(2),
					((Double) list.get(3)).doubleValue());
			}
		});
	}

	/** Constructs a new object of this type using an arithmetic weighting. 
	 * The estimator is assumed not to be in the real mode. Initially, convex linear combinations of two functions 
	 * are built.
	 *
	 * @param function wrapped aggregation function
	 */
	public AdaptiveAggregationFunction(AggregationFunction function) {
		this(function, new AdaptiveWeightFunctions.ArithmeticWeights(), false, new AbstractFunction<Object,LinearCombination>() {
			public LinearCombination invoke(List<? extends Object> list) {
				return new LinearCombination(
					(Function) list.get(0),
					((Double) list.get(1)).doubleValue(),
					(Function) list.get(2),
					((Double) list.get(3)).doubleValue());
			}
		});
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>.
	 * There, <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet been initialized.
	 * 
	 * @param old result of the aggregation function in the previous computation step
	 * @param next next object used for computation
	 * @return aggregation value after n steps 
	 */
	public Object invoke(Object old, Object next) {
		Object na = function.invoke(internalOld, next);
		if (na == null)
			return null;
		// The first one?
		if (old == null) {
			internalOld = na;
			step = 1;
			return na;
		}
		else { // no, not the first yet
			// new result of wrapped aggregation?
			if (internalOld == na) { // no
				return old; // return "old" result
			}
			else { // yes, new internal estimator
				internalOld = na;
				step++;
				double w = weights.eval(step);
				return convexMergeFunction.invoke(Arrays.asList(old, new Double((1.0 - w)), na, new Double(w)));
			}
		}
	}
}
