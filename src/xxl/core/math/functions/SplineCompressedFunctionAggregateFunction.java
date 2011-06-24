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

import xxl.core.functions.Function;
import xxl.core.math.Statistics;
import xxl.core.math.numerics.splines.CubicBezierSpline;
import xxl.core.math.numerics.splines.RB1CubicBezierSpline;
import xxl.core.math.statistics.parametric.aggregates.OnlineAggregation;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * This class provides a compression algorithm based upon the approximation of real-valued
 * one-dimensional functions by a cubic Bezier-Spline interpolate
 * used as an aggregation 
 * function for online aggregation
 * of given {@link java.lang.Number numerical data} without any error control.
 * One is able to reduce the required memory to
 * store a real-valued function as a result of a aggregation step by substituting the
 * function by an approximating cubic Bezier-Spline interpolate with the first boundary condition.
 * <br><br>
 * Unlike aggregation functions provided by {@link xxl.core.math.statistics.parametric.aggregates.ConfidenceAggregationFunction}
 * objects of
 * this class don't compute confidence intervals for the processed data.
 * Furthermore, this class implements the interface given by
 * {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation}.
 * This interface provides special functionality of aggregation functions by supporting
 * <tt>online</tt> features like watching and controlling .
 * See {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation} for further details.
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * <br>
 * This class shows how one can use an
 * estimation function as an aggregate function using the
 * {@link xxl.core.cursors.mappers.Aggregator aggregator cursor}.
 * {@link xxl.core.functions.Function Functions} used in the context of online
 * aggregation must support two-figured function calls.
 *
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps,
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers only <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized.
 *
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.parametric.aggregates.OnlineAggregation
 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.math.numerics.splines.CubicBezierSpline
 * @see xxl.core.math.numerics.splines.RB1CubicBezierSpline
 */

public class SplineCompressedFunctionAggregateFunction extends AggregationFunction implements OnlineAggregation {

	/** aggregation function delivering real-valued one-dimensional functions
	 */
	protected AggregationFunction function;

	/** function to compress, delivered by {@link #function} */
	protected Object na = null;

	/** used predicate to determine when to build up a new spline */
	protected Predicate buildup;

	/** indicates whether the internally used estimator function is initialized or not */
	protected boolean initialized = false;

	/** left interval border to evaluate the given function and
	 * also the support of the splines to build */
	protected double a;

	/** right interval border to evaluate the given function and
	 * also the support of the splines to build */
	protected double b;

	/** number of steps used to build the spline for compression*/
	protected int n;

	/** used grid for evaluating the function */
	protected double[] grid;

	/** internally built spline */
	protected CubicBezierSpline spline;

	/** Indicates built splines will be in cdf mode, i.e., evaluating
	 * the spline at x > maximum causes the spline to return 1.0 instead of 0.0.
	 * That is necessary for the approximation of a cumulative distribution function.
	 */
	protected boolean cdfMode;

	/** function delivering the left border */
	protected Function minimum = null;
	
	/** function delivering the right border */
	protected Function maximum = null;
	
	/** function determining the grid points */
	protected Function gridPoints = null;
	
	/**
	 * number of build blocks 
	 */
	int no=0;

	/** Constructs a new Object of this class.
	 * 
	 * @param function function to compress
	 * @param buildup predicate determining when to build up a new spline
	 * @param minimum function delivering the left border 
	 * @param maximum function delivering the right border
	 * @param gridPoints function determining the grid points
	 * @param cdfMode indicates whether the spline is in cdf mode
	 */
	public SplineCompressedFunctionAggregateFunction(
			AggregationFunction function,
		Predicate buildup,
		Function minimum,
		Function maximum,
		Function gridPoints,
		boolean cdfMode) {
		this.function = function;
		this.buildup = buildup;
		grid = null;
		spline = null;
		this.minimum = minimum;
		this.maximum = maximum;
		this.gridPoints = gridPoints;
		this.cdfMode = cdfMode;
	}

	/** Constructs a new Object of this class. 
	 * 
	 * @param function function to compress
	 * @param buildup predicate determining when to build up a new spline
	 * @param a left border of the supported real-valued interval
	 * @param b right border of the supported real-valued interval
	 * @param n number of evaluation points used
	 * @param cdfMode indicates whether the spline is in cdf mode
	 */
	public SplineCompressedFunctionAggregateFunction(
			AggregationFunction function,
		Predicate buildup,
		double a,
		double b,
		int n,
		boolean cdfMode) {
		this.function = function;
		this.buildup = buildup;
		this.a = a;
		this.b = b;
		this.n = n;
		grid = null;
		spline = null;
		this.cdfMode = cdfMode;
	}

	/** Constructs a new Object of this class.
	 * 
	 * @param function function to compress
	 * @param a left border of the supported real-valued interval
	 * @param b right border of the supported real-valued interval
	 * @param n number of evaluation points used
	 * @param cdfMode indicates whether the spline is in cdf mode
	 */
	public SplineCompressedFunctionAggregateFunction(AggregationFunction function, double a, double b, int n, boolean cdfMode) {
		this.function = function;
		buildup = new AbstractPredicate() {// indicates whether the current object and the last seen object are equal
			Object last = null;
			public boolean invoke(Object o) {
				boolean r = !(o == last);
				last = o;
				return r;
			}
		};
		this.a = a;
		this.b = b;
		this.n = n;
		grid = null;
		spline = null;
		this.cdfMode = cdfMode;
	}

	/** Constructs a new Object of this class.
	 * 
	 * @param function function to compress
	 * @param buildup predicate determining when to build up a new spline
	 * @param grid sorted grid determining the evaluation points
	 * @param cdfMode indicates whether the spline is in cdf mode
	 */
	public SplineCompressedFunctionAggregateFunction(
			AggregationFunction function,
		Predicate buildup,
		double[] grid,
		boolean cdfMode) {
		this.function = function;
		this.buildup = buildup;
		this.n = -1;
		this.grid = grid;
		spline = null;
		this.cdfMode = cdfMode;
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param old result of the aggregation function in the previous computation step
	 * @param next next object used for computation
	 * @return aggregation value after n steps 
	 */
	public Object invoke(Object old, Object next) {
		na = function.invoke(old, next);
		if (na == null) {
			spline = null;
			buildup.invoke( na);
		} else {
			initialized = true;
			if (buildup.invoke(na)) {
				if (grid == null) {
					if (minimum != null)
						a = ((Number) minimum.invoke(next)).doubleValue();
					if (maximum != null)
						b = ((Number) maximum.invoke(next)).doubleValue();
					if (gridPoints != null)
						n = ((Number) gridPoints.invoke(next)).intValue();
					// building up spline with new (?) grid
					spline =
						new RB1CubicBezierSpline(
							a,
							b,
							n,
							Statistics.evalRealFunction(a, b, n, (RealFunction) na),
							cdfMode);
				} else {
					System.out.println("Build block: "+(++no));
					spline =
						new RB1CubicBezierSpline(grid, Statistics.evalRealFunction(grid, (RealFunction) na), cdfMode);
				}
			}
		}
		return spline;
	}

	/** Returns the current status of the online aggregation function
	 * implementing the OnlineAggregation interface.
	 * 
	 * @return current status of this function (here the current function to compress)
	 */
	public Object getState() {
		if (initialized) {
			return na;
		} else
			return null;
	}

	/** Sets a new status of the online aggregation function
	 * implementing the OnlineAgggregation interface (optional).
	 * This method is not supported by this class.
	 * It is implemented by throwing an UnsupportedOperationException.
	 * 
	 * @param state current state of the function
	 * @throws UnsupportedOperationException if this method is not supported by this class
	 */
	public void setState(Object state) {
		throw new UnsupportedOperationException("not supported");
	}

	/** Sets a new evaluation grid.
	 * 
	 * @param newGrid new evaluation grid
	 */
	public void setGrid(double[] newGrid) {
		n = -1;
		grid = newGrid;
	}

	/** Sets a new evaluation grid.
	 * 
	 * @param a left border of the supported real-valued interval
	 * @param b right border of the supported real-valued interval
	 * @param n number of evaluation points used
	 */
	public void setGrid(double a, double b, int n) {
		this.a = a;
		this.b = b;
		this.n = n;
		grid = null;
	}
}
