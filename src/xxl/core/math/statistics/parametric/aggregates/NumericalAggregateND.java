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

import java.util.Arrays;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.functions.AggregationFunction;

/**
 * This class combines a given number of aggregate functions for numerical data to 
 * process data of type <tt>double []</tt> with aggregate functions consuming objects
 * of type {@link java.lang.Number number} in one pass. To do so, the results
 * of the aggregation functions are internally stored and combined to
 * objects of type <tt>double []</tt>. Each column of the returned <tt>double []</tt>
 * represents the same column in the given data.
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers only <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized.
 * <br>
 * See {@link xxl.core.math.statistics.parametric.aggregates.OnlineAggregation OnlineAggregation} for further details about 
 * aggregation functions using internally stored information.
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.parametric.aggregates.OnlineAggregation
 */

public class NumericalAggregateND extends AggregationFunction<double[],double[]> {

	/** aggregation function for n-dimensional numerical data delivering variances (each for a row)*/
	public static AggregationFunction VARIANCE_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new StatefulVariance();
		}
	});

	/** aggregation function for n-dimensional numerical data delivering standard deviations (each for a row)*/
	public static AggregationFunction STANDARDDEVIATION_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new StatefulStandardDeviation();
		}
	});

	/** aggregation function for n-dimensional numerical data delivering averages (each for a row)*/
	public static AggregationFunction AVERAGE_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new StatefulAverage();
		}
	});

	/** aggregation function for n-dimensional numerical data delivering minima (each for a row)*/
	public static AggregationFunction MINIMUM_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new Minimum();
		}
	});

	/** aggregation function for n-dimensional numerical data delivering maxima (each for a row)*/
	public static AggregationFunction MAXIMUM_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new Maximum();
		}
	});

	/** aggregation function for n-dimensional numerical data delivering sums (each for a row)*/
	public static AggregationFunction SUM_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new Sum();
		}
	});

	/** aggregation function for n-dimensional numerical data delivering counts (each for a row; should all be similar)*/
	public static AggregationFunction COUNT_ND = new NumericalAggregateND(new AbstractFunction() {
		public Object invoke() {
			return new Count();
		}
	});

	/** indicates whether this function has been initialized */
	protected boolean initialized = false;

	/** indicates whether the wrapped functions have been initialized */
	protected boolean init[];

	/** indicates whether all wrapped functions have been initialized */
	protected boolean initAll;

	/** internally stored status of the used aggregation functions */
	protected Number[] status;

	/** internally used one-dimensional aggregate functions working on numerical data */
	protected AggregationFunction<Number,Number>[] aggregates1D;

	/** dimension of the combination of the numerical aggregate functions */
	protected int dim;

	/** factory used to obtain the internally used aggregation functions */
	protected Function factory;

	/** Constructs a new object of this class.
	 * The dimension of the consumed objects of type <tt>double []</tt> needs to match the first
	 * consumed object. The factory needs to deliver at least <tt>dim</tt> aggregate functions
	 * to initialize to internally used <tt>function []</tt>.
	 *
	 * @param aggregateFunctionFactory factory delivering internally used aggregation functions.
	 */
	public NumericalAggregateND(Function aggregateFunctionFactory) {
		factory = aggregateFunctionFactory;
		initialized = false;
	}

	/** Constructs a new object of this class.
	 * The dimension of the consumed objects of type <tt>double []</tt> treated as real-valued vector
	 * needs to match the number of given aggregate functions.
	 *
	 * @param aggregateFunctions internally used aggregation functions based on numerical data
	 */
	public NumericalAggregateND(AggregationFunction[] aggregateFunctions) {
		aggregates1D = aggregateFunctions;
		initialized = false;
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param previous result of the aggregation function in the previous computation step
	 * @param next next object used for computation given as object of type <tt>double []</tt>
	 * @return aggregation value after n steps
	 */
	public double[] invoke(double[] previous, double[] next) {
		double[] n = next;
		if (!initialized) {
			if (aggregates1D == null) { // a factory has been given
				dim = n.length; // get the dimension from the first given double []
				aggregates1D = new AggregationFunction[dim]; // int the function []
				for (int i = 0; i < dim; i++) {
					aggregates1D[i] = (AggregationFunction) factory.invoke(); // filling the function [] with agg. functions
				}
			} // init the rest or with the given Function []
			dim = aggregates1D.length;
			status = new Number[dim];
			init = new boolean[dim];
			Arrays.fill(init, false);
			initialized = true;
			initAll = false;
		}
		double[] p = null;
		if (previous == null)
			p = new double[dim];
		else {
			p = previous;
			for (int i = 0; i < dim; i++) {
				status[i] = new Double(p[i]);
			}
		}

		int numberOfInits = 0;
		for (int i = 0; i < dim; i++) {
			status[i] = aggregates1D[i].invoke(status[i], new Double(n[i]));
			if (status[i] != null) { // checking if the result is not null
				init[i] = true;
				numberOfInits++; // count the non trivial answers
			}
		}
		if (numberOfInits == dim)
			initAll = true; // all answers not null => set allInit =  true
		if (initAll) { // if all used agg. functions are init., return an object != null
			double[] r = new double[dim];
			for (int i = 0; i < dim; i++) {
				r[i] = status[i].doubleValue();
			}
			return r;
		} else { // not all agg. functions are already init., so return null
			return null;
		}
	}
}
