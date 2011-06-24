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

package xxl.core.math.statistics.nonparametric.aggregates;

import java.util.List;

import xxl.core.functions.Function;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.statistics.nonparametric.kernels.HybridKernelCDF;
import xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths;
import xxl.core.math.statistics.nonparametric.kernels.KernelFunction;
import xxl.core.math.statistics.parametric.aggregates.OnlineAggregation;

/** In the context of online aggregation, running aggregates are built. Given an 
 * iterator of data, an {@link xxl.core.cursors.mappers.Aggregator Aggregator}
 * computes iteratively aggregates. For instance, the current maximum
 * of the already processed data is determined. An internal aggregation function processes
 * the computation of the new element by consuming the old aggregate and the new element
 * from the input cursor.
 * 
 * Generally, each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>. <br>
 * There, <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> represents the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * <br>
 * 
 * This class implements an aggregation function that computes the <tt>hybrid kernel cumulative 
 * distribution function</tt>. 
 * Given an old and a new sample as processed objects, this function computes the hybrid kernel cdf based on the new sample. 
 * <br>
 * Consider the following example that displays a concrete application of a
 * hybrid kernel cdf aggregation function combined with an aggregator:
 * <code><pre>
 	Aggregator aggregator =
			new Aggregator(
				new Aggregator( input, mapSamplingStrategy (sampleSize, samplingType)),
				new HybridKernelCDFAggregateFunction()
			);
 * </pre></code>
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.math.functions.AdaptiveAggregationFunction
 * @see xxl.core.math.statistics.nonparametric.aggregates.Aggregators
 * @see xxl.core.math.statistics.nonparametric.kernels.HybridKernelCDF
 */
public class HybridKernelCDFAggregateFunction extends AggregationFunction<List,HybridKernelCDF> implements OnlineAggregation {

	/** used kernel function*/
	protected KernelFunction kf;

	/** used estimator function for online aggregation */
	protected HybridKernelCDF hkse;

	/** lower border used for reflection */
	protected double min;

	/** upper border used for reflection */
	protected double max;

	/** internally used to store the given samples */
	protected Object[] sample;

	/** internally used to store the given change points */
	protected Object[] cps;

	/** internally used to store the current variance respectively spread of the data */
	protected double variance;

	/** used type of bandwidth */
	protected int bandwidthType;

	/** used CDF factory for building internally used estimators */
	protected Function factory;

	/** Constructs a new hybrid kernel cdf aggregate function for given kernel function,
	 * bandwidth strategy and FACTORY for the resulting estimators.
	 * 
	 * @param kf used kernel function for building the kernel based estimation function
	 * @param bandwidthType indicates the type of bandwidth used 
	 * @param factory function constructing the resulting estimators
	 */
	public HybridKernelCDFAggregateFunction(KernelFunction kf, int bandwidthType, Function factory) {
		this.kf = kf;
		this.bandwidthType = bandwidthType;
		this.factory = factory;
	}

	/**Constructs a new hybrid kernel cdf aggregate function for a given kernel function.
	 * Initially, the normal scale rule is used for the bandwidth and a FACTORY for
	 * constructing hybrid kernel cdf's.
	 * 
	 * @param kf used kernel function for building the kernel based estimation function
	 */
	public HybridKernelCDFAggregateFunction(KernelFunction kf) {
		this(kf, KernelBandwidths.THUMB_RULE_1D, HybridKernelCDF.FACTORY);
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
	 * @return aggregation value after n steps, i.e., a hybrid kernel cdf
	 */
	public HybridKernelCDF invoke(
		HybridKernelCDF old,
		List next) { // next[0] = sample, next[1] = Double-Object with variance, next[2] = min , next[3] = max, next[4] = (Object[]) with jump points
		if (next == null)
			return null;
		if ((Object[]) next.get(0) == null)
			return null;
		// if given next (Object[]) != null, all fields of the array are filled
		min = (Double)next.get(2);
		max = (Double)next.get(3);
		variance = (Double)next.get(1);
		sample = (Object[]) next.get(0);
		cps = (Object[]) next.get(4);
		if (hkse == null)
			hkse = new HybridKernelCDF(kf, sample, bandwidthType, min, max, cps, factory);
		else {
			hkse.setSample(sample);
			hkse.setBounds(min, max);
			hkse.setChangePoints(cps);
		}
		return hkse;
	}

	/** Returns the current status of the on-line aggregation function
	 * implementing the OnlineAggregation interface.,
	 * 
	 * @return current state of this function
	 */
	public Object getState() {
		if (hkse != null) {
			return sample;
		} else
			return null;
	}

	/** Sets a new status of the on-line aggregation function
	 * implementing the OnlineAgggregation interface (optional).
	 * This method is not supported by this class.
	 * It is implemented by throwing an UnsupportedOperationException.
	 * 
	 * @param status current state of the function
	 * @throws UnsupportedOperationException if this method is not supported by this class
	 */
	public void setState(Object status) {
		throw new UnsupportedOperationException("not supported");
	}
}
