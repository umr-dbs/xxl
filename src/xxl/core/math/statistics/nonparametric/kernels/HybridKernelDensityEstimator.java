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

package xxl.core.math.statistics.nonparametric.kernels;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import xxl.core.comparators.ComparableComparator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.Statistics;

/**
 * This class provides an implementation of a <tt>kernel based density estimator</tt>
 * based upon a simple random sample
 * for data given by objects of type <tt>Number</tt>.
 * The estimator is updatable, so one can use it as an aggregate function
 * combined with a special {@link xxl.core.cursors.mappers.Aggregator aggregation iterator}.
 *
 * This class implements a special kind of kernel density estimators using a hybrid
 * technique first performed by Blohsfeld et. al. in <BR>
 * [BKS99]: Bjoern Blohsfeld, Dieter Korus, Bernhard Seeger:
 * A Comparison of Selectivity Estimators for Range Queries on Metric Attributes.
 * SIGMOD Conference 1999: 239-250. 
 * <BR>
 * Generally, a set of separate kernel based density estimators is given whereas each one
 * is assigned to a partial interval respectively bin. The bins in turn build a
 * partition of the x-Axis. There, the borders should correspond to change points of the
 * estimated pdf.
 * These change points can be estimated, if they are not a priori known.
 * The estimators of the bins are kernel density estimators
 * using reflection as a boundary treatment. See 
 * {@link xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelDensityEstimator ReflectionKernelDensityEstimator}
 * for details about using reflection. <BR>
 *
 * The estimator is implemented as a one-dimensional
 * {@link xxl.core.functions.Function function} expecting
 * objects of type <tt>Number</tt> as well as a
 * {@link xxl.core.math.functions.RealFunction RealFunction} expecting
 * data of type <tt>double</tt>.
 * A sample of any kind of data given by an iterator could
 * be easily obtained through the {@link xxl.core.cursors.mappers.ReservoirSampler reservoir sampler
 * cursor}.
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see java.util.Iterator
 * @see xxl.core.functions.Function
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelFunction
 * @see xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths
 * @see xxl.core.math.statistics.nonparametric.kernels.AbstractKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelDensityEstimator
 * @see xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator#FACTORY
 * @see xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelDensityEstimator#FACTORY
 */

public class HybridKernelDensityEstimator extends AbstractKernelDensityEstimator {

	/** minimum of the data (estimated or computed), i.e., the left border of the data */
	protected double min;

	/** maximum of the data (estimated or computed), i.e., the right border of the data */
	protected double max;

	/** border points of the used partitioning of type <tt>Number</tt>,
	 * so called change points */
	protected Object[] changePoints;

	/** bandwidth used for every bin. If set to <tt>null</tt>, automatic smoothing will be performed. */
	protected double[] hs;

	/** functions estimating the assigned bins */
	protected Function[] binEstimators;

	/** the factory delivering the estimators used for the bins */
	protected Function kernelDensityEstimatorFactory;

	/** index of last used estimator */
	protected int lastUsedFunction = -1;

	/** sample assigned to the enclosing bins */
	protected Object[] binSamples;

	/** Indicates whether the bandwidths are computed based upon the normal scale rule
	 * using the sample. If set to false, the user has to assign the bandwidths*/
	protected boolean automaticBandwidthMode = false;

	/** Provides a factory for a
	 * {@link xxl.core.math.statistics.nonparametric.kernels.HybridKernelDensityEstimator hybrid kernel density estimator}
	 * The parameters needed for construction are passed to the factory by an
	 * <tt>Object[]</tt> <code>o</code> containing: <BR>
	 * <code>o[0]</code>: used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function} <BR>
	 * <code>o[1]</code>: used sample as <tt>Object[]</tt> containing numerical data <BR>
	 * <code>o[2]</code>: used change points as <tt>Object[]</tt> containing numerical data <BR>
	 * <code>o[3]</code>: used bandwidths as <tt>Object[]</tt> <BR>
	 * <code>o[4]</code>: minimum <BR>
	 * <code>o[5]</code>: maximum <BR>
	 * <code>o[6]</code>: confidence level <BR>
	 */
	public static Function FACTORY = new AbstractFunction<Object,HybridKernelDensityEstimator>() {
		public HybridKernelDensityEstimator invoke(List<? extends Object> list) {
				return new HybridKernelDensityEstimator((KernelFunction) list.get(0), // kernel function
				 (Object[]) list.get(1), // sample
				 (Object[]) list.get(2), // change points
				 (double[]) list.get(3), // bandwidths
				 ((Number) list.get(4)).doubleValue(), // min
				 ((Number) list.get(5)).doubleValue(), // max
				 ((Number) list.get(6)).doubleValue() // alpha
				);
		}
	};

	/**
	 * Constructs an estimator for a density function using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>
	 * @param changePoints points where to "break" the sample
	 * @param hs used bandwidths for the bins. If set to <tt>null</tt> automatic smoothing using the 
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performed
	 * @param min left border of the data space
	 * @param max right border of the data space
	 * @param alpha confidence level used for computing an asymptotic pointwise confidence interval
	 * @param kernelDensityEstimatorFactory a factory delivering kernel estimators used for the bins
	 */
	public HybridKernelDensityEstimator(
		KernelFunction kf,
		Object[] sample,
		Object[] changePoints,
		double[] hs,
		double min,
		double max,
		double alpha,
		Function kernelDensityEstimatorFactory) {
		super(kf, sample, 0.0, alpha);
		this.min = min;
		this.max = max;
		if (hs == null)
			automaticBandwidthMode = true;
		else
			this.hs = hs;
		this.changePoints = changePoints;
		Arrays.sort(this.changePoints);
		this.kernelDensityEstimatorFactory = kernelDensityEstimatorFactory;
		hasChanged = true;
	}

	/**
	 * Constructs an estimator for a density function using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param changePoints points where to "break" the sample rsp.&nbspwhere to bound the bins
	 * @param hs used bandwidths for the bins. If set to <tt>null</tt> automatic smoothing using the 
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performmed.
	 * @param min left border of the data space
	 * @param max right border of the data space
	 */
	public HybridKernelDensityEstimator(
		KernelFunction kf,
		Object[] sample,
		Object[] changePoints,
		double[] hs,
		double min,
		double max) {
		this(kf, sample, changePoints, hs, min, max, -1.0, ReflectionKernelDensityEstimator.FACTORY);
	}

	/**
	 * Constructs an estimator for a density function using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param changePoints points where to "break" the sample rsp.&nbspwhere to bound the bins
	 * @param hs used bandwidths for the bins. If set to <tt>null</tt> automatic smoothing using the 
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performmed.
	 * @param min left border of the data space
	 * @param max right border of the data space
	 * @param alpha confidence level used for computing an asymtotic pointwise confidence interval
	 */
	public HybridKernelDensityEstimator(
		KernelFunction kf,
		Object[] sample,
		Object[] changePoints,
		double[] hs,
		double min,
		double max,
		double alpha) {
		this(kf, sample, changePoints, hs, min, max, alpha, ReflectionKernelDensityEstimator.FACTORY);
	}

	/** Evaluates the kernel based density estimator at given point x.
	 * 
	 * @param x argument where to evaluate the density estimation
	 * @return value of the estimated density at x
	 */
	protected double evalKDE(double x) {
		if ((x < min) || (x > max))
			return 0.0; // return zero because function is bounded
		else {
			double r = 0.0;
			// finding the "right" bin, evaluating the "assigned" estimator
			for (int i = 0; i < binEstimators.length; i++) {
				double f = ((Number) binEstimators[i].invoke(new Double(x))).doubleValue();
				if (f != 0.0) {
					lastUsedFunction = i;
					// weighting of the used estimator according to the number of sample values in this bin
					f = f * ((Object[]) binSamples[i]).length / sample.length;
				}
				r += f;
			}
			// ---
			return r;
		}
	}

	/** Sets the new bounds ,e.g.,&nbsp;lower and upper border of the given data or the treated domain space.
	 * 
	 * @param newMin new lower border
	 * @param newMax new upper border
	 */
	public void setBounds(double newMin, double newMax) {
		min = newMin;
		max = newMax;
		hasChanged = true;
	}

	/** Sets the new change points ,e.g.,&nbsp;the data points determining the bins.
	 * If the number of used change points needs to be changed, do it first, than
	 * give the new bandwidths.
	 * 
	 * @param cp new change points used for partitioning
	 */
	public void setChangePoints(Object[] cp) {
		changePoints = cp;
		Arrays.sort(changePoints);
		hasChanged = true;
	}

	/** Sets the new bandwidths. If the number of used change points needs to be changed,
	 * the new change points must be given BEFORE updating the bandwidths or an
	 * {@link java.lang.IllegalArgumentException IllegalArgumentException} occurs.
	 * The given bandwidths are used in the given order corresponding to the ascending
	 * orderd bins.
	 * 
	 * @param newBandwidths new bandwidths to use. If set to <tt>null</tt> automatic smoothing is enabled.
	 * @throws IllegalArgumentException if the number of bandwidths does not correspond to the number of bins used for estimation
	 * (the number of change points +1)
	 */
	public void setBandwidths(double[] newBandwidths) throws IllegalArgumentException {
		if (newBandwidths == null) {
			automaticBandwidthMode = true;
			return;
		} else {
			if (newBandwidths.length != changePoints.length + 1)
				throw new IllegalArgumentException("wrong number of bandwidths given!");
			automaticBandwidthMode = false;
			hs = newBandwidths;
		}
	}

	/** Reinitilizes the estimator after changes. */
	protected void reinit() {
		// splitting the sample
		// building from change points and borders a new Object[]
		Object[] borders = new Object[changePoints.length + 2];
		borders[0] = new Double(min);
		borders[borders.length - 1] = new Double(max);
		//
		if (changePoints.length == 0) {
			binSamples = new Object[] { sample };
		} else {
			System.arraycopy(changePoints, 0, borders, 1, changePoints.length);
			// split the data with the borders
			binSamples = split(sample, borders);
		}
		// automatic bandwidth mode? -> computed the variances and the bandwidth for each splitted sample
		if (automaticBandwidthMode) {
			hs = new double[binSamples.length];
			for (int i = 0; i < hs.length; i++) {
				double var = Math.sqrt(Statistics.variance((Object[]) binSamples[i]));
				hs[i] = KernelBandwidths.normalScaleRule(((Object[]) binSamples[i]).length, kf, var);
			}
		}
		// computing new bandwidth ??? if number of change points changed?
		// --> No, changed bandwidths needed to be given (from outer)
		// producing new estimators for the bins
		binEstimators = new Function[binSamples.length];
		// first bin
		List<Object> parameters = null;
		for (int i = 0; i < binEstimators.length; i++) {
			parameters =
				Arrays.asList( kf, binSamples[i], new Double(hs[i]), borders[i], borders[i + 1], new Double(alpha));
			binEstimators[i] = (Function) kernelDensityEstimatorFactory.invoke(parameters);
		}
		//
		hasChanged = false;
	}

	/**
	 * Computes the confidence intervals for given confidence level alpha.
	 * 
	 * @return confidence intervals
	 * @throws UnsupportedOperationException operation not supported
	 */
	public double[] epsilon() throws UnsupportedOperationException {
		double[] r = null;
		if (alpha < 0) {
			throw new UnsupportedOperationException("computing of confidence intervals not supported by this estimator");
		} else {
			if (hasChanged)
				reinit();
			// find the "right" estimator
			r = ((AbstractKernelDensityEstimator) binEstimators[lastUsedFunction]).epsilon();
			r[0] *= ((double) ((Object[]) binSamples[lastUsedFunction]).length / (double) sample.length);
			r[1] *= ((double) ((Object[]) binSamples[lastUsedFunction]).length / (double) sample.length);
		}
		return r;
	}

	/** Splits given data into subarrays bounded by the given borders.
	 *
	 * @param data points to split into disjoint subarrays
	 * @param borders borders of the bins
	 * @return an <tt>Object[]</tt> containing Objects of type <tt>Object[]</tt>
	 * filled with data that hold <BR><tt>
	 * borders[i] <= min(i) <= max(i) < borders[i+1], i=0, ...,  boerders.length-1</tt><br>. 
	 */
	public static Object[] split(Object[] data, Object[] borders) {
		return split(data, borders, new ComparableComparator());
	}

	/** Splits given data into subarrays bounded by the given borders with a user-defined comparator.
	 *
	 * @param data points to split into disjoint subarrays
	 * @param borders borders of the bins
	 * @param comparator user-defined comparator
	 * @return an <tt>Object[]</tt> containing Objects of type <tt>Object[]</tt>
	 * filled with data that hold <BR><tt>
	 * borders[i] <= min(i) <= max(i) < borders[i+1], i=0, ...,  boerders.length-1</tt><br>. 
	 */
	public static Object[] split(Object[] data, Object[] borders, Comparator comparator) {
		Object[] r = new Object[borders.length - 1];
		// sorting the data and the borders
		Arrays.sort(data, comparator);
		Arrays.sort(borders, comparator);
		int from = 0;
		int i = 0;
		// Find start ( only if NOT borders[0] <= data[0])
		while (comparator.compare(data[i], borders[0]) < 0)
			i++;
		from = i;
		for (int j = 1; j < borders.length - 1; j++) {
			// find index, where  data[index] < borders[i+1] <= data[index+1];
			while ((i < data.length) && (comparator.compare(data[i], borders[j]) <= 0))
				i++;
			int l = i - from;
			Object[] temp = new Object[l];
			System.arraycopy(data, from, temp, 0, l);
			r[j - 1] = temp;
			from = i;
		}
		// Find end (only if NOT data[end] < borders[end]) --> to prove
		while ((i < data.length) && (comparator.compare(data[i], borders[borders.length - 1]) < 0))
			i++;
		int end = i;
		// --
		Object[] temp = new Object[end - from];
		System.arraycopy(data, from, temp, 0, temp.length);
		r[r.length - 1] = temp;
		// ---
		return r;
	}
}
