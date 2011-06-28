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
import java.util.List;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.Statistics;

/**
 * This class provides an implementation of a kernel based estimator for
 * a <tt>cumulative distribution function (cdf)</tt>.
 * based upon a simple random sample
 * for data given by objects of type <tt>Number</tt>.
 * The estimator is updatable, so one can use it as an aggregate function
 * combined with a special {@link xxl.core.cursors.mappers.Aggregator aggregation iterator}.
 * <br>
 * This class implements a special kind of kernel cdf estimators using a hybrid
 * technique first performed by Blohsfeld et. al. in <BR>
 * [BKS99]: Bjoern Blohsfeld, Dieter Korus, Bernhard Seeger:
 * A Comparison of Selectivity Estimators for Range Queries on Metric Attributes.
 * SIGMOD Conference 1999: 239-250. 
 * <BR>
 * Generally, a set of separate kernel based cdf estimators is given whereas each one
 * is assigned to a partial interval respectively bin. The bins in turn build a
 * partition of the x-Axis. There, the borders should correspond to change points of the
 * estimated pdf.
 * These change points can be estimated, if they are not a priori known.
 * The estimators of the bins are kernel cdf estimators
 * using reflection as a boundary treatment. See 
 * {@link xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelCDF ReflectionKernelCDFEstimator}
 * for details about using reflection. <BR>
 * <br>
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

public class HybridKernelCDF extends AbstractKernelCDF {

	/** Provides a factory for a
	 * {@link xxl.core.math.statistics.nonparametric.kernels.HybridKernelCDF hybrid kernel distribution estimator}.
	 * The parameters needed for construction are passed to the factory by an
	 * <tt>Object[]</tt> <code>o</code> containing: <BR>
	 * <code>o[0]</code>: used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function} <BR>
	 * <code>o[1]</code>: used sample as <tt>Object[]</tt> containing numerical data <BR>
	 * <code>o[2]</code>: indicates the type of the used bandwidths as Object of type <tt>Integer</tt> <BR>
	 * <code>o[3]</code>: minimum of the data space as Object of type <tt>Number</tt><BR>
	 * <code>o[4]</code>: maximum of the data space as Object of type <tt>Number</tt><BR>
	 * <code>o[5]</code>: used bin borders to build up the estimator given as Objects of type <tt>Number</tt><BR>
	 */
	public static Function FACTORY = new AbstractFunction<Object,HybridKernelCDF>() {
		public HybridKernelCDF invoke(List<? extends Object> list) {
			int h = KernelBandwidths.THUMB_RULE_1D;
			try {
				h = ((Integer) list.get(2)).intValue();
			} catch (ClassCastException cce) {}
			//
			return new HybridKernelCDF((KernelFunction) list.get(0),
			// kernel function
			 (Object[]) list.get(1), // sample
			h, // type of the used bandwidths
			 ((Number) list.get(3)).doubleValue(), // minimum
			 ((Number) list.get(4)).doubleValue(), // maximum
			 (Object[]) list.get(5) // jump points/change points/bin borders
			);
		}
	};

	/** minimum of the data (estimated or computed), i.e., left border of the data*/
	protected double min;

	/** maximum of the data (estimated or computed), i.e., right border of the data */
	protected double max;

	/** border points of the used partitioning of type <tt>Number</tt>,
	 * so called change points */
	protected Object[] changePoints;

	/** bandwidth for every bin. If set to <tt>null</tt> automatic smoothing will be performed */
	protected double[] hs;

	/** functions estimating the assigned bin */
	protected Function[] binEstimators;

	/** factory delivering the estimators used for the bins */
	protected Function cdfEstimatorFactory;

	/** index of last used estimator */
	protected int lastUsedFunction = -1;

	/** sample assigned to the enclosing bins */
	protected Object[] binSamples;

	/** indicates whether the bandwidths are computed based upon the normal scale rule
	 * using the sample. If set to false, the bandwidths needed to be given by the user */
	protected boolean automaticBandwidthMode = false;

	/** used type of bandwidth */
	protected int bandwidthType = -1;

	/**
	 * Constructs an estimator for a cdf using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param bandwidthType type of bandwidth used in automatic mode
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performed
	 * @param min left border of the data space
	 * @param max right border of the data space
	 * @param changePoints points where to "break" the sample
	 * @param cdfEstimatorFactory a factory delivering kernel estimators used for the bins
	 */
	public HybridKernelCDF(
		KernelFunction kf,
		Object[] sample,
		int bandwidthType,
		double min,
		double max,
		Object[] changePoints,
		Function cdfEstimatorFactory) {
		super(kf, sample, 0.0);
		this.min = min;
		this.max = max;
		hs = null;
		this.bandwidthType = bandwidthType;
		automaticBandwidthMode = true;
		//
		this.changePoints = changePoints;
		Arrays.sort(this.changePoints);
		this.cdfEstimatorFactory = cdfEstimatorFactory;
		hasChanged = true;
	}

	/**
	 * Constructs an estimator for a cdf using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis. The factory delivers reflection kernel based cdf's.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param bandwidthType type of bandwidth used in automatic mode
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performed.
	 * @param min left border of the data space
	 * @param max right border of the data space
	 * @param changePoints points where to "break" the sample
	 */
	public HybridKernelCDF(
		KernelFunction kf,
		Object[] sample,
		int bandwidthType,
		double min,
		double max,
		Object[] changePoints) {
		this(kf, sample, bandwidthType, min, max, changePoints, ReflectionKernelCDF.FACTORY);
	}

	/**
	 * Constructs an estimator for a cdf using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param hs used bandwidths for the bins. If set to <tt>null</tt> automatic smoothing using the 
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performed.
	 * @param min left border of the data space
	 * @param max right border of the data space
	 * @param changePoints points where to "break" the sample
	 * @param cdfEstimatorFactory a factory delivering kernel estimators used for the bins
	 */
	public HybridKernelCDF(
		KernelFunction kf,
		Object[] sample,
		double[] hs,
		double min,
		double max,
		Object[] changePoints,
		Function cdfEstimatorFactory) {
		super(kf, sample, 0.0);
		this.min = min;
		this.max = max;
		this.hs = hs;
		automaticBandwidthMode = false;
		this.changePoints = changePoints;
		Arrays.sort(this.changePoints);
		this.cdfEstimatorFactory = cdfEstimatorFactory;
		hasChanged = true;
	}

	/**
	 * Constructs an estimator for a cdf using the given
	 * kernel function by defining separate estimators for a well-defined partition
	 * of the x-axis.
	 *
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction Kernel function}.
	 * @param sample sample of a data set given as <tt>Object[]</tt> containing
	 * objects of type <tt>Number</tt>.
	 * @param hs used bandwidths for the bins. If set to <tt>null</tt> automatic smoothing using the 
	 * {@link xxl.core.math.statistics.nonparametric.kernels.KernelBandwidths#normalScaleRule(int,double,double,double) normal scale rule}
	 * will be performed.
	 * @param min left border of the data space
	 * @param max right border of the data space
	 * @param changePoints points where to "break" the sample rsp.&nbspwhere to bound the bins
	 */
	public HybridKernelCDF(
		KernelFunction kf,
		Object[] sample,
		double[] hs,
		double min,
		double max,
		Object[] changePoints) {
		this(kf, sample, hs, min, max, changePoints, ReflectionKernelCDF.FACTORY);
	}

	/** Evaluates the kernel based density estimator at given point x.
	 * 
	 * @param x argument where to evaluate the cdf estimation
	 * @return value of the estimated density at x
	 */
	protected double evalKDE(double x) {
		if (x < min)
			return 0.0; // return zero because function is bounded
		if (x > max)
			return 1.0; // return 1 because function is bounded
		// else
		double r = 0.0;
		// finding the "right" bin
		int bin = 0;
		if (x > ((Number) changePoints[changePoints.length - 1]).doubleValue()) {
			// on the right
			bin = changePoints.length;
		} else {
			for (int i = 0; i < changePoints.length - 1; i++) {
				if ((x > ((Number) changePoints[i]).doubleValue())
					& (x <= ((Number) changePoints[i + 1]).doubleValue())) {
					bin = i + 1;
					break;
				}
			}
		}
		// found the bin
		for (int i = 0; i <= bin - 1; i++)
			r += ((Object[]) binSamples[i]).length;
		r += ((Object[]) binSamples[bin]).length * ((Number) binEstimators[bin].invoke(new Double(x))).doubleValue();
		// ---
		return r / sample.length;
	}

	/** Returns an estimation of the window (interval) given.
	 * 
	 * @param left left border of the window
	 * @param right right border of the window
	 * @return an estimation of the given window
	 * @throws IllegalArgumentException invalid arguments
	 */
	public double windowQuery(Object left, Object right) throws IllegalArgumentException {
		double a = ((Number) left).doubleValue();
		double b = ((Number) right).doubleValue();
		if (b <= a)
			throw new IllegalArgumentException("Invalid window query given!");
		return eval(b) - eval(a);
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
	 * If the number of used change points needs to be changed do it first, than
	 * give the new bandwidths or set to automatic.
	 * 
	 * @param cp new change points used for partitioning
	 */
	public void setChangePoints(final Object[] cp) {
		changePoints = cp;
		Arrays.sort(changePoints);
		hasChanged = true;
	}

	/** Sets the new used bandwidths. If the number of used change points needs to be changed,
	 * the new change points must be given BEFORE updating the bandwidths or an
	 * {@link java.lang.IllegalArgumentException IllegalArgumentException} occurs.
	 * The given bandwidths are used in the given order corresponding to the ascending
	 * ordered bins.
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
			//hasChanged = true;
		}
	}

	/** Switches to automatic bandwidth mode and sets the used strategy to obtain
	 * the bandwidths needed.
	 * 
	 * @param newBandwidthType new strategy for computing the bandwidths
	 */
	public void setBandwidthType(int newBandwidthType) {
		hs = null;
		automaticBandwidthMode = true;
		bandwidthType = newBandwidthType;
		hasChanged = true;
	}

	/** Reinitilizes the estimator after changes */
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
			binSamples = HybridKernelDensityEstimator.split(sample, borders);
		}
		// automatic bandwidth mode? -> computed the variances and the bandwidth for each splitted sample
		if (automaticBandwidthMode) {
			hs = new double[binSamples.length];
			for (int i = 0; i < hs.length; i++) {
				double var = Statistics.variance((Object[]) binSamples[i]);
				hs[i] = KernelBandwidths.computeBandWidth1D(bandwidthType, (Object[]) binSamples[i], kf, var, 0.0, 0.0);
				if ((h == Double.NaN) && (h == 0.0)) {
					throw new IllegalArgumentException("The combination of jump points (bin borders) and sample doesn't work! Please check the number of samples for each bin and the strategy for computing the bandwidth!");
				}
			}
		}

		// computing new bandwidth ??? if number of change points changed?
		// --> No, changed bandwidths needed to be given (from outer)
		// producing new estimators for the bins
		binEstimators = new Function[binSamples.length];
		List<Object> parameters = null;
		for (int i = 0; i < binEstimators.length; i++) {
			parameters = Arrays.asList( kf, binSamples[i], new Double(hs[i]), borders[i], borders[i + 1] );
			binEstimators[i] = (Function) cdfEstimatorFactory.invoke(parameters);
		}
		hasChanged = false;
	}
}
