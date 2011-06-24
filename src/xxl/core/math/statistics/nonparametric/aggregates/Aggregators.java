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

import java.util.Iterator;
import java.util.List;

import xxl.core.cursors.mappers.Aggregator;
import xxl.core.cursors.mappers.ReservoirSampler;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.math.Maths;
import xxl.core.math.functions.AdaptiveAggregationFunction;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.functions.RealFunction;
import xxl.core.math.functions.SplineCompressedFunctionAggregateFunction;
import xxl.core.math.statistics.nonparametric.EmpiricalCDF;
import xxl.core.math.statistics.nonparametric.kernels.KernelBasedBlockEstimatorAggregationFunction;
import xxl.core.math.statistics.nonparametric.kernels.KernelFunction;
import xxl.core.math.statistics.parametric.aggregates.LastN;
import xxl.core.math.statistics.parametric.aggregates.Maximum;
import xxl.core.math.statistics.parametric.aggregates.Minimum;
import xxl.core.math.statistics.parametric.aggregates.ReservoirSample;
import xxl.core.math.statistics.parametric.aggregates.StatefulVariance;
import xxl.core.predicates.EveryNth;

/**
 * This class provides some static methods showing how one is able to use aggregation functions
 * and online aggregation functions of higher orders, i.e.,&nbsp;initial statistical functions and
 * aggregation functions based on different aggregation functions. In regard to more complex applications,
 * preimplementations of kernel based methods are particularly provided.
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.math.functions.AdaptiveAggregationFunction
 * @see xxl.core.cursors.mappers.ReservoirSampler
 * @see xxl.core.math.functions.AdaptiveWeightFunctions
 * @see xxl.core.math.functions.SplineCompressedFunctionAggregateFunction
 * @see xxl.core.math.statistics.nonparametric.aggregates
 * @see xxl.core.math.statistics.nonparametric.kernels
 */

public class Aggregators {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Aggregators() {}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering a {@link xxl.core.math.statistics.nonparametric.kernels.NativeKernelDensityEstimator native kernel density estimator}
	 * as result of the aggregation.
	 * The aggregates base on an input iterator delivering data of type <tt>Object</tt>. While consuming 
	 * the iterator, an <tt>iid</tt> sample of the previously seen data is maintained that in turn is used
	 * for establishing a new native kernel density estimator.   
	 * <br>
	 * Generally, the following steps are required:<br>
	 * <P>1. Use a {@link xxl.core.math.statistics.parametric.aggregates.ReservoirSample reservoir sampling
	 * function} (or any other online sampling algorithm) to obtain
	 * an {@link java.util.Iterator iterator} delivering samples of
	 * an input iterator.
	 * <BR>
	 * 2. Use an aggregation function delivering an estimation
	 * of the spread (e.g. standard deviation, inter quartil range, ...) of the data.
	 * <BR>
	 * 3. Combine the
	 * aggregation functions above within a new aggregator and use the tuples delivered
	 * by this iteration as input for an aggregation function of higher order.
	 * 
	 * </P>
	 * 
	 * <br><br>
	 * <code><pre>
	 * return new Aggregator(
	 *	new Aggregator( input,
	 *			 new Function [] {
	 *			 	mapSamplingStrategy( sampleSize, samplingType),
	 *				new StatefulVariance()}
	 *			),
	 *	new NKDEAggregateFunction( kf)
	 * );
	 * </pre></code>
	 *
	 *
	 * @param input data used to obtain an estimation of the pdf
	 * @param kf used kernel function to obtain an estimator
	 * @param sampleSize used sample size
	 * @param samplingType used type of sampling
	 * @param bandwidthType used bandwidth strategy
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering density estimators based on an
	 * input iterator
	 */
	public static Aggregator getNKDEAggregator(
		Iterator input,
		KernelFunction kf,
		int sampleSize,
		int samplingType,
		int bandwidthType)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(input, Maths.multiDimAggregateFunction(new AggregationFunction[] { mapSamplingStrategy(sampleSize, samplingType), new StatefulVariance()})),
			new NKDEAggregateFunction(kf, bandwidthType));
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering a {@link xxl.core.math.statistics.nonparametric.kernels.NativeKernelCDF native kernel cdf}
	 * as result of the aggregation.
	 * The aggregates base on an input iterator delivering data of type <tt>Object</tt>. While consuming 
	 * the iterator, an <tt>iid</tt> sample of the previously seen data is maintained that in turn is used
	 * for establishing a new native kernel cdf.   
	 * <br>
	 * Generally, the following steps are required:<br>
	 * <P>1. Use a {@link xxl.core.math.statistics.parametric.aggregates.ReservoirSample reservoir sampling
	 * function} (or any other online sampling algorithm) to obtain
	 * an {@link java.util.Iterator iterator} delivering samples of
	 * an input iterator.
	 * <BR>
	 * 2. Use an aggregation function delivering an estimation
	 * of the spread (e.g. standard deviation, inter quartil range, ...) of the data.
	 * <BR>
	 * 3. Combine the
	 *  aggregation functions above within a new aggregator and use the tuples delivered
	 * by this iteration as input for an aggregation function of higher order.
	 * 
	 * </P>
	 * <code><pre>
	 * return new Aggregator(
			new Aggregator(
				input,
				new Function[] {
					mapSamplingStrategy(sampleSize, samplingType),
					new StatefulVariance()}),
			new NativeKernelCDFAggregateFunction(kf, bandwidthType));
	 * </pre></code> 
	 * 
	 * @param input data used to obtain an estimation
	 * @param kf used kernel function to obtain an estimator
	 * @param sampleSize used sample size
	 * @param samplingType used type of sampling
	 * @param bandwidthType used bandwidth strategy
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering native kernel cdf's based on an
	 * input iterator
	 */
	public static Aggregator getNKCDFAggregator(
		Iterator input,
		KernelFunction kf,
		int sampleSize,
		int samplingType,
		int bandwidthType)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(input, Maths.multiDimAggregateFunction(new AggregationFunction[] { mapSamplingStrategy(sampleSize, samplingType), new StatefulVariance()})),
			new NativeKernelCDFAggregateFunction(kf, bandwidthType));
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering an {@link xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelDensityEstimator reflection kernel density estimator}
	 * using reflection as the result of the aggregation. 
	 * The aggregates base on an input iterator delivering data of type <tt>Object</tt>. While consuming 
	 * the iterator, an <tt>iid</tt> sample of the previously seen data is maintained that in turn is used
	 * for establishing a new reflection kernel density estimator.   
	 * <br>
	 * Generally, the following steps are required:<br>
	 * <P>1. Use a {@link xxl.core.math.statistics.parametric.aggregates.ReservoirSample reservoir sampling
	 * function} (or any other online sampling algorithm) to obtain
	 * an {@link java.util.Iterator iterator} delivering samples of
	 * an input iterator.
	 * <BR>
	 * 2. Use an aggregation function delivering an estimation
	 * of the spread (e.g. standard deviation, inter quartil range, ...) of the data.
	 * <BR>
	 * 3. Combine the
	 *  aggregation functions above within a new aggregator and use the tuples delivered
	 * by this iteration as input for an aggregation function of higher order.
	 * 
	 * </P>
	 * 
	 * <br><br>
	 * <code><pre>
	 * return new Aggregator(
	 *	new Aggregator( input,
	 *			 new Function [] {
	 *			 	mapSamplingStrategy( sampleSize, samplingType),
	 *				new StatefulVariance(),
	 *				 new Minimum(),
	 *				 new Maximum()}
	 *			),
	 *	new RKDEAggregateFunction( kf)
	 * );
	 * </pre></code>
	 * 
	 * @param input data used to obtain an estimation
	 * @param kf used kernel function to obtain an estimator
	 * @param sampleSize used sample size
	 * @param samplingType used type of sampling
	 * @param bandwidthType used bandwidth strategy
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering reflection kernel 
	 * density estimators based on an
	 * input iterator
	 */
	public static Aggregator getRKDEAggregator(
		Iterator input,
		KernelFunction kf,
		int sampleSize,
		int samplingType,
		int bandwidthType)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(
				input,
				Maths.multiDimAggregateFunction(new AggregationFunction[] {
					mapSamplingStrategy(sampleSize, samplingType),
					new StatefulVariance(),
					new Minimum(),
					new Maximum()
				}
			)),
			new RKDEAggregateFunction(kf, bandwidthType));
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering a {@link xxl.core.math.statistics.nonparametric.kernels.ReflectionKernelCDF reflection kernel cdf}
	 * as result of the aggregation.
	 * The aggregates base on an input iterator delivering data of type <tt>Object</tt>. While consuming 
	 * the iterator, an <tt>iid</tt> sample of the previously seen data is maintained that in turn is used
	 * for establishing a new reflection kernel cdf.   
	 * <br>
	 * Generally, the following steps are required:<br>
	 * <P>1. Use a {@link xxl.core.math.statistics.parametric.aggregates.ReservoirSample reservoir sampling
	 * function} (or any other on-line sampling algorithm) to obtain
	 * an {@link java.util.Iterator iterator} delivering samples of
	 * an input iterator.
	 * <BR>
	 * 2. Use an aggregation function delivering an estimation
	 * of the spread (e.g. standard deviation, inter quartil range, ...) of the data.
	 * <BR>
	 * 3. Combine the
	 *  aggregation functions above within a new aggregator and use the tuples delivered
	 * by this iteration as input for an aggregation function of higher order.
	 *
	 * </P>
	 * <code><pre>
	 * return new Aggregator(
			new Aggregator(
				input,
				new Function[] {
					mapSamplingStrategy(sampleSize, samplingType),
					new StatefulVariance(),
					new Minimum(),
					new Maximum()
				}
			),
			new ReflectionKernelCDFAggregateFunction(kf, bandwidthType)
		);
	 * </pre></code>
	 * 
	 * @param input data used to obtain an estimation
	 * @param kf used kernel function to obtain an estimator
	 * @param sampleSize used sample size
	 * @param samplingType used type of sampling
	 * @param bandwidthType used bandwidth strategy
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering reflection kernel cdf's based on an
	 * input iterator
	 */
	public static Aggregator getRKCDFAggregator(
		Iterator input,
		KernelFunction kf,
		int sampleSize,
		int samplingType,
		int bandwidthType)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(
				input,
				Maths.multiDimAggregateFunction(new AggregationFunction[] {
					mapSamplingStrategy(sampleSize, samplingType),
					new StatefulVariance(),
					new Minimum(),
					new Maximum()
				}
			)),
			new ReflectionKernelCDFAggregateFunction(kf, bandwidthType)
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering estimators
	 * as result of the aggregation. They rely on a user-defined aggregate function for building
	 * estimators based on a sample and statistical values (e.g. variance).
	 * The aggregates base on an input iterator delivering data of type <tt>Object</tt>. While consuming 
	 * the iterator, an <tt>iid</tt> sample of the previously seen data is maintained that in turn is used
	 * for establishing a new kernel based estimator.   
	 * <br>
	 * Generally, the following steps are required:<br>
	 * <P>1. Use a {@link xxl.core.math.statistics.parametric.aggregates.ReservoirSample reservoir sampling
	 * function} (or any other online sampling algorithm) to obtain
	 * an {@link java.util.Iterator iterator} delivering samples of
	 * an input iterator.
	 * <BR>
	 * 2. Use an aggregation function delivering an estimation
	 * of the spread (e.g. standard deviation, inter quartil range, ...) of the data.
	 * <BR>
	 * 3. Combine the
	 *  aggregation functions above within a new aggregator and use the tuples delivered
	 * by this iteration as input for an aggregation function of higher order.
	 * 
	 * </P>
	 * 
	 * <br><br>
	 * <code><pre>
	 * return new Aggregator(
			new Aggregator(
				input,
				new Function[] {
					mapSamplingStrategy(sampleSize, samplingType),
					new StatefulVariance(),
					new Minimum(),
					new Maximum()
				}
			),
			kernelBasedAggregateFunction
		);
	 * </pre></code>
	 * 
	 * @param input data used to obtain an estimation
	 * @param kernelBasedAggregateFunction aggregation function
	 * @param sampleSize used sample size
	 * @param samplingType used type of sampling
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering reflection kernel cdf's based on an
	 * input iterator
	 */
	public static Aggregator getReservoirBasedKernelEstimatorAggregator(
		Iterator input,
		AggregationFunction kernelBasedAggregateFunction,
		int sampleSize,
		int samplingType)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(
				input,
				Maths.multiDimAggregateFunction(new AggregationFunction[] {
					mapSamplingStrategy(sampleSize, samplingType),
					new StatefulVariance(),
					new Minimum(),
					new Maximum()
				}
			)),
			kernelBasedAggregateFunction
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering estimators as result of the aggregation. An iterator containing Objects of type <TT>Number</TT> 
	 * is partitioned into blocks of a predefined size.
	 * While consuming the iterator, separate kernel based estimators with a chosen bandwidth strategy 
	 * are established for each block. In each step, after the new estimator is built, a convex linear combination
	 * of the 'old' and the 'new' estimator will be returned as the actual aggregation result. There exist different strategies
	 * for the weights of the estimators. They are provided in {xxl.core.math.functions.AdaptiveWeightFunctions
	 * AdaptiveWeightFunctions}. The construction of the aggregates according to the current step and weights
	 * is realized in {@link xxl.core.math.functions.AdaptiveAggregationFunction AdaptiveAggregationFunction}.  
	 * </P>
	 * 
	 * <br>
	 * <code><pre>
	 * return new Aggregator(
			KernelBasedBlockEstimatorAggregationFunction.inputCursor(input, blocksize),
			new AdaptiveAggregationFunction(
				new KernelBasedBlockEstimatorAggregationFunction(factory, kf, bandwidthType),
				weights,
				realMode
			)
		);
	 * </pre></code>
	 *
	 * @param factory factory delivering kernel based estimators for each block 
	 * @param input data stream to build an adaptive estimator (must contain Objects of type <TT>Number</TT>)
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function} to build up the block based estimator
	 * @param bandwidthType type of bandwidth used by each block estimator
	 * @param blocksize size of each block performed separately and used to build an estimator
	 * @param weights RealFunction delivering weights used to combine the blockestimators
	 * @param realMode indicates that the adaptive aggregation function assumes to combine Objects of type {@link xxl.core.math.functions.RealFunction}
	 * instead of Objects of type {@link xxl.core.functions.Function} consuming Objects of type <TT>Number</TT>.
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering kernel based estimators that 
	 * are iteratively combined
	 */
	public static Aggregator getAdaptiveKernelBasedAggregator(
		Function factory,
		Iterator input,
		KernelFunction kf,
		int bandwidthType,
		int blocksize,
		RealFunction weights,
		boolean realMode)
		throws IllegalArgumentException {

		return new Aggregator(
			KernelBasedBlockEstimatorAggregationFunction.inputCursor(input, blocksize),
			new AdaptiveAggregationFunction(
				new KernelBasedBlockEstimatorAggregationFunction(factory, kf, bandwidthType),
				weights,
				realMode
			)
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering kernel based estimators as result of the aggregation. An iterator containing Objects of type <TT>Number</TT> 
	 * is partitioned into blocks of a predefined size.
	 * While consuming the iterator, separate kernel based estimators with a chosen bandwidth strategy 
	 * are established for each block. In each step, after the new estimator is built, a convex linear combination
	 * of the 'old' and the 'new' estimator will be returned as the actual aggregation result. There exist different strategies
	 * for the weights of the estimators. They are provided in {xxl.core.math.functions.AdaptiveWeightFunctions
	 * AdaptiveWeightFunctions}. The construction of the aggregates according to the current step and weights
	 * is realized in {@link xxl.core.math.functions.AdaptiveAggregationFunction AdaptiveAggregationFunction}.
	 * In regard to the limited memory resources, the current aggregate is compressed. Therefore, the {@link
	 * xxl.core.math.numerics.splines.CubicBezierSpline cubic Bezier-Spline interpolate} for the aggregate 
	 * is computed, whereas a predefined number of function values of the aggregate on an interval is computed.
	 * The compression of a new aggregate is realized in 
	 * {@link xxl.core.math.functions.SplineCompressedFunctionAggregateFunction}. The compression range, i.e.,
	 * the interval for the compression, has to be known.
	 * </P>
	 * 
	 * <br>
	 * <code><pre>
	 * return new Aggregator(
			KernelBasedBlockEstimatorAggregationFunction.inputCursor(input, blocksize),
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new KernelBasedBlockEstimatorAggregationFunction(factory, kf, bandwidthType),
					weights,
					realMode
				),
				new EveryNth(blocksize),
				left,
				right,
				n,
				cdfMode
			)
		);
	 * </pre></code> 
	 *
	 * @param factory factory delivering kernel based estimators for each block 
	 * @param input data stream to build an adaptive estimator (must contain Objects of type <TT>Number</TT>)
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function} to build up the block based estimator
	 * @param bandwidthType type of bandwidth used by each block estimator
	 * @param blocksize size of each block performed separately and used to build up an estimator
	 * @param weights RealFunction delivering weights used to combine the block estimators
	 * @param left left border of the valid compression range
	 * @param right right border of the valid compression range
	 * @param n number of points in the compression interval
	 * @param realMode indicates that the adaptive aggregation function assumes to combine Objects of type {@link xxl.core.math.functions.RealFunction}
	 * instead of Objects of type {@link xxl.core.functions.Function} consuming Objects of type <TT>Number</TT>.
	 * @param cdfMode indicates spline is in cdf mode, i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering kernel based estimators that 
	 * are iteratively combined and compressed in each aggregation step
	 */
	public static Aggregator getSplineCompressedAdaptiveKernelBasedAggregator(
		Function factory,
		Iterator input,
		KernelFunction kf,
		int bandwidthType,
		int blocksize,
		RealFunction weights,
		double left,
		double right,
		int n,
		boolean realMode,
		boolean cdfMode)
		throws IllegalArgumentException {
		return new Aggregator(
			KernelBasedBlockEstimatorAggregationFunction.inputCursor(input, blocksize),
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new KernelBasedBlockEstimatorAggregationFunction(factory, kf, bandwidthType),
					weights,
					realMode
				),
				new EveryNth(blocksize),
				left,
				right,
				n,
				cdfMode
			)
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering kernel based estimators as result of the aggregation. An iterator containing Objects of type <TT>Number</TT> 
	 * is partitioned into blocks of a predefined size.
	 * While consuming the iterator, separate kernel based estimators with a chosen bandwidth strategy 
	 * are established for each block. In each step, after the new estimator is built, a convex linear combination
	 * of the 'old' and the 'new' estimator will be returned as the actual aggregation result. There exist different strategies
	 * for the weights of the estimators. They are provided in {xxl.core.math.functions.AdaptiveWeightFunctions
	 * AdaptiveWeightFunctions}. The construction of the aggregates according to the current step and weights
	 * is realized in {@link xxl.core.math.functions.AdaptiveAggregationFunction AdaptiveAggregationFunction}.
	 * In regard to the limited memory resources, the current aggregate is compressed. Therefore, the {@link
	 * xxl.core.math.numerics.splines.CubicBezierSpline cubic Bezier-Spline interpolate} for the aggregate 
	 * is computed, whereas a predefined number of function values of the aggregate on an interval is computed.
	 * The compression of a new aggregate is realized in 
	 * {@link xxl.core.math.functions.SplineCompressedFunctionAggregateFunction}. The compression range in turn relies on 
	 * the current extrema. 
	 * </P>
	 * 
	 * <br>
	 * <code><pre>
	 * return new Aggregator(
			KernelBasedBlockEstimatorAggregationFunction.inputCursor(input, blocksize),
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new KernelBasedBlockEstimatorAggregationFunction(factory, kf, bandwidthType),
					weights,
					realMode
				),
				new EveryNth(blocksize),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(
					KernelBasedBlockEstimatorAggregationFunction.MINIMUM),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(
					KernelBasedBlockEstimatorAggregationFunction.MAXIMUM),
				new Constant(new Double(n)),
				cdfMode
			)
		);
	 * </pre></code> 
	 *
	 * @param factory factory delivering kernel based estimators for each block 
	 * @param input data stream to build an adaptive estimator (must contain Objects of type <TT>Number</TT>)
	 * @param kf used {@link xxl.core.math.statistics.nonparametric.kernels.KernelFunction kernel function} to build up the block based estimator
	 * @param bandwidthType type of bandwidth used by each block estimator
	 * @param blocksize size of each block performed separately and used to build up an estimator
	 * @param weights RealFunction delivering weights used to combine the block estimators
	 * @param n number of points in the compression interval
	 * @param realMode indicates that the adaptive aggregation function assumes to combine Objects of type {@link xxl.core.math.functions.RealFunction}
	 * instead of Objects of type {@link xxl.core.functions.Function} consuming Objects of type <TT>Number</TT>.
	 * @param cdfMode indicates spline is in cdf mode, i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering kernel based estimators that 
	 * are iteratively combined and compressed in each aggregation step
	 */
	public static Aggregator getSplineCompressedAdaptiveKernelBasedAggregator(
		Function factory,
		Iterator input,
		KernelFunction kf,
		int bandwidthType,
		int blocksize,
		RealFunction weights,
		int n,
		boolean realMode,
		boolean cdfMode)
		throws IllegalArgumentException {
		return new Aggregator(
			KernelBasedBlockEstimatorAggregationFunction.inputCursor(input, blocksize),
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new KernelBasedBlockEstimatorAggregationFunction(factory, kf, bandwidthType),
					weights,
					realMode
				),
				new EveryNth(blocksize),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(
					KernelBasedBlockEstimatorAggregationFunction.MINIMUM),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(
					KernelBasedBlockEstimatorAggregationFunction.MAXIMUM),
				new Constant(new Double(n)),
				cdfMode
			)
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering estimators based on a FACTORY as result of the aggregation. An iterator containing Objects of type <TT>Number</TT> 
	 * is partitioned into blocks of a predefined size.
	 * While consuming the iterator, separate estimators 
	 * are established for each block. In each step, after the new estimator is built, a convex linear combination
	 * of the 'old' and the 'new' estimator will be returned as the actual aggregation result. There exist different strategies
	 * for the weights of the estimators. They are provided in {xxl.core.math.functions.AdaptiveWeightFunctions
	 * AdaptiveWeightFunctions}. The construction of the aggregates according to the current step and weights
	 * is realized in {@link xxl.core.math.functions.AdaptiveAggregationFunction AdaptiveAggregationFunction}.
	 * In regard to the limited memory resources, the current aggregate is compressed. Therefore, the {@link
	 * xxl.core.math.numerics.splines.CubicBezierSpline cubic Bezier-Spline interpolate} for the aggregate 
	 * is computed, whereas a predefined number of function values of the aggregate on an interval is computed.
	 * The compression of a new aggregate is realized in 
	 * {@link xxl.core.math.functions.SplineCompressedFunctionAggregateFunction}. The compression range, i.e.,
	 * the interval for the compression, has to be known.
	 * </P>
	 * 
	 * <br>
	 * <code><pre>
	 * return new Aggregator(
			input,
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new AbstractFunction() {
						int c = 0;
						public Object invoke(Object old, Object next) {
							c++;
							if (next == null)
								return null;
							if (c % blocksize == 0)
								return factory.invoke((Object[]) next);
							else
								return old;
					}
				}, 
				weights, realMode), new EveryNth(blocksize), left, right, n, cdfMode
			)
		);
	 * </pre></code> 
	 *
	 * @param factory factory delivering estimators for each block 
	 * @param input data stream to build an adaptive estimator (must contain Objects of type <TT>Number</TT>)
	  * @param blocksize size of each block performed separately and used to build up an estimator
	 * @param weights RealFunction delivering weights used to combine the block estimators
	 * @param left left border of valid compression range
	 * @param right right border of valid compression range
	 * @param n number of points in the compression interval
	 * @param realMode indicates that the adaptive aggregation function assumes to combine Objects of type {@link xxl.core.math.functions.RealFunction}
	 * instead of Objects of type {@link xxl.core.functions.Function} consuming Objects of type <TT>Number</TT>.
	 * @param cdfMode indicates spline is in cdf mode, i.e., evaluating the spline at x > maximum causes the spline
	 * to return 1.0 instead of 0.0
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering estimators that 
	 * are iteratively combined and compressed in each aggregation step
	 */
	public static Aggregator getSplineCompressedAdaptiveAggregator(
		final Function factory,
		Iterator input,
		final int blocksize,
		RealFunction weights,
		double left,
		double right,
		int n,
		boolean realMode,
		boolean cdfMode)
		throws IllegalArgumentException {

		return new Aggregator(
			input,
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new AggregationFunction<List,Object>() {
						int c = 0;
						public Object invoke(Object old, List next) {
							c++;
							if (next == null)
								return null;
							if (c % blocksize == 0)
								return factory.invoke(next);
							else
								return old;
					}
				}, 
				weights, realMode), new EveryNth(blocksize), left, right, n, cdfMode
			)
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering an {@link xxl.core.math.statistics.nonparametric.EmpiricalCDF empirical cdf}
	 * as result of the aggregation.
	 * The aggregates base on an input iterator delivering data of type <tt>Object</tt>. While consuming 
	 * the iterator, an <tt>iid</tt> sample of the previously seen data is maintained that in turn is used
	 * for establishing a new empirical cdf.   
	 * <br>
	 * Generally, the following steps are required:<br>
	 * <P>1. Use a {@link xxl.core.math.statistics.parametric.aggregates.ReservoirSample reservoir sampling
	 * function} (or any other online sampling algorithm) to obtain
	 * an {@link java.util.Iterator iterator} delivering samples of
	 * an input iterator.
	 * <BR>
	 * 2. Use an aggregation function delivering an estimation
	 * of the spread (e.g. standard deviation, inter quartil range, ...) of the data.
	 * <BR>
	 * 3. Combine the
	 * aggregation functions above within a new aggregator and use the tuples delivered
	 * by this iteration as input for an aggregation function of higher order.
	 * 
	 * </P>
	 * 
	 * <br><br>
	 * <code><pre>
	 * return new Aggregator(
			new Aggregator(input, mapSamplingStrategy(sampleSize, samplingType)),
			new EmpiricalCDFAggregateFunction()
		);
	 * </pre></code>
	 * 
	 * @param input data used to obtain an estimation of the pdf
	 * @param sampleSize used sample size
	 * @param samplingType used type of sampling
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering the empirical cdf
	 */
	public static Aggregator getEmpiricalCDFAggregator(Iterator input, int sampleSize, int samplingType)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(input, mapSamplingStrategy(sampleSize, samplingType)),
			new EmpiricalCDFAggregateFunction()
		);
	}

	/** Returns an {@link java.util.Iterator iterator} of type {@link xxl.core.cursors.mappers.Aggregator aggregator}
	 * delivering empirical cdf's as result of the aggregation. An iterator containing Objects of type <TT>Number</TT> 
	 * is partitioned into blocks of a predefined size.
	 * While consuming the iterator, separate empirical cdf's
	 * are established for each block. In each step, after the new estimator is built, a convex linear combination
	 * of the 'old' and the 'new' estimator will be returned as the actual aggregation result. There exist different strategies
	 * for the weights of the estimators. They are provided in {xxl.core.math.functions.AdaptiveWeightFunctions
	 * AdaptiveWeightFunctions}. The construction of the aggregates according to the current step and weights
	 * is realized in {@link xxl.core.math.functions.AdaptiveAggregationFunction AdaptiveAggregationFunction}.
	 * In regard to the limited memory resources, the current aggregate is compressed. Therefore, the {@link
	 * xxl.core.math.numerics.splines.CubicBezierSpline cubic Bezier-Spline interpolate} for the aggregate 
	 * is computed, whereas a predefined number of function values of the aggregate on an interval is computed.
	 * The compression of a new aggregate is realized in 
	 * {@link xxl.core.math.functions.SplineCompressedFunctionAggregateFunction}. The compression range relies on 
	 * the current extrema. 
	 * </P>
	 * 
	 * <br>
	 * <code><pre>
	 * return new Aggregator(
			new Aggregator(input, new Function[] { new LastN(blocksize), new Minimum(), new Maximum()}),
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new AbstractFunction() {
						int c = 0;
						public Object invoke(Object old, Object next) {
							c++;
							if (next == null)
								return null;
							if (c % blocksize == 0)
								return EmpiricalCDF.FACTORY.invoke(((Object[]) next)[0]);
							else
								return old;
						}
					},
					weights, true // real mode
				),
				new EveryNth(blocksize),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(1),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(2),
				new Constant(new Double(n)),
				true // cdf mode
			)
		);
	 * </pre></code> 
	 *
	 * @param input data stream to build an adaptive estimator (must contain Objects of type <TT>Number</TT>)
	 * @param blocksize size of each block performed separately and used to build up an estimator
	 * @param weights RealFunction delivering weights used to combine the block estimators
	 * @param n number of points in the compression interval
	 * @throws IllegalArgumentException if the given samplingType is not known
	 * @return an {@link xxl.core.cursors.mappers.Aggregator aggregator} delivering empirical cdf's that 
	 * are iteratively combined and compressed in each aggregation step
	 */
	public static Aggregator getSplineCompressedAdaptiveEmpiricalCDFAggregator(
		Iterator input,
		final int blocksize,
		RealFunction weights,
		int n)
		throws IllegalArgumentException {

		return new Aggregator(
			new Aggregator(input, Maths.multiDimAggregateFunction(new AggregationFunction[] { new LastN(blocksize), new Minimum(), new Maximum()})),
			new SplineCompressedFunctionAggregateFunction(
				new AdaptiveAggregationFunction(
					new AggregationFunction<List,Object>() {
						int c = 0;
						public Object invoke(Object old, List next) {
							c++;
							if (next == null)
								return null;
							if (c % blocksize == 0)
								return EmpiricalCDF.FACTORY.invoke(next.get(0));
							else
								return old;
						}
					},
					weights, true // real mode
				),
				new EveryNth(blocksize),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(1),
				KernelBasedBlockEstimatorAggregationFunction.accessValue(2),
				new Constant(new Double(n)),
				true // cdf mode
			)
		);
	}

	/** The method returns a {@link xxl.core.functions.Function function} representing a
	 * strategy used with the {@link xxl.core.cursors.mappers.ReservoirSampler} cursor.
	 *
	 * @param sampleSize size of the sample
	 * @param type type of the reservoir sampling strategy
	 * @throws IllegalArgumentException if an unknown or not supported strategy has been given
	 * @return a function representing a reservoir sampling strategy
	 */
	public static AggregationFunction mapSamplingStrategy(int sampleSize, int type) throws IllegalArgumentException {
		AggregationFunction function = null;
		switch (type) {
			case ReservoirSampler.RTYPE :
				function = new ReservoirSample(sampleSize, new ReservoirSample.RType(sampleSize));
				break;
			case ReservoirSampler.XTYPE :
				function = new ReservoirSample(sampleSize, new ReservoirSample.XType(sampleSize));
				break;
			case ReservoirSampler.YTYPE :
				throw new IllegalArgumentException("Type y is not supported so far. See javadoc xxl.core.functions.ReservoirSample for details!");
			case ReservoirSampler.ZTYPE :
				function = new ReservoirSample(sampleSize, new ReservoirSample.ZType(sampleSize));
				break;
			default :
				throw new IllegalArgumentException("unknown sampling strategy given!");
		}
		return function;
	}
}
