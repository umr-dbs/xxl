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

package xxl.core.math;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.queues.Heap;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Aggregator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.functions.RealFunction;
import xxl.core.math.functions.RealFunctionFunction;
import xxl.core.math.numerics.integration.SimpsonsRuleRealFunctionArea;
import xxl.core.math.statistics.nonparametric.kernels.GaussianKernel;
import xxl.core.math.statistics.parametric.aggregates.StatefulAverage;
import xxl.core.math.statistics.parametric.aggregates.Count;
import xxl.core.math.statistics.parametric.aggregates.Maximum;
import xxl.core.math.statistics.parametric.aggregates.Minimum;
import xxl.core.math.statistics.parametric.aggregates.StatefulStandardDeviation;
import xxl.core.util.Distance;
import xxl.core.util.DoubleArrays;

/**
	This class contains some useful static methods for dealing with statistical functions.
*/

public class Statistics {

	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double A0 = 2.50662823884;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double A1 = -18.61500062529;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double A2 = 41.39119773534;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double A3 = -25.44106049637;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double B1 = -8.47351093090;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double B2 = 23.08336743743;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double B3 = -21.06224101826;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double B4 = 3.13082909833;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double C0 = -2.78718931138;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double C1 = -2.29796479134;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double C2 = 4.85014127135;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double C3 = 2.32121276858;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double D1 = 3.54388924762;
	
	/** Precomputed constant value used for computing p-quantils of the
	 * standard normal distribution. */
	protected static final double D2 = 1.63706781897;

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Statistics() {
	}

	/** Computes the p-quantil (lower tail area) of the standard normal distribution.
	 * Based on:<br>P.Griffiths, I.D.Hill: Applied Statistics Algorithms:
	 * Ellis Horwood Limited: 1985 :p.188 ff. (AS 111).
	 *
	 * @param p quantil to compute
	 * @return p-quantil of the N(0,1)-Distribution.
	*/
	public static double normalQuantil(double p) {
		if (p <= 0 || p >= 1) {
			throw new IllegalArgumentException("quantile to compute must be in (0,1)!");
		}
		double ppnd = 0.0;
		double r = 0.0;

		if (p <= 0.0)
			return 0.0 / 0.0;
		if (p >= 1.0)
			return 0.0;

		double q = p - 0.5;
		if (!(q >= 0.42)) {
			r = q * q;
			ppnd =
				q
					* (((A3 * r + A2) * r + A1) * r + A0)
					/ ((((B4 * r + B3) * r + B2) * r + B1) * r + 1.0);
		} else {
			if (q >= 0.0)
				r = 1.0 - p;
			if (r <= 0.0)
				return 0.0 / 0.0;
			r = Math.sqrt(-1.0 * Math.log(r));
			ppnd =
				(((C3 * r + C2) * r + C1) * r + C0) / ((D2 * r + D1) * r + 1.0);
			if (q <= 0.0)
				ppnd = -1.0 * ppnd;
		}
		return ppnd;
	}

	/** Converts a cumulative frequency distribution of numerical data into two
	 * arrays of types <tt>double []</tt> for the data and <tt>int []</tt> for
	 * the frequencies.
	 *
	 * @param cfd cumulative frequency distribution of numerical data
	 * @param mapping function that maps the data values to an alternative representation
	 * @return an <tt>Object []</tt> filled with a <tt>double []</tt> containing the data
	 * and an <tt>int []</tt> containing the corresponding frequencies
	 */
	public static Object[] doubleArrayCFD(Iterator cfd, Function mapping) {
		List a = new ArrayList();
		Cursors.toList(cfd, a);
		double[] data = new double[a.size()];
		int[] freqencies = new int[a.size()];
		Iterator it = a.iterator();
		int pos = -1;
		Object[] temp = null;
		while (it.hasNext()) {
			pos++;
			temp = (Object[]) it.next();
			data[pos] = ((Double) mapping.invoke(temp[0])).doubleValue();
			freqencies[pos] = ((Long) temp[1]).intValue();
		}
		return new Object[] { data, freqencies };
	}

	/** Converts a cumulative frequency distribution of numerical data into two
	 * arrays of types <tt>double []</tt> for the data and <tt>int []</tt> for
	 * the frequencies.
	 *
	 * @param cfd cumulative frequency distribution of numerical data
	 * @return an <tt>Object []</tt> filled with a <tt>double []</tt> containing the data
	 * and a <tt>int []</tt> containing the corresponding frequencies
	 */
	public static Object[] doubleArrayCFD(Iterator cfd) {
		return doubleArrayCFD(cfd, new AbstractFunction() {
			public Object invoke(Object o) {
				return new Double(((Number) o).doubleValue());
			}
		});
	}

	/** Evaluates a real-valued function on a given grid of double values.
	 * Real-valued function means that the given function consumes objects of type <tt>Double</tt>
	 * and returns Objects of type {@link java.lang.Number Number}.
	 * 
	 * @param grid given data points to evaluate
	 * @param realFunction real-valued function (one dimensional) to evaluate
	 * @return a <tt>double []</tt> filled with the function values on the grid
	 */
	public static double[] evalReal1DFunction(
		double[] grid,
		Function realFunction) {
		double[] r = new double[grid.length];
		for (int i = 0; i < r.length; i++) {
			r[i] = ((Number) realFunction.invoke(new Double(grid[i]))).doubleValue();
		}
		return r;
	}

	/** Evaluates a {@link xxl.core.math.functions.RealFunction real-valued function}
	 * on a given grid of double values.
	 * 
	 * @param grid given data points to evaluate
	 * @param f function to evaluate
	 * @return a <tt>double []</tt> filled with the function values on the grid
	 */
	public static double[] evalRealFunction(double[] grid, RealFunction f) {
		double[] r = new double[grid.length];
		for (int i = 0; i < r.length; i++) {
			r[i] = f.eval(grid[i]);
		}
		return r;
	}

	/** Evaluates a given real-valued function on an interval [left,right] with a
	 * predefined number of points.
	 * Real-valued function means that the given function consumes objects of type <tt>Double</tt>
	 * and returns Objects of type {@link java.lang.Number Number}.
	 * 
	 * @param a left border of the interval to evaluate
	 * @param b right border of the interval to evaluate
	 * @param n number of points to evaluate
	 * @param realFunction real-valued function (one dimensional) to evaluate
	 * @return a <tt>double []</tt> filled with the function values on the grid
	 */
	public static double[] evalReal1DFunction(
		double a,
		double b,
		int n,
		Function realFunction) {
		return evalReal1DFunction(DoubleArrays.equiGrid(a, b, n), realFunction);
	}

	/** Evaluates a {@link xxl.core.math.functions.RealFunction real-valued function}
	 * on equally spaced, real-valued data points.
	 * 
	 * @param a left border of the interval to evaluate
	 * @param b right border of the interval to evaluate
	 * @param n number of points to evaluate
	 * @param f function to evaluate
	 * @return <tt>double []</tt> filled with the function values on the grid
	 */
	public static double[] evalRealFunction(
		double a,
		double b,
		int n,
		RealFunction f) {
		return evalRealFunction(DoubleArrays.equiGrid(a, b, n), f);
	}

	/** Evaluates a given real-valued function on a given grid of double values.
	 * Real-valued function means that the given function consumes objects of type <tt>Double</tt>
	 * and returns Objects of type {@link java.lang.Number Number}.
	 * 
	 * @param grid given data points to evaluate
	 * @param realFunction real-valued function (one dimensional) to evaluate
	 * @return a <tt>double []</tt> filled with the evaluated real-valued function points alternately with the 
	 * data points, meaning the returned array consists of {x0, y0, x1, y1, x2, y2, ..., xn-1, yn-1}
	 * with f(xi) = yi for all i = 0, ... , n-1
	 */
	public static double[] evalReal1DFunctionX(
		double[] grid,
		Function realFunction) {
		double[] r = new double[2 * grid.length];
		for (int i = 0; i < r.length; i = i + 2) {
			int k = (i / 2);
			r[i] = grid[k];
			r[i + 1] =
				((Number) realFunction.invoke(new Double(grid[k])))
					.doubleValue();
		}
		return r;
	}

	/** Evaluates a given real-valued function on equally spaced real-valued data points.
	 * Real-valued function means that the given function consumes objects of type <tt>Double</tt>
	 * and returns Objects of type {@link java.lang.Number Number}.
	 * 
	 * @param a left border of the interval to evaluate
	 * @param b right border of the interval to evaluate
	 * @param n number of points to evaluate
	 * @param realFunction real-valued function (one dimensional) to evaluate
	 * @return a <tt>double []</tt> filled with the evaluated real-valued function points alternately with the 
	 * data points, meaning the returned array consists of {x0, y0, x1, y1, x2, y2, ..., xn-1, yn-1}
	 * with f(xi) = yi for all i = 0, ... , n-1
	 */
	public static double[] evalReal1DFunctionX(
		double a,
		double b,
		int n,
		Function realFunction) {
		return evalReal1DFunctionX(
			DoubleArrays.equiGrid(a, b, n),
			realFunction);
	}

	/**
	 * Computes the value of the Gaussian probability density function (pdf) at a given point x.
	 * 
	 * @param x argument where to evaluate the pdf
	 * @param mean mean of the pdf
	 * @param variance variance of the pdf
	 * @throws IllegalArgumentException if variance < 0
	 * @return value of the pdf at position x
	 */
	public static double gaussian(double x, double mean, double variance)
		throws IllegalArgumentException {
		if (variance < 0.0)
			throw new IllegalArgumentException("variance has to be >= 0!");
		return Math.exp(Math.pow(x-mean,2)/(-2.0*Math.pow(variance,2)))/(variance*Math.sqrt(2.0*Math.PI));
	}

	/**
	 * Computes the probability density function (pdf) of the standard Gaussian distribution (normal pdf).
	 * An expectation value of 0.0 and a variance of 1.0 will be assumed.
	 * 
	 * @param x argument where to evaluate the pdf
	 * @return value of the pdf at position x
	 * @throws IllegalArgumentException invalid argument
	 */
	public static double gaussian(double x) throws IllegalArgumentException {
		return gaussian(x, 0.0, 1.0);
	}
	
	/**
	 * Returns a numerical approximation of the primitive of the standard Gaussian distribution (normal pdf).
	 * 
	 * @param argument where to evaluate the pdf
	 * @param n iteration number for the numerical approximation
	 * @return value of the pdf at position x
	 */
	public static double gaussianPrimitive(double x, int n) {
		if(x < -3) return 0;
		if(x > 3) return 1;
		return SimpsonsRuleRealFunctionArea.simpson(-3, x, new RealFunctionFunction(new GaussianKernel()), n);
	}
	
	/**
	 * Returns a numerical approximation of the primitive of the standard Gaussian distribution (normal pdf).
	 * 
	 * @param argument where to evaluate the pdf
	 * @return value of the pdf at position x
	 */
	public static double gaussianPrimitive(double x) {
		return gaussianPrimitive(x, 100);
	}

	/* --- kernel functions ---*/

	/** Evaluates the Epanechnikow kernel. 
	 * 
	 * @param x function argument
	 * @return f(x) = 0.75 ( 1 - x^2) * I(x)_[-1,1]
	 * @see xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel
	 */
	public static double epanechnikow(double x) {
		if ((x < -1) || (x > 1))
			return 0;
		return 0.75 * (1 - x * x);
	}

	/** Evaluates the primitive of the Epanechnikow kernel.
	 * 
	 * @param x function argument
	 * @return value of the primitive of the Epanechnikow kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel
	 */
	public static double epanechnikowPrimitive(double x) {
		return epanechnikowPrimitive(
			x,
			(-1.0) * epanechnikowPrimitive(-1.0, 0.0));
	}

	/** Evaluates the primitive of the Epanechnikow kernel and adds a constant c.
	 * 
	 * @param x function argument
	 * @param c constant added to the primitive
	 * @return value of the primitive of the Epanechnikow kernel plus a constant c
	 * @see xxl.core.math.statistics.nonparametric.kernels.EpanechnikowKernel
	 */
	public static double epanechnikowPrimitive(double x, double c) {
		double r = 0.25 * (3 * x - x * x * x) + c;		
		if(x < -1) return 0;
		if(x > 1) return 1;		
		return r;
	}

	/** Evaluates the Biweight kernel.
	 * 
	 * @param x function argument
	 * @return value of the Biweight kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.BiweightKernel
	 */
	public static double biweight(double x) {
		double r = 0.9375 * (1 - x * x) * (1 - x * x);
		if ((x < -1) || (x > 1))
			r = 0.0;
		return r;
	}

	/** Evaluates the primitive of the Biweight kernel and adds a constant c.
	 * 
	 * @param x function argument
	 * @param c constant added to the primitive
	 * @return value of the primitive of the Biweight kernel plus a constant c
	 * @see xxl.core.math.statistics.nonparametric.kernels.BiweightKernel
	 */
	public static double biweightPrimitive(double x, double c) {
		if(x < -1) return 0;
		if(x > 1) return 1;
		return 0.1875 * x * x * x * x * x
			- 0.625 * x * x * x
			+ 0.9375 * x
			+ c;
	}

	/** Evaluates the primitive of the Biweight kernel.
	 * 
	 * @param x function argument
	 * @return value of the primitive of the Biweight kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.BiweightKernel
	 */
	public static double biweightPrimitive(double x) {
		return biweightPrimitive(x, (-1.0) * biweightPrimitive(-1.0, 0.0));
	}

	/** Evaluates the derivative of the Biweight kernel.
	 * 
	 * @param x function argument
	 * @return value of the derivative of the Biweight kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.BiweightKernel
	 */
	public static double biweightDerivative(double x) {
		double r = 0.0;
		if ((x >= -1) && (x <= 1)) {
			r += 3.75 * x * (x * x - 1);
		}
		return r;
	}

	/** Evaluates the Triweight kernel.
	 * 
	 * @param x function argument
	 * @return value of the Triweight kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.TriweightKernel
	 */
	public static double triweight(double x) {
		double r = 1.09375 * (1 - x * x) * (1 - x * x) * (1 - x * x);
		if ((x < -1) || (x > 1))
			r = 0.0;
		return r;
	}

	/** Evaluates the primitive of the Triweight kernel and adds a constant c.
	 * 
	 * @param x function argument
	 * @param c constant added to the primitive
	 * @return the primitive of the Triweight function evaluated at the given argument
	 * @see xxl.core.math.statistics.nonparametric.kernels.TriweightKernel
	 */
	public static double triweightPrimitive(double x, double c) {
		if(x < -1) return 0;
		if(x > 1) return 1;
		double r = -Math.pow(x,7)/7 + Math.pow(x,5)*0.6 - Math.pow(x,3) + x + 16/35.0;
		r *= 35/32.0;		
		return r;
	}

	/** Evaluates the primitive of the Triweight kernel.
	 * 
	 * @param x function argument
	 * @return value of the primitive of the Triweight kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.TriweightKernel
	 */
	public static double triweightPrimitive(double x) {
		return triweightPrimitive(x, (-1.0) * triweightPrimitive(-1.0, 0.0));
	}

	/** Evaluates the derivative of the Triweight kernel.
	 * 
	 * @param x function argument
	 * @return value of the derivative of the Triweight kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.TriweightKernel
	 */
	public static double triweightDerivative(double x) {
		double r = 0.0;
		if ((x >= -1) && (x <= 1)) {
			r += 2.0 * (x * x * x) - (x * x * x * x * x) - x; 
			r *= 6.5625;
		}
		return r;
	}

	/** Evaluates the CosineArch kernel.
	 * 
	 * @param x function argument
	 * @return value of the CosineArch kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.CosineArchKernel
	 */
	public static double cosineArch(double x) {
		double r = 0.25 * Math.PI * Math.cos(0.5 * Math.PI * x);
		if ((x < -1) || (x > 1))
			r = 0.0;
		return r;
	}

	/** Evaluates the primitive of the CosineArch kernel and adds a constant c.
	 * 
	 * @param x function argument
	 * @param c constant added to the primitive
	 * @return value of the primitive of the CosineArch kernel plus a constant c
	 * @see xxl.core.math.statistics.nonparametric.kernels.CosineArchKernel
	 */
	public static double cosineArchPrimitive(double x, double c) {
		if(x < -1) return 0;
		if(x > 1) return 1;
		return 0.5 * Math.sin(0.5 * Math.PI * x) + c;
	}

	/** Evaluates the primitive of the CosineArch kernel.
	 * 
	 * @param x function argument
	 * @return value of the primitive of the CosineArch kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.CosineArchKernel
	 */
	public static double cosineArchPrimitive(double x) {
		return cosineArchPrimitive(x, (-1.0) * cosineArchPrimitive(-1.0, 0.0));
	}

	/** Evaluates the derivative of the CosineArch kernel.
	 * 
	 * @param x function argument
	 * @return value of the derivative of the CosineArch kernel
	 * @see xxl.core.math.statistics.nonparametric.kernels.CosineArchKernel
	 */
	public static double cosineArchDerivative(double x) {
		double r = 0.0;
		if ((x >= -1) && (x <= 1)) {
			r += -0.125 * Math.PI * Math.PI * Math.sin(0.5 * Math.PI * x);
		}
		return r;
	}

	/* common statistics */

	/** Returns the variance of a given dataset. The data is assumed to be
	 * numerical, i.e. of type {@link java.lang.Number Number}.
	 * 
	 * @param data numerical data
	 * @return variance of the data
	 */
	public static double variance(Object[] data) {
		double r = 0.0;
		double avg = average(data);
		double x = 0.0;
		for (int i = 0; i < data.length; i++) {
			x = ((Number) data[i]).doubleValue();
			r += (x - avg) * (x - avg);
		}
		return r / data.length;
	}

	/** Returns the unbiased variance of a given sample. The data is assumed to be
	 * numerical ,i.e., of type {@link java.lang.Number Number}.
	 * 
	 * @param data numerical data
	 * @return variance of the data
	 */
	public static double sampleVariance(Object[] data) {
		double r = 0.0;
		double avg = average(data);
		double x = 0.0;
		for (int i = 0; i < data.length; i++) {
			x = ((Number) data[i]).doubleValue();
			r += (x - avg) * (x - avg);
		}
		return r / (data.length - 1);
	}
	
	

	/** Returns the average of a given dataset. The data is assumed to be
	 * numerical i.e. of type {@link java.lang.Number Number}.
	 * 
	 * @param data numerical data
	 * @return average of the data
	 */
	public static double average(Object[] data) {
		double r = 0.0;
		for (int i = 0; i < data.length; i++) {
			r += ((Number) data[i]).doubleValue();
		}
		return r / data.length;
	}

	/** Computes different aggregates (statistics) of a given set of data.
	 * 
	 * @param data data given as stream containing Objects of type <TT>Number</TT>
	 * @return An <TT>Object[] o</TT> containing Objects representing the computed statistics as:<BR>
	 * <TT>o[0]</TT>: number of processed data <BR>
	 * <TT>o[1]</TT>: minimum of processed data <BR>
	 * <TT>o[2]</TT>: maximum of processed data <BR>
	 * <TT>o[3]</TT>: average of processed data <BR>
	 * <TT>o[4]</TT>: standard deviation of processed data <BR>
	 */
	public static Object[] getStatistics(Iterator data) {
		return ((List)(new Aggregator(data,
			Maths.multiDimAggregateFunction(new AggregationFunction[] {
				new Count(),
				new Minimum(),
				new Maximum(),
				new StatefulAverage(),
				new StatefulStandardDeviation()})))
			.last()).toArray();
	}
	
	/** This method computes the n maximum differences in the given data. To do so, a
	 * {@link xxl.core.util.Distance distance} must be defined for the data.
	 * For example, the cumulative
	 * frequency distribution could be used with a difference function defined as
	 * the differences of the frequencies of the data points.
	 *
	 * @param data data used to compute the max-diff-positions
	 * @param distance distance function used for determining the n biggest differences
	 * @param n number of differences to find
	 * @param noZero indicates whether the distance of two objects which is equal to zero is allowed
	 * @throws IllegalArgumentException if the number of differences doesn't match the number
	 * of given data ( n too big or too less data)
	 * @return the indices (positions where the data occurred in the iterator) of the n biggest 
	 * differences according to the given distance function, starting with the (left) index of the n-th
	 * biggest difference and ending with the biggest one
	 */
	public static int [] maxDiff ( Iterator data, Distance distance, int n, boolean noZero) {
		// init the used min-heap, ordering according to the distances
		// the objects inserted into the heap will be Object[]
		Heap heap = new Heap ( n, new Comparator (){
			public int compare ( Object o1, Object o2){
				double d1 = ((Number) ((Object []) o1)[0]).doubleValue();
				double d2 = ((Number) ((Object []) o2)[0]).doubleValue();
				return d1 < d2 ? -1 : d1 == d2 ? 0 : 1;
			}
		});
		heap.open();
		// get the first element
		Object previous = null;
		if ( data.hasNext () ) {
			previous = data.next();
		}
		else throw new IllegalArgumentException("input data is empty, but need at least " + (n+1) + " objects!");		
		// init helping vars
		int inserted = 0; // number of inserted distances or number of Objects in the heap
		int position = 0; // numbering the processed data (index)
		Object next = null;
		// processing the data
		while ( data.hasNext() ){
			next = data.next();
			position++; // increase the index
			// computing difference
			double dist = distance.distance ( previous, next);
			// should trivial distances be allowed
			if (!( noZero && (dist == 0.0))) {
				// enough data inserted into the heap
				if ( inserted < n ){ // no
					heap.enqueue( new Object[]{ new Double (dist ), new Integer (position-1) });
					inserted++;
				}
				else { // yes
					// smallest value in the heap < current dist?
					if ( ((Double) ((Object []) heap.peek () )[0]).doubleValue() < dist ) { // yes
						// insert current distance
						heap.replace ( new Object[]{ new Double (dist ), new Integer (position-1) });
					}
				}
			}
			previous = next;
		}
		//
		if ( heap.size() != n) throw new IllegalArgumentException ("Not enough data given. Check parameters!");
		//
		int [] indices = new int [n];
		int pos = 0;
		while (!heap.isEmpty()) {
			indices [pos++] = ((Integer) ((Object[]) heap.dequeue())[1]).intValue();
		}
		heap.close();
		return indices;
	}
}
