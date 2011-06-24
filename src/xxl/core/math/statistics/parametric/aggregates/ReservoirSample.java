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

import java.util.Random;

import xxl.core.math.functions.AggregationFunction;
import xxl.core.util.random.ContinuousRandomWrapper;
import xxl.core.util.random.DiscreteRandomWrapper;
import xxl.core.util.random.JavaContinuousRandomWrapper;
import xxl.core.util.random.JavaDiscreteRandomWrapper;

/** This class provides sampling algorithms for the usage with the
 * {@link xxl.core.cursors.mappers.Aggregator aggregator cursor}.
 * Anytime after initialization the returned array of objects
 * represents an <tt>iid</tt> (identically independent
 * distributed) random sample of the already consumed data.
 * Note that the size of the used entirety must not be known in advance.
 * Generally, there are four types of implementation available based on
 * <b>[Vit85]</b>: Jeffrey Scott Vitter, Random Sampling with a Reservoir, in
 * ACM Transactions on Mathematical Software, Vol. 11, NO. 1, March 1985, Pages 37-57.
 * The reservoir sampling strategy describes how the sample is drawn
 * from the given dataset.
 * In [Vit85] four types are intended: Type R, X, Y and Z.
 * <br>
 * <p>Performance of Algorithms R, X, Y, and Z</p>
 * <table width="100%" border="0">
 *  <tr>
 *    <td width="14%"><b>Algorithm </b></td>
 *    <td width="39%"><b>StatefulAverage number of uniform random variates</b></td>
 *    <td width="47%"><b>StatefulAverage CPU time </b></td>
 *  </tr>
 *  <tr>
 *    <td width="14%">R</td>
 *    <td width="39%">N-n</td>
 *    <td width="47%">O(N)</td>
 *  </tr>
 *  <tr>
 *    <td width="14%">X</td>
 *    <td width="39%">2n ln (N/n)</td>
 *    <td width="47%"> O(N)</td>
 *  </tr>
 *  <tr>
 *    <td width="14%">Y</td>
 *    <td width="39%">2n ln (N/n)</td>
 *    <td width="47%">O ( n^2 ( 1+ log (N/n)*log (log(N/n))))</td>
 *  </tr>
 *  <tr>
 *    <td width="14%">Z</td>
 *    <td width="39%">3n ln (N/n)</td>
 *    <td width="47%">O(n(1+log(N/n)))</td>
 *  </tr>
 *</table>
 * <br>
 * Type Y is not implemented so far due to the lack of information for computing
 * the needed distribution for the used random variable. All the other three types
 * of <tt>how to sample</tt> are implemented as described in [Vit85].
 *
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * <br>
 *
 * Consider the following example:
 * <code><pre>
 *  Iterator it = new Aggregator( new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100),50), new ReservoirSample ( 10, new XType(10))  );
	while ( it.hasNext() ){
		Object [] o = (Object []) it.next();
		if(o==null) continue;
		for (int i=0; i< o.length; i++)
			System.out.print(": " + o[i]); // print the current output
	}
 * <\code><\pre>
 * <br>
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 * @see java.util.Iterator
 * @see xxl.core.cursors.mappers.ReservoirSampler
 *
 */

public class ReservoirSample extends AggregationFunction<Number,Number[]> {

	/** Interface providing a strategy for reservoir sampling techniques.
	 * For further informations see {@link xxl.core.cursors.mappers.ReservoirSampler}.
	 */
	public static interface ReservoirSamplingStrategy {

		/** Computes the position where to store the currently treated object.
		 *
		 * @return position where to store the currently treated object
		 * in the reservoir used for sampling. Returns -1 if the object
		 * will be skipped.
		 */
		public int invoke();
	}

	/** This class provides a preimplementation of the ReservoirSamplingStrategy
	 * interface to minimize the effort required to implement this interface.
	 *
	 * For further informations see {@link xxl.core.cursors.mappers.ReservoirSampler}.
	 */
	public static abstract class AbstractStrategy implements ReservoirSamplingStrategy {

		/** Pseudo Random Number Generator (PRNG) used for continuous random variables */
		protected ContinuousRandomWrapper cRandom;

		/** Pseudo Random Number Generator (PRNG) used for discrete random variables */
		protected DiscreteRandomWrapper dRandom;

		/** size of the reservoir and also of the drawn sample */
		protected int reservoirSize;

		/** number of objects to skip in the underlying stream for increasing
		 * performance of the sampling strategy. 
		 */
		protected int skip = -1;

		/** Number of objects already processed. Necessary to compute the sampling distribution */
		protected int count = 0; // CAUTION: checking paper if count needs to start at 0
		// ERROR in paper [Vit85] at formula (3.2), Z-Type alg.:

		/** Default constructor for random sampling strategies.
		 * 
		 * @param reservoirSize size of the used reservoir
		 * @param cRandom pseudo random number generator (PRNG) delivering
		 * uniform distributed random numbers
		 * @param dRandom pseudo random number generator (PRNG) delivering
		 * equally discrete distributed random numbers
		 */
		public AbstractStrategy(int reservoirSize, ContinuousRandomWrapper cRandom, DiscreteRandomWrapper dRandom) {
			this.reservoirSize = reservoirSize;
			this.cRandom = cRandom;
			this.dRandom = dRandom;
		}

		/** Default constructor for random sampling strategies.
		 * Using this constructor means using the java built-in PRNG.
		 * 
		 * @param reservoirSize size of the used reservoir
		 */
		public AbstractStrategy(int reservoirSize) {
			this(
				reservoirSize,
				new JavaContinuousRandomWrapper(new Random()),
				new JavaDiscreteRandomWrapper(new Random()));
		}

		/** Preimplementation of the invoke-method to compute the skip value
		 * for the sampling strategy. One only needs to implement computeSkip and
		 * computePos to fulfill the requirements of reservoir sampling.
		 * 
		 * @return  computed position where to store the currently treated
		 * object in the used reservoir. If the given position exceeds the
		 * bounds of the reservoir the element has to be skipped.
		 */
		public int invoke() {
			if (skip == -1)
				computeSkip();
			int pos = -1;
			if (skip > 0) {
				skip--;
				pos = -1;
			} else { // skip == 0
				// Compute position in reservoir
				pos = computePos();
				skip = -1;
				// compute new skip value
			}
			count++;
			return pos;
		}

		/** Computes the internally used skip value.
		 *
		 */
		protected abstract void computeSkip();

		/** Computes the internally used position where to store the element
		 * in the used reservoir.
		 * 
		 * @return the computed position
		 */
		protected abstract int computePos();
	}

	/**
	 * Type R strategy computes for every object of the underlying iterator
	 * the designated position in the reservoir. If the computed
	 * position exceeds the reservoir the object will be skipped.
	 * For further informations see {@link xxl.core.cursors.mappers.ReservoirSampler}.
	 */
	public static class RType extends AbstractStrategy {

		/** Constructs a new object of the Rtype reservoir sampling strategy.
		 * 
		 * @param reservoirSize size of the used reservoir
		 * @param cRandom pseudo random number generator (PRNG) delivering
		 * uniform distributed
		 * random numbers between 0.0d (inclusively) and 1.0d (exclusively).
		 * @param dRandom pseudo random number generator (PRNG) delivering
		 * equally discrete distributed
		 * random numbers between zero (inclusively) and any given integer (exclusively)
		 */
		public RType(int reservoirSize, ContinuousRandomWrapper cRandom, DiscreteRandomWrapper dRandom) {
			super(reservoirSize, cRandom, dRandom);
		}

		/** Constructs a new object of the Rtype reservoir sampling strategy.
		 * Using this constructor
		 * means using the Java Random Engine (which has a bad reputation).
		 * In package connectivity.colt wrappers for the usage with the Colt
		 * Library are available. 
		 * 
		 * @param reservoirSize size of the used reservoir
		 */
		public RType(int reservoirSize) {
			this(
				reservoirSize,
				new JavaContinuousRandomWrapper(new Random()),
				new JavaDiscreteRandomWrapper(new Random()));
		}

		/** Computes the skip value for type R strategy.
		 */
		protected void computeSkip() {
			int r = count;
			// U is denoted in uppercase in cause of indicating the presence of a random variable
			double U = cRandom.nextDouble();
			while (U * (reservoirSize + r + 1) > reservoirSize) {
				//s++;
				r++;
				U = cRandom.nextDouble();
			}
			skip = r - count;
		}

		/** Computes the position where to store the element in the reservoir.
		 * 
		 * @return computed position
		 */
		protected int computePos() {
			//return dRandom.nextInt (reservoirSize) ;
			double t = cRandom.nextDouble();
			t = t * reservoirSize;
			if (t == 1.0)
				return reservoirSize - 1;
			else
				return (int) Math.floor(t);
		}
	}

	/**
	 * Type X strategy computes first the number of objects to skip and then
	 * the position in the reservoir for the object followed by the last one skipped.
	 * For further informations see {@link xxl.core.cursors.mappers.ReservoirSampler}.
	 */
	public static class XType extends AbstractStrategy {

		/** Constructs a new object of the Xtype reservoir sampling strategy.
		 * 
		 * @param reservoirSize size of the used reservoir
		 * @param cRandom pseudo random number generator (PRNG) delivering
		 * uniform distributed
		 * random numbers between 0.0d (inclusively) and 1.0d (exclusively)
		 * @param dRandom pseudo random number generator (PRNG) delivering
		 * equally discrete distributed
		 * random numbers between zero (inclusively) and any given integer (exclusively)
		 */
		public XType(int reservoirSize, ContinuousRandomWrapper cRandom, DiscreteRandomWrapper dRandom) {
			super(reservoirSize, cRandom, dRandom);
		}

		/** Constructs a new object of the Xtype reservoir sampling strategy.
		 * Using this constructor
		 * means using the Java Random Engine (which has a bad reputation).
		 * In package connectivity.colt wrappers for the usage with the Colt
		 * Library are available. 
		 * 
		 * @param reservoirSize size of the used reservoir
		 */
		public XType(int reservoirSize) {
			this(
				reservoirSize,
				new JavaContinuousRandomWrapper(new Random()),
				new JavaDiscreteRandomWrapper(new Random()));
		}

		/** Computes the skip value for type X strategy.
		 */
		protected void computeSkip() {
			// double rand = random number in [0,1]
			double U = cRandom.nextDouble();
			int s = 0;
			//int z = reservoirSize + count - 1 ;
			int z = count + 1;
			int n = count + reservoirSize + 1;
			double val = (double) z / (double) n;
			while (val >= U) {
				z++;
				n++;
				val = val * z * (1.0 / n);
				s++;
			}
			skip = s;
		}

		/** Computes the position where to store the element in the reservoir.
		 * 
		 * @return computed position
		 */
		protected int computePos() {
			//return dRandom.nextInt(reservoirSize);
			double t = cRandom.nextDouble();
			t = t * reservoirSize;
			if (t == 1.0)
				return reservoirSize - 1;
			else
				return (int) Math.floor(t);
		}
	}

	/**
	 * Type Z strategy computes first the number of objects to skip and then
	 * the position in the reservoir for the object followed by the last one skipped.
	 * For further informations see {@link xxl.core.cursors.mappers.ReservoirSampler}.
	 */
	public static class ZType extends AbstractStrategy {

		/** Constructs a new object of the Ztype reservoir sampling strategy.
		 * 
		 * @param reservoirSize size of the used reservoir
		 * @param cRandom pseudo random number generator (PRNG) delivering
		 * uniform distributed
		 * random numbers between 0.0d (inclusively) and 1.0d (exclusively).
		 * @param dRandom pseudo random number generator (PRNG) delivering
		 * equally discrete distributed
		 * random numbers between zero (inclusively) and any given integer (exclusively)
		 */
		public ZType(int reservoirSize, ContinuousRandomWrapper cRandom, DiscreteRandomWrapper dRandom) {
			super(reservoirSize, cRandom, dRandom);
		}

		/** Constructs a new object of the Ztype reservoir sampling strategy.
		 * Using this constructor
		 * means using the Java Random Engine (which has a bad reputation).
		 * In package connectivity.colt wrappers for the usage with the Colt
		 * Library are available. 
		 * 
		 * @param reservoirSize size of the used reservoir
		 */
		public ZType(int reservoirSize) {
			this(
				reservoirSize,
				new JavaContinuousRandomWrapper(new Random()),
				new JavaDiscreteRandomWrapper(new Random()));
		}

		/** Computes the skip value for type Z strategy.
		 */
		protected void computeSkip() {
			boolean isValid = false;
			do {
				isValid = true;
				double U = cRandom.nextDouble();
				double V = cRandom.nextDouble();
				double G =
					(count + reservoirSize) * Math.pow(1.0 / V, 1.0 / reservoirSize) - (count + reservoirSize);
				double f;
				double g =
					(reservoirSize / (reservoirSize + count + G))
						* Math.pow(
							(reservoirSize + count) / (reservoirSize + count + G),
							reservoirSize);
				double h =
					((double) reservoirSize / (double) (reservoirSize + count + 1))
						* Math.pow(
							(double) (count + 1) / (double) (count + 1 + (int) Math.floor(G)),
							(reservoirSize + 1));
				double c = (double) (reservoirSize + count + 1) / (double) (count + 1);
				// ---
				if (U <= h / (c * g)) {
					skip = (int) Math.floor(G);
				} else {
					double z = (double) count + 1;
					double n = (reservoirSize + count + 1);
					double val = 1.0;
					// This if is used due to a bug in the original paper [Vit85]:
					// The authors transformation in formula (3.2) holds for
					// every n and t but not for n=t. In this case
					// in the process of transforming the formula there occurs
					// a division by zero ( 1/ (t-n))!
					if (Math.floor(G) == 0) {
						f = (double) reservoirSize / (double) (reservoirSize + count + 1);
					} else {
						for (int i = 1; i <= (int) Math.floor(G); i++) {
							val *=  z / n;
							z++;
							n++;
						}
						f = (reservoirSize / (reservoirSize + count + Math.floor(G) + 1)) * val;
					} //end of else
					if (U <= f / (c * g)) {
						skip = (int) Math.floor(G);
					} else {
						isValid = false;
					}
				}
			} while (!isValid);
		}

		/** Computes the position where to store the element in the reservoir.
		 * 
		 * @return computed position
		 */
		protected int computePos() {
			//return dRandom.nextInt(reservoirSize);
			double t = cRandom.nextDouble();
			t = t * reservoirSize;
			if (t == 1.0)
				return reservoirSize - 1;
			else
				return (int) Math.floor(t);
		}
	}

	/** strategy used for sampling */
	protected ReservoirSamplingStrategy strategy;

	/** indicates whether the reservoir is initialized or not */
	protected boolean initialized = false;

	/** number of already processed objects */
	protected long counts = 0;

	/** reservoir used for storing the sampled objects */
	protected Number[] reservoir;

	/** Constructs a new aggregation function of type sampling.
	 * 
	 * @param n size of the sample to draw
	 * @param strategy sampling strategy used for determine the
	 * position of the treated object
	 * in the sampling reservoir
	 * @throws IllegalArgumentException if the size of the reservoir isn't at least 1
	 */
	public ReservoirSample(int n, ReservoirSamplingStrategy strategy) throws IllegalArgumentException {
		this(new Number[n], strategy);
	}

	/** Constructs a new aggregation function of type sampling.
	 * 
	 * @param reservoir reservoir used to store the sampling
	 * @param strategy sampling strategy used for determine the
	 * position of the treated object
	 * in the sampling reservoir
	 * @throws IllegalArgumentException if the size of the reservoir isn't at least 1
	 */
	public ReservoirSample(Number[] reservoir, ReservoirSamplingStrategy strategy) throws IllegalArgumentException {
		if (reservoir.length < 1)
			throw new IllegalArgumentException("reservoir must have at least size 1");
		this.reservoir = reservoir;
		this.strategy = strategy;
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param oldReservoir result of the aggregation function in the previous computation step
	 * (<tt>iid</tt> sample of previous treated data)
	 * @param next next object used for computation
	 * @return aggregation value after n steps (new reservoir containing an <tt>iid>/tt> sample)
	 */
	public Number[] invoke(Number[] oldReservoir, Number next) {
		if (!initialized) {
			reservoir[(int) counts] = next;
			counts++;
			if (counts >= reservoir.length) {
				initialized = true;
				return reservoir;
			} else {
				return null;
			}
		} else {
			counts++;
			int pos = strategy.invoke();
			//if ( ( pos >= 0) && ( pos < reservoir.length) ) reservoir [pos] = next;
			try {
				reservoir[pos] = next;
			} catch (ArrayIndexOutOfBoundsException e) {}
			return reservoir;
		}
	}
}
