/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2013 Prof. Dr. Bernhard Seeger
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
package xxl.core.spatial.histograms.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import xxl.core.cursors.Cursor;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 * 
 * see: S. Acharya, V. Poosala, and S. Ramaswamy.
 * Selectivity estimation in spatial databases. SIGMOD '99
 *
 */
public class MinSkewHist {

	/**
	 * Precision of the space filling curve.
	 */
	protected static int FILLING_CURVE_PRECISION = 128;

	/**
	 * 
	 */
	public static boolean verbose = false;
	/**
	 * 
	 * @param sfcPrecision
	 * @return
	 */
	@SuppressWarnings("serial")
	public static UnaryFunction<double[], int[]> getSFCFunction(final int sfcPrecision){
		return new UnaryFunction<double[], int[]>() {

			@Override
			public int[] invoke(double[] from) {
				int[] to = new int[from.length];
				for (int i = 0; i < to.length; i++) {
					to[i] = (int) (from[i] * sfcPrecision);
				}
				return to;
			}
		};
	}

	// Space MUST have been normalized to [0.0,1.0]
	/**
	 * 
	 * @param rectangles
	 * @param bitsPerDim
	 * @param dimensions
	 * @return
	 */
	public static Map<Long, Integer> computeGrid(
			Cursor<DoublePointRectangle> rectangles, int bitsPerDim,
			int dimensions) {
		Map<Long, Integer> grid = new HashMap<Long, Integer>();
		rectangles.reset();
		UnaryFunction<double[], int[]> convertToFillingCurveValues = getSFCFunction( (1 << (bitsPerDim-1))) ;
		for (; rectangles.hasNext();) {
			DoublePointRectangle actRectangle = rectangles.next();
			double[] lowleft = (double[]) actRectangle.getCorner(false)
					.getPoint();
			double[] upright = (double[]) actRectangle.getCorner(true)
					.getPoint();
			int[] intLowLeft = convertToFillingCurveValues.invoke(lowleft);
			int[] intUpRight = convertToFillingCurveValues.invoke(upright);
			try {
				List<long[]> zcodesList = SpaceFillingCurves.computeZBoxRanges(
						intLowLeft, intUpRight, bitsPerDim, dimensions);
				for (long[] zcodes : zcodesList) {
					for (long zcode = zcodes[0]; zcode <= zcodes[1]; zcode++) {
						Integer value = 1;
						if (grid.containsKey(zcode)) {
							value = grid.get(zcode);
							value++;
						}
						grid.put(zcode, value);
					}
				}
			} catch (Exception ex) {
				throw new RuntimeException("Z-Curve box computation problem",ex); 
			}
		}
		return grid;

	}
	
	// Space MUST have been normalized to [0.0,1.0]
	/**
	 * 
	 * @param rectangles
	 * @param bitsPerDim
	 * @param dimensions
	 * @return
	 */
	public static Map<Long, Integer> computeGridForForest(
				Cursor<DoublePointRectangle> rectangles, int bitsPerDim,
				int dimensions) {
			Map<Long, Integer> grid = new HashMap<Long, Integer>();
			UnaryFunction<double[], int[]> convertToFillingCurveValues = getSFCFunction( (1 << (bitsPerDim-1))) ;
			for (; rectangles.hasNext();) {
				DoublePointRectangle actRectangle = rectangles.next();
				double[] lowleft = (double[]) actRectangle.getCorner(false)
						.getPoint();
				double[] upright = (double[]) actRectangle.getCorner(true)
						.getPoint();
				int[] intLowLeft = convertToFillingCurveValues.invoke(lowleft);
				int[] intUpRight = convertToFillingCurveValues.invoke(upright);
				try {
					List<long[]> zcodesList = SpaceFillingCurves.computeZBoxRanges(
							intLowLeft, intUpRight, bitsPerDim, dimensions);
					for (long[] zcodes : zcodesList) {
						for (long zcode = zcodes[0]; zcode <= zcodes[1]; zcode++) {
							Integer value = 1;
							if (grid.containsKey(zcode)) {
								value = grid.get(zcode);
								value++;
							}
							grid.put(zcode, value);
						}
					}
				} catch (Exception ex) {
					throw new RuntimeException("Z-Curve box computation problem",ex);
				}
			}
			return grid;
	}
		
	/**
	 * 
	 * 
	 * 	
	 * @param rectangles
	 * @param universe
	 * @param bitsdPerDim
	 * @param dimensions
	 * @param maxBuckets
	 * @param refinements
	 * @return
	 */
	public static List<SpatialHistogramBucket> buildProgressiveRefinement(Cursor<DoublePointRectangle> rectangles,
			DoublePointRectangle universe, int bitsdPerDim, int dimensions,
			int maxBuckets, int refinements){
		PriorityQueue<Bucket> buckets = new PriorityQueue<Bucket>(100,
				new Comparator<Bucket>() {

					@Override
					public int compare(Bucket o1, Bucket o2) {
						if (o2.getBestReduction() - o1.getBestReduction() > 0) {
							return 1;
						} else if (o2.getBestReduction()
								- o1.getBestReduction() < 0) {
							return -1;
						} else {
							return 0;
						}
					}

				});
		
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>();
		List<Bucket> tempList = new ArrayList<>();
		Map<Long, Integer> grid; // grid 
		int refStep =  refinements; 
		int step = maxBuckets/(refinements + 1);
		int bucketsProRefimenement = maxBuckets/(refinements + 1);
		grid = computeGrid(rectangles, bitsdPerDim - refStep,
				dimensions);
		// 2. initial bucket berechnen
		Bucket initialBucket = new Bucket(
				new DoublePointRectangle(dimensions)
						.normalize(new DoublePointRectangle(dimensions)));
		initialBucket.computeSkew(grid, bitsdPerDim - refStep);
		
		buckets.add(initialBucket);
		// 3. restliche buckets berechnen
		if(verbose)
			System.out.println("start");
		while (tempList.size() + buckets.size() <= maxBuckets) {
			if(verbose)
				System.out.println("Buckets: " + buckets.size() + " max Buckets: "
						+ maxBuckets);
			
			Bucket bucketTosplit = buckets.poll();
			bucketTosplit.computeBestSplit(grid, bitsdPerDim - refStep);
			// check if it exists
			if(bucketTosplit.getBestOne() == null){
				// cannot be splitted add to temp list
				tempList.add(bucketTosplit);
			}else{
				bucketTosplit.getBestOne().computeBestSplit(grid, bitsdPerDim - refStep);
				bucketTosplit.getBestTwo().computeBestSplit(grid, bitsdPerDim - refStep);
				buckets.add(bucketTosplit.getBestOne());
				buckets.add(bucketTosplit.getBestTwo());
			}
			if (buckets.isEmpty() ){
				if (!tempList.isEmpty()){
						buckets.addAll(tempList);
						tempList.clear();
				}
				if (refStep <= 0){ // case all buckets are in temp and we cannot refine   
					break;
				}
				refStep--; // try to refine and compute grid
				grid = computeGrid(rectangles, bitsdPerDim - refStep,
						dimensions); //
				bucketsProRefimenement += step;					
			}else if (tempList.size() + buckets.size() >=  bucketsProRefimenement){
				refStep--; // try to refine and compute grid
				grid = computeGrid(rectangles, bitsdPerDim - refStep,
						dimensions); //
				// add temp buckets to list
				buckets.addAll(tempList);
				tempList.clear();
				bucketsProRefimenement += step;
			}
		}
		 buckets.addAll(tempList);
		rectangles.reset();
		
		while (rectangles.hasNext()) {
			DoublePointRectangle dpr = rectangles.next();
			DoublePoint mitte = dpr.getCenter();
			for (Bucket bucket : buckets) {
				if (bucket.contains(mitte)) {
					bucket.setWeight(bucket.getWeight() +1);
					bucket.updateAverage(dpr);
				}
			}
			for (SpatialHistogramBucket bucket : histogram) {
				if (bucket.contains(mitte)) {
					bucket.setWeight(bucket.getWeight() +1);
					bucket.updateAverage(dpr);
				}
			}
		}
		histogram.addAll(buckets);
		
		rectangles.reset();
		
	
		
		return histogram;
		
	}	
		
		
	/**
	 * 
	 * @param rectangles
	 * @param gridSize
	 * @param dimensions
	 * @return
	 */
	public static List<SpatialHistogramBucket> buildHistogram(
			Cursor<DoublePointRectangle> rectangles,
			DoublePointRectangle universe, int bitsdPerDim, int dimensions,
			int maxBuckets) {
		PriorityQueue<Bucket> buckets = new PriorityQueue<Bucket>(100,
				new Comparator<Bucket>() {

					@Override
					public int compare(Bucket o1, Bucket o2) {
						if (o2.getBestReduction() - o1.getBestReduction() > 0) {
							return 1;
						} else if (o2.getBestReduction()
								- o1.getBestReduction() < 0) {
							return -1;
						} else {
							return 0;
						}
					}

				});
		
		List<SpatialHistogramBucket> histogram = new ArrayList<SpatialHistogramBucket>();
		// 1.initial grid
		Map<Long, Integer> grid = computeGrid(rectangles, bitsdPerDim,
				dimensions);
		// 2.first bucket
		Bucket initialBucket = new Bucket(
				new DoublePointRectangle(dimensions)
						.normalize(new DoublePointRectangle(dimensions)));
		initialBucket.computeSkew(grid, bitsdPerDim);
		initialBucket.computeBestSplit(grid, bitsdPerDim);
		buckets.add(initialBucket);
		// 3. restliche buckets berechnen
		if(verbose)
			System.out.println("starte!!!");

		while ((buckets.size() + histogram.size()) <= maxBuckets && buckets.size() > 0) {
			if(verbose)
				System.out.println("Buckets: " + buckets.size() + " max Buckets: "
						+ maxBuckets);

			Bucket bucketTosplit = buckets.poll();
			if(bucketTosplit.getBestOne() == null){
				histogram.add(bucketTosplit);
				continue;
			}
			if(verbose)
				System.out.println("P: " + bucketTosplit + "BestReduction: "
						+ bucketTosplit.getBestReduction());
			bucketTosplit.getBestOne().computeBestSplit(grid, bitsdPerDim);
			bucketTosplit.getBestTwo().computeBestSplit(grid, bitsdPerDim);
			if(verbose){
				System.out.println("1: " + bucketTosplit.getBestOne()
						+ "BestReduction: "
						+ bucketTosplit.getBestOne().getBestReduction());
	
				System.out.println("2: " + bucketTosplit.getBestTwo()
						+ "BestReduction: "
						+ bucketTosplit.getBestTwo().getBestReduction());
			}
			buckets.add(bucketTosplit.getBestOne());
			buckets.add(bucketTosplit.getBestTwo());
		}
		rectangles.reset();
		while (rectangles.hasNext()) {
			DoublePointRectangle dpr = rectangles.next();
			DoublePoint mitte = dpr.getCenter();
			for (Bucket bucket : buckets) {
				if (bucket.contains(mitte)) {
					bucket.setWeight(bucket.getWeight() +1);
					bucket.updateAverage(dpr);
				}
			}
			for (SpatialHistogramBucket bucket : histogram) {
				if (bucket.contains(mitte)) {
					bucket.setWeight(bucket.getWeight() +1);
					bucket.updateAverage(dpr);
				}
			}
		}
		histogram.addAll(buckets);
		return histogram;
	}
	/**
	 * 
	 * This static class is used minskew computation. It reperesents a grid cell.
	 *
	 */
	@SuppressWarnings("serial")
	public static class Bucket extends SpatialHistogramBucket {
		/**
		 * 
		 */
		private Double skew = Double.MAX_VALUE;
		/**
		 * 
		 */
		private Double bestReduction = 0.0;
		/**
		 * 
		 */
		private Bucket bestOne = null;
		/**
		 * 
		 */
		private Bucket bestTwo = null;
		/**
		 * 
		 */
		private int numberOfReferencedObjects;
		/**
		 * 	
		 */
		private int localBitsProDim = 0;
		/**
		 * 
		 * @param dimension
		 */
		public Bucket(int dimension) {
			super(dimension);
		}
		/**
		 * 
		 * @param dimension
		 * @param localBitsProDim
		 */
		public Bucket(int dimension,int localBitsProDim) {
			super(dimension);
			this.localBitsProDim = localBitsProDim; 
		}
		/**
		 * 
		 * @param rec
		 */
		public Bucket(DoublePointRectangle rec) {
			super(rec);
		}
		/**
		 * 
		 * @param grid
		 * @param bitsdPerDim
		 */
		public void computeSkew(Map<Long, Integer> grid, int bitsdPerDim) {
			UnaryFunction<double[], int[]> convertToFillingCurveValues = getSFCFunction( (1 << (bitsdPerDim-1))) ;
			int[] intLowLeft = convertToFillingCurveValues
					.invoke(this.leftCorner);
			int[] intUpRight = convertToFillingCurveValues
					.invoke(this.rightCorner);
			List<long[]> zcodesList = SpaceFillingCurves.computeZBoxRanges(
					intLowLeft, intUpRight, bitsdPerDim, this.dimensions());

			List<Integer> allValues = new LinkedList<Integer>();
			for (long[] zcodes : zcodesList) {
				for (long zcode = zcodes[0]; zcode <= zcodes[1]; zcode++) {
					Integer value = 0;
					if (grid.containsKey(zcode)) {

						value = grid.get(zcode);
					}
					allValues.add(value);
				}
			}
			Double summe = 0.0;
			for (Integer i : allValues) {
				summe += i;
			}
			Double schnitt = summe / allValues.size();

			Double skew = 0.0;
			for (Integer i : allValues) {
				skew += (i - schnitt) * (i - schnitt);
			}
			setNumberOfReferencedObjects(allValues.size());
			setSkew(skew);
		}
		/**
		 * 
		 * @param size
		 */
		public void setNumberOfReferencedObjects(int size) {
			this.numberOfReferencedObjects = size;
		}
		/**
		 * 
		 * @return
		 */
		public int getNumberOfReferencedObjects() {
			return numberOfReferencedObjects;
		}

		public void computeBestSplit(Map<Long, Integer> grid, int bitsdPerDim) {
			double gridsize = 0.0;
			if (bitsdPerDim > localBitsProDim)
				gridsize = 1.0 / Math.pow(2, bitsdPerDim-1);
			for (int dim = 0; dim < dimensions(); dim++) {
				for (int step = 1; (this.leftCorner[dim] + (step) * gridsize) < this.rightCorner[dim]; step++) {// splitstelle?!?
																					// ausrechenen
					Bucket one = new Bucket(dimensions());
					Bucket two = new Bucket(dimensions());
					
					double[] cutPointL = new double[this.rightCorner.length];
					System.arraycopy(this.rightCorner, 0, cutPointL, 0,
							this.rightCorner.length);
					cutPointL[dim] = this.leftCorner[dim] + step * gridsize;
					
					double[] cutPointR = new double[this.leftCorner.length];
					System.arraycopy(this.leftCorner, 0, cutPointR, 0,
							this.leftCorner.length);
					cutPointR[dim] = this.leftCorner[dim] + (step) * gridsize;

					one.leftCorner = this.leftCorner;
					one.rightCorner = cutPointL;
					
					two.leftCorner = cutPointR;
					two.rightCorner = this.rightCorner;

					one.computeSkew(grid, bitsdPerDim);
					two.computeSkew(grid, bitsdPerDim);

					Double actReduction = getSkew()  - (one.getSkew() + two.getSkew());
					
					if (bestReduction < actReduction ) {
						bestReduction = actReduction;
						setBestOne(one);
						setBestTwo(two);
					} 
				}
			}
			if (bestOne == null && verbose){
				System.out.println("Problem!");
			}
		}
		/**
		 * 
		 * @return
		 */
		public Double getSkew() {
			return skew;
		}
		/**
		 * 
		 * @param skew
		 */
		public void setSkew(Double skew) {
			this.skew = skew;
		}
		/**
		 * 
		 * @return
		 */
		public Bucket getBestOne() {
			return bestOne;
		}
		/**
		 * 
		 * @param bestOne
		 */
		private void setBestOne(Bucket bestOne) {
			this.bestOne = bestOne;
		}
		/**
		 * 
		 * @return
		 */
		public Bucket getBestTwo() {
			return bestTwo;
		}
		/**
		 * 
		 * @param bestTwo
		 */
		private void setBestTwo(Bucket bestTwo) {
			this.bestTwo = bestTwo;
		}
		/**
		 * 
		 * @return
		 */
		public Double getBestReduction() {
			return bestReduction;
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.spatial.histograms.utils.SpatialHistogramBucket#toString()
		 */
		@Override
		public String toString() {
			return  this.bestReduction +  " ; skew "  +  this.skew  + "  " + super.toString();
		}
		
	}
}
