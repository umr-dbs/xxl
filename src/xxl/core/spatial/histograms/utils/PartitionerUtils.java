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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import xxl.core.cursors.Cursor;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.rtrees.GenericPartitioner;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 * 
 * This class contains cost function for spatial histogram computation
 * 
 * @see MHistograms
 * @see GenericPartitioner
 * 
 */
public class PartitionerUtils {
	
	public static enum ProcessorType{
		VOLUME,
		GRID_SSE,
		RK_HIST,
	}
	
	
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
	
	public static Map<Long, Integer> computeGrid(
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
				ex.printStackTrace();
			}
		}
		return grid;

	}
	
	
	
	public static class SpatialSkewProcessor implements  CostFunctionArrayProcessor<DoublePointRectangle>{
		
		protected double[][] arrayCosts; 
 		
		protected boolean useStorage;
		
		protected int B;
		
		protected int n; 
		
		protected int bitsPerDim;  
		
		protected Map<Long, Integer> grid;
		
		protected int dimension; 
		
		protected UnaryFunction<double[], int[]>  getSfcKey; 
		
		
		public SpatialSkewProcessor(Cursor<DoublePointRectangle> gridConsIterator, 
				int precision, int dimension, int B, int storageSlots, boolean useStorage) {
			super();
			if (useStorage){
				arrayCosts = new double[storageSlots][B];
				Arrays.fill(arrayCosts, null);
			}
			this.B = B;
			this.n = storageSlots;
			this.useStorage = useStorage;
			this.bitsPerDim= precision;
			this.dimension = dimension;
			this.getSfcKey = getSFCFunction((1 << (bitsPerDim-1)));
			this.grid = computeGrid(gridConsIterator, precision, dimension); 
		}
		
		
		
		
		
		
		@Override
		public double[] processList(
				DoublePointRectangle[] rectangles,
				int b,
				int B,
				int startIndex) {
			if (arrayCosts[startIndex]  == null ){
				double[] array = new double[B];
				DoublePointRectangle universe = null; 
				for(int i = 0, j = startIndex; j >=0 && i < B ; i++, j--){
						if(universe == null)
							universe = new DoublePointRectangle(rectangles[j]);
						else
							universe.union(rectangles[j]);
						if (i >=b-1 ){
							array[i] = computeSkew (universe); 
						}
					
				}
				arrayCosts[startIndex] =  array;
				return array;
			}
			return arrayCosts[startIndex]; 
		}
		
		
		
		
		
		
		public void reset(){
			if (useStorage){
				arrayCosts = new double[n][B];
				Arrays.fill(arrayCosts, null);
			}
		}
		
		
		protected double computeSkew(DoublePointRectangle rectangle ){
			//
			double skew = 0;
			double average = 0;
			double cells = 0;
			// compute 
			DoublePointRectangle actRectangle =  rectangle;
			double[] lowleft = (double[]) actRectangle.getCorner(false)
					.getPoint();
			double[] upright = (double[]) actRectangle.getCorner(true)
					.getPoint();
			int[] intLowLeft = getSfcKey.invoke(lowleft);
			int[] intUpRight = getSfcKey.invoke(upright);
			List<long[]> zcodesList = SpaceFillingCurves.computeZBoxRanges(
					intLowLeft, intUpRight, bitsPerDim, dimension);
			
			for(long[] zcodes : zcodesList){
				cells +=( zcodes[1] - zcodes[0] + 1); 
			}
			
			double sum = 0; 
			
			for (long[] zcodes : zcodesList) {
				for (long zcode = zcodes[0]; zcode <= zcodes[1]; zcode++) {
					if (grid.containsKey(zcode)) {
						double value = grid.get(zcode);
						sum +=value;
					}
				}
			}
			
			average = sum / cells; // x^		
					
			for (long[] zcodes : zcodesList) {
				for (long zcode = zcodes[0]; zcode <= zcodes[1]; zcode++) {
					if (grid.containsKey(zcode)) {
						double value = grid.get(zcode);
						skew += Math.pow((value - average), 2); 
					}
				}
			}
			return skew;
		}
		
		public double[] processInitialList(	DoublePointRectangle[] rectangles, int b, int B){
			double[] array = new double[B];
			DoublePointRectangle universe = null;
			for(int i = 0;i < B ; i++ ){
				if(universe == null)
					universe = new DoublePointRectangle(rectangles[i]);
				else
					universe.union(rectangles[i]);
				array[i] = computeSkew(universe); 
			}
			return array;
		}

		@Override
		public double[][] precomputeAllCosts(DoublePointRectangle[] rectangles)
				throws UnsupportedOperationException {
			double[][] costs = new double[rectangles.length][rectangles.length];
			for(int i = 0; i < rectangles.length; i++){
				DoublePointRectangle rec = null;
				for(int j = i ; j <rectangles.length; j++){
					if(rec == null){
						rec = new DoublePointRectangle(rectangles[j]);
					}
					else{
						rec.union(rectangles[j]);
					}
					costs[i][j] =  computeSkew(rec);
				}
			}
			return costs;
		}
		
	}
	
	
	
	/**
	 * 
	 * @author RK-Hist Partitioner
	 *
	 */
	public static class RKHistMetrikProcessor implements  CostFunctionArrayProcessor<DoublePointRectangle> {
		
		double[][] arrayCosts; 
 		
		boolean useStorage;
		
		int B;
		
		int n; 

		public RKHistMetrikProcessor (int B, int storageSlots, boolean useStorage) {
			super();
			if (useStorage){
				arrayCosts = new double[storageSlots][B];
				Arrays.fill(arrayCosts, null);
			}
			this.B = B;
			this.n = storageSlots;
			this.useStorage = useStorage;
		}
		
		
		
		
		
		
		@Override
		public double[] processList(
				DoublePointRectangle[] rectangles,
				int b,
				int B,
				int startIndex) {
			if (arrayCosts[startIndex]  == null ){
				double[] array = new double[B];
				DoublePointRectangle universe = null; 
				List<DoublePoint> asList = new LinkedList<DoublePoint>();
				for(int i = 0, j = startIndex; j >=0 && i < B ; i++, j--){
						asList.add(rectangles[j].getCenter());
						if(universe == null)
							universe = new DoublePointRectangle(rectangles[j]);
						else
							universe.union(rectangles[j]);
						if (i >=b-1 ){
							array[i] = computeRKMetric(asList, universe); 
						}
					
				}
				arrayCosts[startIndex] =  array;
				return array;
			}
			return arrayCosts[startIndex]; 
		}
		
		
		
		
		
		
		public void reset(){
			if (useStorage){
				arrayCosts = new double[n][B];
				Arrays.fill(arrayCosts, null);
			}
		}
		
		
		protected double computeRKMetric(List<DoublePoint> recs, DoublePointRectangle uni){
			return RKhist.kMetricCost(recs, uni);
		}
		
		
		
		public double[] processInitialList(	DoublePointRectangle[] rectangles, int b, int B){
			double[] array = new double[B];
			DoublePointRectangle universe = null;
			
			List<DoublePoint> asList = new LinkedList<DoublePoint>();
			for(int i = 0;i < B ; i++ ){
				asList.add(rectangles[i].getCenter());
				if(universe == null)
					universe = new DoublePointRectangle(rectangles[i]);
				else
					universe.union(rectangles[i]);
				array[i] = computeRKMetric(asList, universe);; 
			}
			return array;
		}

		@Override
		public double[][] precomputeAllCosts(DoublePointRectangle[] rectangles)
				throws UnsupportedOperationException {
			return null;
		}
		
	}
	
	
	
	/**
	 * 
	 * one dim SSE processor  see Thorsten Suel et. Al. 
	 * 
	 * computes the sfc value of a multidim object 
	 * and then process the list of scfs with SSE Metric
	 *
	 */
	public static class SSEArrayProcessor  implements CostFunctionArrayProcessor<DoublePointRectangle> {

		private final UnaryFunction<DoublePointRectangle, Long> sfcFunction;
		
		public SSEArrayProcessor(
				UnaryFunction<DoublePointRectangle, Long> sfcFunction) {
			super();
			this.sfcFunction = sfcFunction;
		}

		@Override
		public double[] processList(
				DoublePointRectangle[] rectangles,
				int b,
				int B,
				int startIndex) {
			double[] array = new double[B];
			double count = 0;
			double sum = 0;
			double qSum = 0;
			double avg = 0;
			double sse = 0; 
			for(int i = 0, j = startIndex; j >=0 && i < B ; i++, j-- ){
				double key = sfcFunction.invoke(rectangles[j]);
				qSum += key * key;
				sum +=key;
				count++;
				avg = count/sum;
				sse = qSum -(count)*avg*avg;
				array[i] = sse; 
			}
			return array;
		}

		@Override
		public double[] processInitialList(DoublePointRectangle[] rectangles,
				int b, int B) {
			double[] array = new double[B];
			double count = 0;
			double sum = 0;
			double qSum = 0;
			double avg = 0;
			double sse = 0; 
			for(int i = 0;  i < B ; i++){
				double key = sfcFunction.invoke(rectangles[i]);
				qSum += key * key;
				sum +=key;
				count++;
				avg = count/sum;
				sse = qSum -(count)*avg*avg;
				array[i] = sse; 
			}
			return array;
		}

		@Override
		public double[][] precomputeAllCosts(DoublePointRectangle[] rectangles)
				throws UnsupportedOperationException {
			
			double[][] costs = new double[rectangles.length][rectangles.length];
			for(int i = 0; i < rectangles.length; i++){
				double count = 0;
				double sum = 0;
				double qSum = 0;
				double avg = 0;
				double sse = 0; 
				for(int j = i ; j <rectangles.length; j++){
					double key = sfcFunction.invoke(rectangles[j]);
					qSum += key * key;
					sum +=key;
					count++;
					avg = count/sum;
					sse = qSum -(count)*avg*avg;
					costs[i][j] = sse;
				}
			}
			return costs;
		}

		@Override
		public void reset() {
			// TODO Auto-generated method stub
			
		}
		

	}
	
	
	/**
	 * 
	 * Default generic OPT list processor. Parameterized with a UnarayFunction DoublePointRectangle Double 
	 *
	 */
	public static class DeafultWeightedArrayProcessor  implements CostFunctionArrayProcessor<SpatialHistogramBucket>{

		final UnaryFunction< DoublePointRectangle, Double> costFunction; 
		

		public DeafultWeightedArrayProcessor(
				UnaryFunction<DoublePointRectangle, Double> costFunction) {
			super();
			this.costFunction = costFunction;
		}


		@Override
		public double[] processList(
				SpatialHistogramBucket[] rectangles,
				int minWeight,
				int maxWeight,
				int startIndex
				) {
			List<Double> list = new ArrayList<Double>(100);
			DoublePointRectangle universe = null;
			int weight = 0;
			for(int j = startIndex; j >=0 && weight < maxWeight ; j-- ){
				weight += rectangles[j].getWeight();
				if(universe == null)
					universe = new DoublePointRectangle(rectangles[j]);
				else
					universe.union(rectangles[j]);
				if( weight >= minWeight)
					list.add( costFunction.invoke(universe));
				else list.add(0d);
			}
			double[] array = new double[list.size()];
			for(int i = 0 ; i < array.length; i++){
				array[i] = list.get(i);
			}
			return array;
		}


		@Override
		public double[] processInitialList(
				SpatialHistogramBucket[] rectangles, int b, int B) {
			List<Double> list = new ArrayList<Double>(100);
			DoublePointRectangle universe = null;
			int weight = 0;
			for(int i = 0; i < rectangles.length && weight <= B ; i++ ){
				weight += rectangles[i].getWeight();
				if(universe == null)
					universe = new DoublePointRectangle(rectangles[i]);
				else
					universe.union(rectangles[i]);
				if( weight >= b)
					list.add( costFunction.invoke(universe));
				else list.add(0d);
			}
			double[] array = new double[list.size()];
			for(int i = 0 ; i < array.length; i++){
				array[i] = list.get(i);
			}
			return array;
		}


		
		@Override
		public double[][] precomputeAllCosts(
				SpatialHistogramBucket[] rectangles)
				throws UnsupportedOperationException {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public void reset() {
			// TODO Auto-generated method stub
			
		}
	}
	
	
	

	/**
	 * 
	 * one dim SSE processor  see Thorsten Suel et. Al. 
	 * 
	 * computes the sfc value of a multidim object 
	 * and then process the list of scfs with SSE Metric
	 *
	 */
	public static class SSEWeightedArrayProcessor  implements CostFunctionArrayProcessor<SpatialHistogramBucket> {

		private final UnaryFunction<DoublePointRectangle, Long> sfcFunction;
		

		public SSEWeightedArrayProcessor(
				UnaryFunction<DoublePointRectangle, Long> sfcFunction) {
			super();
			this.sfcFunction = sfcFunction;
		}

		@Override
		public double[] processList(
				SpatialHistogramBucket[] rectangles,
				int minWeight,
				int maxWeight,
				int startIndex
				) {
			List<Double> list = new ArrayList<Double>(100);
			int weight = 0;
			double count = 0;
			double sum = 0;
			double qSum = 0;
			double avg = 0;
			double sse = 0; 
			for(int i = 0, j = startIndex; j >=0 && weight <= maxWeight ; i++, j-- ){
				double key = sfcFunction.invoke(rectangles[j]);
				qSum += key * key;
				sum +=key;
				count++;
				avg = count/sum;
				sse = qSum -(count)*avg*avg;
				if( weight >= minWeight)
					list.add( sse);
				else list.add(0d);
			}
			double[] array = new double[list.size()];
			for(int i = 0 ; i < array.length; i++){
				array[i] = list.get(i);
			}
			return array;
		}

		@Override
		public double[] processInitialList(
				SpatialHistogramBucket[] rectangles, int b, int B) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public double[][] precomputeAllCosts(
				SpatialHistogramBucket[] rectangles)
				throws UnsupportedOperationException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void reset() {
			// TODO Auto-generated method stub
			
		}
	}
	
	
	
}
