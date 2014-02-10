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
package xxl.core.indexStructures.rtrees;

import java.util.HashMap;
import java.util.Map;

import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;

/**
 * This class implements generic partitioning methods.
 * 
 * 
 * 
 * @see  D Achakeev, B Seeger and P Widmayer:
 * "Sort-based query-adaptive loading of R-trees" in CIKM 2012  
 * 
 */
public class GenericPartitioner {
	
	
	
	/**
	 * new Interface List Processor Interface for OPT computation
	 * 
	 *
	 */
	public static interface CostFunctionArrayProcessor<T extends Rectangle>{     //TODO


		/**
		 * Computes costs of starting from position startIndex
		 * @param rectangles
		 * @param b
		 * @param B
		 * @param startIndex
		 * @return output of cost array [startIndex, startIndex-1, ... , startIndex-B]
		 */
		public double[] processList(
				T[] rectangles, 
				int b, 
				int B,
				int startIndex);
		/**
		 * Computes initial costs starting from 0 
		 * 
		 * 
		 * @param rectangles
		 * @param b
		 * @param B
		 * @return output of cost array [1, 2, ... ,B]
		 */
		public double[] processInitialList(	T[] rectangles, int b, int B);
		
		/**
		 * 
		 * returns costs array for each sub sequence i,j 
		 * [i][j] is a cost for a sub sequence (i, ..., j)  with i <= j
		 * @param rectangles TODO
		 * 
		 * @return
		 * @throws UnsupportedOperationException
		 */
		public double[][] precomputeAllCosts(T[] rectangles) throws UnsupportedOperationException;
		
		public void reset();
		
	} 
	/**
     * Bucket object to represent current costs 
     * 
     */
	public static class Bucket{
		public double cost;
		public int start;
		public int end;
		public Bucket predecessor;
		public int number = 0;
		public Rectangle rec; 
		/**
		 * 
		 * @param cost
		 * @param start
		 * @param end
		 * @param predecessor
		 */
		public  Bucket(double cost, int start, int end, Bucket predecessor) {
			this(cost, start,  end, predecessor, 0);
		}
		/**
		 * 
		 * @param cost
		 * @param start
		 * @param end
		 * @param predecessor
		 * @param number
		 */
		public Bucket(double cost, int start, int end, Bucket predecessor,
				int number) {
			super();
			this.cost = cost;
			this.start = start;
			this.end = end;
			this.predecessor = predecessor;
			this.number = number;
		}
		@Override
		public String toString() {
			return "Bucket [cost=" + cost + ", end=" + end + ", number="
					+ number + ", predecessor=" + predecessor + ", start="
					+ start + "]";
		}		
	}
	
	
	/**
	 * 
	 * @param bucket
	 * @return
	 */
	public static int[] getDistribution(Bucket bucket){
		int[] array = new int[bucket.number];
		Bucket next = bucket;
		for(int i = array.length-1; i >= 0 ; i--){
			array[i] = next.end  - next.start  +1;
			next = next.predecessor;
		}
		return array;
	}
	
	
	/***************************************************************************************************
	 * Partitioner: 
	 **************************************************************************************************/
	/**
	 * unbounded variant 
	 * Thorsten Suel algorithm
	 * Time Complexity k*N*N 
	 * 
	 * nopt(i,k) = max_0<=j<=i {nopt(i-j, k-1) + costF([i-j+1, i]) }
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public static Bucket[][] computeNOPT(DoublePointRectangle[] rectangles, int n, 
			CostFunctionArrayProcessor processor){
		Bucket[][] costMatrix = new Bucket[n][rectangles.length];
		double[][] allCosts = processor.precomputeAllCosts(rectangles);
		// step 1 compute cost forward as starting point of n = 1
		for(int i = 0; i < rectangles.length; i++){ 
			costMatrix[0][i] =	new Bucket(allCosts[0][i], 0, i, null, 1); 
		}
		// process main loop
		for(int i = 1; i < n; i++){
			// compute best cost for given j and i 
			for(int j = i; j < rectangles.length; j++){
				// search for minimal cost 
				double minCost = Double.MAX_VALUE;  
				for(int l = j; l >= i; l--){	
					double newNewCost = allCosts[l][j];
					double lastRowCost = costMatrix[i-1][l-1].cost;
					double candidateCosts = lastRowCost + newNewCost; 
					if (candidateCosts < minCost){
						minCost = candidateCosts;
						costMatrix[i][j] = new Bucket(minCost, l, j,
								costMatrix[i-1][l-1], 
								costMatrix[i-1][l-1].number+1);
					}
				}
			}
		}
		return costMatrix;
	}
	/**
	 * non weighted version
	 * @param rectangles
	 * @param b
	 * @param B
	 * @param n
	 * @param processor
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Bucket[][] computeOPTF(DoublePointRectangle[] rectangles, int b, int B, int n, 
			CostFunctionArrayProcessor processor){
		Bucket[][] costMatrix = new Bucket[n][rectangles.length];
		double[] costs = processor.processInitialList(rectangles, b, B);
		for(int i = 0; i < B; i++){ 
			costMatrix[0][i] = (i < b-1) ? // TODO
					new Bucket(Double.MAX_VALUE, 0, i, null, 1) : 
						new Bucket(costs[i], 0, i, null, 1); 
		}
		for(int i = 1; i < n; i++){
			int nMin = ((i+1) * b)-1; 
			int nMax = (((i+1) * B)-1 >= rectangles.length) ? rectangles.length-1 : ((i+1) * B)-1 ;
			// compute best cost for given j and i 
			for(int j = nMin; j <= nMax ; j++){
				// search for minimal cost 
				double minCost = Double.MAX_VALUE;  
				costs = processor.processList(rectangles, b, B, j);
				for(int l = b-1; j-l >= b && l < B; l++){	
					// check if it possible assignment exists
					if (costMatrix[i-1][j-l-1] != null  ){ 
						double newNewCost = costs[l];
						double lastRowCost = costMatrix[i-1][j-l-1].cost;
						
						double candidateCosts = lastRowCost + newNewCost; 
						if (candidateCosts < minCost){
							minCost = candidateCosts;
							costMatrix[i][j] = new Bucket(minCost, j-l, j,
									costMatrix[i-1][j-l-1] , 
									costMatrix[i-1][j-l-1].number+1);
						}
					}
//					otherwise there is no assignment possible for current j and n  
				}
			}
		}
		return costMatrix;
	}
	
	/**
	 *
	 *    
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Bucket[] computeGOPT(DoublePointRectangle[] rectangles, 
		int b, int B, 
		CostFunctionArrayProcessor arrayProcessor){
		Bucket[] costArray = new Bucket[rectangles.length]; 
		Bucket dummy = new Bucket(Double.MAX_VALUE, 0, 0, null);
		int index = 0; 
		for(int t = index; t < b; t++ ){ // TODO initial computation
			costArray[t]= dummy;
			index = t;
		}
		for(int t = index; t < rectangles.length; t ++ ){
			double[] costs = arrayProcessor.processList(rectangles,  b, B,  t);
			double	mincost = (costArray[t] !=  null) ? costArray[t].cost : dummy.cost;
			int st = 0;
			int et = 0;
			Bucket argMin = null; 	// look back for a better costs
			for(int  j= b-1 ;  j < B ;  j++){ 
				if(t-j == 0){
					Bucket interval = new Bucket(costs[j], 0, t, null);
					interval.number += 1;
					costArray[t]= interval;
				}else if (t-j > 0){
					Bucket candidate = costArray[t-j-1];
					double costOfBucket  = costs[j];
					double costOfExtension = candidate.cost + costOfBucket;
					if(costOfExtension < mincost){
						// overwrite
						argMin = candidate;
						mincost = costOfExtension;
						st = t-j;
						et = t;
						// overwrite 
						Bucket newBck = new Bucket(mincost, st, et, argMin, argMin.number +1);
						costArray[t]= newBck;
					}
				}
			}
		}
		return costArray;
	}
	/*****************************************************************************************************
	 * Array Processors
	 ****************************************************************************************************/	
	/**
	 * 
	 * Default generic OPT list processor. Parameterized with a UnarayFunction DoublePointRectangle Double 
	 *
	 */
	public static class DefaultArrayProcessor  implements CostFunctionArrayProcessor<DoublePointRectangle>{

		final UnaryFunction< DoublePointRectangle, Double> costFunction; 
		
		
		Map<Integer, double[]> costs = null; 
		
		double[][] cArray = null;
		
		boolean mode = false;
		int n = 0;

		public DefaultArrayProcessor (
				UnaryFunction<DoublePointRectangle, Double> costFunction) {
			super();
			this.costFunction = costFunction;
			costs = new HashMap<Integer, double[]>();
			
		}
		
		public DefaultArrayProcessor (
				UnaryFunction<DoublePointRectangle, Double> costFunction, int n) {
			super();
			this.costFunction = costFunction;
			cArray = new double[n][];
			this.n = n;
		}
		
		public DefaultArrayProcessor (
				UnaryFunction<DoublePointRectangle, Double> costFunction, boolean mode) {
			super();
			this.costFunction = costFunction;
			this.mode = mode;
			costs = new HashMap<Integer, double[]>();
		}
		
		
		@Override
		public double[] processList(
				DoublePointRectangle[] rectangles,
				int b,
				int B,
				int startIndex) {
			if (mode)
				return processListWithout(
						rectangles,
						 b,
						 B,
						 startIndex);
			if(cArray !=null)
				return  processListArray(
						rectangles,
						 b,
						 B,
						 startIndex);
			
			if (!costs.containsKey(startIndex)){
				double[] array = new double[B];
				DoublePointRectangle universe = null;
				for(int i = 0, j = startIndex; j >=0 && i < B ; i++, j--){
					
						if(universe == null)
							universe = new DoublePointRectangle(rectangles[j]);
						else
							universe.union(rectangles[j]);
						if (i >=b-1 ){
							array[i] = costFunction.invoke(universe); 
							
						}
					
				}
				costs.put(startIndex, array);
				return array;
			}
			return costs.get(new Integer(startIndex)); 
		}
		
		private double[] processListArray(DoublePointRectangle[] rectangles,
				int b,
				int B,
				int startIndex){
			if (cArray[startIndex]==null){
				double[] array = new double[B];
				DoublePointRectangle universe = null;
				for(int i = 0, j = startIndex; j >=0 && i < B ; i++, j--){
					
						if(universe == null)
							universe = new DoublePointRectangle(rectangles[j]);
						else
							universe.union(rectangles[j]);
						if (i >=b-1 ){
							array[i] = costFunction.invoke(universe); 
							
						}
					
				}
				cArray[startIndex] = array;
				return array;
			}
			return cArray[startIndex]; 
		}
		
		
		private double[] processListWithout(
				DoublePointRectangle[] rectangles,
				int b,
				int B,
				int startIndex){
			double[] array = new double[B];
			DoublePointRectangle universe = null;
			for(int i = 0, j = startIndex; j >=0 && i < B ; i++, j-- ){
					if(universe == null)
						universe = new DoublePointRectangle(rectangles[j]);
					else
						universe.union(rectangles[j]);
					if (i >=b-1 ){
						array[i] = costFunction.invoke(universe); 
					}
			}
			return array;
		}
		
		
		public void reset(){
			costs = new HashMap<Integer, double[]>();
			if(cArray != null){
				cArray = new double[n][];
			}
		}
		
		
		public double[] processInitialList(	DoublePointRectangle[] rectangles, int b, int B){
			double[] array = new double[B];
			DoublePointRectangle universe = null;
			for(int i = 0;i < B ; i++ ){
				if(universe == null)
					universe = new DoublePointRectangle(rectangles[i]);
				else
					universe.union(rectangles[i]);
				array[i] = costFunction.invoke(universe); 
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
					costs[i][j] = costFunction.invoke(rec);
				}
			}
			return costs;
		}
	}
}
