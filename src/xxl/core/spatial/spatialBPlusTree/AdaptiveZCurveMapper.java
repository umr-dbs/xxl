package xxl.core.spatial.spatialBPlusTree;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import xxl.core.util.Pair;
/**
 * 
 * @author achakeye
 *
 */
public class AdaptiveZCurveMapper {
	
	
	/*******************************************************
	 * Z Curve optimizations
	 *******************************************************/
	/**
	 * This is an implementation of asymmetric z-curve. 
	 * 
	 * This method can be used for generation of adaptive sorting
	 * 
	 * see D. Achakeev, B. Seeger P. Widmayer "Sort-based query-adaptive loading of R-trees" Technical Report, Philipps-University Marburg, 2012 
	 * and CIKM 2012
	 * 
	 * @param dimIndex  sorted in ascending order g to the length of the query of dimensions e.g. [2,1,0] if the size of the query z dimension is smaller than x and y than 
	 * @param prefixLength
	 * @param resolutions array of e.g. [6, 4 , 2] dimension z has resolution of 2 bits
	 * @return returns array with dimensions as a function e.g. for symmetric z-curve we return [0,1,0,1,0,1,0,1] if we have a 4 bits for each dimension  
	 */
	public static int[]  computeShuffleFunctionAspectRatio(int[] dimIndex,  int[] prefixLength, int[] dimensionResolutions){
		// compute the length of the key
		int length = 0; 
		int[] resolutions = new int[dimensionResolutions.length];
		System.arraycopy(dimensionResolutions, 0, resolutions, 0, resolutions.length);
		for(int resolution : resolutions){
			length+=resolution;
		}
		int[] mapFunction = new int[length];
		// compute prefixes
		Stack<Integer> suffix = new Stack<>();
		int index=0; 
		for(int i = 0 ; i < prefixLength.length; i++){  // foreach prefix
			int L = prefixLength[i]; 
			for(int k = 0; k < L; k++){ // shuffle bits but only dimensions 
				for(int j = 0; j < dimIndex.length-i; j++){ // foreach sorted dimension value do 
					int dimension = dimIndex[j];
					if(resolutions[dimension] > 0){
						mapFunction[index]=dimension;  
						index++;
						resolutions[dimension]--; // decrement available length
					}
				}
				// put
			}
			suffix.push(dimIndex[prefixLength.length-i]);
		}
		suffix.push(dimIndex[0]); // last dimension 
		// compute suffix
		while(!suffix.isEmpty()){
			int dimension = suffix.pop();
			int remainResolution =  resolutions[dimension];
			for(int i = 0; i < remainResolution; i++){
				mapFunction[index] = dimension; 
				index++; 
			}
		}
		return mapFunction;
	}
	
	/**
	 * computes z value using a given z map function
	 * @param xyz
	 * @param mapFunction
	 * @param masks
	 * @return
	 */
	public static long computeZKey(int[] ddd, int[] mapFunction, int[] resolutions){
		int[] res = new int[resolutions.length];
		System.arraycopy(resolutions, 0, res, 0, res.length);
		long key = 0L;
		long maskPosition = 0L; 
		for(int i = 0, position = mapFunction.length-1; i < mapFunction.length; i++, position--){
			int dimension = mapFunction[i]; 
			//get 
			long dimensionVal = ddd[dimension];
			if(res[dimension] > 0){
				long mask = 1 << res[dimension]-1; 
				res[dimension]--;
				dimensionVal &= mask; // get 0 or 1
				if(dimensionVal > 0){ // set  
					maskPosition = 1L << position;
					key |=maskPosition;
				}
			}
		}
		return key; 
	}
	
	/**
	 * computes a ranges of a z-curve for a given rectangle. 
	 * rectangle is given by low-left and upper-right point. 
	 * 
	 * @return list of ranges
	 */
	public static List<SpatialZQueryRange> computesRanges(int[] lowPoint, int[] highPoint, 
			final int[] mapFunction, final int[] resolutions,  final int[] resolutionsAcc,  final int lastDimensionFirstIndex, 	int hyperPlaneIndex){
		// compute z values
		List<SpatialZQueryRange> list = new LinkedList<>();
		long low = computeZKey(lowPoint, mapFunction, resolutions);
		long high = computeZKey(highPoint, mapFunction, resolutions);
		if (low > high){
			throw new RuntimeException("Low point > High point!"); 
		}
		//
		if( low == high){
			list.add(new SpatialZQueryRange(low, high)); 
			return list;
		}
		//1. check if after (inclusive) index the lower point has following pattern 0000000...00000
		//2. check if after (inclusive) index the upper point has following pattern 1111111...11111
		// if not cut using hyperplane 
		// go through and define hyperplane cut on dimension where the firts 0 at low and 1 high
		int hyperplaneIndex = hyperPlaneIndex; 
		long lowVal  = 0L;
		long highVal = 0L; 
		if (hyperplaneIndex <= lastDimensionFirstIndex){
			list.add(new SpatialZQueryRange(low, high)); 
			return list;  
		}
		boolean equal = false; 
		for(int i = hyperPlaneIndex;  (equal =  (lowVal == highVal)) && i > lastDimensionFirstIndex; i--, hyperplaneIndex--){
			// check if we need a cut
			long mask = 1L << i;
			lowVal = low & mask; 
			highVal = high & mask; 
			int index = mapFunction.length-1; 
			index = index-hyperplaneIndex; 
			int hyperPlane = mapFunction[index]; 
			resolutionsAcc[hyperPlane] -=1;
		}
		if (hyperplaneIndex+1 <= lastDimensionFirstIndex || equal){
			list.add(new SpatialZQueryRange(low, high)); 
			return list;  
		}
		long maskOnes = 1L << (hyperplaneIndex+2);// 
		maskOnes -=1L; //1111111111
		//
		boolean lowCheck = ((low & maskOnes) == 0L); 
		boolean highCheck = ((high & maskOnes) == maskOnes); 
		if(lowCheck && highCheck){
			list.add(new SpatialZQueryRange(low, high)); 
			return list;
		}
		int index = mapFunction.length-1; 
		index = index-hyperplaneIndex-1; 
		int hyperPlane = mapFunction[index]; 
		int hyperPlaneValue = 1 << (resolutionsAcc[hyperPlane]);
		Pair<QueryBox, QueryBox> split =  cutQueryBox(lowPoint, highPoint, hyperPlane, hyperPlaneValue, resolutions); 
		QueryBox lowBox = split.getElement1();
		QueryBox highBox = split.getElement2();
		int[] lowAcc = new int[resolutionsAcc.length];
		System.arraycopy(resolutionsAcc, 0, lowAcc, 0, resolutionsAcc.length);
		List<SpatialZQueryRange> lowList = computesRanges(lowBox.getElement1(), lowBox.getElement2(),  mapFunction, resolutions,  lowAcc, lastDimensionFirstIndex, 	hyperplaneIndex); 
		int[] highAcc = new int[resolutionsAcc.length];
		System.arraycopy(resolutionsAcc, 0, highAcc, 0, resolutionsAcc.length);
		List<SpatialZQueryRange> highList = computesRanges(highBox.getElement1(), highBox.getElement2(),  mapFunction, resolutions, highAcc, lastDimensionFirstIndex, 	hyperplaneIndex); 
		list.addAll(lowList);
		list.addAll(highList);
		lowList = null; 
		highList = null;
		return list; 
	}
	/**
	 * Cuts the hyperrectangles along a hyperplane
	 * 
	 * Assumptions
	 * 
	 * 1. box is reperesented by integer values low and high
	 * 2. 
	 * 
	 * 
	 * we cut the box along the hyperplane set 
	 * 
	 * query ox is split in two boxes &rarr; 
	 *  1. Box 
	 *  	low point is the old lower point 
	 * 		high point  is dimension values | H where H := 0000000111111...11111 
	 * 		other dimensions have value of high point  
	 * 	2. Box 
	 *      low point is the old lower point dimension of hyperplane = (value & H) + 1 
	 *      
	 *
	 */
	private static Pair<QueryBox, QueryBox> cutQueryBox(int[] lowPoint, int[] highPoint, 
			int  hyperPlaneDimension, int hyperplaneValue,  final int[] resolutions){
		int[] lowLow = new int[lowPoint.length]; 
		int[] lowHigh = new int[lowPoint.length];
		int[] highLow =  new int[lowPoint.length]; 
		int[] highHigh =  new int[lowPoint.length]; 
		System.arraycopy(lowPoint, 0, lowLow, 0, lowPoint.length);
		System.arraycopy(highPoint, 0, lowHigh, 0, lowPoint.length);
		System.arraycopy(lowPoint, 0, highLow, 0, lowPoint.length);
		System.arraycopy(highPoint, 0, highHigh, 0, lowPoint.length);
		int mask = hyperplaneValue-1; 
		lowHigh[hyperPlaneDimension] = lowLow[hyperPlaneDimension] | mask;  //cut ->  
		highLow[hyperPlaneDimension] = lowHigh[hyperPlaneDimension] + 1;  
		return new Pair<>(new QueryBox(lowLow, lowHigh), new QueryBox(highLow, highHigh));
	}
	
	/**
	 * 
	 * @param prefixLength
	 * @param dimensionResolutions
	 * @return
	 */
	public static int getLastDimensionPrefixIndex(int firstL, int bitsLastDim ){
		int suffix = bitsLastDim - firstL;
		return suffix-1; 
	}
	
	/**
	 * 
	 * @param dimensionResolutions
	 * @return
	 */
	public static int getIndexOfHighestBit(int[] dimensionResolutions){
		int length = 0; 
		for(int resolution : dimensionResolutions){
			length+=resolution;
		}
		return length-1; 
	}
	/**
	 * type def class
	 * @author achakeye
	 *
	 */
	protected static class QueryBox extends Pair<int[], int[]>{
		/**
		 * 
		 * @param low
		 * @param high
		 */
		public QueryBox(int[] low, int[] high){
			super(low, high);
		}
	}
	/**
	 * 
	 * @author achakeev
	 *
	 */
	public static class SpatialZQueryRange extends Pair<Long, Long>{
		
		
		public SpatialZQueryRange(long min, long max){
			super(min, max); 
		}
		
	}
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) {
//		// e.g. x,y,z
//		// query Z > Y > X 
//		// resolutions
//		// length L0 = 2; L1 = 3;  
//		// resolution x = 10 , y = 8 , z = 4 -> key = 22 bits
//		// X= 0000 0000 00 , Y = 0000 0000, Z = 00 00
//		// mapfunction [xyzxyz + xyxyxy + xxxxxyyy + zz]
//		int x = 0; 
//		int y = 1; 
//		int z = 2; 
//		int[] dimensions = {0,1,2};
//		
//		int[] length = {2,3};
//		int[] resolution = {10, 8, 4}; 
//		int[] mapFunction =  computeShuffleFunctionAspectRatio( 
//				dimensions,  length, resolution); 
//		String arrayString = Arrays.toString(mapFunction);
//		System.out.println(arrayString);
//		// e.g. x,y,z
//		// query Z > Y > X 
//		// resolutions
//		// length L0 = 2; L1 = 4;  
//		// resolution x = 10 , y = 5 , z = 4 -> key = 19 bits
//		// X= 0000 0000 00 , Y = 0000 0, Z = 00 00
//		// mapfunction [xyzxyz + xyxyxyx + xxxx + zz]
//		dimensions = new int[]{0,1,2};
//		length = new int[]{2,4};
//		resolution = new int[] {10, 5, 4}; 
//		mapFunction =  computeShuffleFunctionAspectRatio( 
//				dimensions,  length, resolution); 
//		arrayString = Arrays.toString(mapFunction);
//		System.out.println(arrayString);
//		// function 
//		long key =  computeZKey(new int[]{4,0}, new int[]{0,1,0,1,0,1}, new int[]{3,3});
//		System.out.println(key);
//		key =  computeZKey(new int[]{3,3}, new int[]{0,1,0,1,0,1}, new int[]{3,3});
//		System.out.println(key);
//	}

}
