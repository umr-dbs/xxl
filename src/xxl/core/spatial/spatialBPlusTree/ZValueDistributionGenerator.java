package xxl.core.spatial.spatialBPlusTree;

import java.util.Iterator;
import java.util.List;




import xxl.core.collections.MappedList;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.spatial.rectangles.DoublePointRectangle;
/**
 * 
 * @author d
 *
 */
public class ZValueDistributionGenerator {
	
	
	public static class Bucket{
		double cost  = 0;
		int start = 0;
		int end = 0;
		Bucket predecessor = null;
		int number = 0;
		
		public Bucket(double cost, int start, int end,
				Bucket predecessor,
				int number) {
			super();
			this.cost = cost;
			this.start = start;
			this.end = end;
			this.predecessor = predecessor;
			this.number = number;
		}

		public double getCost() {
			return cost;
		}

		public void setCost(double cost) {
			this.cost = cost;
		}

		public int getStart() {
			return start;
		}

		public void setStart(int start) {
			this.start = start;
		}

		public int getEnd() {
			return end;
		}

		public void setEnd(int end) {
			this.end = end;
		}

		public Bucket getPredecessor() {
			return predecessor;
		}

		public void setPredecessor(Bucket predecessor) {
			this.predecessor = predecessor;
		}

		public int getNumber() {
			return number;
		}

		public void setNumber(int number) {
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
	 * @param rectangles
	 * @param sfcMappingFunction
	 * @param costFunction
	 * @param b
	 * @param B
	 * @param n
	 * @return
	 */
	@SuppressWarnings("serial")
	public static int[] computeDistribution(
			List<DoublePointRectangle> rectangles, 
			final UnaryFunction<DoublePointRectangle, Long> sfcMappingFunction, 
			BinaryFunction<Long, Long, Double> costFunction, int b, int B, int n){
		Bucket bucket = computeDistribution( new MappedList<DoublePointRectangle, Long>(rectangles, 
				new AbstractFunction<DoublePointRectangle,  Long>() {
				@Override
				public Long invoke(DoublePointRectangle argument) {
					return sfcMappingFunction.invoke(argument);
				}
			}),  
			createCostFunction(), 
				b,  B,  n);
		int[] distr = getDistribution(bucket);
		return distr;
	}
	
	/**
	 * 
	 * @param rectangles
	 * @param sfcMappingFunction
	 * @param costFunction
	 * @param b
	 * @param B
	 * @param n
	 * @return
	 */
	@SuppressWarnings("serial")
	public static int[] computeZKeysDistribution(
			List<Long> zKeys,   
			int b, 
			int B, 
			int n){
		Bucket bucket = computeDistribution(zKeys,  
			createCostFunction(), 
				b,  B,  n);
		int[] distr = getDistribution(bucket);
		return distr;
	}
	
	/**
	 * 
	 * @param rectangles
	 * @param sfcMappingFunction
	 * @param costFunction
	 * @param b
	 * @param B
	 * @param n
	 * @return
	 */
	@SuppressWarnings("serial")
	public static int[] computeZKeysDistributionApprox(
			List<Long> zKeys,   
			int b, 
			int B){
		Bucket bucket = computeDistributionApprox(zKeys,  
			createCostFunction(), 
				b,  B);
		int[] distr = getDistribution(bucket);
		return distr;
	}
	
	
	/**
	 * 
	 * @param zCodes
	 * @param costFunction
	 * @param b
	 * @param B
	 * @param n
	 * @return
	 */
	public static <T> Bucket computeDistribution(List<T> zCodes,  
			BinaryFunction<T, T, Double> costFunction, 
			int b, int B, int n){
		// allocate array
		Bucket[][] matrix = new Bucket[zCodes.size()][n];
		//FIXME change to lazy computation  assure that the input list is array!
		double[] costs = precomputeCosts( zCodes,  
				costFunction, b);
		// initialize first entries
		for(int i = 0; i < B; i++){
			matrix[i][0] = (i < b-1) ? 
					new Bucket(Double.MAX_VALUE, 0, i, null, 1) : 
					new Bucket(0, 0, i,null, 1); 
		}
		// 
		for(int i = 1; i < n; i++){
			//  
			int nMin = ((i+1) * b)-1; 
			int nMax = ((i+1) * B)-1;
			// compute best cost for given j and i 
			for(int j = nMin; j < nMax && j < matrix.length ; j++){
				// search for minimal cost 
				double minCost = Double.MAX_VALUE;  
				for(int l = b; j-l >= b && l < B; l++){
					if (matrix[j - l-1][i-1] != null  ){
						double newNewCost = costs[j-l];
						double lastRowCost = matrix[j-l-1][i-1].getCost();
						double candidateCosts = lastRowCost + newNewCost; 
						if (candidateCosts < minCost){
							minCost = candidateCosts;
							matrix[j][i] = new Bucket(minCost, j - l, j,
									matrix[j - l-1][i-1] , 
									matrix[j - l-1][i-1].number + 1);
						}
					}
//					otherwise there is no assignment possible for current j and n  
				}
			}
		}
		if(matrix[zCodes.size()-1][n-1] == null){
			throw new RuntimeException("No assignment Possible");
		}
		return matrix[zCodes.size()-1][n-1];
	} 
	
	
	/**
	 * 
	 * @param zCodes
	 * @param costFunction
	 * @param b
	 * @param B
	 * @param n
	 * @return
	 */
	public static <T> Bucket computeDistributionApprox(List<T> zCodes,  
			BinaryFunction<T, T, Double> costFunction, 
			int b, int B){
		Bucket[] binCosts = new Bucket[zCodes.size()];
		double[] costs = precomputeCosts(zCodes,  costFunction,  b);
		// initialize first [b, B]  buckets 
		// initialize first entries
		for(int i = 0; i < B; i++){
			binCosts[i] = (i < b-1) ? 
					new Bucket(Double.MAX_VALUE, 0, i, null, 1) : 
					new Bucket(0, 0, i,null, 1); 
		}
		// compute costs
		for(int i = 2*b-1; i < zCodes.size(); i ++){
			// search for best costs strating from pos i 
			double bestCost = (binCosts[i] == null) ? Double.MAX_VALUE : 
				binCosts[i].cost;
			for(int l = b; i-l >= b && l < B; l++){
				double prefixCost  = costs[i-l-1];
				double binCost = binCosts[i-l-1].cost;
				double candidateCost = prefixCost + binCost;
				if( candidateCost < bestCost){
					bestCost = candidateCost;
					binCosts[i] = new Bucket(bestCost, i-l, i, 
							binCosts[i-l-1],  
							binCosts[i-l-1].number+1);
				}
			}
		}
		return binCosts[zCodes.size()-1];
	}
	
	
	/**
	 * computes array 
	 * @param bucket
	 * @return
	 */
	public static  int[] getDistribution(Bucket bucket){
		
		int[] array = new int[bucket.number];
		Bucket next = bucket;
		for(int i = array.length-1; i >= 0 ; i--){
			array[i] = next.end  - next.start  +1;
			next = next.predecessor;
		}
		return array;
	}
	
	
	/**
	 * compute prefix as a cost function
	 * @return
	 */
	public static BinaryFunction<Long, Long, Double> createCostFunction(){
		
		return new BinaryFunction<Long, Long, Double>() {
			@Override
			public Double invoke(Long arg0, Long arg1) {
				// compute 
				long mask= 1L << 63;
				int i;
				for( i = 64; ((mask & arg0)  == (mask & arg1)) & i  >= 0; i--, mask = mask >> 1 );
				return (double)(64-i);
			}
			
		};
	}
	
	/**
	 * 
	 * @param <T>
	 * @param zCodes
	 * @param costFunction
	 * @param b
	 * @return
	 */
	protected static <T> double[] precomputeCosts(List<T> zCodes,  
			BinaryFunction<T, T, Double> costFunction, int b){
		double[] costs = new double[zCodes.size()-1];
		Iterator<T> zCodeIt = zCodes.iterator();
		T currentzCode = null;
		if(zCodeIt.hasNext()){
			currentzCode = zCodeIt.next(); 
		}else 
			throw new RuntimeException("z codes are empty!");
		int index = 0; 
		while(zCodeIt.hasNext()){
			T nextZcode = zCodeIt.next();
			if(index >= b-1){
				costs[index] =  costFunction.invoke(currentzCode, nextZcode);
			}
			currentzCode = nextZcode;
			index++;
		}
		return costs;
	}
	
	
//	/**
//	 * @param args
//	 */
//	public static void main(String[] args) {
//		BinaryFunction< Long, Long, Double> function = createCostFunction();
//		double prefix = function.invoke(8L, 16L);
//		System.out.println(prefix);
//	}

}
