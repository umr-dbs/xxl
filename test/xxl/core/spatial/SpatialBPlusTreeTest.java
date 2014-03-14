package xxl.core.spatial;

import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.indexStructures.BPlusTree;

/**
 * In this example we create an BPlusTree. It will store  Z-values of points.  
 * 
 * 
 * @author achakeev
 *
 */
public class SpatialBPlusTreeTest {

	/**
	 * Path were the R-trees containers ( {@link BlockFileContainer}) will be stored. 
	 * 
	 * NOTE: change
	 */
	public static String RTREE_PATH ="F://zbplustree//";
	
	/**
	 * Path to the California Streets (rea02.rec) data set;
	 * we will map doublepointrectangles to their center points and store doublepoints
	 * 
	 * NOTE: change
	 */
	public static String DATA_PATH="F://rtree//data//rea02.rec";
	
	/**
	 * Path to the query file with point queries.     
	 * NOTE: change
	 */
	public static String POINT_QUERY_PATH = "F://rtree//query_1//rea02.rec";
	
	/**
	 * Path to the query file with range queries. 
	 * NOTE: change
	 */
	public static String RANGE_QUERY_PATH = "F://rtree//query_100//rea02.rec";
	
	/**
	 * In this example we will use  {@link BlockFileContainer}. Therefore, 
	 */
	public static int BLOCK_SIZE = 4096; // 
	
	/**
	 * We use 2-dimensional data
	 */
	public static int DIMENSION = 2; 
	
	/**
	 * 
	 */
	public static boolean BUFFER = false; 
	
	/**
	 * 
	 */
	public static int BUFFER_PAGES = 10;
	
	
	public static BPlusTree createZBplusTree(){
		return null;
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
