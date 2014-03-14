package xxl.core.spatial.spatialBPlusTree.cursors;

/**
 * Interface for B+tree cursor
 * is used with {@link SpatialMultiRangeCursor} and {@link SpatialRangeQueryBPlusCursorVL}
 *  
 * @author d
 *
 * @param <K>
 */
public interface SFCFunctionSpatialCursor<K extends Comparable<K>> {
	/**
	 * 
	 * @return
	 */
	public K getMaxKeyInBox(int[] lPoint, int[] rPoint, boolean max);
	/**
	 * 
	 * @param box
	 * @param max
	 * @return
	 */
	public K getNextPointInBox(int[] lPoint, int[] rPoint, K key, boolean next);
	/**
	 * 
	 * @param key
	 * @return
	 */
	public K getSuccessor(K key);
}
