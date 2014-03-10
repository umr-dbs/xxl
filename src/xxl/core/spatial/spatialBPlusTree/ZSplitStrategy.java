package xxl.core.spatial.spatialBPlusTree;

import java.util.Stack;

import xxl.core.indexStructures.Separator;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.IndexEntry;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node.SplitInfo;
import xxl.core.indexStructures.vLengthBPlusTree.splitStrategy.ShortestKeyStrategy;

/**
 * This strategy can be used with {@link VariableLengthBPlusTree}. 
 * @author d
 *
 */
public class ZSplitStrategy extends ShortestKeyStrategy<Object, Long> {
	
	
	
	
	/**
	 * This strategy tries to split on minimal prefix position
	 */
	@Override
	public SplitInfo runSplit(Stack path, Node newNode, Node node, int min,
			int max) {
		// search in the range from int min to int max bytes for the smallest key
		int nodeLevel = node.getLevel();
		// go from left to rigth
		int startIndex = 0;
		int load = 0;
		int minSizeEntry = min;
		int minSplitCost = Integer.MAX_VALUE; 
		
		for (int i = 0; i < node.number() && load < max; i++ ){
			int entrySize =  node.getEntryByteSize(node.getEntry(i), nodeLevel);
			// go to right
			load += entrySize;
			if (load >= min){ 
				// 
				long currentIndexKey =  keyValue(node, i);
				long predecessorIndexKey = keyValue(node, i-1);
				int splitCost = computePrefixLength(predecessorIndexKey, currentIndexKey);
				
				if ( splitCost < minSplitCost){
					minSplitCost = splitCost; // actual min
					startIndex = i;
				}
			}
		}
		SplitInfo info = newNode.new SplitInfo(path,  startIndex+1);
		// create Separators 
		Separator sepNewNode = null;
		Separator sepratorOfTriggeredNode = null;
		if (node.level() > 0){ //index node 
			sepNewNode = (Separator)((IndexEntry)node.getEntry(node.number()-1)).separator().clone();
			sepratorOfTriggeredNode = (Separator)((IndexEntry)node.getEntry(startIndex)).separator().clone();
		}else{ // leaf node
			sepNewNode =   createSeparator(getKey((Object) node.getEntry(node.number()-1)));
			sepratorOfTriggeredNode = createSeparator(getKey((Object) node.getEntry(startIndex)));
		}
		info.initialize( sepNewNode, sepratorOfTriggeredNode);
		return info;
	}
	
	/**
	 * 
	 * @param arg0
	 * @param arg1
	 * @return
	 */
	private int computePrefixLength(long arg0, long arg1){
		// compute 
		long mask= 1L << 63;
		int i;
		for( i = 64; ((mask & arg0)  == (mask & arg1)) & i  >= 0; i--, mask = mask >> 1 );
		return (64-i);
	}
	
	/**
	 * 
	 * @param node
	 * @param index
	 * @return
	 */
	private Long keyValue(Node node, int index){
		if(node.level() > 0)
			return (Long) ((IndexEntry)node.getEntry(index)).separator().sepValue();
		return getKey(node.getEntry(index));
	}
	
}
