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
package xxl.core.indexStructures.vLengthBPlusTree.splitStrategy;


import java.util.Iterator;
import java.util.Stack;

import xxl.core.indexStructures.Separator;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.IndexEntry;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node.SplitInfo;


/**
 * Implements a shortest key strategy for the split of the index nodes and simple prefix strategy for the leaf nodes. 
 * It searches in the bounds [min, max] which defined by the minimal allowed byte capacity of the node for the shortest key (in bytes).   
 * Assumption: keys are strings. 
 * see
 * "Prefix B-trees" 
 * Bayer, Rudolf and Unterauer, Karl
 * ACM Trans. Database Syst. 1977
 * 
 */
public class SimplePrefixBPlusTreeSplit extends ShortestKeyStrategy<Object, String> {
	

	public SimplePrefixBPlusTreeSplit() {
		super();
	}
	

	@Override
	public SplitInfo runSplit(Stack path, Node newNode, Node node, int min,
			int max) {
		// search in the range from int min to int max bytes for the smallest key
		// if it index node and run shortest prefix when it is leaf node
		int nodeLevel = node.getLevel();
		if (nodeLevel == 0) 
			return leafLevelSplit(path,newNode,  node, min,max);
		return super.runSplit(path, newNode, node, min, max); 
	}
	
	
	protected SplitInfo leafLevelSplit(Stack path, Node newNode, Node node, int min,
			int max){
		final int level = node.level(); 
		int rightLoadBound = max;
		
		
		// get Entries of the split triggered node 
		// get range from minLoad to average + (average-minload) or Nodesize - minload;
		//filter range
		int[] fromTo = this.filterFromTo(node.entries(), min, rightLoadBound, level == 0);
		//search within the range
		// take the first key fromTo[0] - 1 before the minLeft range 
		// 
		int splitIndex = (fromTo[0] > 0)? fromTo[0] - 1  : fromTo[0] ;
		
		int startIndex = splitIndex ;
		String prevKey = (level > 0 ) ?  (String)((IndexEntry)node.getEntry(startIndex)).separator().sepValue() :
			getKey(node.getEntry(startIndex));
		String shortestSeparatorVal = new String(prevKey);
		for (int i = startIndex+1; i < fromTo[1]; i++){
			String nextKey = (level > 0) ? (String)((IndexEntry)node.getEntry(i)).separator().sepValue() :
				getKey(node.getEntry(i));
//			if(nextKey.startsWith("name_1430"))
//				System.out.println("alarm");
			String shortestPrefix = shortestPrefix(prevKey, nextKey);
			if (shortestPrefix.length() < shortestSeparatorVal.length()){
				splitIndex = i;
				shortestSeparatorVal = shortestPrefix; 
			}
			prevKey = nextKey;
		}
		// bug fix 
//		SplitInfo info = newNode.new SplitInfo(path,  splitIndex+1);
		SplitInfo info = newNode.new SplitInfo(path,  splitIndex);
		// create Separatoren 
		Separator sepNewNode = (level > 0) ?  (Separator)((IndexEntry)node.getEntry(node.number()-1)).separator().clone():
				createSeparator(getKey(node.getEntry(node.number()-1)));
		Separator sepratorOfTriggeredNode  = createSeparator(shortestSeparatorVal);
		info.initialize( sepNewNode, sepratorOfTriggeredNode);
		return info;
	}
	
	/**
	 * Assumption key1 <= key 2
	 * @param key1
	 * @param key2
	 * @return
	 */
	private String shortestPrefix(String key1, String key2){
		// check 
		char[] keyArray1 = key1.toLowerCase().toCharArray();
		char[] keyArray2 = key2.toLowerCase().toCharArray();
		StringBuffer shortestString = new StringBuffer();
	
		// iterate
		for (int i = 0; i < Math.min(keyArray1.length, keyArray2.length); i++){
			// check character
			if (keyArray1[i] == keyArray2[i]){
				shortestString.append(key1.charAt(i));
			}else if (keyArray1[i] < keyArray2[i]){
				shortestString.append(key2.charAt(i));
				break;
			}
		}
		return shortestString.toString();
	}
	
	/**
	 * 
	 * @param entries
	 * @param leftRangeLoad
	 * @param rigthRangeLoad
	 * @return index array[2]
	 * array[0] => index of element in the range
	 * array[1] => bound index is not included in the range
	 */
	public int[] filterFromTo(Iterator entries, final int leftRangeLoad, final  int rigthRangeLoad, boolean isLeafNode){
		int load = 0;
		int nrToLeftBound = 0;
		int qualified = 0; 
		while(entries.hasNext()){
			Object  entry = entries.next();
			int size = 0;
			if (isLeafNode){
				size = this.getObjectSize(entry);
			}else{
				size = this.getSeparatorSize(((IndexEntry)entry).separator()) + this.containerIdSize;
			}
			if (load >= leftRangeLoad ){
				if ((load + size) <= rigthRangeLoad ) qualified++;
			}else nrToLeftBound++;
			load += size;
		}
		return new int[]{nrToLeftBound, qualified + nrToLeftBound};
	} 
	

}
