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

import java.util.Stack;

import xxl.core.indexStructures.Separator;
import xxl.core.indexStructures.vLengthBPlusTree.SplitStrategy;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.IndexEntry;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node.SplitInfo;

/**
 * Implements a shortest key strategy for the split. 
 * It searches in the bounds [min, max] which defined by the minimal allowed byte capacity of the node for the shortest key (in bytes).   
 * 
 * @param <D>
 * @param <K>
 */
public class ShortestKeyStrategy<D,K> extends SplitStrategy<D,K> {
	
	
	public ShortestKeyStrategy(){
		super();
	} 
	
	
	@Override
	public SplitInfo runSplit(Stack path, Node newNode, Node node, int min,
			int max) {
		// search in the range from int min to int max bytes for the smallest key
		int nodeLevel = node.getLevel();
		// go from left to rigth
		int startIndex = 0;
		int load = 0;
		int minSizeEntry = min;
	
		for (int i = 0; i < node.number() && load < max; i++ ){
			int entrySize = node.getEntryByteSize(node.getEntry(i), nodeLevel);
			// go to right
			load += entrySize;
			if (load >= min){ 
				// serach for min
				if (entrySize < minSizeEntry){
					minSizeEntry = entrySize; // actual min
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
			sepNewNode =   createSeparator(getKey((D) node.getEntry(node.number()-1)));
			sepratorOfTriggeredNode = createSeparator(getKey((D) node.getEntry(startIndex)));
		}
		
		info.initialize( sepNewNode, sepratorOfTriggeredNode);
		return info;
	}
	
}
