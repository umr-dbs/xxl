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
package xxl.core.indexStructures.vLengthBPlusTree.underflowHandlers;

import java.util.List;

import xxl.core.indexStructures.vLengthBPlusTree.UnderflowHandler;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.IndexEntry;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.Node.MergeInfo;
/**
 * This class implements a classic underflow handler for B+Trees. Underflows are handled immediately after underflow was detected.
 * The handler looks at neighbor node and decides on the capacity of the neighbor whether borrowing or merging to apply.   
 * 
 */
public class StandardUnderflowHandler implements UnderflowHandler {

	@Override
	public MergeInfo runUnderflowHandling(MergeInfo mergeInfo, IndexEntry indexEntry, Node node,
			IndexEntry siblingIndexEntry, Node siblingNode, int min,
			int average, boolean left) {
		int myUnderflowLoad = node.getCurrentLoad();
		int effectivLoad = min + ((average - min) / 2);
		if (left){
			int leftLoad = siblingNode.getCurrentLoad();
        	// check actual load of left sibling
        	if (leftLoad > average ){
        		// can be redistributed 
        		// start searching the possible new key so that new key <= old key
        		// arg min
        		int minKeySize = node.getKeyByteSize(siblingIndexEntry);
        		int actualSiblingLoad = leftLoad;
        		int indexOfNewKey = siblingNode.number()-1 ;
        		for (int i = siblingNode.number()-1; i > 0; i--){
        			int entryWeight = siblingNode.getEntryByteSize(siblingNode.getEntry(i), siblingNode.level());
//        			int entryKey = entryWeight + containerIdSize; 
        			// check key size old vs. new 
        			if ( myUnderflowLoad < effectivLoad ){
        				myUnderflowLoad +=entryWeight;
            			actualSiblingLoad -=entryWeight;
            			if (i > 0){
            				int entryKey = siblingNode.getKeyByteSize(siblingNode.getEntry(i-1)) ; 
                			if ( entryKey < minKeySize ){
                				indexOfNewKey = i-1;
                				minKeySize = entryKey; 
                			}
            			}
        			}
        			else break;
        		}
        		// Check if found 
        		if (indexOfNewKey < siblingNode.number()-1 ){
        			// found perfrom real distribution 
        			List newEntries = siblingNode.getEntries().subList(indexOfNewKey+1, siblingNode.number());
                    node.getEntries().addAll(0, newEntries);
                    newEntries.clear(); 
                    siblingNode.setByteLoad( siblingNode.computeActualLoad());
                    node.setByteLoad(node.computeActualLoad());
//                    mergeInfo.initialize(siblingIndexEntry, siblingNode, VariableLengthBPlusTree.MergeState.DISTRIBUTION_LEFT);
                    mergeInfo.initialize(siblingIndexEntry, siblingNode, VariableLengthBPlusTree.DISTRIBUTION_LEFT);
        		}else{
        			mergeInfo.initialize(siblingIndexEntry, siblingNode, VariableLengthBPlusTree.POSTPONED_MERGE);
        		}
        	}else{
        		siblingNode.getEntries().addAll(siblingNode.number(), node.getEntries());
        		siblingNode.nextNeighbor = node.nextNeighbor(); //
        		siblingNode.setByteLoad( siblingNode.computeActualLoad());
        		mergeInfo.initialize(siblingIndexEntry, siblingNode, VariableLengthBPlusTree.MERGE);
        	}
		}else{
			int rightLoad = siblingNode.getCurrentLoad();
        	if (rightLoad >  average){
        		// can be redistributed 
        		// start searching the possible new key so that new key <= old key
        		// arg min
        		int minKeySize = node.getKeyByteSize(indexEntry);
        		int actualSiblingLoad = rightLoad;
        		int indexOfNewKey = 0 ;
        		for (int i = 0; i < siblingNode.number(); i++){
        			int entryWeight = node.getEntryByteSize(siblingNode.getEntry(i), siblingNode.level());
        			int entryKey = node.getKeyByteSize(siblingNode.getEntry(i)); 
        			// check key size old vs. new 
        			if ( myUnderflowLoad < effectivLoad ){
        				myUnderflowLoad +=entryWeight;
            			actualSiblingLoad -=entryWeight;
            			if (entryKey < minKeySize ){
            				indexOfNewKey = i;
            				minKeySize = entryKey; 
            			}
        			}
        			else break;
        		}
        		// Check if found 
        		if (indexOfNewKey > 0 ){
        			// found perfrom real distribution 
        			List newEntries = siblingNode.getEntries().subList(0, indexOfNewKey+1);
           		 	node.getEntries().addAll(node.number(), newEntries);
                    newEntries.clear();
                    node.setByteLoad(node.computeActualLoad());
                    siblingNode.setByteLoad(siblingNode.computeActualLoad()); 
                    mergeInfo.initialize(siblingIndexEntry, siblingNode, VariableLengthBPlusTree.DISTRIBUTION_RIGHT);
        		}else{
        			mergeInfo.initialize(siblingIndexEntry,siblingNode, VariableLengthBPlusTree.POSTPONED_MERGE);
        		}
        	}else{
        		node.getEntries().addAll(node.number(), siblingNode.getEntries());
                node.nextNeighbor = siblingNode.nextNeighbor();
                node.setByteLoad(node.computeActualLoad());
                mergeInfo.initialize(siblingIndexEntry, siblingNode, VariableLengthBPlusTree.MERGE);
        	}
		}
		return mergeInfo;
	}

}
