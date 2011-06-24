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

package xxl.core.indexStructures;

import java.util.Iterator;
import java.util.List;

import xxl.core.collections.containers.Container;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.rectangles.Rectangle;
/**
 * This class provides functionality to bulk-load a <tt>HilbertRTree</tt>.
 * The tree is created bottom-up.  
 */
public class HilbertRTreeBulkLoading extends BPlusTreeBulkLoading {
	
	 /**
     * Bulk loads the given <tt>tree</tt>
 	* with the given <tt>objects</tt>. 
     * @param tree 
     * @param objects
     * @param order in which the data objects are sorted
     */
     public HilbertRTreeBulkLoading(HilbertRTree tree, Iterator objects){
     	 this( tree, objects, tree.getContainer, HilbertRTreeBulkLoading.ASCENDING_BULKLOAD);
     }
	
	
	 /**
     * Bulk loads the given <tt>tree</tt>
 	* with the given <tt>objects</tt>. 
     * @param tree 
     * @param objects
     * @param order in which the data objects are sorted
     */
     public HilbertRTreeBulkLoading(HilbertRTree tree, Iterator objects, boolean order){
     	 this( tree, objects, tree.getContainer, order);
     }
     /**
      * Bulk loads the given <tt>tree</tt>
 	 * with the given <tt>objects</tt>. 
 	 * 
 	 * @see BPlusTree#bulkLoad(BPlusTree, Iterator , Function&lt;BPlusTree.Node,Container&gt; , Predicate )
      * @param tree
      * @param objects
      * @param determineContainer
      * @param order in which the data objects are sorted
      */
     public HilbertRTreeBulkLoading(HilbertRTree tree, Iterator objects, Function<BPlusTree.Node,Container> determineContainer, boolean order){
     	this(tree,  objects,  determineContainer, tree.overflows , order);
     }
	
	public HilbertRTreeBulkLoading(HilbertRTree tree, Iterator objects, Function<Node, Container> determineContainer, Predicate overflows, boolean order) {
		super(tree, objects, determineContainer, overflows, order);
		Node node = (Node)btree.rootEntry.get();
		Rectangle rootsMBR =  ((HilbertRTree)btree).computeMBR(node.entries());
		((HilbertRTree.ORKeyRange)btree.rootDescriptor).entryMBR = rootsMBR;
	}
	
	/**
	 * Method corrects left flank while bulk loading the data objects in descending order mode
	 * and rigth flank in ascending mode
	 * @param newSep
	 * @param level
	 */ 
	   protected void adjustFlankPath(Separator newSep, int level){
		   adjustFlankPath(newSep,  level, null, null);
	   }
	   
	   
	   protected void adjustFlankPath(Separator newSep, int level, Rectangle flankNodeMBR, Rectangle siblingMBR){
		   Container container =  btree.container();
		   Node flankNode = (Node)treePath.get(level).getValue();
		   Object flankEntry = treePath.get(level).getKey();
		   int secondIndex = 1; // index of the second entry in the current node
		   int firstIndex = 0; // index of the first entry in the current node
		   if (level < bufferPath.size()){
			   Node sibling =  (Node)bufferPath.get(level).getValue(); 
			   Object siblingEntry  = bufferPath.get(level).getKey();
			   Separator newSeparator = null; // 
			   Rectangle nodeNewMBR = null;
  				Rectangle siblingNewMBR = null;	   	
			   if (level > 0  && newSep != null) {//below root node and have created new Separator with redistribute 
	    			if (descending){ // left flank	
	    					((HilbertRTree.ORSeparator)((IndexEntry)flankNode.getFirst()).separator).updateSepValue(newSep.sepValue) ;
	    					((HilbertRTree.ORSeparator)((IndexEntry)flankNode.getFirst()).separator).updateMBR(flankNodeMBR);
	    					if (flankNode.number() > 1)	    					
	    						((HilbertRTree.ORSeparator)((IndexEntry)flankNode.getEntry(secondIndex)).separator).updateMBR(siblingMBR);
	    					else
	    						((HilbertRTree.ORSeparator)((IndexEntry)sibling.getFirst()).separator).updateMBR(siblingMBR);
	    			}else{ // right flank
	        				if (flankNode.number() > 1){//more than one item in right node, therefore update
	        					((HilbertRTree.ORSeparator)((IndexEntry)flankNode.getEntry(flankNode.number()-2)).separator).updateSepValue(newSep.sepValue);
	        					((HilbertRTree.ORSeparator)((IndexEntry)flankNode.getEntry(flankNode.number()-2)).separator).updateMBR(siblingMBR);//father node has more than one entry so we must update last node instead of left sibling.
	        				}
	        				else{
	        					((HilbertRTree.ORSeparator)((IndexEntry)sibling.getLast()).separator).updateSepValue(newSep.sepValue());
	        					((HilbertRTree.ORSeparator)((IndexEntry)sibling.getLast()).separator).updateMBR(siblingMBR);
	        				}
	        				((HilbertRTree.ORSeparator)((IndexEntry)flankNode.getLast()).separator).updateMBR(flankNodeMBR);
	        			
	    			} // end adjust sep
	    		}
			   //redistribute
			   if (flankNode.underflows()){
	   			int D = sibling.level() == 0 ? btree.D_LeafNode : btree.D_IndexNode; //
	   			if (descending){
	   				List newEntries = sibling.entries.subList(firstIndex, D); //
	       			flankNode.entries.addAll(flankNode.number(), newEntries);//
	       			newEntries.clear();
	       			newSeparator =  (Separator)btree.separator(flankNode.getLast()).clone();
	   			}else{
	       			List newEntries = sibling.entries.subList(D, sibling.number()); //
	       			flankNode.entries.addAll(0, newEntries);//
	       			newEntries.clear();
	       			newSeparator = (Separator)btree.separator(sibling.getLast()).clone();
	   			}
	   			// compute MBRs
	   				nodeNewMBR = ((HilbertRTree)btree).computeMBR(flankNode.entries());
	   				siblingNewMBR = ((HilbertRTree)btree).computeMBR(sibling.entries());	   			
	   			//update nodes
	   				container.update(flankEntry, flankNode);
	   				container.update(siblingEntry, sibling);
				}// end redistribute
				//shared parent node without underflow
				if (newSeparator == null && newSep != null ){
						container.update(flankEntry, flankNode);
				}
				//Rekursion
				//TODO
				adjustFlankPath(newSeparator,  level+1, nodeNewMBR, siblingNewMBR);
		   }else{
			   if ( newSep != null ){
				   if (descending){   
		    			((IndexEntry)flankNode.getFirst()).separator = (Separator)newSep.clone();	
				   }else{     			
			   			((IndexEntry)flankNode.getEntry(flankNode.number()-2)).separator = (Separator)newSep.clone();			   			
					  }
				   container.update(flankEntry, flankNode);
			   }
		   }
	   }
	   
	   
	   
	    /**
	     * Saves a node of the tree to external memory. 
	     * (Computes MBR of the Node)
	     * @param id
	     * @param node
	     * @param isDuplicateEnabled
	     * @return
	     */
	    protected  BPlusTree.IndexEntry saveBulk (Object id, BPlusTree.Node node, boolean isDuplicateEnabled){
	    	Container container = determineTreeContainer.invoke(node);
			container.update(id, node);
			Separator sep = (Separator) btree.separator(node.getLast()).clone();
			Rectangle nodesMBR = ((HilbertRTree)btree).computeMBR(node.entries());
			((HilbertRTree.ORSeparator)sep).updateMBR(nodesMBR); 
			return (BPlusTree.IndexEntry)((BPlusTree.IndexEntry)btree.createIndexEntry(node.level+1)).initialize(sep).initialize(container, id);
	    }
}
