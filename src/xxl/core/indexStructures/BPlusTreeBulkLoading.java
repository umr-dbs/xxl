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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.predicates.Predicate;

/**
 * This class provides functionality to bulk-load a BPlusTree.
 * The tree is created bottom-up.  
 */
public class BPlusTreeBulkLoading {
    /**
     * Bulkloading
     *  
     */
    public static final boolean ASCENDING_BULKLOAD = false;
    public static final boolean DESCENDING_BULKLOAD = true;
    
    protected BPlusTree btree; 
    protected Function<BPlusTree.Node,Container> determineTreeContainer;
    protected Predicate treeOverflows;
    protected ArrayList<MapEntry<Object,BPlusTree.Node>> treePath = null; //main buffer
    protected ArrayList<MapEntry<Object,BPlusTree.Node>> bufferPath = null; //buffer for sibling nodes
    protected boolean descending;
   /**
    * Bulk loads the given <tt>tree</tt>
	* with the given <tt>objects</tt> in ascending order. 
    * @param tree 
    * @param objects
    */
    public BPlusTreeBulkLoading(BPlusTree tree, Iterator objects){
    	 this(tree, objects, tree.getContainer);
    }
    /**
     * Bulk loads the given <tt>tree</tt>
	 * with the given <tt>objects</tt> in ascending order.. 
	 * 
	 * @see BPlusTree#bulkLoad(BPlusTree, Iterator , Function&lt;BPlusTree.Node,Container&gt; , Predicate )
     * @param tree
     * @param objects
     * @param determineContainer
     */
    public BPlusTreeBulkLoading(BPlusTree tree, Iterator objects, Function<BPlusTree.Node,Container> determineContainer){
    	this(tree,  objects,  determineContainer,tree.overflows , BPlusTreeBulkLoading.ASCENDING_BULKLOAD);
    }
    
    /**
     * Bulk loads the given <tt>tree</tt>
 	* with the given <tt>objects</tt>. 
     * @param tree 
     * @param objects
     * @param order in which the data objects are sorted
     */
     public BPlusTreeBulkLoading(BPlusTree tree, Iterator objects, boolean order){
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
     public BPlusTreeBulkLoading(BPlusTree tree, Iterator objects, Function<BPlusTree.Node,Container> determineContainer, boolean order){
     	this(tree,  objects,  determineContainer, tree.overflows , order);
     }
     /**
      *  Bulk loads the given <tt>tree</tt>
      * with the given <tt>objects</tt> in ascending order 
      * 
      * @param tree
      * @param objects
      * @param determineContainer
      * @param overflows
      */
     public BPlusTreeBulkLoading(BPlusTree tree, Iterator objects, Function<BPlusTree.Node,Container> determineContainer, Predicate overflows){
    	 this(tree, objects,  determineContainer, BPlusTreeBulkLoading.ASCENDING_BULKLOAD  );
     }
    /**
     * Bulk loads the given <tt>tree</tt>
	 * with the given <tt>objects</tt>. 
     * Ensures that the right or left flank (depending on order)  does not contain less than D items
     * @see BPlusTree#adjustRightPath(Separator, int) 
     * @see BPlusTree#adjustLeftPath(Separator, int) 
     * @param tree
     * @param objects
     * @param determineContainer
     * @param overflows
     * @param order in which the data objects are sorted
     */
    public BPlusTreeBulkLoading(BPlusTree tree, Iterator objects, Function<BPlusTree.Node,Container> determineContainer, Predicate overflows, boolean order){
    	btree = tree;
    	this.determineTreeContainer =  determineContainer;
    	this.treeOverflows = overflows;
    	this.treePath = new ArrayList<MapEntry<Object,BPlusTree.Node>>();
    	this.bufferPath = new ArrayList<MapEntry<Object,BPlusTree.Node>>();
    	boolean isDuplicateEnabled = btree.duplicate;
    	Object first = null, last = null;
		
    	// new code 
    	this.descending = order;
    	
    	// insert all objects
		while (objects.hasNext()) {
			Object obj = objects.next();
			if (first == null) first = obj;
			last = obj;
			insertBulk(obj, 0);
		}		
		// at this point we have a getpath which contains the right flank.
		// write unsaved nodes
		for (int level = 0; level < treePath.size();) {
			BPlusTree.IndexEntry indexEntry = saveBulk(treePath.get(level).getKey(), 
					treePath.get(level).getValue(),  isDuplicateEnabled);

			level++;
			
			if (level < treePath.size())
				insertBulk(indexEntry, level);
			else {
				tree.rootEntry = indexEntry;
				tree.rootDescriptor = tree.createKeyRange(tree.key(first), tree.key(last));
			}
		}
		adjustFlankPath(null, 0);
		
    }
   /**
    * Method corrects left flank while bulk loading the data objects in descending order mode
    * and right flank in ascending mode
    * @param newSep
    * @param level
    */ 
   protected void adjustFlankPath(Separator newSep, int level){
	   Container container =  btree.container();
	   Node flankNode = (Node)treePath.get(level).getValue();
	   Object flankEntry = treePath.get(level).getKey();
	   int secondIndex = 1; // index of the second entry in the current node
	   int firstIndex = 0; // index of the first entry in the current node
	   if (level < bufferPath.size()){
		   Node sibling =  (Node)bufferPath.get(level).getValue(); 
		   Object siblingEntry  = bufferPath.get(level).getKey();
		   Separator newSeparator = null; // 
		   if (level > 0  && newSep != null) {//below root node and have created new Separator with redistribute 
    			if (descending){ // left flank
    				((IndexEntry)flankNode.getFirst()).separator = (Separator)newSep.clone();//father node has more than one entry so we must update last node instead of left sibling.	
    			}else{ // right flank
        				if (flankNode.number() > 1){//more than one item in right node, therfore update
        					((IndexEntry)flankNode.getEntry(flankNode.number()-2)).separator = (Separator)newSep.clone();//father node has more than one entry so we must update last node instead of left sibling.
        				}
        				else{
        					((IndexEntry)sibling.getLast()).separator = (Separator)newSep.clone();
        				} 			
    			} // end adjust separator
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
   			//update nodes
   			container.update(flankEntry, flankNode);
   			container.update(siblingEntry, sibling);
			}// end redistribute
			//shared parent node without underflow
			if (newSeparator == null && newSep != null ){
					container.update(flankEntry, flankNode);
			}
			//Recursion
			adjustFlankPath(newSeparator, level + 1);
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
     * Inserts an entry into the given level.
     * @param entry
     * @param level
     */
    protected  void insertBulk(Object entry, int level){
    	if (treePath.size() <= level) {
			BPlusTree.Node newNode = (BPlusTree.Node)btree.createNode(level);
			Object newId = determineTreeContainer.invoke(newNode).reserve(new Constant(newNode));

			treePath.add(new MapEntry<Object,BPlusTree.Node>(newId, newNode));
		}

		BPlusTree.Node node = treePath.get(level).getValue();
		Object id = treePath.get(level).getKey();
		
		insertIntoNode(node, entry );

		if (treeOverflows.invoke(node)) {
			BPlusTree.Node newNode = (BPlusTree.Node)btree.createNode(node.level);
			Object newId = determineTreeContainer.invoke(newNode).reserve(new Constant(newNode));
			insertIntoNode(newNode, entry);
			treePath.set(level, new MapEntry<Object,BPlusTree.Node>(newId, newNode));

			Iterator entries;
			for (entries = node.entries(); !entries.next().equals(entry); );
			entries.remove();
			if (descending){
				newNode.nextNeighbor = (BPlusTree.IndexEntry)btree.createIndexEntry(level+1);
				newNode.nextNeighbor.initialize(id);	
			}
			else{
				node.nextNeighbor = (BPlusTree.IndexEntry)btree.createIndexEntry(level+1);
				node.nextNeighbor.initialize(newId);
			}
			if (bufferPath.size() <= level){
				bufferPath.add(new MapEntry<Object,BPlusTree.Node>(id, node)); 	
				}
			else{
				bufferPath.set(level, new MapEntry<Object,BPlusTree.Node>(id, node)); 	
			}
			insertBulk(saveBulk(id, node, btree.isDuplicatesEnabled()), level+1);
		}
    }
    
    /**
     * Inserts entry depending on the order 
     * @param node
     * @param entry
     */
    protected  void insertIntoNode(BPlusTree.Node node, Object entry ){
    	if (descending){
			node.entries.add(0,entry);
		}
		else{
			node.entries.add(entry);
		}
    }
    /**
     * Saves a node of the tree to external memory.
     * @param id
     * @param node
     * @param isDuplicateEnabled
     * @return
     */
    protected  BPlusTree.IndexEntry saveBulk (Object id, BPlusTree.Node node, boolean isDuplicateEnabled){
    	Container container = determineTreeContainer.invoke(node);
		container.update(id, node);
		Separator sep = (Separator) btree.separator(node.getLast()).clone();
		return (BPlusTree.IndexEntry)((BPlusTree.IndexEntry)btree.createIndexEntry(node.level+1)).initialize(sep).initialize(container, id);
    }
	
}
