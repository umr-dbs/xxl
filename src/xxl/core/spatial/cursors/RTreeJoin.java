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
package xxl.core.spatial.cursors;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;

import xxl.core.collections.Lists;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.indexStructures.ORTree.Node;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.KPE;
import xxl.core.spatial.rectangles.Rectangle;


/** This is one implementation of the R-Tree-Join-Algorithms proposed in [BKS 93]
 *	(see "[BKS 93] Efficient Processing of Spatial Joins by Thomas Brinkhoff,
 *  Hans-Peter Kriegel and Bernhard Seeger, ACM SIGMOD 1993." for a
 *	detailed description of this method).
 *	<br> 
 *  It differs only slightly from the the third proposed Algorithm (SJ3) in this paper:
 *  SJ3 is defined in a recursive manner, which wasn't suitable for a lazy-implementation
 *  of the operator. A stack is used to emulate the recursive structure of the algorithm 
 *  instead.
 *  <br><br> 
 *  The main idea of the algorithm is to traverse both R-Trees top-down simultaneously.
 *  For each pair of nodes the next pair to visit is computed by performing a main-memory 
 *  plane-sweep join on the descriptors of the nodes' indexentries. The nodes of the entries 
 *  which do not overlap don't have to be visited. At each level the pairs of nodes to visit 
 *  next are put on a stack, before the top-element of that stack is visited. This emulates
 *  a depth-first traversal of the tree. The use of one LRU-Buffer for each tree reduces 
 *  the I/O-Rate on secondary memory: Nodes already visited are very likely to be visited 
 *  again, so the buffer should keep them in memory as long as possible.
 *  <br>
 *  The results of the sweeping-based join on the leaf-entries ({@link KPE}-objects)
 *  are reported as results of this operator.
 *  <br>
 *  In case both trees are of different height the algorithm performs for each leaf-entry 
 *  of the flat tree a window query on the remaining nodes of the other one. 
 *
 */
public class RTreeJoin extends AbstractCursor<KPE[]>{

	/** The R-Trees to be joined */
	protected RTree rTree,sTree;
	
	/** This Stack is needed to emulate the original recursive implementation of the join. */
	protected Stack<Object[]> candidates;
	
	/** Used to collect the join-results from the different stages of processing */
	protected Iterator<Object[]> results;	
	
	/** */
	protected KPE[] nextObject;
	
	/** If the heights of the input-trees don't equal the inputs might be swapped so that the
	 *  first is the higher one. Therefore the inputs might have to be swapped as well as indicated
	 *  by this variable.
	 */ 
	protected boolean swapResults;								

	/** A function to return the descriptor of a given {@link IndexEntry}. 
	 */
	protected Function<IndexEntry, Rectangle> getEntryRectangle = new AbstractFunction<IndexEntry, Rectangle>(){
		public Rectangle invoke(IndexEntry e){
				return (Rectangle) e.descriptor();
		}
	};
	
	/** A function to return the descriptor of {@link KPE}-objects, the data-objects
	 *  in the R-Tree's leafs.
	 */
	protected Function<KPE, Rectangle> getLeafRectangle= new AbstractFunction<KPE, Rectangle>(){
			public Rectangle invoke(KPE k){
				return (Rectangle) k.getData();
			}
		};		
		
		
	/** The only  Constructor of the R-Tree-Join, which requires as input the two 
	 *  {@link RTree}-objects to be joined.
	 * 
	 * @param r the first R-Tree to be processed in the join
	 * @param s the second one
	 */
	public RTreeJoin( RTree r, RTree s ){
		// let r be the highest of both trees 
		if(r.height() < s.height()){
			this.rTree = s;
			this.sTree = r;
			swapResults = true;
		} else {
			this.rTree = r;
			this.sTree = s;
			swapResults = false;
		}
		
		results = new EmptyCursor<Object[]>();
		candidates = new Stack<Object[]>();
		candidates.push( new Object[]{ 	
							(IndexEntry) rTree.rootEntry(),
							(IndexEntry) sTree.rootEntry()
						});
	}
												

	/** The internal Plane-Sweep method as proposed in [BKS 93]. The method returns the pairs
	 *  of index-entries/leaf-entries whose descriptors overlap. The join is processed in main memory
	 *  by sorting the descriptors according to one dimension and determining overlapping pairs with
	 *  a scan-line-algorithm. The join-complexity is further reduced by regarding only those 
	 *  index-entries/leaf-entries whose descriptors also overlap the given query-rectangle, 
	 *  which is assumed to be the intersection of the nodes' descriptors.
	 * 
	 * @param <I> indicates the type of entries (leaf- or index-entries) to join.
	 * @param rect the intersection of the nodes' descriptors which prunes the number of entries regarded in the join
	 * @param n0 the first node whose entries are to be joined
	 * @param n1 the second node whose entries are to be joined
	 * @param getRectangle a function to return the descriptor of the given entry
	 * @return the pairs of entries, whose descriptors overlap
	 */
	protected <I extends Object> LinkedList<Object[]> internalPlaneSweep(Rectangle rect, Node n0, Node n1, final Function< I, Rectangle> getRectangle){
		// retrieve those entries which overlap the given rectangle
		List rSeq = new LinkedList();							
		Iterator it = n0.entries();
		while(it.hasNext()){
			Object e = it.next();
			if(getRectangle.invoke((I)e).overlaps(rect))								
				rSeq.add(e);
		}
		
		// retrieve those entries which overlap the given rectangle
		List sSeq = new LinkedList();
		it = n1.entries();
		while(it.hasNext()){
			Object e = it.next();
			if(getRectangle.invoke((I)e).overlaps(rect))								
				sSeq.add(e);
		}	

		// sort the inputs according to the x-axis
		LinkedList<Object[]> output = new LinkedList<Object[]>();

		Comparator comparator = new Comparator(){
			public int compare(Object o1, Object o2) {
				Double x1 = getRectangle.invoke((I)o1).getCorner(false).getValue(0);
				Double x2 = getRectangle.invoke((I)o2).getCorner(false).getValue(0);				
				return x1< x2 ? -1 : x1 == x2 ? 0 : 1; 
			}			
		};

		Lists.quickSort(rSeq, comparator);
		Lists.quickSort(sSeq, comparator);
		
		// find the next query-object in x-direction and perform
		// retrieve the overlapping elements from the other sequence
		int i=0, j=0;
		while(i < rSeq.size() && j < sSeq.size()){
			if( comparator.compare( rSeq.get(i), sSeq.get(j) ) <= 0){
				internalLoop(rSeq.get(i), j, sSeq, output, getRectangle, false);
				i++;
			} else {
				internalLoop(sSeq.get(j), i, rSeq, output, getRectangle, true);
				j++;
			}					
		}
		return output;
	}
	
	
	/** Perfoms the scan-line-search for overlapping Elements: all elements of the input-sequence
	 *  which intersect the query-object on the x-axis are checked for intersection on the y-axis.
	 *  
	 * @param queryObject the current query object
	 * @param unmarked the first element of the sequence, which intersects the query-object on the first dimension
	 * @param seq the sequence of elements which are checked for intersection with the query-object 
	 * @param output a list of intersecting elemet-tuples
	 * @param getRectangle a function to return the descriptor of the sequence's elements
	 * @param swap determines whether to swap the elements of the result-tuples.
	 */
	protected <I extends Object> void internalLoop(Object queryObject, int unmarked, List seq, List output, Function<I, Rectangle> getRectangle, boolean swap){
		int k=unmarked;
		Rectangle t = getRectangle.invoke((I)queryObject);		 
		
		while(k<seq.size()){
			Rectangle s = getRectangle.invoke((I)seq.get(k));
			
			if( s.getCorner(false).getValue(0) > t.getCorner(true).getValue(0) ) break;
					
			if( t.getCorner(false).getValue(1) <= s.getCorner(true).getValue(1) 
			 && t.getCorner(true).getValue(1) >= s.getCorner(false).getValue(1))
				output.add( swap ? new Object[]{seq.get(k),queryObject} 
								 : new Object[]{queryObject, seq.get(k)}
					);
			
			k++;
		}					
	}
											
	/** Trees of different heights are traversed in parallel until the leaf-level of one tree is reached.
	 *  In this method the remaining nodes of the higher tree are queried with the leaf-entries of the 
	 *  other ones. The queries are not run one after the other, which would cause the remaining tree-nodes
	 *  to be loaded and released over and over again, but in parallel, that means the algorithm keeps the 
	 *  leaf node in memory and continously loads the remaining nodes of the other tree, computes intersection 
	 *  entries which are enqueued in a queue until the leaf-level of the other tree is reached, so the algorithm
	 *  performs a breadth-first-traversal of the remaining nodes.  
	 * 
	 * @param subTreeRoot the IndexEntry of the higher tree whose remaining nodes are to be queried
	 * @param queryObject the IndexEntry whose connected Node is a leaf
	 * @return the leaf-entry-tuples whose descriptors overlap
	 */
	protected Iterator<Object[]> windowQuery(final IndexEntry subTreeRoot, final IndexEntry queryObject){
		
		return new AbstractCursor<Object[]>(){
						
			Node queryNode;					
			LinkedList<IndexEntry> indexEntries;					
			LinkedList<Object[]> results;												
					
			@Override
			public void open(){
				super.open();
				queryNode = (Node) queryObject.get();					
				indexEntries = new LinkedList<IndexEntry>();
				results = new LinkedList<Object[]>();							
				indexEntries.add(subTreeRoot);													
			}
			
			@Override
			protected boolean hasNextObject() {									
				while(results.isEmpty() && !indexEntries.isEmpty()){
					IndexEntry entry = indexEntries.poll();								
					Node n0 = (Node) entry.get();								
					final Rectangle rect = (Rectangle) (entry.descriptor().clone()); 
					rect.intersect((Rectangle)(queryObject.descriptor()));	
			
					if(entry.level() > 0){
						Iterator<Descriptor> dit = new Filter<Descriptor>(
															queryNode.descriptors(null),
															new AbstractPredicate<Descriptor>(){
																public boolean invoke(Descriptor d){
																	return d.overlaps(rect);
																}
															}
														);
						while(dit.hasNext()){
							Iterator<IndexEntry> it = n0.query(dit.next());
							while(it.hasNext()){
								IndexEntry next = it.next();
								if(!indexEntries.contains(next)) // optimierbar?
									indexEntries.addLast(next);		
							}
						}																			
					} else 
						results = internalPlaneSweep(rect, n0, queryNode, getLeafRectangle);																	
				}							
				return !results.isEmpty();
			}

			@Override
			protected Object[] nextObject() {							
				return results.poll();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	
	
	@Override
	protected boolean hasNextObject() {
		nextObject = null;
		while(!results.hasNext() && !candidates.isEmpty()){						
			Object[] entries = candidates.pop();
			IndexEntry e0 = (IndexEntry) entries[0];
			IndexEntry e1 = (IndexEntry) entries[1];
															
			if(e0.level() == e1.level() || (e0.level()> e1.level() && e1.level()>0)){
				final Rectangle rect = (Rectangle) (e0.descriptor().clone()); 
				rect.intersect((Rectangle)(e1.descriptor()));	
		
				if(e0.level() > 0)														
					candidates.addAll( 	internalPlaneSweep(
											rect, 
											(Node)e0.get(true), 
											(Node)e1.get(true), 
											getEntryRectangle											
											)
										);																
					else 
					results = internalPlaneSweep(
									rect, 
									(Node)e0.get(true), 
									(Node)e1.get(true), 
									getLeafRectangle
								).iterator();															
			} else results = windowQuery(e0, e1);													
		}
		try{						
			Object[] tmp = results.next(); 						
			nextObject = swapResults ? new KPE[]{(KPE) tmp[1],(KPE) tmp[0]} 
									 : new KPE[]{(KPE) tmp[0],(KPE) tmp[1]};
			return true;
		}catch(NoSuchElementException e){
			return false;
		}
	}

	@Override
	protected KPE[] nextObject() {
		return nextObject;
	}

	/** This method is not supported */
	public void remove() {
		throw new UnsupportedOperationException();
	}	
}

