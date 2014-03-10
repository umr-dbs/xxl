package xxl.core.spatial.spatialBPlusTree.cursors;

import java.util.Arrays;
import java.util.Iterator;

import xxl.core.collections.MapEntry;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.Tree.IndexEntry;
import xxl.core.indexStructures.Tree.Node;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.rectangles.DoublePointRectangle;
/**
 * 
 * Iterative Cursor which relies on the implementation of the nextInBox Function. 
 * Assumption key are long values; Coordinates are positive integers. 
 *  
 *
 */
public class SpatialRangeQueryBPlusCursorVL extends AbstractCursor{
	
	protected final int leafNodeLevel = 0;
	protected SFCFunctionSpatialCursor<Long> sfcFunction;
	@SuppressWarnings("unchecked")
	protected Function createKeyRange;
	@SuppressWarnings("unchecked")
	protected Predicate filterLeafNode;
	protected Long treeMaxBound;
	protected Long queryMaxKey;
	protected Long nextMinKey;	
	protected DoublePointRectangle queryBox;
	protected VariableLengthBPlusTree tree;
	@SuppressWarnings("unchecked")
	protected Iterator[] duplicateValueIterators; 
	@SuppressWarnings("unchecked")
	protected MapEntry[] path;
	protected int[] lp;
	protected int[] rp;
	
	/*
	 * test variable to count number of leafs touched during querying
	 */
	public int leafCounter = 0;
	
	/**
	 * 
	 * @param tree which is build with long keys;
	 * @param queryRegion
	 * @param sfcFunction
	 * @param createKeyRange
	 * @param targetLevel
	 */
	@SuppressWarnings("unchecked")
	public SpatialRangeQueryBPlusCursorVL(VariableLengthBPlusTree tree,	int[] lPoint, int[] rPoint, 
			SFCFunctionSpatialCursor<Long> sfcFunction,	
			Function createKeyRange, 
			Predicate filterLeafNode){
		this.tree = tree;
		this.sfcFunction = sfcFunction;
		this.createKeyRange = createKeyRange;
		this.filterLeafNode = filterLeafNode;
		duplicateValueIterators = new Iterator[tree.height()+1];
		path = new MapEntry[tree.height()+1];
		// compute querymaxkey queryminkey
		lp = lPoint;
		rp = rPoint;
		nextMinKey = sfcFunction.getMaxKeyInBox(lp,rp, false);
		queryMaxKey = sfcFunction.getMaxKeyInBox(lp,rp, true);
		treeMaxBound = (Long) ((KeyRange)tree.rootDescriptor()).maxBound();
		Arrays.fill(duplicateValueIterators, EmptyCursor.DEFAULT_INSTANCE);
		KeyRange descriptor = (KeyRange)createKeyRange.invoke(nextMinKey, queryMaxKey);
		if (descriptor.overlaps(tree.rootDescriptor()) ){
			duplicateValueIterators[tree.height()] = new SingleObjectCursor(tree.rootEntry());
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	protected boolean hasNextObject() {
		for(int level = leafNodeLevel;;){
			if (duplicateValueIterators[level].hasNext()){ 
				if (level == leafNodeLevel)
					return true;
				else{ // kern
					IndexEntry indexEntry = (IndexEntry)duplicateValueIterators[level].next();
					Node node = (Node)indexEntry.get(true);
					level = node.level();
					path[level] =  new MapEntry(indexEntry, node); // push in Stack
					if (level == leafNodeLevel){
//						System.out.println(path[level].getKey());
//						System.out.println(path[level].getValue());
						leafCounter++;
						duplicateValueIterators[level] = new Filter( node.entries(), filterLeafNode);
					}else{
						KeyRange descriptor = (KeyRange)createKeyRange.invoke(nextMinKey, nextMinKey);
						duplicateValueIterators[level] = (duplicateValueIterators[level].hasNext() ) ? 
								new Sequentializer(node.query(descriptor),
										duplicateValueIterators[level]):node.query(descriptor);
					}
				}
			}
			else{ // iterator empty 
				if (level==tree.height())
					return false;
				else{
					duplicateValueIterators[level++] = EmptyCursor.DEFAULT_INSTANCE;
					if (level > leafNodeLevel && path[level]!= null){
//						Comparable nextmatch =((BPlusTree.IndexEntry)path[level-1].getKey()).separator().sepValue();
////						System.out.println("nextMatch <:" +  nextmatch );
//						if (nextmatch.compareTo(queryMaxKey) > 0)		
//							return false;
						// search new input point in rectangle 
						// at this step level+1 is Iterator must be empty that means that the duplicates are completed 
						if (level == (leafNodeLevel+1) &&  !duplicateValueIterators[level].hasNext()){
							Long nextmatch =(Long) ((VariableLengthBPlusTree.IndexEntry)path[level-1].getKey()).separator().sepValue();
							if (nextmatch.compareTo(queryMaxKey) > 0)		
								return false;
							// search for next point in box
							nextMinKey =  sfcFunction.getNextPointInBox(lp, rp, nextmatch, true);
							nextMinKey = ( (nextMinKey.compareTo(nextmatch)) > 0) ? nextMinKey : sfcFunction.getSuccessor(nextmatch);
							if (nextMinKey.compareTo(queryMaxKey) > 0)  return false;
						}
//						Long lastKeyInNode = (level == tree.height()-1) ? 
//							treeMaxBound : (Long)((BPlusTree.IndexEntry)path[level].getKey()).separator().sepValue();
						Long lastKeyInNode = (Long)((VariableLengthBPlusTree.IndexEntry)path[level].getKey()).separator().sepValue();
						if (lastKeyInNode.compareTo(nextMinKey) < 0  ){
							duplicateValueIterators[level] =  EmptyCursor.DEFAULT_INSTANCE;
						}else{
							Node node = (Node)path[level].getValue();
							KeyRange descriptor = (KeyRange)createKeyRange.invoke(nextMinKey, nextMinKey);
							duplicateValueIterators[level] = node.query(descriptor);
						}	
					}
				}
			}
		}
	}
	/**
	 * 
	 */
	protected Object nextObject() {
		return duplicateValueIterators[leafNodeLevel].next();
	}
	
}
