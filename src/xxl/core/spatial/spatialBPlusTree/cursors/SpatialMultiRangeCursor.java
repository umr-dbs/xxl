package xxl.core.spatial.spatialBPlusTree.cursors;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;







import xxl.core.collections.MapEntry;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.Tree.IndexEntry;
import xxl.core.indexStructures.Tree.Node;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.spatialBPlusTree.AdaptiveZCurveMapper.SpatialZQueryRange;
import xxl.core.spatial.spatialBPlusTree.separators.LongKeyRange;
@SuppressWarnings("rawtypes")
public class SpatialMultiRangeCursor extends AbstractCursor{
	/**
	 * ranges sorted by the start point
	 */
	protected List<SpatialZQueryRange> ranges; 
	/**
	 * BPlusTree to query
	 */
	protected BPlusTree tree; 
	/**
	 * query rectangle 
	 */
	protected DoublePointRectangle query; 
	/**
	 * function get descriptor
	 */
	protected UnaryFunction<Object, DoublePointRectangle> toRectangle; 
	/**
	 * 
	 */
	protected Function createKeyRange;
	/**
	 * 
	 */
	protected Iterator[] duplicateValueIterators; 
	/**
	 * 
	 */
	protected MapEntry[] path;
	/**
	 * 
	 */
	protected final Iterator<SpatialZQueryRange> rangesIterator; 
	/**
	 * 
	 */
	private final int leafNodeLevel = 0; 
	/**
	 * 
	 */
	protected Predicate filterLeafNode; 
	/**
	 * Leaf counter for tests
	 */
	public int leafCounter = 0; 
	/**
	 * 
	 */
	protected long nextMinKey = 0; 
	/**
	 * 
	 */
	protected long queryMaxKey = 0; 
	
	protected boolean nextPage = false; 
	/**
	 * 
	 * @param ranges
	 * @param tree
	 * @param query
	 * @param toRectangle
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	public SpatialMultiRangeCursor(List<SpatialZQueryRange> ranges,
			BPlusTree tree, final DoublePointRectangle query,
			final UnaryFunction<Object, DoublePointRectangle> toRectangle) {
		super();
		this.ranges = ranges;
		//
		Collections.sort(ranges, new Comparator<SpatialZQueryRange>() {

			@Override
			public int compare(SpatialZQueryRange o1, SpatialZQueryRange o2) {
				return o1.getElement1().compareTo(o2.getElement1());
			}
			
			
		}); 
		this.tree = tree;
		this.query = query;
		this.toRectangle = toRectangle;
		this.rangesIterator = ranges.iterator(); 
		this.createKeyRange = LongKeyRange.FACTORY_FUNCTION; 
		duplicateValueIterators = new Iterator[tree.height()+1];
		path = new MapEntry[tree.height()+1];
		Arrays.fill(duplicateValueIterators, EmptyCursor.DEFAULT_INSTANCE);
		this.filterLeafNode = new AbstractPredicate() {
			@Override
			public boolean invoke(Object argument) {
				DoublePointRectangle rectangle = toRectangle.invoke(argument); 
				return query.overlaps(rectangle);
			}
		};
		
//		queryMaxKey = ranges.get(ranges.size()-1).getElement2();
		if(rangesIterator.hasNext()){
			SpatialZQueryRange firstRange = rangesIterator.next(); 
			long maxRange = ranges.get(ranges.size()-1).getElement2();
			KeyRange descriptor = (KeyRange)createKeyRange.invoke(firstRange.getElement1(), maxRange);
			nextMinKey = firstRange.getElement1(); 
			queryMaxKey = firstRange.getElement2(); 
			
			if (descriptor.overlaps(tree.rootDescriptor()) ){
				duplicateValueIterators[tree.height()] = new SingleObjectCursor(tree.rootEntry());
			}
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	@Override
	protected boolean hasNextObject() {
		for(int level = leafNodeLevel;;){
			if (duplicateValueIterators[level].hasNext()){ 
				if (level == leafNodeLevel)
					return true;
				else{ //core
					IndexEntry indexEntry = (IndexEntry)duplicateValueIterators[level].next();
					Node node = (Node)indexEntry.get(true);
					level = node.level();
					path[level] =  new MapEntry(indexEntry, node); // push in Stack
					if (level == leafNodeLevel){
						leafCounter++;
						duplicateValueIterators[level] =  new Filter( node.entries(), filterLeafNode);
					}else{
						KeyRange descriptor = (KeyRange)createKeyRange.invoke(nextMinKey, nextMinKey);
						if(level==leafNodeLevel +1 && nextPage){
							descriptor =  (KeyRange)createKeyRange.invoke(nextMinKey-1L, nextMinKey-1L);
						}
						duplicateValueIterators[level] = (duplicateValueIterators[level].hasNext() ) ? 
								new Sequentializer(node.query(descriptor),
										duplicateValueIterators[level]): 
											node.query(descriptor);
					}
				}
			}
			else{ // iterator empty 
				if (level==tree.height())
					return false;
				else{
					duplicateValueIterators[level++] = EmptyCursor.DEFAULT_INSTANCE; // go up
					if (level > leafNodeLevel && path[level]!= null){
						// search new input point in rectangle 
						// at this step level+1 is Iterator must be empty that means that the duplicates are completed 
						if (level == (leafNodeLevel+1) &&  !duplicateValueIterators[level].hasNext()){
							Long nextmatch =(Long) ((BPlusTree.IndexEntry)path[level-1].getKey()).separator().sepValue();
							if(nextmatch.compareTo(queryMaxKey) <= 0){
								// go to the next 
								nextMinKey = nextmatch+1L;  
								nextPage = true; 
//								Long lastKeyInNode = (Long)((BPlusTree.IndexEntry)path[level].getKey()).separator().sepValue();
//								if(lastKeyInNode.compareTo(nextMinKey) < 0){
//									nextMinKey = nextmatch; 
//								}
								
							}else{
								nextPage = false; 
								// find all ranges such that right interval is greater than nextMatch
								SpatialZQueryRange nextRange = null;
								for(SpatialZQueryRange item = null; rangesIterator.hasNext(); ){
									item = rangesIterator.next();
									if(item.getElement2().compareTo(nextmatch) >= 0){
										nextRange = item;  
										break;
									}
								}
								if (nextRange == null)
									return false;
								// case 1 nextRange left bound is greater
								if (nextRange.getElement1().compareTo(nextmatch) > 0 ){
									nextMinKey = nextRange.getElement1(); 
								
								}else{
									nextMinKey = nextmatch+1L; 
								}
								queryMaxKey = nextRange.getElement2(); 
							}
						}
						if(!duplicateValueIterators[level].hasNext()){
							Long lastKeyInNode = (Long)((BPlusTree.IndexEntry)path[level].getKey()).separator().sepValue();
							if (lastKeyInNode.compareTo(nextMinKey) < 0  ){
								duplicateValueIterators[level] =  EmptyCursor.DEFAULT_INSTANCE;
							}else{
								Node node = (Node)path[level].getValue();
								
								KeyRange descriptor = (KeyRange)createKeyRange.invoke(nextMinKey, nextMinKey); // search again
							
								duplicateValueIterators[level] = node.query(descriptor);
							}
						}
						//
//						if()
						
					}
				}
			}
		}
	}

	@Override
	protected Object nextObject() {
		return duplicateValueIterators[leafNodeLevel].next();
	}

	

}
