package xxl.core.indexStructures.rtrees;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.functions.Constant;
import xxl.core.functions.Functional.NullaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.util.Pair;
import xxl.core.util.Triple;
/**
 * 
 * Implementation of R-tree with buffer technique designed by
 * 
 * Lars Arge, Klaus Hinrichs, Jan Vahrenhold, Jeffrey Scott Vitter: Efficient Bulk Operations on Dynamic R-Trees. Algorithmica 33(1): 104-128 (2002)
 * 
 * Splits are performed in bottom-up manner. Routing algorithm and node splits are from R*tree.  
 * 
 * 
 * Bulk-loading is conducted by calling the {@link #bulkLoad(Iterator, NullaryFunction, int)} function. 
 * 
 * For bulk-loading we provide a factory function for creating a buffers (queues). These are attached to the buffer nodes. 
 * In this implementation the linkage of buffer nodes to their buffers (queues) is managed by a map. This map is memory resident. 
 * 
 * here is an example for initializing a BufferedRTree
 * 
 * 		int memorySizeForBuffers= 1024*1024*10; // assume we provide a 1 MB memory for loading and input objects are  DoublePointRectangles in two-dimensional space
 *  	int dataSize = DIMENSION *  2 * 8; // number of bytes needed to store DoublePointRectangle
 *	   // NOTE: actual size of a memory is larger, since we have a constant amount of an additional memory per java object. 		
 *		int descriptorSize = dataSize; // in our example they are equal
 *		double minMaxFactor = 0.33; // is used to define a minimal number of elements per node
 *		int memoryEntries = memorySizeForBuffers / dataSize; 
 *		int bufferPages = memorySizeForBuffers / BLOCK_SIZE;
 *		RtreeBuffer<DoublePointRectangle> rtree = new RtreeBuffer<>(BLOCK_SIZE, dataSize, DIMENSION); 
 *		//1. create container
 *		// since we initialize container for the first time,  we need two parameter path and blocksize
 *		// otherwise we provide only path parameter, block size is then obtained from the meta information stored in blockfile container
 *		Container fileContainer = new BlockFileContainer(RTREE_PATH + "bufferRtree", BLOCK_SIZE);
 *		//2.now we need to provide converterContainer that serializes (maps rtree nodes to a blocks)
 *		// before we can initialize converterContainer, we need initialize node converter of the rtree
 *		// default descriptor typ of the rtree is DoublePointRectangle. Therefore, we need to provide converter for input objects
 *		//Since, they are also of type DoublePointRectangle we do the following
 *		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
 *		// we wrap file container with counter
 *		CounterContainer ioCounter = new CounterContainer(fileContainer);
 *		Container converterContainer = new ConverterContainer(ioCounter, rtree.nodeConverter(objectConverter, DIMENSION));
 *		//3.converterContainer is now responsible for serializing rtree nodes. 
 *		//4. we use buffer this implements available memory and holds node buffers
 *		LRUBuffer<?, ?, ?> lruBuffer = new LRUBuffer<>(bufferPages);
 *		CounterContainer treeContainer = new  CounterContainer( new BufferedContainer(converterContainer, lruBuffer));
 *		// now we initialize container that manages buffers
 *		final Container bufferedContainer = new BufferedContainer(
 *				new ConverterContainer(new BlockFileContainer(RTREE_PATH + "buffers", BLOCK_SIZE),
 *							QueueBuffer.getPageConverter(Rectangles.getDoublePointRectangleConverter(DIMENSION))), lruBuffer);
 *		NullaryFunction<Queue<DoublePointRectangle>> queueFunction = new NullaryFunction<Queue<DoublePointRectangle>>() {
 *			@Override
 *			public Queue<DoublePointRectangle> invoke() {
 *				return new xxl.core.collections.queues.io.QueueBuffer<>(bufferedContainer,dataSize, BLOCK_SIZE);
 *			}
 *		};
 *		//5. now we can initialize tree
 *		// the first  argument is null 
 *		// if we want to reuse an Rtree we can provide root entry,  but in our case we do it for the first time.
 *		rtree.initialize(null, new Identity<DoublePointRectangle>(), treeContainer, BLOCK_SIZE, dataSize, descriptorSize, minMaxFactor); 
 *		Iterator<DoublePointRectangle> unsortedInput = new FileInputCursor<DoublePointRectangle>(objectConverter, new File(DATA_PATH));  
 *		rtree.bulkLoad(unsortedInput, queueFunction, memoryEntries); 
 * 
 * 
 * 
 * @author achakeye
 *
 * @param <E>
 */
public class BufferedRtree<E> extends RTree{
	
	/**
	 * debug 
	 */
	public static final boolean DEBUG = false; 
	
	/**
	 * This is an internal stack that holds buffer node information.  
	 * 
	 * 
	 * @author achakeye
	 *
	 * @param <E>
	 */
	protected static class WorkStack<E,M> extends Stack<E>{
		/**
		 * 
		 */
		Set<M> lookupSet;
		/**
		 * 
		 */
		UnaryFunction<E,M> getKey; 
		
		/**
		 * 
		 * @param getKey
		 */
		public WorkStack(UnaryFunction<E,M> getKey) {
			this.getKey = getKey; 
			this.lookupSet = new HashSet<>(); 
		}
		
		/**
		 * 
		 */
		public E push(E item) {
			//Do nothing if element conatains
			M mark = getKey.invoke(item);
			if(!lookupSet.contains(mark)){
				super.push(item);
				lookupSet.add(mark);
//				if (DEBUG){
////					System.out.println("Buffer of entry is full -> " + item);
//				}
			}
			return item; 
		};
		
		/**
		 * 
		 */
		@Override
		public synchronized E pop() {
			E pop = super.pop();
			lookupSet.remove(getKey.invoke(pop));
			return pop;
		}
	}
	
	/**
	 * marks leaf level 
	 */
	public static final int LEAF_LEVEL = 0; 
	
	/**
	 * converter for data objects
	 */
	protected Converter<E> dataConverter; 
	
	/**
	 * first  buffer level 
	 */
	protected int firstBufferLevel = 1;
	
	/**
	 * stores index entry buffer information
	 */
	protected Map<Long,Queue<E>> bufferMap; 
	
	/**
	 * factory function for queue creation
	 */
	protected NullaryFunction<Queue<E>> factoryBufferFunction;  
	
	/**
	 * root queue  
	 */
	protected Queue<E> rootQueue;
	
	/**
	 * M/4 entries
	 */
	protected int reducedMemory;
	
	/**
	 * block size
	 */
	protected int blockSize; 
	
	/**
	 * serialized size of a data object
	 */
	protected int dataSize; 
	
	/**
	 * number of entries per leaf node
	 */
	protected int B_LEAF; 
	
	/**
	 * number of entries  per index node
	 */
	protected int B_INDEX; 
	
	/**
	 * extracts id from an index entry
	 */
	UnaryFunction<IndexEntry, Long> getId = new UnaryFunction<ORTree.IndexEntry, Long>() {
		
		@Override
		public Long invoke(IndexEntry arg) {
			return (Long)arg.id();
		}
	};
	
	
	/**
	 * default constructor
	 * 
	 * 
	 * @param blockSize 
	 * @param dataSize serialized size of input data
	 * @param dimension number of dimensions
	 */
	public BufferedRtree(int blockSize, int dataSize, int dimension){
		super(); 
		this.blockSize = blockSize; 
		int payLoad = blockSize - 6;
		B_LEAF = payLoad / dataSize;
		B_INDEX = payLoad / (dimension*2*8+8);
	}
	
	/**
	 * internal initialization function for bulk-loading
	 * @param memory
	 * @param factoryBufferFunction
	 * @return
	 */
	protected BufferedRtree initForLoading(NullaryFunction<Queue<E>> factoryBufferFunction, int memory){
		this.factoryBufferFunction = factoryBufferFunction;
		bufferMap = new HashMap<Long, Queue<E>>();
		this.reducedMemory = memory/4;
		double logLevel = Math.floor(Math.log(reducedMemory/B_INDEX) / Math.log(B_INDEX)); 
		firstBufferLevel = (int) Math.max(logLevel, 1.0); 
		rootQueue = factoryBufferFunction.invoke();
		return this; 
	}
	
	/**
	 * Bulk-loading method.
	 * 
	 * 
	 * @param dataToInsert Iterator with input objects 
	 * @param factoryBufferFunction factory function that creates new queues. These queues are attached to the buffer nodes.
	 * @param memory
	 */
	public void bulkLoad(Iterator<E> dataToInsert, NullaryFunction<Queue<E>> factoryBufferFunction, int memory){
		initForLoading(factoryBufferFunction, memory);
		// create first node
		for(E entry = null; dataToInsert.hasNext();){
			entry = dataToInsert.next();
			DoublePointRectangle rectangle = (DoublePointRectangle) this.descriptor(entry); 
			rootQueue.enqueue(entry);
			if(rootQueue.size() >= reducedMemory){
				if(rootEntry.level() > firstBufferLevel ){
					E entryFromQueue = null; 
					Stack<IndexEntry> workStack = new WorkStack<IndexEntry, Long>(getId);
					for(int i = 0; i < reducedMemory; i++){
						entryFromQueue = rootQueue.dequeue();
						((IndexEntry)rootEntry).descriptor().union(rectangle); 
						Stack<PathEntry> path =  pushDownEntry(entryFromQueue, (IndexEntry)rootEntry, workStack); 
						updatePath(path); 
					}
					processWorkStack(workStack, false); // process
				}else{
					clearLowestBuffer((ORTree.IndexEntry)rootEntry, rootQueue);
				}
			}
			if(rootDescriptor == null && rootEntry == null){
				rootDescriptor = new DoublePointRectangle(rectangle); 
				rootEntry = (IndexEntry) createIndexEntry(LEAF_LEVEL+1); 
				Node firstNode = (Node) createNode(LEAF_LEVEL); 
				Object id = getContainer().reserve(new Constant<Node>(firstNode));
				((ORTree.IndexEntry)(rootEntry).initialize( getContainer(), id)).initialize(new DoublePointRectangle(rectangle)); 
				rootEntry.update(firstNode); 
			}
			// update root descriptor
			rootDescriptor.union(rectangle); 		
		}
		Stack<IndexEntry> workStack = new WorkStack<IndexEntry, Long>(getId);
		for(E entry = null; !rootQueue.isEmpty();){
			entry = rootQueue.dequeue();
			Stack<PathEntry> path =  pushDownEntry(entry, (IndexEntry)rootEntry, workStack); 
			updatePath(path); 
		}
		processWorkStack(workStack, false); // process
		clearAllBuffers((IndexEntry)rootEntry); // clear all 
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.RTree#createNode(int)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public xxl.core.indexStructures.Tree.Node createNode(int level) {
		return new Node().initialize(level, new LinkedList()); 
	}
	
	/**
	 * empties all buffers. This method is called, for example, after the input iterator is consumed. 
	 * 
	 *  @param rootEntry
	 */
	protected void clearAllBuffers(IndexEntry rootEntry){
		java.util.Queue<IndexEntry> levelsOfBUfferEntries = computeAllDownLevelQueues((IndexEntry)rootEntry);		
		for(IndexEntry downEntry = null; !levelsOfBUfferEntries.isEmpty(); ){
			downEntry = levelsOfBUfferEntries.poll(); 
			Queue<E> currentRootBuffer = getBuffer(downEntry);
			if(!currentRootBuffer.isEmpty()){
				List<IndexEntry> indexBufferList =  clearBuffer(downEntry, currentRootBuffer, true);
				for(IndexEntry topEntry: indexBufferList){
					currentRootBuffer = getBuffer(topEntry); 
					if(currentRootBuffer != null && !currentRootBuffer.isEmpty()){
						levelsOfBUfferEntries.offer(topEntry); 
					}
				}
			}
		}
	}
	
	/**
	 * computes all buffer nodes for emptying
	 * 
	 * @param rEntry
	 * @return
	 */
	private java.util.Queue<IndexEntry> computeAllDownLevelQueues(IndexEntry rEntry){
		java.util.Queue<IndexEntry> queue = new LinkedList<>();
		queue.offer(rEntry); 
		int currentLevel = -1; 
		java.util.Queue<IndexEntry> bufferEntryList = new LinkedList<>();  
		for(IndexEntry rootEntry = null ; !queue.isEmpty() ;){
			rootEntry = queue.poll(); 
			if(rootEntry.level() >= firstBufferLevel){
				Node node = (Node) rootEntry.get(true);// 
				for(Iterator currentEntries = node.entries(); currentEntries.hasNext(); ){
					IndexEntry entry = (IndexEntry)currentEntries.next();
					queue.offer(entry); 
					if(isBufferEntry(entry) && bufferMap.containsKey((Long)entry.id())){	// put to level list// level changes 
							bufferEntryList.offer(entry);
					}
				}
			}
		}
		return bufferEntryList; 
	}
	
	/**
	 * processes buffer nodes pushed to the woring stack
	 *  
	 *  
	 * @param workStack
	 */
	protected List<IndexEntry> processWorkStack(Stack<IndexEntry> workStack, boolean clearQueueFully){
		List<IndexEntry> reorganizedBufferEntry = new ArrayList<>(); 
		for(IndexEntry entry = null; !workStack.isEmpty() ;){
			entry = workStack.pop();
			Queue<E> currentRootBuffer = getBuffer(entry); 
			reorganizedBufferEntry.addAll(clearBuffer(entry, currentRootBuffer, clearQueueFully));  
		}
		return reorganizedBufferEntry; 
	}
	
	/**
	 * empties a single buffer of a buffer node
	 * 
	 * @param currentRoot
	 * @param all if true buffer is emptied completely otherwise only a portion
	 */
	protected List<IndexEntry> clearBuffer(IndexEntry currentRoot, Queue<E> currentRootBuffer, boolean all){
		if(isLowestBufferEntry(currentRoot)){
			return clearLowestBuffer(currentRoot, currentRootBuffer); 
		}
		List<IndexEntry> reorganizedBufferEntry = new LinkedList<>();
		Stack<IndexEntry> workStack = new WorkStack<IndexEntry, Long>(getId); 
		int minPushSize = Math.min(reducedMemory, currentRootBuffer.size());  
		for(int i = 0; i < minPushSize; i++){
			E record = currentRootBuffer.dequeue(); 
			Stack<PathEntry> path = pushDownEntry(record, currentRoot, workStack); 
			updatePath(path); 
		}
		reorganizedBufferEntry.addAll(processWorkStack(workStack, false)); 
		if(all){
			for( E record = null; !currentRootBuffer.isEmpty(); ){
				record = currentRootBuffer.dequeue(); 
				Stack<PathEntry> path = pushDownEntry(record, currentRoot, workStack); 
				updatePath(path); 
			}
			reorganizedBufferEntry.addAll(processWorkStack(workStack, false));
		}
		return reorganizedBufferEntry; 
	}
	
	/**
	 * writes dirty nodes to a container
	 * 
	 * @param path
	 */
	protected void updatePath(Stack<PathEntry> path){
		while(!path.isEmpty()){ // process 
			PathEntry pathEntry = path.pop(); 
			if(pathEntry.getElement3()){
				pathEntry.getElement1().update(pathEntry.getElement2(), true); 
			}
		}
	}
	
	
	/**
	 * empties lowest buffer. 
	 * 
	 * this can used e.g. to devise strategies for leaf node layouts
	 * 
	 * 
	 * @param currentRoot
	 */
	@SuppressWarnings("unchecked")
	protected List<IndexEntry> clearLowestBuffer(IndexEntry cRoot, Queue<E> currentRootBuffer){
		Stack<IndexEntry> workStack = new WorkStack<IndexEntry, Long>(getId);
		IndexEntry currentRoot = cRoot; 
		// union descriptor
		List<IndexEntry> reorganizedBufferEntries = new ArrayList<>(); 
		for( ;!currentRootBuffer.isEmpty(); ){
			E record = currentRootBuffer.dequeue(); 
			DoublePointRectangle rectangle = (DoublePointRectangle) this.descriptor(record); 
			currentRoot.descriptor().union(rectangle); 
			Stack<PathEntry> path = pushDownEntry(record, currentRoot, workStack);
//			if (DEBUG){
//				if (!workStack.isEmpty())
//					throw new RuntimeException("workStack must be empty in low buffer level"); 
//			}
			// process buffer split nodes
			List<PathEntry> newEntries =  processBottomUpBuffer(path, reorganizedBufferEntries); // process overflow if applicable
			if(!newEntries.isEmpty()){ 
				Stack<PathEntry> pathToLowestNode = computePath(currentRoot); // compute path from root to the lowest buffer node
				if(!pathToLowestNode.isEmpty()){
					PathEntry newCreateSubRoot = newEntries.get(0); 	// insert only the first one, since the 
					pathToLowestNode.peek().getElement2().grow(newCreateSubRoot.getElement1()); // insert new create node 
					List<PathEntry> entries = processBottomUpBuffer(pathToLowestNode, reorganizedBufferEntries); 
					if(!entries.isEmpty()){ // create new root
						Container container = this.getContainer(); 
						int rootNodeLevel = rootEntry.level()+1; 
						Node rootNode = (Node) createNode(rootNodeLevel);
						DoublePointRectangle universe = null; 
						for(PathEntry entry: entries){
							rootNode.grow(entry.getElement1());
							if(universe == null)
								universe = new DoublePointRectangle((DoublePointRectangle)entry.getElement1().descriptor());
							else 
								universe.union((DoublePointRectangle)entry.getElement1().descriptor() );
						}
						IndexEntry newRootEntry = (IndexEntry) createIndexEntry(rootNodeLevel+1); 
						((IndexEntry)newRootEntry.initialize(container, container.insert(rootNode))).initialize(universe);
						rootEntry = newRootEntry;
					}
				}else{// new root TODO
					Container container = this.getContainer(); 
					int rootNodeLevel = rootEntry.level()+1; 
					Node rootNode = (Node) createNode(rootNodeLevel);
					DoublePointRectangle universe = null; 
					for(PathEntry entry: newEntries){
						rootNode.grow(entry.getElement1());
						if(universe == null)
							universe = new DoublePointRectangle((DoublePointRectangle)entry.getElement1().descriptor());
						else 
							universe.union((DoublePointRectangle)entry.getElement1().descriptor() ); 
					}
					
					IndexEntry newRootEntry = (IndexEntry) createIndexEntry(rootNodeLevel+1); 
					((IndexEntry)newRootEntry.initialize(container, container.insert(rootNode))).initialize(universe);
					rootEntry = newRootEntry; 
					currentRoot = (IndexEntry) rootEntry; 
				}
			}
		}
		return reorganizedBufferEntries;
	}
	
	/**
	 * goes bottom-up the insertion path and performs structure reorganization, if applicable. 
	 *   
	 * @param path
	 * @param newEntries
	 * @return
	 */
	protected List<PathEntry> processBottomUpBuffer(Stack<PathEntry> path, List<IndexEntry> reorganizedBufferEntries){
		List<PathEntry> newEntries = new ArrayList<>(); // go up the stack
		for(PathEntry pathEntry = null ; !path.isEmpty(); ){
			pathEntry = path.pop(); // check if split is needed
			if(pathEntry.getElement2().overflows()){
				PathEntry newPathEntry = reorganize(pathEntry); // split 
				if(isBufferEntry(pathEntry.getElement1())){
					reorganizedBufferEntries.add(pathEntry.getElement1());
					reorganizedBufferEntries.add(newPathEntry.getElement1());
				}
				if(!path.isEmpty()){ 	// insert new node, get parent node and post new entries
					PathEntry parentEntry = path.peek(); 
					parentEntry.getElement2().grow(newPathEntry.getElement1()); 
				}else{			
					newEntries.add(newPathEntry); // new entry 
					newEntries.add(pathEntry); // old entry
				}
			}
			if (pathEntry.getElement3()) {
				pathEntry.getElement1().update(pathEntry.getElement2(), true);// update only if descriptors changed
			}
		}
		return newEntries;
	}

	/**
	 * 
	 * Computes path root to buffer level 
	 *  
	 * 
	 * @param currentRoot
	 * @return path to the currentRoot
	 */
	@SuppressWarnings("unchecked")
	protected Stack<PathEntry> computePath(IndexEntry currentRoot){
		// use descriptor to find the parent node
		int parentLevel = currentRoot.parentLevel(); 
		Stack<PathEntry> path = new Stack<>();
		if(parentLevel > firstBufferLevel  && currentRoot != rootEntry){			
			Stack<Iterator<IndexEntry>> stack  = new Stack<>();  
			stack.push(new SingleObjectCursor<IndexEntry>((IndexEntry)rootEntry)); 
			final DoublePointRectangle entryDescriptor = (DoublePointRectangle)currentRoot.descriptor(); 
			for( ;!stack.isEmpty(); ){
				Iterator<IndexEntry> iterator = stack.peek(); 
				if(iterator.hasNext()){
					IndexEntry indexEntry = iterator.next();	
					if(indexEntry.parentLevel() == parentLevel && indexEntry.id().equals(currentRoot.id())){
						// update 
						indexEntry.initialize(currentRoot.descriptor()); 
						break;//stop
					}else if (!isLeafEntry(indexEntry) && indexEntry.parentLevel() > parentLevel ){
						Node node = (Node)indexEntry.get(false); 
						path.push(new PathEntry(indexEntry, node, true));
						Iterator<IndexEntry> levelIterator =  new Filter(node.entries(), new AbstractPredicate() {
							
							@Override
							public boolean invoke(Object obj) {
								IndexEntry idx = (IndexEntry)obj;
								DoublePointRectangle dpr =  (DoublePointRectangle)idx.descriptor(); 
								boolean contains =  entryDescriptor.contains(dpr) || dpr.contains(entryDescriptor); 
								return contains; 
							}
						});
						stack.push(levelIterator); 
					}else{
						stack.push(new EmptyCursor<IndexEntry>()); 
						path.push(new PathEntry(null, null, null));// dummy 
					} 
				}else{
					stack.pop(); 
					path.pop();
				}
			}
		}
		return path; 
	}
	
	/**
	 * conducts structure reorganization
	 * 
	 * @param oldEntry
	 * @return
	 */
	protected PathEntry reorganize(PathEntry oldEntry){
		PathEntry newPathEntry = oldEntry.getElement2().split(oldEntry.getElement1()); // split
		Container container = this.getContainer(); 
		newPathEntry.getElement1().initialize(container, container.insert(newPathEntry.getElement2())); // insert into container
		if(isBufferEntry(oldEntry.getElement1())){
			 redistributeBuffer(oldEntry, newPathEntry); 
		}
		return newPathEntry; 
	}
	
	
	/**
	 * redistribute buffers
	 *  
	 */
	@SuppressWarnings("unchecked")
	protected void redistributeBuffer(PathEntry entryOld, PathEntry entryNew){
		// take the buffer from the lod node
		Queue<E> oldBuffer = getBuffer(entryOld.getElement1()); 
		if(oldBuffer != null){ // the buffer can be empty in last phase in case of all buffers are emptied in top down level wise manner
		//create mocked node 
			Node node = (Node) createNode(entryOld.getElement2().level()+1); 
			node.grow(entryOld.getElement1());
			node.grow(entryNew.getElement1());
			Queue<E> newBufferOld = factoryBufferFunction.invoke();
			Queue<E> newBufferNew = factoryBufferFunction.invoke(); 
			for(E entry= null; !oldBuffer.isEmpty(); ){
				entry = oldBuffer.dequeue(); 
				DoublePointRectangle descriptor = (DoublePointRectangle) descriptor(entry); 
				Pair<IndexEntry, Boolean> pair = node.chooseSubtree(descriptor); 
				if(pair.getElement1().id().equals(entryOld.getElement1().id())){
					// old one
					newBufferOld.enqueue(entry);
				}else{
					// new one
					newBufferNew.enqueue(entry);
				}
			}
			bufferMap.put((Long)entryOld.getElement1().id(), newBufferOld);
			bufferMap.put((Long)entryNew.getElement1().id(), newBufferNew);
		}else{
			// do nothing!
		}
	}
	
	/**
	 * transports from buffer to buffer level or from the 
	 * lowest buffer level to leaf nodes
	 *  
	 * @param record
	 * @param currentRoot
	 * @param parentNodes
	 * @param workStack
	 * 
	 */
	@SuppressWarnings("unchecked")
	protected Stack<PathEntry> pushDownEntry(E record, IndexEntry currentRoot, Stack<IndexEntry> workStack){
		IndexEntry currentEntry = currentRoot;
		Node currentNode = (Node) currentEntry.get(true); // parenmt node may be also buffer node
		DoublePointRectangle dpr = (DoublePointRectangle) descriptor(record); // XXX descriptors are double point rectangles
		Stack<PathEntry> pathBufferToBuffer = new Stack<>();
		pathBufferToBuffer.push(new PathEntry(currentEntry, currentNode, Boolean.valueOf(false))); 
		if (isLeafEntry(currentEntry)){
			currentNode = (Node) currentEntry.get(true);
			pathBufferToBuffer.peek().setElement3( Boolean.valueOf(true));
			currentNode.grow(record); // insert into leaf node
		}else{
			Pair<ORTree.IndexEntry, Boolean> currentEntryPair = null;
			do{
				currentEntryPair = currentNode.chooseSubtree(dpr); // updates also MBR if applicable
				pathBufferToBuffer.peek().setElement3(currentEntryPair.getElement2()); // FIXME check thi method
				currentEntry = currentEntryPair.getElement1();
				currentNode = (Node) currentEntry.get(true);
				pathBufferToBuffer.push(new PathEntry(currentEntry, currentNode, Boolean.valueOf(false)));
			}
			while(!isBufferEntry(currentEntry) && !isLeafEntry(currentEntry));
//			{
//				
//			}
			if (isLeafEntry(currentEntry)){
				currentNode = (Node) currentEntry.get(true);
				pathBufferToBuffer.peek().setElement3( Boolean.valueOf(true));
				currentNode.grow(record); // insert into leaf node
			}else { // append to buffer  if(currentRoot != rootEntry && isBufferEntry(currentEntry)
				appendToBuffer(record, currentEntry); 
				if (isBufferFull(currentEntry)){
					// search instack
					workStack.push(currentEntry); 
				}
			}
		}
		return pathBufferToBuffer;
	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	protected Container getContainer(){
		return (Container) this.determineContainer.invoke(this); 
	}
	
	/**
	 * Appends to queue if queue not exists allocates new queue 
	 * @param rootEntry
	 */
	protected void appendToBuffer(E record, IndexEntry rootEntry) {
		Queue<E> buffer = bufferMap.get((Long)rootEntry.id());
		if(buffer == null){
			buffer = factoryBufferFunction.invoke();
			bufferMap.put((Long)rootEntry.id(), buffer);
//			if(DEBUG){
//				System.out.println("Buffer assigned to entry -> " + rootEntry.toString());
//			}
		}
		buffer.enqueue(record);
	}
	
	/**
	 * returns buffer of a buffer node
	 * 
	 * @param entry
	 * @return
	 */
	protected Queue<E> getBuffer(IndexEntry entry){
		return this.bufferMap.get(entry.id()); 
	}
	
	/**
	 * attaches buffer to index entry
	 */
	protected void assignBuffer(IndexEntry entry){
		Long id = (Long)entry.id();	
		Queue<E> buffer = this.factoryBufferFunction.invoke();
		bufferMap.put(id, buffer);
	}
	
	/**
	 * remove buffer node from map
	 * 
	 * @param entry
	 */
	protected void removeBuffer(IndexEntry entry){
		bufferMap.remove((Long)entry.id());
	}
	
	/**
	 * checks if the entry is on buffer level 
	 * note that root of the tree is specially treated
	 * @param indexEntry
	 * @return
	 */
	protected boolean isBufferEntry(IndexEntry indexEntry) {
		boolean hasBuffer = (indexEntry.parentLevel()-1 == 0) ? false : 
			(indexEntry.parentLevel()-1) % firstBufferLevel == 0;
		return hasBuffer && indexEntry != rootEntry;
	}
	
	/**
	 * checks if the entry is on buffer level 
	 * note that root of the tree is specially treated
	 * @param indexEntry
	 * @return
	 */
	protected boolean isLowestBufferEntry(IndexEntry indexEntry) {
		boolean hasBuffer = (indexEntry.parentLevel()-1 == 0) ? false : 
			(indexEntry.parentLevel()-1) == firstBufferLevel;
		return hasBuffer && indexEntry != rootEntry;
	}
	
	/**
	 * Indicates whether currenEntry is point to leaf
	 * 
	 * @param currentEntry
	 * @return
	 */
	protected boolean isLeafEntry(IndexEntry currentEntry) {
		return currentEntry.level() == 0;
	}
	
	/**
	 * returns true if index entry point to a buffer node 
	 *  
	 * @param currentEntry
	 * @return
	 */
	protected boolean isBufferFull(IndexEntry currentEntry) {
		if(bufferMap.containsKey((Long)currentEntry.id())){
			Queue<E> buffer = bufferMap.get((Long)currentEntry.id());
			return buffer.size() > reducedMemory;
		}	
		return false;
	}
	
	/**
	 * Typedef for a information object stored in a reorganization path
	 * @author achakeye
	 *
	 */
	@SuppressWarnings("serial")
	public class PathEntry extends Triple<IndexEntry, Node, Boolean>{
		
		public PathEntry(IndexEntry indexEntry, Node node, Boolean update){
			super(indexEntry, node, update);
		}
	}
		
	
	/**
	 * This class  extends R*tree node. It inherits split and routing algorithm. 
	 *
	 */
	public class Node extends RTree.Node{
		/**
		 * 
		 * @param descriptor
		 * @return
		 */
		protected Pair<IndexEntry, Boolean> chooseSubtree (Descriptor descriptor){
			IndexEntry indexEntry = (IndexEntry)super.chooseSubtree(descriptor, this.entries());
			boolean updateNode = false; 
			if (!indexEntry.descriptor().contains(descriptor)) {
				indexEntry.descriptor().union(descriptor);
				updateNode = true; 
			}
			return new Pair<>(indexEntry, Boolean.valueOf(updateNode)); 
		}
		/*
		 * we additionally add update of descriptor
		 * 
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.RTree.Node#chooseSubtree(xxl.core.indexStructures.Descriptor, java.util.Iterator)
		 */
		@SuppressWarnings("rawtypes")
		@Override
		protected ORTree.IndexEntry chooseSubtree (Descriptor descriptor, Iterator minima){
			IndexEntry indexEntry = super.chooseSubtree(descriptor, minima);
			if (!indexEntry.descriptor().contains(descriptor)) {
				indexEntry.descriptor().union(descriptor);
			}
			return indexEntry; 
		}
		/*
		 * 
		 */
		protected void grow (Object data) {
			super.grow(data, null);
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.Tree.Node#overflows()
		 */
		@Override
		protected boolean overflows() {
			return super.overflows();
		}
		/**
		 * 
		 * @return
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected PathEntry split(IndexEntry indexEntry) {
			Node newNode = (Node) createNode(this.level); 
			Stack path = new Stack(); 
			path.push(new MapEntry(indexEntry, this)); 
			SplitInfo info = (SplitInfo) newNode.split(path); 
			newNode = (Node) info.newNode(); 
			IndexEntry newIndexEntry = (IndexEntry) createIndexEntry(this.level()+1).initialize(info);
			return new PathEntry(newIndexEntry, newNode, true);
		}
		
		@Override
		public String toString() {
			return "Node [entries=" + entries + ", level=" + level + "]";
		}
		
		
	}
	
}
