/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2014 Prof. Dr. Bernhard Seeger
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



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import xxl.core.collections.containers.Container;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.collections.queues.io.QueueBuffer;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.unions.Merger;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.functions.Functional.NullaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.util.Pair;
import xxl.core.util.Quadruple;
import xxl.core.util.Triple;

/**
 * This class implements a MVBTPlus bulk-loading algorithm; 
 * 
 * see D. Achakeev, B. Seeger, Efficient bulk updates on multiversion B-trees, PVLDB Vol 6 Issue 14, 2013
 * 
 * 
 * For loading purpose MVBTPlus is initialized as follows (the first two steps are similar to #MVBT)
 * 
 * 1. we create MVBTPlus using a {@link #MVBTPlus(int, float, float, int, int, Comparable)}
 * we provide BlockSize, fraction of minimal number of live entries (D),  fraction of D as minimal number of elements needed for a next reorg.  
 *  and a minimal value from key domain.  
 * 
 * 2. we call {@link #initialize(xxl.core.indexStructures.BPlusTree.IndexEntry, Descriptor, xxl.core.indexStructures.BPlusTree.IndexEntry, Descriptor, Function, Container, Container, MeasuredConverter, MeasuredConverter, MeasuredConverter, Function, Function)}
 *  e.g 	mvbtplus.initialize(null, // rootEntry
 *						null, // Descriptor MVRegion
 *					null, // roots tree root Entry
 *						null, // descriptor KeyRange
 * 						getKey, // getKey Function
 *						rootsPlusContainer, // container roots tree
 *						mainTreeContainer, // main container
 *						LongVersion.VERSION_MEASURED_CONVERTER, // converter for version object 
 *						keyConverter, // key converter 
 *						dataConverter, // data converter mapEntry
 *						LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
 *						LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
 * 3. we provide a factory function for queue creation; Node buffers are implement queue ({@link Queue}) interface.
 * e.g. with a {@link QueueBuffer} 
 * 		final LRUBuffer memory = new LRUBuffer(bufferSlots);
 *     	final Container bufferStorage =  new BlockFileContainer("C:/buffers", blockSize);
 *		final CounterContainer cBufferStorage = new CounterContainer(new ConverterContainer(bufferStorage, QueueBuffer.getPageConverter(recordConverter)));
 *		//the queue buffer pages are managed by LRUbuffer 
 *		final CounterContainer memoryQueueStorage  = new CounterContainer(new BufferedContainer(cBufferStorage, memory));
 *		// this is a factory function that is used for bulk loading
 *		NullaryFunction<Queue<Element>> queueFunction = new NullaryFunction<Queue<Element>>() {
 *			@Override
 *			public Queue<Element> invoke() {
 *				return new QueueBuffer<>(memoryQueueStorage, recordConverter.getMaxObjectSize(), BLOCK_SIZE);
 *			}
 *			
 *		};
 * e.g with a {@link BlockBasedQueue}
 *      final LRUBuffer memory = new LRUBuffer(bufferSlots);
 *     	final Container bufferStorage =  new BlockFileContainer("C:/buffers", blockSize);
 *      // @link {@link BlockBasedQueue}
 *		final CounterContainer cBufferStorage = new CounterContainer(new ConverterContainer(bufferStorage, QueueBuffer.getPageConverter(recordConverter)));
 *		//the queue buffer pages are managed by LRUbuffer 
 *		final CounterContainer memoryQueueStorage  = new CounterContainer(new BufferedContainer(cBufferStorage, memory));
 *		// this is a factory function that is used for bulk loading
 *		NullaryFunction<Queue<Element>> queueFunction = new NullaryFunction<Queue<Element>>() {
 *			@Override
 *			public Queue<Element> invoke() {
 *				return new BlockBasedQueue(memoryQueueStorage, blockSize, recordConverter, new {@link Constant}(recordConverter.getMaxObjectSize()), new {@link Constant}(recordConverter.getMaxObjectSize())  );
 *			}
 *			
 *		};
 * 
 * and branching parameter, number of elements that can be stored in memory (for queue and a sub-tree nodes). 
 * 
 * in the {@link #bulkLoad(Iterator, NullaryFunction, int)} or {@link #bulkInsert(Iterator, NullaryFunction, int)} method. 
 * 
 * 
 * Note in this class: uses {@link LongVersion} as default version implementation; 
 * Additionally the input of the tree are object of type @link {@link Element}.
 * 
 * Experimental version:  In this version we use in-memory map to store pointers to queues (buffers) 
 * 
 */
public class MVBTPlus extends MVBT {
//	Example of serialization and header: 8KB pages; IndexEntry size:=  8  long + 8 key + 8 start ts  + 8 end ts + 4 wCounter + 4 tCounter = 40 Bytes  
//			 * Header: short level + integer number + 2 * previous links = 86 Bytes
//			 * Payload := 8192 - 86 = 8106    
//			 * ~ 200 entries per node
	/**
	 * 
	 */
	public static final int INDEX_FIRST = 0;
	
	/**
	 * 
	 */
	public static final int LEAF_LEVEL = 0;

	/**
	 * Version wrapper. This is a default implementation of a Version interface. 
	 * Internally all versions are implemented as LongVersion.  
	 *  
	 * {@link MVBTPlus#initialize(IndexEntry, Descriptor, IndexEntry, Descriptor, Function, Container, Container, MeasuredConverter, MeasuredConverter, MeasuredConverter, Function, Function)}
	 * 
	 * I order to initialize a MVBTPlus
	 * 
	 * 
	 * Assumption: we manage long time stamps.
	 */
	public static class LongVersion implements MVBTree.Version{
		/**
		 * 
		 */
		protected long version;
		/**
		 * 
		 */
		public static final Converter<LongVersion> VERSION_CONVERTER =	new Converter<LongVersion>(){
			@Override
			public LongVersion read(DataInput dataInput, LongVersion object)
					throws IOException {
				long version = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
				return new LongVersion(version);
			}
			@Override
			public void write(DataOutput dataOutput, LongVersion object)
					throws IOException {
				LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, ((LongVersion)object).version);
			}
		};
		/**
		 * 
		 */
		public static final MeasuredConverter<LongVersion> VERSION_MEASURED_CONVERTER = new MeasuredConverter<LongVersion>() {
			@Override
			public int getMaxObjectSize() {
				return LongConverter.SIZE;
			}
			@Override
			public LongVersion read(DataInput dataInput, LongVersion object)
					throws IOException {
				return VERSION_CONVERTER.read(dataInput) ;
			}
			@Override
			public void write(DataOutput dataOutput, LongVersion object)
					throws IOException {
				VERSION_CONVERTER.write(dataOutput, object);
			}
		};
		/**
		 * 
		 * @param version
		 */
		public LongVersion(long version){
			this.version = version;
		}
		/**
		 * 
		 * @return
		 */
		public Long getTimeStamp(){
			return this.version;
		}
		/*
		 * (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Object o) {
			LongVersion v = (LongVersion)o;
			return (this.version == v.version) ? 0 : ( (this.version > v.version ) ? 1: -1) ;
		}
		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#clone()
		 */
		public Object clone(){
			return new LongVersion(this.version);
		}
		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		public int hashCode() {
			return ((int)version * 157) & 0x7fffffff;
		}
		/**
		 * 
		 */
		public String toString(){
			return "Version: " + this.version; 
		}
	}
	
	/**
	 * Type of operation decoded in {@link Element}. 
	 */
	public static enum OperationType{
		INSERT, 
		UPDATE,
		DELETE,
	}
	
	/**
	 * Data entry wrapper. This MVBT implementation manages this object in buffers. 
	 * 
	 *  {@link #bulkLoad}
	 *  {@link #bulkInsert}
	 *  {@link #insert}   
	 * 
	 */
	public static class Element extends Triple<Object, LongVersion, OperationType>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Element(Object data, LongVersion  version, OperationType type) {
			super(data, version, type);
		}
	}
	/**
	 * This tokens are used for decoding reorganization type
	 */
	protected static enum SplitTocken{
		VERSION_SPLIT,
		KEY_SPLIT,
		MERGE,
		MERGE_KEY_SPLIT,
		BUFFER_FULL,
		BUFFER_FLUSH,
	}
	/**
	 * Current bulk-loading state
	 * used internally to mark the current processing step
	 */
	protected static enum LoadState{
		PUSH_INIT, // initial state 
		PUSH_LOAD, // after root node is created state
		PUSH_ALL, // last state after the initial data is iterator is empty!
	}
	/**
	 * SplitInfo: contains necessary information for conducting a node reorganization operation
	 */
	protected static class ReorgInfo extends Quadruple<SplitTocken, LongVersion, IndexEntry, Boolean>{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		/**
		 * 
		 * @param reorgOperation
		 * @param violationVersion
		 * @param neighbourEntry
		 * @param lowMergeKey
		 */
		public ReorgInfo(SplitTocken reorgOperation, LongVersion violationVersion, IndexEntry neighbourEntry, Boolean lowMergeKey){
			super(reorgOperation, violationVersion, neighbourEntry, lowMergeKey);
		}
	}
	/**
	 * This class implements a tuple. This tuple contains information for needed for buffer emptying. 
	 * 
	 *
	 */
	protected static class StackInfoObject extends Quadruple<IndexEntry, IndexEntry, Node , ReorgInfo>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		/**
		 * 
		 * @param entryNodePair
		 * @param parentindexEntry
		 * @param parentNode
		 * @param reorgInfo
		 */
		public StackInfoObject (IndexEntry entryNodePair, IndexEntry parentindexEntry,  Node parentNode, ReorgInfo reorgInfo){
			super( entryNodePair, parentindexEntry,  parentNode, reorgInfo);
		}
		
	}

	/**
	 * stores index entry buffer information. This a basic implementation. 
	 * This map is stored in main memory. For a advanced application persistent map implementation can be used.
	 *  
	 */
	protected Map<Long,Queue<Element>> bufferMap; 
	/**
	 *  factory function level buffers. creates a generic buffer. Entries of type Element is stored in this queues during the bulk loading or bulk insertion.
	 */
	protected NullaryFunction<Queue<Element>> factoryBufferFunction;  
	/**
	 * indicates current processing state
	 */
	protected LoadState loadState;
	/**
	 * 
	 * branching factor; can be user defined; 
	 * default value is equal to a minimal number of live entries in an index node.
	 * 
	 */
	protected int parameter_A;
	/**
	 * memory capacity expressed in the number of elements 
	 */
	protected int memoryCapacity;
	/**
	 * the maximal number of elements stored in a buffer of node
	 * 
	 *  default value is equal to 1/4 of the available memory; (1/4 of memory capacity)
	 */
	private int reducedMemory;
	/**
	 * the lowest buffer level 
	 */
	protected int firstBufferLevel = 1;
	/**
	 * internal function for weight violation 
	 */
	protected UnaryFunction<IndexEntry, Boolean> violatesWeight;
	/**
	 * internal function minimal live weight
	 */
	protected BinaryFunction<Integer, Integer, Integer> minLiveWeight;
	/**
	 * internal function minimal live weight strong
	 */
	protected BinaryFunction<Integer, Integer, Integer> minLiveWeightStrong;
	/**
	 * internal function maximal live weight
	 */
	protected BinaryFunction<Integer, Integer, Integer> maxLiveWeight;
	/**
	 * internal function maximal live weight strong 
	 */
	protected BinaryFunction<Integer, Integer, Integer> maxLiveWeightStrong;
 	/**
 	 * internal functions returns maximal weight
 	 */
	protected UnaryFunction<Integer, Integer> getMaxWeight;
	/**
	 * internal functions returns true if a level has buffers, in non bulk load case this function returns always false;
	 */
	protected UnaryFunction<Integer, Boolean> isBufferLevel; 
	/**
	 * internal functions special queue for a root node
	 */
	protected Queue<Element> rootQueue;
	/**
	 * auxiliary set, is used to mark buffer overflows
	 */
	protected Set<Long> markSet; 
	
	
	/**
	 * Default constructor. 
	 * 
	 * The minimal number of live entries per node are set to 1/4 of a maximal node node capacity.
	 * Epsilon value is set to 0.5f that means that the amount of entries needed to trigger next leaf node reorganization is equal to 1/8 of the node capacity. 
	 * if {@link #initialize(IndexEntry, Descriptor, IndexEntry, Descriptor, Function, Container, Container, MeasuredConverter, MeasuredConverter, Function, Function)} is called for initialization.
	 * Then weight balancing branching parameter is set also to 1/4 of a maximal node node capacity. 
	 * 
	 * With this parameters the minimal maximal node capacity should be greater equal to 64. 
	 * 
	 * 
	 * @param blockSize is used to determine maximal and minimal number of  entries per node. 
	 * @param keyDomainMinValue the minimal value of a key domain
	 */
	public MVBTPlus(final int blockSize,  Comparable keyDomainMinValue) {
		this(blockSize, 0.25f, 0.5f, keyDomainMinValue);
	}
	
	/**
	 * Generic constructor. 
	 * 
	 * @param blockSize is used to determine maximal and minimal number of  entries per node. 
	 * @param minCapRatio is a fraction of node capacity. this defines the minimal allowed number of live entries per node.
	 * @param e is a fraction of the the minimal allowed number of live entries per node. This will defines the minimal number of entries needed to insert before next reorganization occurs.
	 * @param keyDomainMinValue the minimal value of a key domain
	 */
	public MVBTPlus(final int blockSize,  float minCapRatio, float e, Comparable keyDomainMinValue) {
		super(blockSize, minCapRatio, e);
		this.keyDomainMinValue = keyDomainMinValue;
	}

	/**
	 * Default initialization method. Note: in this implementation we use @link {@link LongVersion} for Versions (time stamps). The Version Converter is set to 
	 * {@link LongVersion#VERSION_MEASURED_CONVERTER}.
	 * 
	 * The value of weight balancing branching parameter is set to equal to the value of minimal live number of entries per node.
	 *  
	 * @param rootEntry of the tree if a tree is used for a first time this value is null
	 * @param rootsRootEntry of the root structure (BPlusTree) if a tree is used for a first time this value is null 
	 * @param getKey function to extract (map) to a key 
	 * @param rootsContainer container that manages nodes of a roots BplusTree
	 * @param treeContainer container that manages MVBT nodes
	 * @param keyConverter converter for keys
	 * @param dataConverter 
	 * @param createMVSeparator factory function for creating {@link MVSeparator}  objects
	 * @param createMVRegion factory function for creating {@link MVRegion}  objects
	 * @return MVBTPlus
	 */
	public MVBTPlus initialize(IndexEntry rootEntry, 
			Descriptor liveRootDescriptor, 
			IndexEntry rootsRootEntry,	
			Descriptor rootsRootDescriptor, 
			final Function getKey,
			final Container rootsContainer, 
			final Container treeContainer, 
			MeasuredConverter keyConverter, 
			MeasuredConverter dataConverter,
			Function createMVSeparator,
			Function createMVRegion){
		 this.initialize(rootEntry, 
				liveRootDescriptor, 
				rootsRootEntry,	
				rootsRootDescriptor, 
				getKey,
				rootsContainer, 
				treeContainer, 
				LongVersion.VERSION_MEASURED_CONVERTER, 
				keyConverter, 
				dataConverter,
				createMVSeparator,
				createMVRegion, 16);
		this.parameter_A = this.D_IndexNode;   
		// update
		initWeightFunctions();
		return this;						
	}
	
	
	/**
	 * Generic initialization method similar to {@link MVBT#initialize(xxl.core.indexStructures.BPlusTree.IndexEntry, Descriptor, xxl.core.indexStructures.BPlusTree.IndexEntry, Descriptor, Function, Container, Container, MeasuredConverter, MeasuredConverter, MeasuredConverter, Function, Function)}
	 * @param rootEntry of the tree if a tree is used for a first time this value is null
	 * @param rootsRootEntry of the root structure (BPlusTree) if a tree is used for a first time this value is null 
	 * @param getKey function to extract (map) to a key 
	 * @param rootsContainer container that manages nodes of a roots BplusTree
	 * @param treeContainer container that manages MVBT nodes
	 * @param versionConverter converter for version objects
	 * @param keyConverter converter for keys
	 * @param dataConverter 
	 * @param createMVSeparator factory function for creating {@link MVSeparator}  objects
	 * @param createMVRegion factory function for creating {@link MVRegion}  objects
	 * @param fanout_parameter_A is used for weight balancing. 
	 * @return MVBTPlus
	 */
	protected MVBTPlus initialize(IndexEntry rootEntry, 
			Descriptor liveRootDescriptor, 
			IndexEntry rootsRootEntry,	
			Descriptor rootsRootDescriptor, 
			final Function getKey,
			final Container rootsContainer, 
			final Container treeContainer, 
			MeasuredConverter versionConverter, 
			MeasuredConverter keyConverter, 
			MeasuredConverter dataConverter,
			Function createMVSeparator,
			Function createMVRegion, 
			final int fanout_parameter_A){
		 super.initialize(rootEntry, 
				liveRootDescriptor, 
				rootsRootEntry,	
				rootsRootDescriptor, 
				getKey,
				rootsContainer, 
				treeContainer, 
				versionConverter, 
				keyConverter, 
				dataConverter,
				createMVSeparator,
				createMVRegion);
		this.parameter_A = fanout_parameter_A;  
		// non buffer node case
		this.memoryCapacity = 0; 
		//
		bufferMap = new HashMap<Long, Queue<Element>>();
		markSet = new HashSet<Long>();
		//return always false
		// if bulk load or bulk update then this will be updated
		isBufferLevel = new UnaryFunction<Integer, Boolean>() {
			@Override
			public Boolean invoke(Integer level) {
				//
				return false;
			}
		};
		//
		initWeightFunctions();
		return this;						
	}
	
	/**
	 * this internal method is called in if bulk load or bulk insert method called
	 * it initializes {@link #isBufferLevel} functions, sets {@link #firstBufferLevel} value, factory function for buffer creation is also initialized
	 * 
	 * @param  factoryQueueFunction
	 * @param memoryCapacity is given by the number of entries, e.g. if entry serialized size is 32 Bytes and the reserved memory for a bulk-loading is 1024 KB 
	 * then mamoryCapacity is equal to (1024*1024)/32.
	 * 
	 * Note: that actual memory size allocated in main memory is greater than the serialized size of entries. 
	 * 
	 */
	protected void initForLoad(NullaryFunction<Queue<Element>> factoryQueueFunction, int memoryCapacity){
		this.memoryCapacity = memoryCapacity;
		loadState = LoadState.PUSH_INIT;
		this.factoryBufferFunction = factoryQueueFunction;
		bufferMap = new HashMap<Long, Queue<Element>>();
		markSet = new HashSet<Long>();
		// M/4,  ceil(log_B (M/4)), 16 <=  A  
		this.reducedMemory = memoryCapacity / 4; 
		double logLevel = Math.floor(Math.log(reducedMemory/B_IndexNode) / Math.log(B_IndexNode)); 
		firstBufferLevel = (int) Math.max(logLevel, 1.0); 
		rootQueue = factoryQueueFunction.invoke();
		//
		isBufferLevel = new UnaryFunction<Integer, Boolean>() {
			@Override
			public Boolean invoke(Integer level) {
				// if leaf level no buffer 
				// otherwise modulo
				boolean hasBuffer = (level == 0) ? false : 
					(level) % firstBufferLevel == 0;
				return hasBuffer;
			}
		};
		//Note:
//		initWeightFunctions();
	}
	
	/**
	 * 
	 * this method initializes weight functions
	 * 
	 */
	protected void initWeightFunctions(){
		/// init functions 
		minLiveWeight = new BinaryFunction<Integer, Integer, Integer>() {
			
			@Override
			public Integer invoke(Integer param_a, Integer level) {
				if(level == LEAF_LEVEL){
					return D_LeafNode;
				}
				int lowB =  D_IndexNode * (int)(Math.pow(param_a, level)); 
				return lowB;
			}
		};
		//
		minLiveWeightStrong = new BinaryFunction<Integer, Integer, Integer>() {
			
			@Override
			public Integer invoke(Integer param_a, Integer level) {
				if(level == LEAF_LEVEL){
					return (D_LeafNode + (int)(D_LeafNode*EPSILON));
				}
				int lowB =  (D_IndexNode + (int)(D_IndexNode*EPSILON)) * (int)(Math.pow(param_a, level)); 
				return lowB;
			}
		};
		//
		maxLiveWeight = new BinaryFunction<Integer, Integer, Integer>() {
			
			@Override
			public Integer invoke(Integer param_a, Integer level) {
				if(level == LEAF_LEVEL){
					return B_LeafNode;
				}
				int lowB =  B_IndexNode * (int)(Math.pow(param_a, level)); 
				return lowB;
			}
		};;
		//
		maxLiveWeightStrong = new BinaryFunction<Integer, Integer, Integer>() {
			
			@Override
			public Integer invoke(Integer param_a, Integer level) {
				if(level == LEAF_LEVEL){
					return B_LeafNode-(int)(D_IndexNode*EPSILON);
				}
				int lowB =  (B_IndexNode - (int)(D_IndexNode*EPSILON)) * (int)(Math.pow(param_a, level)); 
				return lowB;
			}
		};;
	 	// level ->
		getMaxWeight = new UnaryFunction<Integer, Integer>() {
			@Override
			public Integer invoke(Integer level) {
				return maxLiveWeight.invoke(parameter_A, level);
			}
		};
		//
		violatesWeight = new UnaryFunction<IndexEntry, Boolean>() {

			@Override
			public Boolean invoke(IndexEntry entry) {
				int level = entry.level();
				int wCounter = entry.wCounter;
				int tCounter = entry.tCounter;
				int minLive =  minLiveWeight.invoke(parameter_A, level);
				int maxLive =  maxLiveWeight.invoke(parameter_A, level);
				if(entry == rootEntry && wCounter <= minLive){
					return false; // 
				}
				if( tCounter >= maxLive || wCounter <= minLive ){
					return true;
				}
				return false;
			}
			
		};
		//DEBUG
//		DEBUG("PARAMETERS: " + firstBufferLevel +"; " +  B_IndexNode + " ; " + B_LeafNode + ";" +  "Memory To Push  " + reducedMemory);
//		DEBUG("Level one min cap "+  minLiveWeight.invoke(parameter_A, 1) );
//		DEBUG("Level one max cap "+  maxLiveWeight.invoke(parameter_A, 1) );
	}

	/**
	 * Builds MVBT from scratch. If tree is not empty exception will be thrown. 
	 * If tree is not empty, consider to execute {@link #bulkInsert(Iterator, NullaryFunction, int)} or {@link #insert(Element)} method.
	 * 
	 * @param data is an iterator of #Element objects, they decode an actual tuple, time stamp of operation and the operation type #OperationType.
	 * Note: time stamps are implemented as #LongVersion objects.
	 * @param  factoryQueueFunction
	 * @param memoryCapacity is given by the number of entries, e.g. if entry serialized size is 32 Bytes and the reserved memory for a bulk-loading is 1024 KB 
	 * then mamoryCapacity is equal to (1024*1024)/32.
	 * 
	 * Note: that actual memory size allocated in main memory is greater than the serialized size of entries, depending on JVM. 
	 */
	public void bulkLoad(Iterator<Element> data, NullaryFunction<Queue<Element>> factoryQueueFunction, int memoryCapacity){
		if(rootDescriptor != null || rootEntry != null){
			throw new RuntimeException("The tree is not empty! Please execute bulkInsert method!");
		}
		bulkInsert(data, factoryQueueFunction, memoryCapacity); 
	}
	
	/**
	 * Generic bulk insert (update) method.  After executing this method all buffers are emptied.
	 * 
	 * 
	 * @param data is an iterator of #Element objects, they decode an actual tuple, time stamp of operation and the operation type #OperationType.
	 * Note: time stamps are implemented as #LongVersion objects.
	 * @param  factoryQueueFunction
	 * @param memoryCapacity is given by the number of entries, e.g. if entry serialized size is 32 Bytes and the reserved memory for a bulk-loading is 1024 KB 
	 * then mamoryCapacity is equal to (1024*1024)/32.
	 * 
	 * Note: that actual memory size allocated in main memory is greater than the serialized size of entries, depending on JVM. 
	 */
	public void bulkInsert(Iterator<Element> data, NullaryFunction<Queue<Element>> factoryQueueFunction, int memoryCapacity){
		initForLoad(factoryQueueFunction, memoryCapacity);
		LongVersion minVersion = null;
		Stack<StackInfoObject> bufferOverflowWeightViolationStack = new Stack<MVBTPlus.StackInfoObject>();
		while(data.hasNext()){// put into root buffer
			// update root descriptor
			Element entryToInsert = data.next();
			rootQueue.enqueue(entryToInsert);
			MVSeparator mvSep = getMVSepartor(entryToInsert);
			
			if (rootQueue.size() >= reducedMemory){
				for(int i = 0; i < reducedMemory; i++){
					entryToInsert = rootQueue.dequeue();
					Stack<Triple<IndexEntry, Node, Boolean>> stack =  pushEntry(entryToInsert, (IndexEntry)rootEntry, null, bufferOverflowWeightViolationStack);
					updatePath(stack);
				}
				processWorkStack(bufferOverflowWeightViolationStack);
			} 
			if(rootDescriptor == null && rootEntry == null){
				rootDescriptor =  createMVRegion(mvSep.insertVersion(), null, this.keyDomainMinValue , mvSep.sepValue());
				rootEntry = createIndexEntry(LEAF_LEVEL+1); // also creates buffer if necessery 
				Node firstNode = (Node)createNode(LEAF_LEVEL);
				Object id = container().reserve(new Constant<Node>(firstNode));
				((IndexEntry)rootEntry).initialize(id, toMVSeparator((MVRegion)rootDescriptor));
				rootEntry.update(firstNode);
			}else{
				rootDescriptor.union(mvSep);
			}
			if (minVersion == null){
				minVersion = (LongVersion)entryToInsert.getElement2().clone();
			}
			
		}
		// process last portion of buffer
		for( ;!rootQueue.isEmpty(); ){
			Element entryToInsert = rootQueue.dequeue();
			currentVersion = entryToInsert.getElement2();
			Stack<Triple<IndexEntry, Node, Boolean>> stack =  pushEntry(entryToInsert, (IndexEntry)rootEntry, null, bufferOverflowWeightViolationStack);
			updatePath(stack);
		}
		processWorkStack(bufferOverflowWeightViolationStack);
		// clear All
		loadState = LoadState.PUSH_ALL;
		pushAllBuffers((IndexEntry)rootEntry);
		IndexEntry currentRoot = (IndexEntry)rootEntry;
//		List<IndexEntry> overflowChainMain = computeOveflowChain(currentRoot, null);
//		for(IndexEntry idxEntry:  overflowChainMain){
//			MVRegion mvreg= toMVRegion((MVSeparator)((IndexEntry)idxEntry).separator());
//			 Root newOldRoot= new Root(mvreg, idxEntry.id(), idxEntry.parentLevel());
//			 roots.insert(newOldRoot);
//		}
		((Lifespan)roots.rootDescriptor()).updateMinBound(minVersion);
		((Lifespan)roots.rootDescriptor()).updateMaxBound(currentRoot.getInsertVersion());
		loadState = LoadState.PUSH_ALL;
	}
	
	/**
	 * this method is called for emptying all buffers after the input iterator is completely consumed.
	 * 
	 * @param rootEntry
	 */
	protected void pushAllBuffers(IndexEntry rootEntry){
		java.util.Queue<IndexEntry> queue = new LinkedList<>();
		queue.offer(rootEntry); 
		for(IndexEntry rEntry = null ; !queue.isEmpty() ;){
			rEntry = queue.poll(); 
			if(rEntry.level() >= firstBufferLevel){
				//
				if(isBufferIndexEntry(rEntry) &&  bufferMap.containsKey((Long)rEntry.id())){
					ReorgInfo info = new ReorgInfo(SplitTocken.BUFFER_FULL, new LongVersion(Long.MAX_VALUE), null, false);
//					if(VERBOSE){
//						debug_buffer_bufferAll++;
//					}
					pushDownBuffer(rEntry, info, reducedMemory, true);
				}
				Node node = (Node) rEntry.get(true);
				for(Iterator currentEntries = node.entries(); currentEntries.hasNext(); ){
					IndexEntry entry = (IndexEntry)currentEntries.next();
					queue.offer(entry); // next level 
				}
			}
		}
	}

	/**
	 * this method processes overflow buffer node stack 
	 * @param bufferOverflowWeightViolationStack
	 */
	protected void processWorkStack(Stack<StackInfoObject> bufferOverflowWeightViolationStack){
		while(!bufferOverflowWeightViolationStack.empty()){
			StackInfoObject stackInfo = bufferOverflowWeightViolationStack.pop();
			// delete from markSet
			if(markSet.contains(stackInfo.getElement1().id())){
				processStackObject(stackInfo);
				markSet.remove(stackInfo.getElement1().id());
			}
		}
	}
	
	/**
	 * marks buffer as processed
	 * @param id
	 */
	protected void removeFromStack(Long id){
		markSet.remove(id);
	}
	
	/**
	 * processes single buffer node. 
	 * 
	 * @param infoObject
	 */
	protected void processStackObject(StackInfoObject infoObject){
		IndexEntry currentRoot = infoObject.getElement1();
		ReorgInfo reorgInfo = infoObject.getElement4();
		pushDownBuffer(currentRoot, reorgInfo, reducedMemory, false);
	}
	
	/**
	 * processes elements from a single node buffer 
	 *  
	 * @param currentRoot 
	 * @param info object what reorganization operation should be executed 
	 * @param maxSize maximal number of elements pushed down
	 * @param all indicates if buffer should be completely emptied.
	 */
	protected void pushDownBuffer(IndexEntry currentRoot, ReorgInfo info, int maxSize, boolean all){
		Queue<Element> buffer = bufferMap.get(currentRoot.id());
//		if(buffer == null)
//			DEBUG("No buffer " + currentRoot );
		Stack<StackInfoObject> bufferOverflowWeightViolationStack = new Stack<MVBTPlus.StackInfoObject>();
//		if(VERBOSE && buffer.size() > 0 ){
//			debug_flush_buffers++;
//		}
		Node node = (Node) currentRoot.get(false);
		int minSize = Math.min(reducedMemory, buffer.size());
		for(int i=0; i < minSize; i++){// for each entry find  next down entry and push 
			Element element = buffer.dequeue(); // find next buffer entry
			MVSeparator elementMVSeparator = getMVSepartor(element);
			IndexEntry downRoot = (IndexEntry) node.chooseSubtree(elementMVSeparator);
			Stack<Triple<IndexEntry, Node, Boolean>> path = pushEntry(element, downRoot,  node, bufferOverflowWeightViolationStack); 
			updatePath(path);
		}
		processWorkStack(bufferOverflowWeightViolationStack); 
		if(all){
			int size = buffer.size();
			for(int i=0; i < size; i++){// for each entry find  next down entry and push 
				Element element = buffer.dequeue(); // find next buffer entry
				MVSeparator elementMVSeparator = getMVSepartor(element);
				IndexEntry downRoot = (IndexEntry) node.chooseSubtree(elementMVSeparator);
				Stack<Triple<IndexEntry, Node, Boolean>> path = pushEntry(element, downRoot,  node, bufferOverflowWeightViolationStack); 
				updatePath(path);
			}
			processWorkStack(bufferOverflowWeightViolationStack); 
		}
		currentRoot.update(node, true); // 
	}
	
	/**
	 * Enqueues element to a node buffer
	 * 
	 * @param element
	 * @param rootEntry
	 */
	protected void appendToBuffer(Element element, IndexEntry rootEntry) {
		Queue<Element> buffer = bufferMap.get((Long)rootEntry.id());
		if(buffer == null){
			buffer = factoryBufferFunction.invoke();
			bufferMap.put((Long)rootEntry.id(), buffer);
		}
		buffer.enqueue(element);
	}

	/**
	 * pushes an element towards leaf nodes.
	 * 
	 * @param element
	 * @param rootEntry
	 * @param pNode
	 * @param bufferOverflowWeightViolationStack
	 * @return
	 */
	protected Stack<Triple<IndexEntry, Node, Boolean>> pushEntry(Element element, IndexEntry rootEntry,  Node pNode, 	
			Stack<StackInfoObject> bufferOverflowWeightViolationStack){
		Stack<Triple<IndexEntry, Node, Boolean>> path = new Stack<>();
		IndexEntry currentEntry = rootEntry;
		Node currentNullNode = null;
		Node parentNode = pNode;
		boolean down = true;
		IndexEntry parentNodeEntry = null;
		path.push(new Triple<>(currentEntry, currentNullNode, false));
		while(down){
			Quadruple<Pair<IndexEntry, Node>, Pair<IndexEntry, Node>, Boolean, Pair<IndexEntry, Node>> newPathEntry =  null;
			boolean violatesWeight = violatesWeight(currentEntry);
			boolean bufferFull = isBufferFull(currentEntry);
			if(violatesWeight){ // mark and push to 
				MVSeparator mvSeparator = getMVSepartor(element); 
				ReorgInfo reorgInfo = computeReorgInfo(element, currentEntry, parentNode);
				if(isBufferIndexEntry(currentEntry)){
					boolean all = true;
					switch(reorgInfo.getElement1()){ 
					case MERGE : {}; // flush neighbours buffer
					case MERGE_KEY_SPLIT: { // XXX FALL DOWN 
						IndexEntry neighbourEntry = reorgInfo.getElement3(); // neighbour
						// remove from working path
						removeFromStack((Long)neighbourEntry.id());
						pushDownBuffer(neighbourEntry, reorgInfo, reducedMemory, all); 
					}; 
					default:{ 
						removeFromStack((Long)currentEntry.id());
						pushDownBuffer(currentEntry, reorgInfo, reducedMemory, all);
					} break;
					}
				}
				newPathEntry =  reorganize(currentEntry, parentNode, reorgInfo); // XXX
				path.pop().getElement1().unfix(); 
				Triple<MVBTPlus.IndexEntry, MVBTPlus.Node, Boolean> newPathTriple = 
						new Triple<MVBTPlus.IndexEntry, MVBTPlus.Node, Boolean>(newPathEntry.getElement1().getElement1(), 
						newPathEntry.getElement1().getElement2(), true);
				if(reorgInfo.getElement1() == SplitTocken.KEY_SPLIT || reorgInfo.getElement1() == SplitTocken.MERGE_KEY_SPLIT){
					//update 
					MVSeparator highKeyEntry = ((MVSeparator)newPathEntry.getElement2().getElement1().separator());
					if(mvSeparator.sepValue().compareTo(highKeyEntry.sepValue()) >= 0){ // check 
						newPathTriple = new Triple<MVBTPlus.IndexEntry, MVBTPlus.Node, Boolean>(newPathEntry.getElement2().getElement1(), 
								newPathEntry.getElement2().getElement2(), true);
						//update
						currentEntry = newPathEntry.getElement2().getElement1();
						newPathEntry.getElement1().getElement1().update(newPathEntry.getElement1().getElement2(), true);
					}else{//update
						currentEntry = newPathEntry.getElement1().getElement1();
						newPathEntry.getElement2().getElement1().update(newPathEntry.getElement2().getElement2(), true);
					}
					
					if(newPathEntry.getElement4() != null){
						// update the counter of new root since one operation is missed
						newPathEntry.getElement4().getElement1().updateCounter(element);
					}
				}else{
					currentEntry = newPathEntry.getElement1().getElement1();
				}
				path.push(newPathTriple);
			}else if (bufferFull){ // mark and push to work stack
				if(!markSet.contains(currentEntry.id())){
					ReorgInfo reorgInfo = computeReorgInfo(element, currentEntry, parentNode);
					bufferOverflowWeightViolationStack.push(new StackInfoObject(currentEntry, parentNodeEntry , parentNode, reorgInfo));
					markSet.add((Long)currentEntry.id());
				}
			}
			currentEntry.updateCounter(element); // update weight 
			if(isBufferIndexEntry(currentEntry)){// append to buffer
				appendToBuffer(element, currentEntry); // also creates new buffer if not exists
				down = false;	// stop 
			}else if (isLeafEntry(currentEntry)){ // insert into leaf
				if(path.peek().getElement2() == null) {
					Node node = (Node)currentEntry.get(false); 
					path.peek().setElement2(node);
				}
				path.peek().getElement2().growLeafNode(element);
				path.peek().setElement3(true);
				down = false; // stop
			}else{// step down
				MVSeparator mvSeparator = getMVSepartor(element);
				// check if there node exists
				if(path.peek().getElement2() == null) {
					Node node = (Node)currentEntry.get(false); // 
					path.peek().setElement2(node);
				}
				parentNodeEntry = currentEntry;
				IndexEntry downRoot = (IndexEntry) path.peek().getElement2().chooseSubtree(mvSeparator);
				currentEntry = downRoot;
				parentNode = path.peek().getElement2(); // set new parent node
				path.push(new Triple<>(currentEntry, currentNullNode, false)); // 
			}
		}
		return path;
	}
	
	/**
	 * updates(writes dirty pages (nodes) to a container) path 
	 * @param path
	 */
	protected void updatePath(Stack<Triple<IndexEntry, Node, Boolean>> path){
		while(!path.isEmpty()){
			Triple<IndexEntry, Node, Boolean> pathEntry = path.pop();
			//  always update pathEntry.getElement3() &&
			if( pathEntry.getElement2() != null ){
				handlePhysicalOverflow(pathEntry.getElement1(), pathEntry.getElement2());
				pathEntry.getElement1().update(pathEntry.getElement2(), true);
			}
		}
	}
	
	/**
	 * conducts a local structure reorganization such as merge, key-split, node copy or merge key split
	 * @param currentEntry
	 * @param parentNode
	 * @param reorgInfo
	 * @return
	 */
	protected Quadruple<Pair<IndexEntry, Node>, Pair<IndexEntry, Node>, Boolean, Pair<IndexEntry, Node>>  reorganize(IndexEntry currentEntry,
			Node parentNode,
			ReorgInfo reorgInfo) {
		int currentLevel = currentEntry.level(); 
		SplitTocken tocken = reorgInfo.getElement1();
		LongVersion deleteVersion = reorgInfo.getElement2();
		Pair<IndexEntry, Node> newNodeEntryPair  = null;
		Pair<IndexEntry, Node> newNeighbourPair = null;
		Pair<IndexEntry, Node> newSiblingKeyPair = null;
		Pair<IndexEntry, Node> newParentNodePair = null;  
		newNodeEntryPair = versionCopy(currentEntry,  reorgInfo, currentLevel);
//		if (VERBOSE){
//			debug_reorgs++;
//			boolean bufferEntry = isBufferIndexEntry(currentEntry); 
//			if( bufferEntry){
//				debug_buffer_reorg++;
//			}
//			boolean leaf = isLeafEntry(currentEntry);
//			switch(tocken){
//				case VERSION_SPLIT : {
//					debug_timeSplits++;
//					if(bufferEntry){
//						debug_buffer_timeSplits++;
//					}
//					
//					if(leaf){
//						debug_leafnodesCreated++;
//					}
//				} break;
//				case KEY_SPLIT:{ 
//					debug_keySplits++;
//					if(bufferEntry){
//						debug_buffer_keySplits++;
//					}
//					if(leaf){
//						debug_leafnodesCreated+=2;
//					}
//				}break;
//				case MERGE:{
//					debug_merges++;
//					if(bufferEntry){
//						debug_buffer_merges++;
//					}
//					if(leaf){
//						debug_leafnodesCreated++;
//					}
//				}break;
//				case MERGE_KEY_SPLIT:{
//					debug_mergeKeySplits++;
//					if(bufferEntry){
//						debug_buffer_mergeKeySplits++;
//					}
//					if(leaf){
//						debug_leafnodesCreated+=2;
//					}
//				} ; break;
//				default:{
//					
//				} break;
//			} // post processing
//		}
		switch(tocken){
			case MERGE_KEY_SPLIT:{
				newNeighbourPair  = merge(newNodeEntryPair,  reorgInfo, currentLevel);
			} // XXX FALL DOWN 
			case KEY_SPLIT:{ 
				newSiblingKeyPair = keySplit(newNodeEntryPair,  reorgInfo, currentLevel);
			}break;
			case MERGE:{
				newNeighbourPair  = merge(newNodeEntryPair,  reorgInfo, currentLevel);
			}break;
			default: break;
		} // post processing
		if(currentLevel== LEAF_LEVEL){
			// currentEntry is old entry
			linkLeafNodes(currentEntry, 
					(newSiblingKeyPair==null )? null: newSiblingKeyPair.getElement1(),
					(newNeighbourPair == null)? null: newNeighbourPair.getElement1(),
					 newNodeEntryPair.getElement2(),  
					(newSiblingKeyPair==null ) ? null: newSiblingKeyPair.getElement2(), reorgInfo); 
		}// delete current entry 
		((MVSeparator)currentEntry.separator()).delete(deleteVersion); // XXX logical delete 
		if(parentNode != null){
			parentNode.removeFromLiveListAddToOldList(currentEntry);
		}
		// release queue
		releaseQueue((Long)currentEntry.id());
		if(newNeighbourPair != null){
			IndexEntry neighbourEntry = newNeighbourPair.getElement1();
			((MVSeparator)neighbourEntry.separator()).delete(deleteVersion); // XXX logical delete
			parentNode.removeFromLiveListAddToOldList(neighbourEntry); 
		}// post new entries to the parenmt node!
		if(parentNode == null){
			// if multiple operations are allowed in one version, a root with an
			// empty lifespan may be generated. It has to be discarded.
			int oldRootParentLevel= currentEntry.parentLevel(); // currentEntry is rootEntry
			Object oldRootId= currentEntry.id();
			MVRegion oldRootReg= toMVRegion((MVSeparator)((IndexEntry)currentEntry).separator());
//			List<IndexEntry> overflowChainMain = computeOveflowChain(currentEntry, null);
			List<IndexEntry> overflowChainMain = new ArrayList<>();
			if(newSiblingKeyPair!=null){ // key split tree grows level update
				int rootWeight = newNodeEntryPair.getElement1().wCounter + newSiblingKeyPair.getElement1().wCounter; 
				Node rootNode = (Node)createNode(height());
				rootEntry = createIndexEntry(currentEntry.parentLevel()+1); //
				MVSeparator rootSeparator =(MVSeparator)newNodeEntryPair.getElement1().separator.clone(); // 
				((IndexEntry)rootEntry).initialize(toMVSeparator((MVRegion)rootDescriptor));
				Object newRootNodeId= container().reserve(new Constant(null));
				((IndexEntry)rootEntry).initialize(newRootNodeId, rootSeparator);
				rootNode.growIndexNode(newNodeEntryPair.getElement1());
				rootNode.growIndexNode(newSiblingKeyPair.getElement1());
				((IndexEntry)rootEntry).setWeights(rootWeight, rootWeight);
				rootEntry.update(rootNode, true);
				newParentNodePair = new Pair<MVBTPlus.IndexEntry, MVBTPlus.Node>((IndexEntry)rootEntry, rootNode);
			}else{ 
				rootEntry = newNodeEntryPair.getElement1(); // just emerge in time dimension 
			} // prepare historical root
			oldRootReg.updateMaxBound(((MVRegion)rootDescriptor).maxBound());
			Root oldRoot= new Root(oldRootReg, oldRootId, oldRootParentLevel);
			if (!oldRootReg.lifespan().isPoint()) {//FIXME check this 
				roots.insert(oldRoot);
				currentVersion = reorgInfo.getElement2();
				((Lifespan)roots.rootDescriptor()).updateMaxBound(currentVersion);
			}
			for(IndexEntry idxEntry: overflowChainMain){
				MVRegion mvreg= toMVRegion((MVSeparator)((IndexEntry)idxEntry).separator());
				 Root newOldRoot= new Root(mvreg, idxEntry.id(), idxEntry.parentLevel());
				 roots.insert(newOldRoot);
			}
		}
		else{ // 
			if (currentLevel > LEAF_LEVEL){
				List<IndexEntry> overflowChainMain = new ArrayList<>();
				List<IndexEntry> overflowChainNeighbour = new ArrayList<>();
//				overflowChainMain = computeOveflowChain(currentEntry, parentNode);
//				if (newNeighbourPair != null){
//					overflowChainNeighbour = computeOveflowChain(reorgInfo.getElement3(), parentNode);
//				}
				for(IndexEntry idx: overflowChainMain){
					parentNode.growIndexNode(idx);
				}
				for(IndexEntry idx: overflowChainNeighbour){
					parentNode.growIndexNode(idx);
				}
			}
			
			parentNode.growIndexNode(newNodeEntryPair.getElement1());
			if(newSiblingKeyPair!=null){ // in case of key split
				parentNode.growIndexNode(newSiblingKeyPair.getElement1());
			}
		}
		// delete old buffer 
		return new Quadruple<>(newNodeEntryPair, newSiblingKeyPair, true, newParentNodePair);
	}
	
	/**
	 * removes a link between buffer queue and buffer node
	 *  
	 * @param id
	 */
	protected void releaseQueue(Long id){
		bufferMap.remove(id);
	}
	
	/**
	 * executes version copy (node copy) 
	 * 
	 * @param entry
	 * @param reorgInfo
	 * @param level
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Pair<IndexEntry, Node> versionCopy(IndexEntry entry, Quadruple<SplitTocken, LongVersion, IndexEntry, Boolean> reorgInfo, int level){
		MVSeparator separator = (MVSeparator) entry.separator.clone();
		separator.setInsertVersion((LongVersion)reorgInfo.getElement2().clone()); // set insert version!
		separator.setDeleteVersion(null);// live
		Node dataNode = (Node)entry.get(true);
		Node tempNode = (Node) createNode(dataNode.level());
		Iterator liveData = dataNode.query(new Lifespan(reorgInfo.getElement2()));
		while(liveData.hasNext()){
			Object liveEntry = liveData.next();
			liveEntry = copyEntry(liveEntry);
			if(level == LEAF_LEVEL){
//				((Node)tempNode).growLeafNode((LeafEntry)liveEntry);
				((Node)tempNode).getDataEntries().add(liveEntry);
			}else{
//				((Node)tempNode).growIndexNode((IndexEntry)liveEntry);
				((Node)tempNode).getLiveEntries().add(liveEntry);
			}
		}
		IndexEntry newEntry =  (IndexEntry) createIndexEntry(tempNode.level() + 1); // 
		Object id = container().reserve(new Constant<Node>(tempNode)); // only reserve // check update function
		newEntry.initialize(id, separator);	//set weights 
		newEntry.setWeights(entry.wCounter, entry.wCounter); // set only live counter!!! 
		return new Pair<IndexEntry,Node>(newEntry, tempNode);
	}
	
	/**
	 * conducts node merge operation 
	 *  
	 * @param currentNodePair
	 * @param reorgInfo
	 * @param level
	 * @return
	 */
	protected Pair<IndexEntry, Node> merge(Pair<IndexEntry, Node> currentNodePair, 
			Quadruple<SplitTocken, LongVersion, IndexEntry, Boolean> reorgInfo, int level){  
		IndexEntry neighbourEntry = reorgInfo.getElement3();
		Node neighbourNodeMVBT = (Node)neighbourEntry.get(true); ////get neighbour node mvbt
		int sumWeight = 0;
		Node newNode = currentNodePair.getElement2();
		if(level == LEAF_LEVEL){
			Iterator liveDataFromNeighbour = neighbourNodeMVBT.query(new Lifespan(reorgInfo.getElement2())) ;
			while(liveDataFromNeighbour.hasNext()){ // Copy Data
				Object liveEntry = liveDataFromNeighbour.next();
				liveEntry = copyEntry(liveEntry);
				if(level == LEAF_LEVEL){
					sumWeight++;
					newNode.growLeafNode((LeafEntry)liveEntry);
//					newNode.getDataEntries().add((LeafEntry)liveEntry);
				}else{
					IndexEntry indexEntry = (IndexEntry)liveEntry;
					sumWeight += indexEntry.wCounter; // FIXME
					newNode.growIndexNode(indexEntry);
//					newNode.getLiveEntries().add(indexEntry);
				}
			}
		}else{
			List<IndexEntry>  newLiveEntries = new ArrayList<>(); 
			Iterator<IndexEntry> mergedEntries = new Merger<IndexEntry>(liveIndexEntryComparator, newNode.liveEntries.iterator(), 
					neighbourNodeMVBT.liveEntries.iterator()); 
			while(mergedEntries.hasNext()){
				IndexEntry idxEntry = mergedEntries.next();
				sumWeight += idxEntry.wCounter;
				newLiveEntries.add(idxEntry);
			}
			newNode.liveEntries = newLiveEntries; 
		}
		
		boolean lowerKey = reorgInfo.getElement4();
		IndexEntry newIndexEntry = currentNodePair.getElement1();
		int currentWeight = (level == LEAF_LEVEL) ? newIndexEntry.wCounter +sumWeight : sumWeight;
		newIndexEntry.setWeights(currentWeight, currentWeight); // FIXME 
		if(lowerKey){// copy separator if the node is merged with loewer key neighbour
			newIndexEntry.separator = (MVSeparator)neighbourEntry.separator.clone();
			newIndexEntry.getMVSeparator().setInsertVersion((LongVersion)reorgInfo.getElement2().clone());
			newIndexEntry.getMVSeparator().setDeleteVersion(null);
		}
		return new Pair<MVBTPlus.IndexEntry, MVBTPlus.Node>(neighbourEntry,neighbourNodeMVBT); //
	}
	
	/**
	 * conducts node key split operation
	 * 
	 * @param currentNodePair
	 * @param reorgInfo
	 * @param level
	 * @return
	 */
	@SuppressWarnings({ "deprecation", "unchecked" })
	protected Pair<IndexEntry, Node> keySplit(Pair<IndexEntry, Node> currentNodePair, 
			Quadruple<SplitTocken, LongVersion, IndexEntry, Boolean> reorgInfo, int level){
		Node keyNodeMVBT = (Node) createNode(level);
		IndexEntry keySplitEntry = null;
		///START///
		IndexEntry newTempIndexEntry = currentNodePair.getElement1();
		Node newTempNode = currentNodePair.getElement2();
		newTempNode.sortKeyDimension();
		int oldSumWeight =newTempIndexEntry.wCounter;
		int index = newTempNode.getDataEntries().size()/ 2;
		int sumWeight = 0;
		List<Object> sublist = null;  newTempNode.getDataEntries().subList(index, newTempNode.getDataEntries().size());
		if(level == LEAF_LEVEL){
			sublist = newTempNode.getDataEntries().subList(index, newTempNode.getDataEntries().size());
			sumWeight = sublist.size();
			
			for(Object obj : sublist){
				keyNodeMVBT.getDataEntries().add(obj);
				if(level != LEAF_LEVEL){
					IndexEntry indexEntry = (IndexEntry)obj;
					sumWeight += indexEntry.wCounter; 
				}
			}
		}else{ // find balanced split
			index = 0;
			int weight = 0;
			int maxWeight = getMaxWeight.invoke(level);
			for(int i = 0; i < newTempNode.getLiveEntries().size(); i++){
				index++;
				weight+= ((IndexEntry)newTempNode.getLiveEntries().get(i)).wCounter;
				if (weight > maxWeight/2 )
					break;
			}
			sublist = newTempNode.getLiveEntries().subList(index, newTempNode.getLiveEntries().size());
			for(Object obj : sublist){
				keyNodeMVBT.getLiveEntries().add(obj);
				if(level != LEAF_LEVEL){
					IndexEntry indexEntry = (IndexEntry)obj;
					sumWeight += indexEntry.wCounter; 
				}
			}
		}
		sublist.clear();
		///END///
		keySplitEntry = (IndexEntry) createIndexEntry(level + 1); 
		MVSeparator keySeparator = (MVSeparator) newTempIndexEntry.separator.clone();
		keySeparator.setInsertVersion((LongVersion)reorgInfo.getElement2().clone()); // check !
		keySeparator.setDeleteVersion(null);
		Object firstEntry = (level > LEAF_LEVEL) ? keyNodeMVBT.getLiveEntries().get(INDEX_FIRST) :
			keyNodeMVBT.getDataEntries().get(INDEX_FIRST);
		Comparable key = null;
		if(level == LEAF_LEVEL){
			LeafEntry leafEntry = (LeafEntry)firstEntry;
			key = key(leafEntry.data());
		}else{
			IndexEntry indexEntry = (IndexEntry)firstEntry;
			key = indexEntry.separator().sepValue();
		}
		keySeparator.updateSepValue(key);
		Object id = container().reserve(new Constant<Node>(keyNodeMVBT)); /// check update function
		keySplitEntry.initialize(id, keySeparator);	//set weights 
		keySplitEntry.setWeights(sumWeight, sumWeight); // 
		newTempIndexEntry.setWeights(oldSumWeight-sumWeight, oldSumWeight-sumWeight);
		return new Pair<MVBTPlus.IndexEntry, MVBTPlus.Node>(keySplitEntry, keyNodeMVBT);
	}
	
//	/**
//	 * Auxiliary method. in this implementation
//	 * 
//	 * @param entry
//	 * @param parentNode
//	 * @return
//	 */
//	protected List<IndexEntry> computeOveflowChain(final IndexEntry entry,  Node parentNode){
//		List<IndexEntry> overflowChain = new ArrayList<>();
//		return overflowChain;
//	}
	
	/**
	 * this method extends node with additional pages.  
	 * 
	 * @param currentLiveEntry
	 * @param currentLiveNode
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	protected List<IndexEntry> handlePhysicalOverflow(IndexEntry currentLiveEntry, Node currentLiveNode){
		// check if the number of elements > than B 
		// leave only live elements in the node
		// allocate historical node
		// link the node 
		List<IndexEntry> newEntries = new ArrayList<>();
		if(!currentLiveNode.isLeaf() && isPhysicalOverflow(currentLiveNode)){ //
			//DEBUG--
			//DEBUG("Physical Overflow " + currentLiveEntry.toString());
			//DEBUG--
			Node overflowNode = currentLiveNode;
			IndexEntry temporalPredecessor = (overflowNode.predecessors().isEmpty()) ? null:  (IndexEntry) overflowNode.predecessors().get(INDEX_FIRST);
			Pair<List<IndexEntry>, LongVersion> temporalInfo = overflowNode.removeOldIndexEntries(); 
			List<IndexEntry> oldEntries = temporalInfo.getElement1(); // sorted on time
			LongVersion maxDeadVersion = temporalInfo.getElement2();
			//update  
			LongVersion histNewOldVersion  = (LongVersion)currentLiveEntry.getInsertVersion();
			//update version
			if(temporalPredecessor !=null){
				//get historical node
				Node histNode = (Node) temporalPredecessor.get(false);
				// check if the place in the node to put entries -> pack 
				int numberToPack =  B_IndexNode-histNode.number();
				histNewOldVersion = (LongVersion) temporalPredecessor.getDeleteVersion();
				if (numberToPack > 0){
					//DEBUG("More than Physical Overflow ");
					List<IndexEntry> subList = oldEntries.subList(0, numberToPack);
					for(IndexEntry entry : subList){
						histNode.growIndexNode(entry);
					}//
					histNewOldVersion = (LongVersion)((MVSeparator)subList.get(subList.size()-1).separator).deleteVersion().clone();
					// update index entry 
					temporalPredecessor.getMVSeparator().lifespan().updateMaxBound(histNewOldVersion);
					subList.clear(); // deletes entries from oldEntries
				}
			}
			if(!oldEntries.isEmpty()){
				// allocate new node
				Node newHistNode = (Node) createNode(currentLiveNode.level());
				IndexEntry newIndexEntry = (IndexEntry) createIndexEntry(newHistNode.level()+1);
				Object id = container().reserve(new Constant<Node>(null));
				MVSeparator keySeparator = (MVSeparator)currentLiveEntry.separator.clone();
				keySeparator.lifespan().delete(maxDeadVersion); 
				keySeparator.lifespan().updateMinBound(histNewOldVersion);
				newIndexEntry.initialize(id, keySeparator);
				for(IndexEntry entry : oldEntries){
					newHistNode.growIndexNode(entry);
				}
				if(temporalPredecessor != null){
					newHistNode.predecessors().add(temporalPredecessor);
				}
				newIndexEntry.update(newHistNode, true);
				//link
				if(overflowNode.predecessors().isEmpty()){
					overflowNode.predecessors().add(newIndexEntry);
				}else{
					overflowNode.predecessors().set(0, newIndexEntry);
				}
				newEntries.add(newIndexEntry);
				return newEntries;
			}
		}
		return newEntries;
	}
	
	/**
	 * computes information about a structure reorganization information e.g. merge, key split, node copy etc
	 *  
	 * @param element
	 * @param currentEntry
	 * @param parentNode
	 * @return
	 */
	protected ReorgInfo computeReorgInfo(Element element, 
			IndexEntry currentEntry, Node parentNode) {
		ReorgInfo reorgInfo = new ReorgInfo(SplitTocken.BUFFER_FULL, element.getElement2(), null, false); 
		int wCounter = currentEntry.wCounter;
		int tCounter = currentEntry.tCounter;
		int level = currentEntry.level();
		int minLive =  minLiveWeight.invoke(parameter_A, level);
		int maxLive =  maxLiveWeight.invoke(parameter_A, level);
		int minLiveStrong =  minLiveWeightStrong.invoke(parameter_A, level);
		int maxLiveStrong =  maxLiveWeightStrong.invoke(parameter_A, level);
		SplitTocken tocken = null;
		if(wCounter >=  maxLiveStrong &&  tCounter >= maxLive){ // key split
			tocken = SplitTocken.KEY_SPLIT;
			reorgInfo = new ReorgInfo(tocken, element.getElement2(), null, false);
		}
		else if (parentNode == null || parentNode.number() == 1){// FIXME kann so was passieren?
			tocken = SplitTocken.VERSION_SPLIT;
			reorgInfo = new ReorgInfo(tocken, element.getElement2(), null, false);
		}
		else if( minLiveStrong < wCounter   &&   wCounter < maxLiveStrong &&  tCounter >= maxLive ){ // version split
			tocken = SplitTocken.VERSION_SPLIT;
			reorgInfo = new ReorgInfo(tocken, element.getElement2(), null, false);
		}else if(wCounter <= minLive || (tCounter >= maxLive && wCounter <= minLiveStrong) ){ // find neighbour
			IndexEntry neighbour = parentNode.findNeighbour(currentEntry);
			boolean lowKey = neighbour.getMVSeparator().sepValue().compareTo(currentEntry.getMVSeparator().sepValue()) < 0;
			int wCounterNeighbour = neighbour.wCounter;
			if(wCounter + wCounterNeighbour >= maxLiveStrong){
				tocken = SplitTocken.MERGE_KEY_SPLIT;
				reorgInfo = new ReorgInfo(tocken, element.getElement2(), neighbour, lowKey);
			}else{
				tocken = SplitTocken.MERGE;
				reorgInfo = new ReorgInfo(tocken, element.getElement2(), neighbour, lowKey);
			}
		}
		return reorgInfo;
	}
	
	/**
	 * returns true on node weight violation
	 * 
	 * @param indexEntry
	 * @return
	 */
	protected boolean violatesWeight(IndexEntry indexEntry){
		return this.violatesWeight.invoke(indexEntry);
	}
	
	/**
	 * returns true if buffer should be emptied
	 *  
	 * @param currentEntry
	 * @return
	 */
	protected boolean isBufferFull(IndexEntry currentEntry) {
		if(bufferMap.containsKey((Long)currentEntry.id())){
			Queue<Element> buffer = bufferMap.get((Long)currentEntry.id());
			return buffer.size() > reducedMemory;
		}	
		return false;
	}
	
	/**
	 *  returns true on node overflow
	 */
	protected boolean isPhysicalOverflow(Node node) {
		if(node.isLeaf()){
			return node.number() > B_LeafNode;
		}
		return node.number() > B_IndexNode;
	}

	/**
	 * creates {@link MVSeparator} from {@link Element}
	 * 
	 * @param element
	 * @return
	 */
	protected MVSeparator getMVSepartor(Element element){
		Lifespan lifespan = new Lifespan((Version)element.getElement2().clone());
		LeafEntry leafentry =  new LeafEntry(lifespan, element.getElement1());
		return (MVSeparator) this.separator(leafentry);
	}
	
	/**
	 * returns true if entry points to node on buffer level
	 *  
	 * @param entry
	 * @return
	 */
	protected boolean isBufferIndexEntry(IndexEntry entry){
		boolean hasBuffer = isBufferLevel.invoke(entry.parentLevel()-1);
//		old code
//		boolean hasBuffer = (entry.parentLevel()-1 == 0) ? false : 
//			(entry.parentLevel()-1) % firstBufferLevel == 0;
		return hasBuffer && entry != rootEntry;
	}
	
	/**
	 * extracts key from {@link Element}
	 *  
	 * @param element
	 * @return
	 */
	protected Comparable getKeyFromElement(Element element){
		return this.key(element.getElement1()); 
	}
	
	/**
	 * returns true if currenEntry points to leaf
	 * @param currentEntry
	 * @return
	 */
	protected boolean isLeafEntry(IndexEntry currentEntry) {
		return currentEntry.level() == 0;
	}
	
	/**
	 * creates predecessors link for leaf nodes
	 * 
	 * @param tocken
	 */
	@SuppressWarnings("unchecked")
	protected void linkLeafNodes(IndexEntry oldEntry, IndexEntry keySplitEntry, IndexEntry neighbourIndexEntry,
			Node tempNode,  Node keyNodeMVBT, Quadruple<SplitTocken, LongVersion, IndexEntry, Boolean> reorgInfo){
		SplitTocken tocken = reorgInfo.getElement1();
		if(tocken == SplitTocken.VERSION_SPLIT){
			tempNode.predecessors().add(oldEntry);
		}else if(tocken == SplitTocken.MERGE){
			tempNode.predecessors().add(oldEntry);
			tempNode.predecessors().add(neighbourIndexEntry);
		}else if(tocken == SplitTocken.KEY_SPLIT){
			tempNode.predecessors().add(oldEntry);
			keyNodeMVBT.predecessors().add(oldEntry);
		}else if (tocken ==  SplitTocken.MERGE_KEY_SPLIT){
			// check the key
			boolean lowerKey = reorgInfo.getElement4();
			if(lowerKey){
				Comparable keySplit = keySplitEntry.separator.sepValue();
				Comparable oldKey = oldEntry.separator.sepValue();
				if (keySplit.compareTo(oldKey)  > 0){
					keyNodeMVBT.predecessors().add(oldEntry);
					tempNode.predecessors().add(neighbourIndexEntry);
					tempNode.predecessors().add(oldEntry);
				}else if (keySplit.compareTo(oldKey)  < 0){
					// key split node get two predecessors
					keyNodeMVBT.predecessors().add(oldEntry);
					keyNodeMVBT.predecessors().add(neighbourIndexEntry);
					tempNode.predecessors().add(neighbourIndexEntry);
				}else if (keySplit.compareTo(oldKey) == 0){
					tempNode.predecessors().add(neighbourIndexEntry);
					keyNodeMVBT.predecessors().add(oldEntry);
				}
			}else{
				Comparable keySplit = keySplitEntry.separator.sepValue();
				Comparable oldKey = neighbourIndexEntry.separator.sepValue();
				if (keySplit.compareTo(oldKey)  > 0){
					// key split node get two predecessors
					keyNodeMVBT.predecessors().add(neighbourIndexEntry);
					tempNode.predecessors().add(oldEntry);
					tempNode.predecessors().add(neighbourIndexEntry);
				}else if (keySplit.compareTo(oldKey)  < 0){
					tempNode.predecessors().add(oldEntry);
					keyNodeMVBT.predecessors().add(oldEntry);
					keyNodeMVBT.predecessors().add(neighbourIndexEntry);
				}else if (keySplit.compareTo(oldKey) == 0){
					tempNode.predecessors().add(oldEntry);
					keyNodeMVBT.predecessors().add(neighbourIndexEntry);
				}
			}
		}
	}
	
	/**
	 * attaches buffer to a buffer node
	 * 
	 * @param entry
	 */
	protected void assignBuffer(IndexEntry entry){
		Long id = (Long)entry.id();	
		Queue<Element> buffer = this.factoryBufferFunction.invoke();
		bufferMap.put(id, buffer);
	}
	
	/**
	 * remove buffer linkage
	 *  
	 * @param entry
	 */
	protected void removeBuffer(IndexEntry entry){
		bufferMap.remove((Long)entry.id());
	}
	/**************************************************************************************
	 * end new methods
	 * 
	 **************************************************************************************/
	/**************************************************************************************
	 * Legacy Part of MVBT
	 * 
	 **************************************************************************************/
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.MVBTree#copyEntry(java.lang.Object)
	 */
	protected Object copyEntry(Object entry) {
		if(entry instanceof LeafEntry) {
			LeafEntry leafEntry=(LeafEntry)entry;
			return createLeafEntry((Lifespan)leafEntry.getLifespan().clone(), leafEntry.data());
		}
		IndexEntry indexEntry=(IndexEntry)entry;
		IndexEntry cpy=(IndexEntry)createIndexEntry(indexEntry.parentLevel());
		cpy.initialize(indexEntry.id(), (Separator)indexEntry.separator.clone());
		cpy.setWeights(indexEntry.wCounter, indexEntry.tCounter);
		return cpy;
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.BPlusTree#createIndexEntry(int)
	 */
	@Override
	public xxl.core.indexStructures.Tree.IndexEntry createIndexEntry(
			int parentLevel) {
		boolean hasBuffer = isBufferLevel.invoke(parentLevel-1);
//		old code; change to more generic version
//		boolean hasBuffer = (parentLevel-1 == 0) ? false : 
//			(parentLevel-1) % firstBufferLevel == 0;
		IndexEntry entry = new IndexEntry(parentLevel, hasBuffer);
		return entry;
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.MVBTree#createNode(int)
	 */
	@Override
	public xxl.core.indexStructures.Tree.Node createNode(int level) {
		return new Node(level);
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.BPlusTree#nodeConverter()
	 */
	@Override
	public NodeConverter nodeConverter() {
		return new NodeConverter();
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.MVBTree#createNodeConverter()
	 */
	protected NodeConverter createNodeConverter() {
		return new NodeConverter();
	}
	
	/**
	 * 
	 * @param insertVersion
	 * @param data
	 */
	@Override
	public void insert(Version insertVersion, Object data) {
		throw new UnsupportedOperationException("Please call insert(Element entryToInsert) method!");
	}
	
	/**
	 * 
	 * @param removeVersion
	 * @param data
	 * @return
	 */
	@Override
	public Object remove(Version removeVersion, Object data) {
		throw new UnsupportedOperationException("Please call insert(Element entryToInsert) method!");
	}
	
	/**
	 * 
	 * @param updateVersion
	 * @param oldData
	 * @param newData
	 */
	@Override
	public void update(Version updateVersion, Object oldData, Object newData) {
		throw new UnsupportedOperationException("Please call insert(Element entryToInsert) method!");
	}
	
	/**
	 * this method is used for inserting elements.
	 * 
	 * @param element
	 */
	public void insert(Element entryToInsert){
		LongVersion minVersion = null;
		MVSeparator mvSep = getMVSepartor(entryToInsert);
		if(rootDescriptor == null && rootEntry == null){
			rootDescriptor =  createMVRegion(mvSep.insertVersion(), null, this.keyDomainMinValue , mvSep.sepValue());
			rootEntry = createIndexEntry(LEAF_LEVEL+1); // also creates buffer if necessery 
			Node firstNode = (Node)createNode(LEAF_LEVEL);
			Object id = container().reserve(new Constant<Node>(firstNode));
			((IndexEntry)rootEntry).initialize(id, toMVSeparator((MVRegion)rootDescriptor));
			rootEntry.update(firstNode);
		}else{
			rootDescriptor.union(mvSep);
		}
		if (minVersion == null){
			minVersion = (LongVersion)entryToInsert.getElement2().clone();
		}
		Stack<StackInfoObject> bufferOverflowWeightViolationStack = new Stack<MVBTPlus.StackInfoObject>();
		Stack<Triple<IndexEntry, Node, Boolean>> stack =  pushEntry(entryToInsert, (IndexEntry)rootEntry, null, bufferOverflowWeightViolationStack);
		updatePath(stack);
	}
	
	/**************************************************************************************
	 * End of Legacy Part of MVBT
	 * 
	 **************************************************************************************/
	/**
	 * This class extends {@link BPlusTree.IndexEntry}. 
	 * It manages an address to the node. Additionally it maintains two weight counters. These are updated if new elements are inserted to attached nodes. 
	 *  	
	 */
	public class IndexEntry extends BPlusTree.IndexEntry{
		/**
		 *  tracks the number of operations since the node creation 
		 */
		private int tCounter;
		/**
		 * tracks the number of live entries since the node creation
		 */
		private int wCounter;
		/**
		 * true if index entry points to buffer node
		 */
		private boolean hasBuffer;
		/**
		 * default constructor
		 *  
		 * @param parentLevel
		 * @param hasbuffer
		 */
		public IndexEntry(int parentLevel, boolean hasbuffer) {
			super(parentLevel);
			this.hasBuffer = hasbuffer; 
		}
		/**
		 * sets counters
		 * 
		 * @param wCounter
		 * @param tCounter
		 */
		public void setWeights(int wCounter, int tCounter){
			
			this.wCounter = wCounter;
			this.tCounter = tCounter;
		}
		/**
		 * updates counters 
		 *  
		 * @param element
		 */
		protected void updateCounter(Element element){ 
			OperationType ops = element.getElement3();
			switch(ops){
				case INSERT: {
					this.wCounter++;
				}; 
				case UPDATE:{
					this.tCounter++;
				}; break;
				case DELETE: {
					this.wCounter--;
				}; break;
				default: break;
			}
		}
		
		/**
		 * returns attributed {@link MVSeparator}
		 * @return
		 */
		protected MVSeparator getMVSeparator(){
			return (MVSeparator)this.separator();
		}
		
		/**
		 * returns delete version  
		 * @return
		 */
		protected Version getDeleteVersion(){
			return getMVSeparator().deleteVersion();
		}
		
		/**
		 * returns insert version
		 * @return
		 */
		protected Version getInsertVersion(){
			return getMVSeparator().insertVersion();
		}
		
		/**
		 * returns true if node has a buffer attached
		 * 
		 * @return
		 */
		public boolean hasBuffer(){
			return hasBuffer;
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.BPlusTree.IndexEntry#toString()
		 */
		@Override
		public String toString() {
			return "IndexEntry [tCounter=" + tCounter + ", wCounter="
					+ wCounter + ", separator=" + separator + ", id=" + id
					+ ", parentLevel=" + parentLevel + "]";
		}
		
	}
	/**
	 * This is a MVBT node implementation. Note that MVBTPlus maps i node to a O(B) number of physical blocks. Maximal number of pages one index node can span is 6, if default settings used.
	 * Leaf nodes have one-to-one mapping to blocks. 
	 * 
	 * @author achakeye
	 *
	 */
	public class Node extends MVBT.Node{
		
		/**
		 * internal comparator
		 */
		private Comparator<IndexEntry> deadIndexEntryComparator = new Comparator<MVBTPlus.IndexEntry>() {
			
			@Override
			public int compare(IndexEntry entry1, IndexEntry entry2) {
				LongVersion v1 = (LongVersion) ((MVSeparator)entry1.separator).deleteVersion();
				LongVersion v2 = (LongVersion) ((MVSeparator)entry2.separator).deleteVersion();
				return v1.compareTo(v2);
			}
		};
		
		/**
		 * default constructor
		 * 
		 * @param level
		 */
		public Node(int level) {
			super(level);
		}
		
		/**
		 * returns true if node is a leaf node
		 * @return
		 */
		protected boolean isLeaf(){
			return level == LEAF_LEVEL;
		}
		
		/**
		 * insert an index entry
		 * 
		 * @param entry
		 */
		protected void growIndexNode(IndexEntry entry){
			this.grow(entry, new Stack<>());
		}
		
		/**
		 * insert a an element in leaf node
		 *  
		 * @param entry
		 */
		protected void growLeafNode(LeafEntry entry){
			this.grow(entry, new Stack<>());
		}
		
		/**
		 * insert a an element in leaf node
		 *  
		 * @param element
		 */
		@SuppressWarnings({ "unchecked", "unused", "rawtypes" })
		protected void growLeafNode(Element element){
			OperationType ops = element.getElement3(); 
			LongVersion version = element.getElement2();
			Stack<Object> dummyStack = new Stack<>();
			Comparable key = getKeyFromElement(element);
			switch(ops){
			case DELETE: {
				// find entry 
				Iterator nodeData = this.query(new Lifespan(version));
				boolean done = false;
				// ret version 
				LongVersion retVersion = (LongVersion) currentVersion();
				currentVersion = version;
				
				while(nodeData.hasNext() ){
					Object data = nodeData.next();
					LeafEntry leafEntry = (LeafEntry)data ;
					if ((leafEntry.getKey()).compareTo(key) == 0){
						nodeData.remove();
						done = true;
						break;
					}
				}
				
				currentVersion = retVersion;
			}; break;
			case UPDATE: {
				// find entry 
				Iterator nodeData = this.getCurrentEntries();
				LongVersion retVersion = (LongVersion) currentVersion();
				currentVersion = version;
				while(nodeData.hasNext() ){
					LeafEntry leafEntry = (LeafEntry) nodeData.next();
					// get data
					if ((leafEntry.getKey()).compareTo(key) == 0 ){
						nodeData.remove();
						break;
					}
				}
				currentVersion = retVersion;
			}; 
			case INSERT: { 
				// create leaf entry
				Lifespan lifespan = new Lifespan((Version) version.clone());
				LeafEntry leafentry =  new LeafEntry(lifespan, element.getElement1());
				this.grow(leafentry, dummyStack); 
			}; break;
			default : break;
			}
		}
		
		/**
		 * finds neighbour for merge and key-merge structure reorganization
		 * 
		 * @param entry
		 * @return
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected IndexEntry findNeighbour(IndexEntry entry){
			// FIXME O(B) to O(logB)
			Iterator liveEntries =  getCurrentEntries();
			List currentEntries = new ArrayList(Math.max(B_LeafNode, B_IndexNode));
			while( liveEntries.hasNext()) {
				IndexEntry entryOther = (IndexEntry)liveEntries.next();
				if(entryOther.id().equals(entry.id()))
					continue; 
				currentEntries.add(entryOther);
			}
			int index= searchMinimumKey(currentEntries);
			if(index==-1) {
				return null;
			}
			IndexEntry subtree= (IndexEntry)currentEntries.get(index);
			Iterator iterator = currentEntries.iterator();
			while(iterator.hasNext()) {
				IndexEntry entryOther= (IndexEntry)iterator.next();
				if((entryOther.separator.compareTo(entry.getMVSeparator())<=0)&&(entryOther.separator.compareTo(subtree.separator)>0)){	
					subtree=entryOther; 
				}
			}
			
			return (IndexEntry)subtree;
		}
		
		/**
		 * return index of an entry with a minimal key
		 * 
		 * @param entries
		 * @return
		 */
		@SuppressWarnings("rawtypes")
		protected int searchMinimumKey(List entries) {
			if(entries.isEmpty()) return -1;
			int index=0;
			for(int i=1; i<entries.size(); i++) {
				index=separator(entries.get(i)).compareTo(separator(entries.get(index)))<0 ?i:index; 
			}
			return index;
		}
		
		/**
		 * returns entry list 
		 * @return
		 */
		protected List getEntriesList(){
			return this.entries;
		}
		
		
		/** 
		 * Searches all entries of this <tt>Node</tt> whose <tt>Lifespans</tt> overlap the given <tt>Lifespan</tt>.
		 * If node spans multiple pages, all pages are traversed.
		 * 
		 * @param lifespan the <tt>Lifespan</tt> of the query.
		 * @return a <tt>Iterator</tt> pointing to all responses (i.e. all entries of this <tt>Node</tt> whose 
		 * <tt>Lifespans</tt> overlap the given <tt>Lifespan</tt>).
		 */
		@SuppressWarnings({ "rawtypes", "unchecked", "serial", "unused" })
		public Iterator query(final Lifespan lifespan) {
			if (level == LEAF_LEVEL){
				return new Filter(	iterator(),
								new AbstractPredicate() {
									public boolean invoke(Object entry) {									 	
										return ((MVSeparator)separator(entry)).lifespan().overlaps(lifespan);
									}
								});
			}
			final Node node = this;
			Iterator<Iterator> nodes = new Iterator<Iterator>() {
				
				Node currentNode = node;
				@Override
				public boolean hasNext() {
					return currentNode != null && !currentNode.predecessors().isEmpty();
				}
				
				@Override
				public Iterator next() {
					Iterator it = currentNode.iterator();
					IndexEntry nextEntry =(IndexEntry)currentNode.predecessors().get(INDEX_FIRST);
					currentNode = (Node) nextEntry.get(true); 
					return currentNode.iterator();
				}
				
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
			List<Iterator> iterators = new ArrayList<>();
			iterators.add(iterator());
			while(nodes.hasNext()){
				Iterator it = nodes.next();
				iterators.add(it);
			}
			Iterator sequentializer = new Sequentializer<Object>(iterators.toArray(new Iterator[]{}));
			return new Filter(	sequentializer,
					new AbstractPredicate() {
				public boolean invoke(Object entry) {									 	
					return ((MVSeparator)separator(entry)).lifespan().overlaps(lifespan);
				}
			});
		}
		
		/**
		 * search for a index entry in a given node
		 * 
		 * @param id
		 * @return
		 */
		protected IndexEntry findIndexEntry(Object id){
			if (isLeaf()){
				throw new RuntimeException("This operation is defined only for index nodes");
			}
			List indexEntr = entries;
			for(Object ent : indexEntr ){
				IndexEntry entry = (IndexEntry)ent;
				if(entry.id().equals(id)){
					return entry;
				}
				
			}
			throw new RuntimeException("Check correctness entry not found");
		}
		
		/**
		 * auxilary method for index nodes
		 * 
		 * @return
		 */
		protected Pair<List<IndexEntry>, LongVersion> removeOldIndexEntries(){
			if (isLeaf()){
				throw new RuntimeException("This operation is defined only for index nodes");
			}
			LongVersion maxDeadVersion = new LongVersion(Long.MIN_VALUE);
			List<IndexEntry> list = new ArrayList<>();
			for(int i = 0; i < entries.size(); i++) {
				IndexEntry entry = (IndexEntry)entries.get(i);
				if (!((MVSeparator)entry.separator).isAlive()){
					LongVersion entryVersion = (LongVersion) ((MVSeparator)entry.separator).deleteVersion(); 
					list.add(entry);
					entries.remove(i);
					i--;
					maxDeadVersion = new LongVersion(Math.max(maxDeadVersion.version, entryVersion.version));
				}
				
			}
			// sort on time dead time
			Collections.sort(list, deadIndexEntryComparator);
			//
			return new Pair<>(list, maxDeadVersion);
		} 
	
	} 
	
	/**
	 * 
	 * Converter for MVBTPlus Nodes.
	 * 
	 * If index node spans multiple pages only one page of the index is loaded, since the MVBTPlus is designed such way that the maximal number of live entries per index node is B. 
	 * Therefore, for inserting only live part of index node is loaded. For querying {@link Node#query(Lifespan)} is used by query cursor. 
	 * This method reads also historical pages of an index node.  
	 * 
	 * @author 
	 *
	 */
	@SuppressWarnings("serial")
	public class NodeConverter extends MVBT.NodeConverter{
		
		public Object read (DataInput dataInput, Object object) throws IOException {
			int level=dataInput.readInt();
			int number=dataInput.readInt();
			Node node = (Node)createNode(level);
			readPredecessors(dataInput, node);
			for (int i=0; i<number; i++) {
				Object entry;
				if(node.level()==0) entry= readLeafEntry(dataInput);
				else entry=readIndexEntry(dataInput,node.level());
				node.getEntriesList().add(entry);
			}
			int liveNumber = dataInput.readInt(); 
			for( int i = 0; i < liveNumber; i++){
				IndexEntry entry=readIndexEntry(dataInput,node.level());
				node.liveEntries.add(entry);
			}
			return node;
		}
		
		public void write (DataOutput dataOutput, Object object) throws IOException {
			Node node = (Node)object;
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.level());
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.getDataEntries().size());
			writePredecessors(dataOutput, node);
			for(int i=0; i< node.getDataEntries().size() ;i++) {
				Object entry=node.getEntry(i);
				if(node.level()==0) writeLeafEntry(dataOutput,(LeafEntry)entry);
					else writeIndexEntry(dataOutput,(IndexEntry)entry);
			}
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, node.liveEntries.size());
			for(int i=0; i<node.liveEntries.size() ;i++) {
				Object entry = node.liveEntries.get(i);
				writeIndexEntry(dataOutput,(IndexEntry)entry);
			}
		}
		
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.NodeConverter#readIndexEntry(java.io.DataInput, int)
		 */
		protected IndexEntry readIndexEntry(DataInput input, int parentLevel) throws IOException {
			IndexEntry indexEntry=(IndexEntry)createIndexEntry(parentLevel);
			int wCounter = IntegerConverter.DEFAULT_INSTANCE.readInt(input);
			int tCounter = IntegerConverter.DEFAULT_INSTANCE.readInt(input);
			Object id= container().objectIdConverter().read(input, null);
			Comparable sepValue=(Comparable)keyConverter.read(input, null);
			Lifespan life= (Lifespan)lifespanConverter.read(input, null);
			MVSeparator mvSeparator= createMVSeparator(life.beginVersion(), life.endVersion(), sepValue);
			indexEntry.initialize(id, mvSeparator);
			indexEntry.setWeights(wCounter, tCounter); // XXX new code
			return indexEntry;
		}
		/*
		 * (non-Javadoc)
		 * @see xxl.core.indexStructures.MVBTree.NodeConverter#writeIndexEntry(java.io.DataOutput, xxl.core.indexStructures.BPlusTree.IndexEntry)
		 */
		@SuppressWarnings("unchecked")
		protected void writeIndexEntry(DataOutput output, BPlusTree.IndexEntry bplusentry) throws IOException {
			IndexEntry entry = (IndexEntry)bplusentry;
			IntegerConverter.DEFAULT_INSTANCE.writeInt(output, entry.wCounter); // XXX new code
			IntegerConverter.DEFAULT_INSTANCE.writeInt(output, entry.tCounter); // XXX new code
			container().objectIdConverter().write(output, entry.id());
			keyConverter.write(output, entry.separator.sepValue());
			lifespanConverter.write(output, ((MVSeparator)entry.separator()).lifespan());
		}
		
		@Override
		protected int indexEntrySize() {
			return super.indexEntrySize() + 2* IntegerConverter.SIZE;
		}
	}
	
	
	/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	 * DEBUG Variables
	 * 
	 * see debug code 
	 * lines are commented
	 * 
	 * 
	 *++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
	
	private int debug_leafnodesCreated = 0;
	private int debug_reorgs = 0;
	private int debug_timeSplits = 0;
	private int debug_merges = 0;
	private int debug_mergeKeySplits = 0;
	private int debug_keySplits = 0;
	private int debug_flush_buffers = 0;
	private int debug_buffer_reorg = 0; 
	private int debug_buffer_timeSplits = 0;
	private int debug_buffer_merges = 0;
	private int debug_buffer_keySplits = 0;
	private int debug_buffer_mergeKeySplits = 0;
	private int debug_buffer_bufferAll = 0; 
	
	public static final boolean VERBOSE = false; 
	
	public static void  DEBUG(String message){
		if(VERBOSE)
			System.out.println(message);
	}
	
	public void PRINT_STAT(){
		if(VERBOSE){
			System.out.println();
			System.out.println("Nodes; LeafNodes; TimeSplits; KeySplits; Merges; MergeKeySplits; BuffersEmpty; BufferNodeReorg;");
			System.out.printf(Locale.GERMANY,
					" \n %d; %d; %d; %d; %d; %d; %d; %d; %d; \n", 
					debug_reorgs, 
					debug_leafnodesCreated, 
					debug_timeSplits, 
					debug_keySplits, 
					debug_merges, 
					debug_mergeKeySplits, 
					debug_flush_buffers, 
					debug_buffer_reorg, 
					debug_buffer_bufferAll);
			System.out.println();
			System.out.printf(Locale.GERMANY,
					" \n %d; %d; %d; %d; \n", 
					debug_buffer_timeSplits, 
					debug_buffer_keySplits, 
					debug_buffer_merges, 
					debug_buffer_mergeKeySplits);
			System.out.println();
		}
	};
}
