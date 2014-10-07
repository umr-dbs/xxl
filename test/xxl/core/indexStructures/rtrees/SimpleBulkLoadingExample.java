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
package xxl.core.indexStructures.rtrees;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;




import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.collections.queues.io.QueueBuffer;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.NullaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.functions.Identity;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.SortBasedBulkLoading;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader.ProcessingType;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.indexStructures.rtrees.GenericPartitioner.DefaultArrayProcessor;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.SpatialUtils;
import xxl.core.spatial.TestPlot;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangles;
import xxl.core.util.Pair;



/**
 * This class is a brief tutorial for bulk loading R-tree. 
 * 
 * Firstly, we show how to create and to set parameter for R-trees. 
 * Secondly, we bulk load them from scratch using three different bulk loading methods. 
 * We use the following data set: California Streets (rea02.rec); 
 * The data set is obtained from a census TIGER data set and contains 1.888.880 2-dimensional rectangles of California streets segments. 
 * 
 * The data set can be downloaded from http://www.mathematik.uni-marburg.de/~achakeye/data/data/rea02.rec
 * 
 * point query set from http://www.mathematik.uni-marburg.de/~achakeye/data/query_1/rea02.rec
 * 
 * range query set http://www.mathematik.uni-marburg.de/~achakeye/data/query_100/rea02.rec
 * 
 * Please, note, that for testing data set in normalized to a unit cube. 
 * 
 * 
 * 1. {@link SortBasedBulkLoading} is a generic sort based bulk loading 
 * e.g.( N. Roussopoulos and D. Leifker. Direct spatial search  on pictorial databases using packed r-trees.
 * In SIGMOD , pages 17{31, 1985, I. Kamel and C. Faloutsos. On packing r-trees. In CIKM '93, pages 490-499).
 *  We will use both Hilbert and Z-Curve.   
 * 2. {@link STRBulkLoader} is  bulk loading approach developped by Leutenegger et al. (see: 
 * Scott Leutenegger and Mario A. Lopez and J. Edgington
 * STR: A Simple and Efficient Algorithm for R-Tree Packing (ICDE 1997))
 * 3. {@link RtreeIterativeBulkloader}  is a generic sort based bulk loading that uses one dimensional optimal partitioning method proposed in 
 * D Achakeev, B Seeger and P Widmayer: "Sort-based query-adaptive loading of R-trees" in CIKM 2012
 * 
 * Thirdly, we test the query performance of R-trees using the following query files: 
 * TODO
 * 
 * 
 * In our example the data is stored as {@link DoublePointRectangle} objects. 
 * Rtree in XXL uses descriptor that are also of type {@link DoublePointRectangle}. Using {@link Tree#getDescriptor} 
 * function we extract or map from input objects their keys (descriptors). In our case this function is an identity.   
 * 
 * 
 *
 */
public class SimpleBulkLoadingExample {
	/**
	 * Path were the R-trees containers ( {@link BlockFileContainer}) will be stored. 
	 * 
	 * NOTE: change
	 */
	public static String RTREE_PATH ="F://rtree//";
	
	/**
	 * Path to the California Streets (rea02.rec) data set;
	 * NOTE: change
	 */
	public static String DATA_PATH="F://rtree//data//rea02.rec";
	
	/**
	 * Path to the query file with point queries.     
	 * NOTE: change
	 */
	public static String POINT_QUERY_PATH = "F://rtree//query_1//rea02.rec";
	
	/**
	 * Path to the query file with range queries. 
	 * NOTE: change
	 */
	public static String RANGE_QUERY_PATH = "F://rtree//query_100//rea02.rec";
	
	/**
	 * In this example we will use  {@link BlockFileContainer}. Therefore, 
	 */
	public static int BLOCK_SIZE = 4096; // 
	
	/**
	 * We use 2-dimensional data
	 */
	public static int DIMENSION = 2; 
	
	/**
	 * 
	 */
	public static boolean BUFFER = false; 
	
	/**
	 * 
	 */
	public static int BUFFER_PAGES = 10;
	
	/**
	 * 
	 */
	public static boolean HILBERT = true; 
	
	/**
	 * Hilbert two-dimensional comparator
	 */
	public static Comparator<DoublePointRectangle> rectangleComparator2DHilbert = new Comparator<DoublePointRectangle>() {
		
		int defaultSpaceResolution = 1 << 30; // we need this variable to provide a space resolution
		
		@Override
		public int compare(DoublePointRectangle o1, DoublePointRectangle o2) {
			// we use a center point for mapping to a SFC
			// please note, that we assume that input rectangles are already mapped into unit cube
			// otherwise we should map them before (this can be done by providing of an MBR of the space aka universe) 
			double center1[] = (double[])o1.getCenter().getPoint();
			double center2[] = (double[])o2.getCenter().getPoint();
			// now we map double values to integers
			// we use a  resolution of java.Integer 31 bits
			int[] coord1 = new int[center1.length];
			int[] coord2 = new int[center1.length];
			for(int i = 0; i < coord1.length; i++){
				coord1[i] = (int) (center1[i] * (defaultSpaceResolution));
				coord2[i] = (int) (center2[i] * (defaultSpaceResolution));
			}
			long h1 = SpaceFillingCurves.hilbert2d(coord1[0], coord1[1], defaultSpaceResolution); // as we have two-dimensional space
			long h2 =  SpaceFillingCurves.hilbert2d(coord2[0], coord2[1], defaultSpaceResolution);
			return (h1<h2)?-1: ((h1==h2)?0:+1);
		}
	}; 
	
	
	/**
	 * 
	 * Z-Curve comparator
	 * 
	 */
	public static Comparator<DoublePointRectangle> rectangleComparatorZ = new Comparator<DoublePointRectangle>() {
		
		int defaultSpaceResolution = 1 << 30; 
		
		@Override
		public int compare(DoublePointRectangle o1, DoublePointRectangle o2) {
			// we use a center point for mapping to a SFC
			// please note, that we assume that input rectangles are already mapped into unit cube
			// otherwise we should map them before (this can be done by providing of an MBR of the space aka universe) 
			double center1[] = (double[])o1.getCenter().getPoint();
			double center2[] = (double[])o2.getCenter().getPoint();
			// now we map double values to integers
			// we use a  resolution of java.Integer 31 bits
			int[] coord1 = new int[center1.length];
			int[] coord2 = new int[center1.length];
			for(int i = 0; i < coord1.length; i++){
				coord1[i] = (int) (center1[i] * (defaultSpaceResolution));
				coord2[i] = (int) (center2[i] * (defaultSpaceResolution));
			}
			long h1 = SpaceFillingCurves.computeZCode(coord1, 30);
			long h2 =  SpaceFillingCurves.computeZCode(coord2, 30);
			return (h1<h2)?-1: ((h1==h2)?0:+1);
		
		}
	}; 
	
	
	/**
	 * Executes sort based bulk-loading. We in order to conduct bulk-loading we execute the following steps:
	 * 1. Create R-tree 
	 * 2. Initialize container for storage
	 * 3. create converter/serializer
	 * 4. sort data according to sfc 
	 * 5. execute level-by-level loading
	 *  
	 * @return a pair Rtree and CounterContainer, this is used for counting I/Os
	 */
	public static Pair<RTree, CounterContainer> createAndLoadSortBased(){
		// create Rtree
		RTree rtree = new RTree(); 
		//1. create container
		// since we initialize container for the first time,  we need two parameter path and blocksize
		// otherwise we provide only path parameter, block size is then obtained from the meta information stored in blockfile container
		Container fileContainer = new BlockFileContainer(RTREE_PATH + "rtree", BLOCK_SIZE);
		//2.now we need to provide converterContainer that serializes (maps rtree nodes to a blocks)
		// before we can initialize converterContainer, we need initialize node converter of the rtree
		// default descriptor typ of the rtree is DoublePointRectangle. Therefore, we need to provide converter for input objects
		//Since, they are also of type DoublePointRectangle we do the following
		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
		// we wrap file container with counter
		CounterContainer ioCounter = new CounterContainer(fileContainer);
		Container converterContainer = new ConverterContainer(ioCounter, rtree.nodeConverter(objectConverter, DIMENSION));
		Container treeContainer = converterContainer;
		//3.converterContainer is now responsible for serializing rtree nodes. 
		//4.alternatively we can also provide  main memory buffer  
		//in our case we will use a small buffer of 10 pages 
		//. Since want to test actual I/O query performance, default setting is without buffer. 
		if(BUFFER){
			LRUBuffer<?, ?, ?> lruBuffer = new LRUBuffer<>(BUFFER_PAGES);
			treeContainer = new BufferedContainer(converterContainer, lruBuffer);
		}
		//5. now we can initialize tree
		// the first  argument is null 
		// if we want to reuse an Rtree we can provide root entry,  but in our case we do it for the first time.
		int dataSize = DIMENSION *  2 * 8; // number of bytes needed to store DoublePointRectangle
		int descriptorSize = dataSize; // in our example they are equal
		double minMaxFactor = 0.33; // is used to define a minimal number of elements per node
		rtree.initialize(null, new Identity<DoublePointRectangle>(), treeContainer, BLOCK_SIZE, dataSize, descriptorSize, minMaxFactor); 
		// now we are finished with initializing Rtree before we start with a bulk-loading we initialize  iterator that conducts external sorting
		// first, initialize a temporal container where we will store intermidiate runs
		Container sortedRunsContainer = new BlockFileContainer(RTREE_PATH + "sortedRuns", BLOCK_SIZE);
		// In order to execute external sorting we need to provide a factory function that creates a queue
		// this queue stores a sorted run
		// since we store a queue in container 
		// we can use the following factory method of BlockBasedQueue 
		Function<Function<?, Integer>, Queue<DoublePointRectangle>> queueFactoryFunction = 
				BlockBasedQueue.createBlockBasedQueueFunctionForMergeSorter(sortedRunsContainer, BLOCK_SIZE, objectConverter); 
		// now we initialize a lazy cursor for external sorting
		// we initialize first a cursor that reads a data from a file. for this prupose we need to provide a file and a serializer 
		Iterator<DoublePointRectangle> unsortedInput = new FileInputCursor<DoublePointRectangle>(objectConverter, new File(DATA_PATH));  
		Comparator<DoublePointRectangle> sFCComparator = (HILBERT) ? rectangleComparator2DHilbert : rectangleComparatorZ;
		// we set 10 MB memory for external sorting
		int memorySizeForRuns = 1024*1024*10; // with this value we provide an available memeory for initial run generation; 
		int memoryLastRuns = memorySizeForRuns; // here we provide how much memory is needed for last merge
		Iterator<DoublePointRectangle> sortedRectIterator = new MergeSorter<DoublePointRectangle>(unsortedInput, sFCComparator, dataSize, memorySizeForRuns, 
				memoryLastRuns, queueFactoryFunction, false);
		// now we run sort based bulk loading
		// here we provide a predicate that is used for detecting overflow and triggering next node creation
		// we fill the nodes by 80%
		// Note that we the rtree node header is a 6 bytes large. 
		// the rtree index entry contains an address of a node this is a long value of 8 bytes 
		// and it contains a DoublePointRectangle a serialized size if DIMENSION * 2 *  8 bytes
		final xxl.core.predicates.Predicate<ORTree.Node> overflows = new AbstractPredicate<ORTree.Node>() {
			public boolean invoke(ORTree.Node node){
				if(node.level() == 0)
					return node.number() > ( (int)(((BLOCK_SIZE-6)/(DIMENSION*2*8)) * 0.8) ) ; 
				return node.number() > ( (int)(((BLOCK_SIZE-6)/(DIMENSION*2*8+8)) * 0.8) );
			}
		};
		new SortBasedBulkLoading(rtree, sortedRectIterator, rtree.determineContainer, overflows);
		return new Pair<RTree, CounterContainer>(rtree, ioCounter); 
	}
	
	/**
	 * This method uses {@link STRBulkLoader} class. 
	 * This bulk loader sort data recursively by considering one dimension at time. 
	 * 
	 * For initializing the STR loader we need 
	 * an Rtree, number of dimensions, blocksize, storage utilization per node in percent and so called sortign function
	 * this provides the ordering of dimensions, since str sorts and partitions data according to one dimension at one step
	 * sorting function provides which dimension should be taken as next
 	 * e.g. in two dimensional space we have 4 different sorting functions x,x or x,y or y,y, or x,x
	 * in this example we use a default one x,y
	 * 
	 * We decode x with 0 , y with 1 and etc...
	 * 
	 * @return a pair Rtree and CounterContainer, this is used for counting I/Os
	 * @throws IOException 
	 */
	public static Pair<RTree, CounterContainer> createAndLoadSTR() throws IOException{
		RTree rtree = new RTree(); 
		// and we provide again the object size, since it as DoublePointRectangle in two-dimensional space
		//1. create container
		// since we initialize container for the first time,  we need two parameter path and blocksize
		// otherwise we provide only path parameter, block size is then obtained from the meta information stored in blockfile container
		Container fileContainer = new BlockFileContainer(RTREE_PATH + "strrtree", BLOCK_SIZE);
		//2.now we need to provide converterContainer that serializes (maps rtree nodes to a blocks)
		// before we can initialize converterContainer, we need initialize node converter of the rtree
		// default descriptor typ of the rtree is DoublePointRectangle. Therefore, we need to provide converter for input objects
		//Since, they are also of type DoublePointRectangle we do the following
		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
		// we wrap file container with counter
		CounterContainer ioCounter = new CounterContainer(fileContainer);
		Container converterContainer = new ConverterContainer(ioCounter, rtree.nodeConverter(objectConverter, DIMENSION));
		Container treeContainer = converterContainer;
		//3.converterContainer is now responsible for serializing rtree nodes. 
		//4.alternatively we can also provide  main memory buffer  
		//in our case we will use a small buffer of 10 pages 
		//. Since want to test actual I/O query performance, default setting is without buffer. 
		if(BUFFER){
			LRUBuffer<?, ?, ?> lruBuffer = new LRUBuffer<>(BUFFER_PAGES);
			treeContainer = new BufferedContainer(converterContainer, lruBuffer);
		}
		//5. now we can initialize tree
		// the first  argument is null 
		// if we want to reuse an Rtree we can provide root entry,  but in our case we do it for the first time.
		int dataSize = DIMENSION *  2 * 8; // number of bytes needed to store DoublePointRectangle
		int descriptorSize = dataSize; // in our example they are equal
		double minMaxFactor = 0.33; // is used to define a minimal number of elements per node
		rtree.initialize(null, new Identity<DoublePointRectangle>(), treeContainer, BLOCK_SIZE, dataSize, descriptorSize, minMaxFactor); 
		// this sorting function is used by str to sort first and to partition  by x-axis and then by y-axis respectively
		int[] sortingFunction = {0,1};  
		STRBulkLoader<DoublePointRectangle> strBulkloader = new STRBulkLoader<>(rtree, RTREE_PATH+"str", DIMENSION, BLOCK_SIZE, 0.33, 0.8, sortingFunction); 
		// before we can start a bulk loading we need to provide
		// the number of objects
		// this can be computed e.g. using the following code pattern
		int number = Cursors.count(new FileInputCursor<>(objectConverter, new File(DATA_PATH))); 
		// again we use 10MB memory for external sorting
		int memorySizeForRuns = 1024*1024*10; // with this value we provide an available memeory for initial run generation and for last merging; 
		// 
//		UnaryFunction<DoublePointRectangle, DoublePointRectangle> identity = (x -> x);  for java 8
		strBulkloader.init(number, memorySizeForRuns, dataSize, objectConverter, new UnaryFunction<DoublePointRectangle, DoublePointRectangle>() {
			
			@Override
			public DoublePointRectangle invoke(DoublePointRectangle arg) {
				return arg;
			}
		}); 
		// conduct bulk-loading
		strBulkloader.buildRTree(new FileInputCursor<>(objectConverter, new File(DATA_PATH)));
		return new Pair<RTree, CounterContainer>(strBulkloader.getRTree(), ioCounter); 
	}

	
	/**
	 * 
	 *  In this method is similar to the {@link #createAndLoadSortBased()}. 
	 *  
	 *  
	 * 
	 * @return
	 * @throws IOException 
	 */
	public static Pair<RTree, CounterContainer> createAndLoadSortBasedOptimal() throws IOException{
		// create Rtree
		RTree rtree = new RTree(); 
		//1. create container
		// since we initialize container for the first time,  we need two parameter path and blocksize
		// otherwise we provide only path parameter, block size is then obtained from the meta information stored in blockfile container
		Container fileContainer = new BlockFileContainer(RTREE_PATH + "rtree_gopt", BLOCK_SIZE);
		//2.now we need to provide converterContainer that serializes (maps rtree nodes to a blocks)
		// before we can initialize converterContainer, we need initialize node converter of the rtree
		// default descriptor typ of the rtree is DoublePointRectangle. Therefore, we need to provide converter for input objects
		//Since, they are also of type DoublePointRectangle we do the following
		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
		// we wrap file container with counter
		CounterContainer ioCounter = new CounterContainer(fileContainer);
		Container converterContainer = new ConverterContainer(ioCounter, rtree.nodeConverter(objectConverter, DIMENSION));
		Container treeContainer = converterContainer;
		//3.converterContainer is now responsible for serializing rtree nodes. 
		//4.alternatively we can also provide  main memory buffer  
		//in our case we will use a small buffer of 10 pages 
		//. Since want to test actual I/O query performance, default setting is without buffer. 
		if(BUFFER){
			LRUBuffer<?, ?, ?> lruBuffer = new LRUBuffer<>(BUFFER_PAGES);
			treeContainer = new BufferedContainer(converterContainer, lruBuffer);
		}
		//5. now we can initialize tree
		// the first  argument is null 
		// if we want to reuse an Rtree we can provide root entry,  but in our case we do it for the first time.
		int dataSize = DIMENSION *  2 * 8; // number of bytes needed to store DoublePointRectangle
		int descriptorSize = dataSize; // in our example they are equal
		double minMaxFactor = 0.33; // is used to define a minimal number of elements per node
		rtree.initialize(null, new Identity<DoublePointRectangle>(), treeContainer, BLOCK_SIZE, dataSize, descriptorSize, minMaxFactor); 
		// now we are finished with initializing Rtree before we start with a bulk-loading we initialize  iterator that conducts external sorting
		// first, initialize a temporal container where we will store intermidiate runs
		Container sortedRunsContainer = new BlockFileContainer(RTREE_PATH + "sortedRunsGopt", BLOCK_SIZE);
		// In order to execute external sorting we need to provide a factory function that creates a queue
		// this queue stores a sorted run
		// since we store a queue in container 
		// we can use the following factory method of BlockBasedQueue 
		Function<Function<?, Integer>, Queue<DoublePointRectangle>> queueFactoryFunction = 
				BlockBasedQueue.createBlockBasedQueueFunctionForMergeSorter(sortedRunsContainer, BLOCK_SIZE, objectConverter); 
		// now we initialize a lazy cursor for external sorting
		// we initialize first a cursor that reads a data from a file. for this prupose we need to provide a file and a serializer 
		Iterator<DoublePointRectangle> unsortedInput = new FileInputCursor<DoublePointRectangle>(objectConverter, new File(DATA_PATH));  
		Comparator<DoublePointRectangle> sFCComparator = (HILBERT) ? rectangleComparator2DHilbert : rectangleComparatorZ;
		// we set 10 MB memory for external sorting
		int memorySizeForRuns = 1024*1024*10; // with this value we provide an available memeory for initial run generation; 
		int memoryLastRuns = memorySizeForRuns; // here we provide how much memory is needed for last merge
		Iterator<DoublePointRectangle> sortedRectIterator = new MergeSorter<DoublePointRectangle>(unsortedInput, sFCComparator, dataSize, memorySizeForRuns, 
				memoryLastRuns, queueFactoryFunction, false);
		// for initializing we need to provide a partition size
		// since we run a linear versio of optimal partitioning algorith we set this value to 50_000
		int partitionSize = 50_000;
		RtreeIterativeBulkloader<DoublePointRectangle> optBulkloader = new RtreeIterativeBulkloader<>(rtree, RTREE_PATH +"gopt", DIMENSION, BLOCK_SIZE, 0.33, 0.8, partitionSize);
		// this is a deafalt array processor that is used by optimal partitioning algorithms
		// in our case we use volume of MBR for a optimal cost computation
		CostFunctionArrayProcessor<DoublePointRectangle> arrayProcessor = new DefaultArrayProcessor(AbstractIterativeRtreeBulkloader.generateDefaultFunctionVolume()); 
		UnaryFunction<DoublePointRectangle, DoublePointRectangle> toRectangle = new UnaryFunction<DoublePointRectangle, DoublePointRectangle>() {
			
			@Override
			public DoublePointRectangle invoke(DoublePointRectangle arg) {
				return arg;
			}
		};
		optBulkloader.init(arrayProcessor,ProcessingType.GOPT, dataSize, objectConverter, toRectangle);
		optBulkloader.buildRTree(sortedRectIterator);
		return new Pair<RTree, CounterContainer>(optBulkloader.getRTree(), ioCounter); 
	}
	
	
	/**
	 * Builds an Rtree using 
	 * Bulk loading technique
	 * 
	 * Lars Arge, Klaus Hinrichs, Jan Vahrenhold, Jeffrey Scott Vitter: Efficient Bulk Operations on Dynamic R-Trees. Algorithmica 33(1): 104-128 (2002)
	 * 
	 * 
	 * @return
	 */
	public static Pair<? extends RTree, CounterContainer>  loadRtreeBufferDoublePointRectangle(){
		int memorySizeForBuffers= 1024*1024*10; // we provide the same amount of memory for buffers 10 MB
		final int dataSize = DIMENSION *  2 * 8; // number of bytes needed to store DoublePointRectangle
		int descriptorSize = dataSize; // in our example they are equal
		double minMaxFactor = 0.33; // is used to define a minimal number of elements per node
		// you can change this size 
		int memoryEntries = memorySizeForBuffers / dataSize;   // NOTE: actual size of a memory is larger, since we have a constant amount of an additional memory per java object. 	
		int bufferPages = memorySizeForBuffers / BLOCK_SIZE;
		System.out.println(bufferPages);
		System.out.println(memoryEntries);
		BufferedRtree<DoublePointRectangle> rtree = new BufferedRtree<>(BLOCK_SIZE, dataSize, DIMENSION); 
		//1. create container
		// since we initialize container for the first time,  we need two parameter path and blocksize
		// otherwise we provide only path parameter, block size is then obtained from the meta information stored in blockfile container
		Container fileContainer = new BlockFileContainer(RTREE_PATH + "bufferRtree", BLOCK_SIZE);
		//2.now we need to provide converterContainer that serializes (maps rtree nodes to a blocks)
		// before we can initialize converterContainer, we need initialize node converter of the rtree
		// default descriptor typ of the rtree is DoublePointRectangle. Therefore, we need to provide converter for input objects
		//Since, they are also of type DoublePointRectangle we do the following
		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
		// we wrap file container with counter
		CounterContainer ioCounter = new CounterContainer(fileContainer);
		Container converterContainer = new ConverterContainer(ioCounter, rtree.nodeConverter(objectConverter, DIMENSION));
		//3.converterContainer is now responsible for serializing rtree nodes. 
		//4. we use buffer this implements available memory and holds node buffers
		LRUBuffer<?, ?, ?> lruBuffer = new LRUBuffer<>(bufferPages);
		CounterContainer treeContainer = new  CounterContainer( new BufferedContainer(converterContainer, lruBuffer));
		// now we initialize conatiner that manages buffers
		final Container bufferedContainer = new BufferedContainer(
					new ConverterContainer(new BlockFileContainer(RTREE_PATH + "buffers", BLOCK_SIZE),
							QueueBuffer.getPageConverter(Rectangles.getDoublePointRectangleConverter(DIMENSION))), lruBuffer);
		NullaryFunction<Queue<DoublePointRectangle>> queueFunction = new NullaryFunction<Queue<DoublePointRectangle>>() {
			@Override
			public Queue<DoublePointRectangle> invoke() {
				return new xxl.core.collections.queues.io.QueueBuffer<>(bufferedContainer,dataSize, BLOCK_SIZE);
			}
		};
		//5. now we can initialize tree
		// the first  argument is null 
		// if we want to reuse an Rtree we can provide root entry,  but in our case we do it for the first time.
		rtree.initialize(null, new Identity<DoublePointRectangle>(), treeContainer, BLOCK_SIZE, dataSize, descriptorSize, minMaxFactor); 
		Iterator<DoublePointRectangle> unsortedInput = new FileInputCursor<DoublePointRectangle>(objectConverter, new File(DATA_PATH));  
		rtree.bulkLoad(unsortedInput, queueFunction, memoryEntries); 
		return new Pair<>(rtree, treeContainer); 
	} 
	
	
	/**
	 * This method uses {@link TGSBulkLoader} class. 
	 *  
	 * For initializing the TGS loader we need 
	 * an Rtree, number of dimensions, blocksize, storage utilization per node in percent, and MBR of a data space, this will be used for cost function computation.  
	 * 
	 * 
	 * 
	 * @return a pair Rtree and CounterContainer, this is used for counting I/Os
	 * @throws IOException 
	 */
	public static Pair<RTree, CounterContainer> createAndLoadTGS() throws IOException{
		RTree rtree = new RTree(); 
		// and we provide again the object size, since it as DoublePointRectangle in two-dimensional space
		//1. create container
		// since we initialize container for the first time,  we need two parameter path and blocksize
		// otherwise we provide only path parameter, block size is then obtained from the meta information stored in blockfile container
		Container fileContainer = new BlockFileContainer(RTREE_PATH + "tgsrtree", BLOCK_SIZE);
		//2.now we need to provide converterContainer that serializes (maps rtree nodes to a blocks)
		// before we can initialize converterContainer, we need initialize node converter of the rtree
		// default descriptor typ of the rtree is DoublePointRectangle. Therefore, we need to provide converter for input objects
		//Since, they are also of type DoublePointRectangle we do the following
		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
		// we wrap file container with counter
		CounterContainer ioCounter = new CounterContainer(fileContainer);
		Container converterContainer = new ConverterContainer(ioCounter, rtree.nodeConverter(objectConverter, DIMENSION));
		Container treeContainer = converterContainer;
		//3.converterContainer is now responsible for serializing rtree nodes. 
		//4.alternatively we can also provide  main memory buffer  
		//in our case we will use a small buffer of 10 pages 
		//. Since want to test actual I/O query performance, default setting is without buffer. 
		if(BUFFER){
			LRUBuffer<?, ?, ?> lruBuffer = new LRUBuffer<>(BUFFER_PAGES);
			treeContainer = new BufferedContainer(converterContainer, lruBuffer);
		}
		//5. now we can initialize tree
		// the first  argument is null 
		// if we want to reuse an Rtree we can provide root entry,  but in our case we do it for the first time.
		int dataSize = DIMENSION *  2 * 8; // number of bytes needed to store DoublePointRectangle
		int descriptorSize = dataSize; // in our example they are equal
		double minMaxFactor = 0.33; // is used to define a minimal number of elements per node
		rtree.initialize(null, new Identity<DoublePointRectangle>(), treeContainer, BLOCK_SIZE, dataSize, descriptorSize, minMaxFactor); 
		// this sorting function is used by str to sort first and to partition  by x-axis and then by y-axis respectively
		TGSBulkLoader<DoublePointRectangle> strBulkloader = new TGSBulkLoader<DoublePointRectangle>(rtree, RTREE_PATH+"tgs", DIMENSION, BLOCK_SIZE, 0.33, 0.8, Rectangles.getUnitUniverseDoublePointRectangle(DIMENSION)); 
		// before we can start a bulk loading we need to provide
		// the number of objects
		// this can be computed e.g. using the following code pattern
		int number = Cursors.count(new FileInputCursor<>(objectConverter, new File(DATA_PATH))); 
		// again we use 10MB memory for external sorting
		int memorySizeForRuns = 1024*1024*10; // with this value we provide an available memeory for initial run generation and for last merging; 
		// 
//		UnaryFunction<DoublePointRectangle, DoublePointRectangle> identity = (x -> x);  for java 8
		strBulkloader.init(number, memorySizeForRuns, dataSize, objectConverter, new UnaryFunction<DoublePointRectangle, DoublePointRectangle>() {
			
			@Override
			public DoublePointRectangle invoke(DoublePointRectangle arg) {
				return arg;
			}
		}); 
		// conduct bulk-loading
		strBulkloader.buildRTree(new FileInputCursor<>(objectConverter, new File(DATA_PATH)));
		return new Pair<RTree, CounterContainer>(strBulkloader.getRTree(), ioCounter); 
	}
	
	/**
	 * 
	 */
	public static void testQuery(Pair<? extends RTree, CounterContainer>  rtreePair, String queryPath){
		double ios = 0;
		double resultsPerQuery = 0; 
		double counter = 0; 
		Converter<DoublePointRectangle> objectConverter = new ConvertableConverter<>(Rectangles.factoryFunctionDoublePointRectangle(DIMENSION));
		
		for(Iterator<DoublePointRectangle> queryRectangles = new FileInputCursor<>( objectConverter, new File(queryPath));
				queryRectangles.hasNext(); ){
			DoublePointRectangle query = queryRectangles.next(); 
			// reset counter before test
			rtreePair.getElement2().reset();
			rtreePair.getElement2().flush(); // if buffer 
			// run query and count results
			int cT = Cursors.count(rtreePair.getElement1().query(query));
			counter++; 
			resultsPerQuery+=cT; 
			ios += rtreePair.getElement2().gets;
			
		}
		System.out.printf("queries %f, avg. per query I/O  %.2f\n", counter, (ios/counter));	
	}
	
	/**
	 * visualizes node MBRs
	 */
	public static void showRtreeLevel(String name, int level, final RTree tree){
		if(level > tree.height())
			throw new RuntimeException("level!");
		Iterator<DoublePointRectangle> levelDescriptors = new Mapper<Object, DoublePointRectangle>(new AbstractFunction<Object, DoublePointRectangle>() {
			
			@Override
			public DoublePointRectangle invoke(Object argument) {
				return (DoublePointRectangle)tree.descriptor(argument);
			}
			
		}, tree.query(level));
		new TestPlot( name, levelDescriptors, 500,  SpatialUtils.universeUnit(DIMENSION));
	}
	
	public static void main(String[] args) throws IOException {
		boolean showTrees = true;
		//create Rtree 
		Pair<RTree, CounterContainer> rtree = createAndLoadSortBased();
		//create STR
		Pair<RTree, CounterContainer> strRtree = createAndLoadSTR();
		//create opt Rtree
		Pair<RTree, CounterContainer> optRtree = createAndLoadSortBasedOptimal();
		//craete buffer rtree
		Pair<? extends RTree, CounterContainer>  bufferRTree = loadRtreeBufferDoublePointRectangle();
		//craete buffer rtree
		Pair<RTree, CounterContainer>  tgsRTree = createAndLoadTGS();
		//conduct point queries
		System.out.println("Point queries");
		testQuery(rtree, POINT_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(strRtree, POINT_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(optRtree, POINT_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(bufferRTree, POINT_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(tgsRTree, POINT_QUERY_PATH);
		System.out.println("\n\n");
		System.out.println("Range queries");
		testQuery(rtree, RANGE_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(strRtree, RANGE_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(optRtree, RANGE_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(bufferRTree, RANGE_QUERY_PATH);
		System.out.println("*********************\n");
		testQuery(tgsRTree, RANGE_QUERY_PATH);
		// show mbr of leaf level
		if(showTrees){
			int leafLevel = 1; 
			showRtreeLevel("RTree Hilbert Curve", leafLevel, rtree.getElement1());
			showRtreeLevel("RTree STR", leafLevel, strRtree.getElement1());
			showRtreeLevel("RTree Hilbert Curve GOPT volume optimized", leafLevel, optRtree.getElement1());
			showRtreeLevel("RTree R* Split top down Arge et al. Buffer loaded", leafLevel, bufferRTree.getElement1());
			showRtreeLevel("RTree TGS loaded with volume as a cost function", leafLevel, tgsRTree.getElement1());
		}
		
	}

}
