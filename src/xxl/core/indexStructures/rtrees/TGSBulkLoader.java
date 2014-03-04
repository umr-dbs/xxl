package xxl.core.indexStructures.rtrees;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.BlockBasedQueue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.BinaryFunction;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.functions.Identity;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.indexStructures.ORTree.Node;
import xxl.core.indexStructures.RTree;
import xxl.core.io.converters.Converter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;





/**
 * This is an implementation of TGS R tree loading approach:
 * 
 * 	Yvan J. Garcia R, Mario A. Lopez and  Scott T. Leutenegger A greedy algorithm for bulk loading R-trees
 * 
 * 
 * Note: This is an experimental version.   This loading implementation is conducted in main memory. 
 * 
 * @author achakeye
 *
 */
public class TGSBulkLoader<T> extends AbstractIterativeRtreeBulkloader<T>{
	/**
	 * path to store auxiliary file
	 */
	protected String path = null; 
	/**
	 * auxiliary storage as file
	 */
	protected File file = null;
	/**
	 * stream to the file
	 */
	protected DataOutputStream out = null; 
	/**
	 * 
	 */
	protected Container queueContainer; 
	/**
	 * 
	 */
	protected int numberOfRectangles;
	/**
	 * 
	 */
	protected int SORT_BUFFER_SIZE = 10*1024*1024;
	
	/**
	 * 
	 */
	protected static boolean ESORT = true;
	
	/**
	 * 
	 */
	protected DoublePointRectangle universe = null;
	
	// default function 
	/**
	 * cost function 
	 */
	protected BinaryFunction<DoublePointRectangle, DoublePointRectangle , Double> costFunction = null;
	
	/**
	 * 
	 */
	// B_Leaf or B_Index
	protected int maxRecordsProNode = 0 ;
	
	/**
	 * average side length
	 */
	protected double[] a = null; 
	
	/**
	 * 
	 * @param rtree
	 * @param path
	 * @param dimension
	 * @param blockSize
	 * @param ratio
	 * @param nodeUtil
	 * @param universe
	 * @param averageQuerySideLength
	 */
	public TGSBulkLoader(RTree rtree, 
			String path, 
			int dimension,
			int blockSize, 
			double ratio, 
			double nodeUtil, DoublePointRectangle universe){
		this(rtree, path, dimension,  blockSize, ratio, nodeUtil, universe, null);
	}

	/**
	 * 
	 * @param rtree
	 * @param path
	 * @param dimension
	 * @param blockSize
	 * @param ratio
	 * @param nodeUtil
	 * @param universe
	 * @param averageQuerySideLength
	 */
	public TGSBulkLoader(RTree rtree, 
			String path, 
			int dimension,
			int blockSize, 
			double ratio, 
			double nodeUtil, DoublePointRectangle universe, 
			double[] averageQuerySideLength) {
		super(rtree, dimension, blockSize, ratio, nodeUtil, 20_000);
		this.universe  = universe;
		this.path = path;
		//check if it right
		queueContainer = new BlockFileContainer(path  + "_queues.dat", blockSize);
		this.a = averageQuerySideLength;
	}


	/**
	 * 
	 * @param number
	 * @param sortMemoryBufferSize
	 * @param dataSize
	 * @param dataConverter
	 * @param toRectangle
	 * @return
	 */
	public IterativeBulkLoaderInterface<T> init(int number, int sortMemoryBufferSize, int dataSize, 	
			final Converter<T> dataConverter, 
			UnaryFunction<T, DoublePointRectangle> toRectangle){
		super.init(null, ProcessingType.SIMPLE, dataSize, dataConverter, toRectangle);
		int payload = blockSize-6;
		B_Leaf = (int)((double)(payload / (dataSize)) * storageUtil);
		B_Index = (int)((double)(payload / (dimension * 16 + 8 ))*storageUtil);
		this.numberOfDataObjects = number; 
		this.SORT_BUFFER_SIZE = sortMemoryBufferSize;
		costFunction = (a== null) ?  new BinaryFunction<DoublePointRectangle, DoublePointRectangle , Double>() {
			@Override
			public Double invoke(DoublePointRectangle arg, DoublePointRectangle arg1) {
				DoublePointRectangle rec =   new DoublePointRectangle(arg);
				rec.normalize(universe);
				double[] deltas = rec.deltas();
				double cost = 1d;
				for(int i = 0; i < deltas.length; i++ ){
					cost *= (deltas[i]) ; 
				}
				DoublePointRectangle rec1 =   new DoublePointRectangle(arg1);
				rec1.normalize(universe);
				double[] deltas1 = rec1.deltas();
				double cost1 = 1d;
				for(int i = 0; i < deltas1.length; i++ ){
					cost1 *= (deltas1[i]); 
				}
				return  cost + cost1 ;
			}		
		}:  new BinaryFunction<DoublePointRectangle, DoublePointRectangle , Double>() {
			@Override
			public Double invoke(DoublePointRectangle arg, DoublePointRectangle arg1) {
				DoublePointRectangle rec =   new DoublePointRectangle(arg);
				rec.normalize(universe);
				double[] deltas = rec.deltas();
				double cost = 1d;
				for(int i = 0; i < deltas.length; i++ ){
					cost *= (deltas[i]+a[i]) ; 
				}
				DoublePointRectangle rec1 =   new DoublePointRectangle(arg1);
				rec1.normalize(universe);
				double[] deltas1 = rec1.deltas();
				double cost1 = 1d;
				for(int i = 0; i < deltas1.length; i++ ){
					cost1 *= (deltas1[i] +a[i]); 
				}
				return  cost + cost1 ;
			}		
			};
		
		return this; 
	}

	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#buildRTree(java.util.Iterator)
	 */
	public void buildRTree(Iterator<T> rectangles) throws IOException{
		Iterator tempIterator = rectangles;
		int level = 0;
		int numberOfRecs = numberOfDataObjects;
		while(tempIterator.hasNext()){
			File file = File.createTempFile("levelRecs_", "dat");
			DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
			// read data in memory 
			// consume data 
			List data = new ArrayList<Object>();
			while(tempIterator.hasNext()){
				data.add(tempIterator.next());
			}
			// main call 
			maxRecordsProNode = (level > 0) ? B_Index : B_Leaf; 
			int written = 	tileData(data, data.size(), level, out);
			//System.out.println("level " + level);
			level++;
			numberOfRecs = written; 
			tempIterator  = new FileInputCursor<MapEntry<Long,DoublePointRectangle>>(mapEntryConverter, file);
			if (written <= 1){
				break;
			}
			file.deleteOnExit();
		}
		// create rtree
		MapEntry<Long, DoublePointRectangle> entry = (MapEntry<Long, DoublePointRectangle>)(tempIterator.next());
		DoublePointRectangle rootDescriptor = entry.getValue();
		IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
		IndexEntry rootEntry = (IndexEntry) ((ORTree.IndexEntry)indexEntry.initialize(entry.getKey())).initialize(rootDescriptor);
		//
		storeMetaData(path, entry.getKey(),  rootEntry,  rootDescriptor);
		// init tree
		rtree.initialize(rootEntry, rootDescriptor, Identity.DEFAULT_INSTANCE, blockSize, 
				treeContainer, dimension * 8 *2 , dimension * 8 *2, ratio);
	} 
	
	
	
	
	/**
	 * 
	 * @param entries
	 * @param level
	 * @return
	 */
	public MapEntry<Long, DoublePointRectangle> writeNode(List entries, int level) {
		DoublePointRectangle descriptor = null;
		for (Object o : entries ){
			DoublePointRectangle rec = (level == 0) ? (DoublePointRectangle)o: (DoublePointRectangle)((IndexEntry)o).descriptor();
			if (descriptor == null)
				descriptor = new DoublePointRectangle(rec);
			else 
				descriptor.union(rec);
		}
		final Node node = (Node) rtree.createNode(level);
		Long nodeId = (Long) treeContainer.reserve(new Constant<Node>(node));
		node.initialize(level, entries);
		treeContainer.update(nodeId, node);
		return new MapEntry<Long, DoublePointRectangle>(nodeId, descriptor);
		
	}
	
	/**
	 * 
	 * @param data
	 * @param number
	 * @param level
	 * @param out
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public int writeNode(Iterator data, int number, int level, DataOutput out) throws IOException{
		// write rectangles to 
		Iterator sorter = data;
		int written = 0;
		int nodeSize = (level > 0) ? B_Index: B_Leaf;
		while(sorter.hasNext() ){
			List entries = new ArrayList(nodeSize);
			for (int i = 0; i <  nodeSize && sorter.hasNext(); i++ ){
				DoublePointRectangle rec = null;
				if(level != 0 ){
					MapEntry<Long, DoublePointRectangle> mapEntry = (MapEntry<Long, DoublePointRectangle>) sorter.next();
					rec = mapEntry.getValue();
					// create index entry 
					IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
					((ORTree.IndexEntry)indexEntry.initialize(mapEntry.getKey())).initialize(rec);
					entries.add(indexEntry);
				}else{
					rec = (DoublePointRectangle) sorter.next();
					entries.add(rec);
				}
			}
			MapEntry<Long, DoublePointRectangle> entry = writeNode(entries, level);
			written++;
			mapEntryConverter.write(out, entry );
		}
		return written;
	}
	
	/**
	 * 
	 * @param dataF
	 * @param number
	 * @param level
	 * @param out
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public int tileData(List dataF, int number, int level, DataOutput out) throws IOException{
		if (number <= maxRecordsProNode){ 
			return  writeNode( dataF.iterator(), number, level,  out); 
		}
		List data = dataF;
		int writtenNodes = 0;
		// pre-process data 
		// sort take only center for orderings 
		double costs = Double.MAX_VALUE;
		int argMin = 0;
		int argDim = 0;
		
		int approxTreeHeight = ((int)(Math.ceil( Math.log(number)/ Math.log(maxRecordsProNode))))-1;
		double  MM =  Math.floor(Math.pow(maxRecordsProNode, approxTreeHeight))  ;
		int M = (int)MM;
		
		List[] sortedLists = new List[dimension];
	
		for(int i = 0; i < dimension; i++){
			// sort data 
			// 
			if (ESORT && (dataF.size() > B_Leaf*B_Leaf*20)){
				sortedLists[i] =  sort(dataF,  getDimensionComparator(i, level),level);
				data = sortedLists[i];
			}
			else{
				Collections.sort(data, getDimensionComparator(i, level));
			}
			// 
			List<DoublePointRectangle> forwardList = computeCosts(data,   true , level);
			List<DoublePointRectangle> backwardList = computeCosts(data,  false, level );		
			int splits = number/M;
			for(int k = 1; k <= splits; k++){
				// 
				int splitIndex = k*M;
				if (splitIndex  < number){
					DoublePointRectangle costLeft = forwardList.get(splitIndex-1);
					DoublePointRectangle costRight = backwardList.get(splitIndex);
					double fcost = costFunction.invoke(costLeft, costRight);
					if (fcost < costs){
						costs = fcost;
						argMin = splitIndex-1;
						argDim = i;
					}
				}
			}
		
		}
	//	System.out.println("Processed " + costs + " number " + number  + " index " + argMin + " dim " + argDim);
		if (ESORT && (dataF.size() > B_Leaf*B_Leaf*20)){
			data =  sort(dataF,  getDimensionComparator(argDim, level), level);
			data = sortedLists[argDim];
		}
		else{
			Collections.sort(data, getDimensionComparator(argDim, level));
		}
		List left = new ArrayList();
		left.addAll(data.subList(0, argMin+1));
		List right =  new ArrayList();
		right.addAll(data.subList(argMin+1, data.size()));
		data.clear();
		return tileData( left, left.size(), level,  out ) 
		+  tileData(right, right.size(), level, out);
	}
	
	/**
	 * 
	 * @param data
	 * @param forward
	 * @param level
	 * @return
	 */
	protected List<DoublePointRectangle> computeCosts(List data,  boolean forward, int level){
		DoublePointRectangle[] costs = new DoublePointRectangle[data.size()];
		DoublePointRectangle union = null;
		if (forward)
			for(int i = 0; i < data.size() ; i++){
				Object o1 = data.get(i);
				DoublePointRectangle rec = (level > 0) ? ((MapEntry<Long,DoublePointRectangle>)o1).getValue(): (DoublePointRectangle)o1;
				if(union == null )
					union = new DoublePointRectangle(rec);
				else 
					union.union(rec);
				costs[i]= new DoublePointRectangle(union);
			}
		else
			for(int i = data.size()-1; i >= 0 ; i--){
				Object o1 = data.get(i);
				DoublePointRectangle rec = (level > 0) ? ((MapEntry<Long,DoublePointRectangle>)o1).getValue(): (DoublePointRectangle)o1;
				if(union == null )
					union = new DoublePointRectangle(rec);
				else 
					union.union(rec);
				costs[i]= new DoublePointRectangle(union);
			}
		return Arrays.asList(costs); 
	}
	
	/**
	 * 
	 * @param dim
	 * @param level
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Comparator getDimensionComparator(final int dim, final int level){
		return  new Comparator() {

			@Override
			public int compare(Object  o1, Object o2) {
				DoublePointRectangle rec1 = (level > 0) ? ((MapEntry<Long,DoublePointRectangle>)o1).getValue(): (DoublePointRectangle)o1;
				DoublePointRectangle rec2 = (level > 0) ? ((MapEntry<Long,DoublePointRectangle>)o2).getValue(): (DoublePointRectangle)o2;
				DoublePoint first = rec1.getCenter();
				DoublePoint second = rec2.getCenter();
				return ( first.getValue(dim) == second.getValue(dim)) ? 0 :  ( first.getValue(dim) < second.getValue(dim)) ? 
						-1 : 1;
			}
		};
	}
	
	/**
	 * 
	 * @param data
	 * @param comp
	 * @param level
	 * @return
	 * @throws IOException
	 */
	protected List sort(List data, Comparator comp, int level) throws IOException{
		Iterator sorteddata = sort(data.iterator(),  level, comp ) ;
		List list = new ArrayList<>(data.size());
		while(sorteddata.hasNext()){
			list.add(sorteddata.next());
		}
		return list; 
	}
	
	/**
	 * 
	 * @param data
	 * @param level
	 * @param comp
	 * @return
	 * @throws IOException
	 */
	protected Iterator sort(Iterator data,  int level, Comparator comp ) throws IOException{
		final Converter converter = (level > 0 ) ? mapEntryConverter: dataConverter;
		String tmp = "tmp";
		int objectSize = (level > 0 ) ? dimension *  16 : dimension * 16  + 8; 
		Container container = new BlockFileContainer(path  + "tmpsortqueue.tmp", blockSize);
		final Container queueContainer = container;
		final Function<Function<?, Integer>, Queue<?>> queueFunction =
			new AbstractFunction<Function<?, Integer>, Queue<?>>() {
			public Queue<?> invoke(Function<?, Integer> function1, Function<?, Integer> function2) {
				return new BlockBasedQueue(queueContainer, blockSize, converter,
						function1, function2);
			}
		};
		//5% buffer
		return  new MergeSorter(data, 
				comp, objectSize ,  SORT_BUFFER_SIZE, SORT_BUFFER_SIZE, queueFunction, false);
	}
	
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#reinitTempLevelStorage()
	 */
	protected void reinitTempLevelStorage() throws IOException{
		file = File.createTempFile("levelRecs_", "dat");
		out = new DataOutputStream(new FileOutputStream(file));
	}
	
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#storeTempIndexEntry(xxl.core.collections.MapEntry)
	 */
	protected void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException{
		mapEntryConverter.write(out, entry);
	}
	
	/*
	 * (non-Javadoc)
	 * @see xxl.core.indexStructures.rtrees.AbstractIterativeRtreeBulkloader#getLevelIterator()
	 */
	protected Cursor getLevelIterator(){
		return  new FileInputCursor<MapEntry<Long,DoublePointRectangle>>(mapEntryConverter, file);
	}





	


}
