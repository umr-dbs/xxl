package xxl.core.indexStructures.rtrees;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.ConverterContainerRawAccess;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.ORTree.IndexEntry;

import xxl.core.indexStructures.ORTree.Node;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.rtrees.GenericPartitioner.Bucket;
import xxl.core.indexStructures.rtrees.GenericPartitioner.CostFunctionArrayProcessor;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.LongConverter;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.spatial.rectangles.Rectangles;



/**
 * 
 * This class implements level by level loading with partitioning methods proposed in: 
 * 
 * D Achakeev, B Seeger and P Widmayer:
 * "Sort-based query-adaptive loading of R-trees" in CIKM 2012
 * 
 * @author
 *
 */
public class RtreeIterativeBulkloader<T>  {
	public static boolean verbose = false;
	/**
	 * 
	 * 
	 *
	 */
	public static enum ProcessingType{
		SOPT_F,
		GOPT,
		SIMPLE,
	}
	
	/**
	 * 
	 */
	public static UnaryFunction<DoublePointRectangle, Double>  generateDefaultFunction(
			final double[] normalizedQuerySideLength){
		return new UnaryFunction<DoublePointRectangle, Double>() {

			@Override
			public Double invoke(DoublePointRectangle arg) {
				DoublePointRectangle rec =   new DoublePointRectangle(arg);
				double[] deltas = rec.deltas();
				double cost = 1d;
				double costD = 1d;
				for(int i = 0; i < deltas.length; i++ ){
					cost *= (deltas[i] + normalizedQuerySideLength[i]); 
					costD *=  deltas[i];
				}
				return  cost;
			}
			
		};
	}; 
	
	
	// data converter
	public  Converter<T> dataConverter;
	//
	public int dataSize; 
	// temp level entries converter 
	public  Converter<MapEntry<Long, DoublePointRectangle>> mapEntryConverter; 
	// dimension 
	public int dimension; 
	// target rtree
	public RTree rtree;
	// block size
	public int blockSize;
	// ratio to define b  
	public double ratio; 
	//
	private int partitionSize;
	// path for temp files
	public String path; 
	// 
	public int B_Leaf;
	//
	public int B_Index;
	//
	public int b_Index;
	//
	public int b_Leaf;
	// fan Out if weight balanced version
	public int fanOut;
	//
	public Container treeContainer; 
	//
	public CostFunctionArrayProcessor<? extends Rectangle> arrayProcessor; 
	// 
	public boolean weightFlag = true;
	//
	public int numberOfDataObjects;
	//
	protected ProcessingType pType;
	//
	protected double storageUtil;
	//
	DataOutputStream out = null;
	//
	File file = null;
	//Unary
	UnaryFunction<T, DoublePointRectangle> toRectangle;
	//
	public int level = 0;
	/**
	 * 
	 * @param path to store temporal level data
	 * @param dimension
	 * @param blockSize
	 * @param ratio for minimal block occupation e.g. 0.33 
	 * @param storageUtil mainly used for OPT algorithm, provides average storage utilization
	 * @param partitionSize size of partition to be hold in memory for partitioning algorithms
	 * @param universe
	 */
	@SuppressWarnings("deprecation")
	public RtreeIterativeBulkloader(RTree tree, 
			String path, 
			int dimension, 
			int blockSize, 
			double ratio,
			double storageUtil, 
			int partitionSize) {
		super();
		this.rtree = tree;
		this.dimension = dimension;
		this.blockSize = blockSize;
		this.ratio = ratio;
		this.path = path;
		this.partitionSize = partitionSize;
		this.storageUtil = storageUtil;
		// redirect container
		this.treeContainer = (Container) rtree.determineContainer.invoke(); 
		
	}
	/**
	 * 
	 * @param arrayProcessor
	 * @param pType
	 * @param treeContainer
	 * @param dataSize
	 * @param dataConverter
	 * @param toRectangle
	 * @return
	 */
	public RtreeIterativeBulkloader<T> initTreeBulkloader(
			CostFunctionArrayProcessor<? extends DoublePointRectangle>	arrayProcessor,
			ProcessingType pType,  
			int dataSize, 	
			final Converter<T> dataConverter, 
			UnaryFunction<T, DoublePointRectangle> toRectangle ){
		this.arrayProcessor = arrayProcessor;
		int payload = blockSize-6;
		b_Leaf = (int)( (payload *  ratio)/ (dataSize ));
		B_Leaf = payload / (dataSize);
		b_Index =  (int)( (payload *  ratio)/ (dimension * 16 + 8 )); 
		B_Index = payload / (dimension * 16 + 8 );
		this.pType = pType;
		fanOut = B_Index/4; 
		//
		this.dataSize = dataSize; 
		this.dataConverter = dataConverter; 
		//
		final Converter<DoublePointRectangle> rectangleConverter = new ConvertableConverter<DoublePointRectangle>(
				Rectangles.factoryFunctionDoublePointRectangle(dimension));
		//
		this.toRectangle = toRectangle;
		//
		mapEntryConverter = new Converter<MapEntry<Long, DoublePointRectangle>>(){

			@Override
			public MapEntry<Long, DoublePointRectangle> read(DataInput arg0,
					MapEntry<Long,DoublePointRectangle> arg1) throws IOException {
				long key = LongConverter.DEFAULT_INSTANCE.readLong(arg0);
				DoublePointRectangle value = rectangleConverter.read(arg0);
				return new MapEntry<Long, DoublePointRectangle>(key, value) ;
			}

			@Override
			public void write(DataOutput arg0,
					MapEntry<Long, DoublePointRectangle> arg1) throws IOException {
				LongConverter.DEFAULT_INSTANCE.write(arg0, arg1.getKey());
				arg1.getValue().write(arg0);
			}};
		return this;
	}
	
	/**
	 * 
	 * @param rectangles
	 * @throws IOException
	 */
	public void buildRTree(Iterator rectangles) throws IOException {
		Iterator tempIterator = rectangles;
		while(tempIterator.hasNext()){
			reinitTempLevelStorage();
			int B = (level > 0) ? B_Index : B_Leaf;
			int written = writeLevel(tempIterator, level, partitionSize,  rtree, treeContainer,   B);
			level++;
			Cursor levelIterator = getLevelIterator();
			tempIterator = levelIterator;
			 numberOfDataObjects = written;
			if (written <= 1){
				break;
			}
		}
		// create rtree
		MapEntry<Long, DoublePointRectangle> entry = (MapEntry<Long, DoublePointRectangle>)(tempIterator.next());
		DoublePointRectangle rootDescriptor = entry.getValue();
		IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
		IndexEntry rootEntry = (IndexEntry) ((ORTree.IndexEntry)indexEntry.initialize(entry.getKey())).initialize(rootDescriptor);
		// init tree set rootEntry and root descriptor
		rtree.initialize(rootEntry, rootDescriptor, new AbstractFunction() {
			@Override
			public Object invoke(Object argument) {
				return toRectangle.invoke((T)argument);
			}
		},
		blockSize, 
		treeContainer, dataSize, dimension * 8 *2, ratio);
		
	}
	/**
	 * 
	 * @throws IOException
	 */
	protected void reinitTempLevelStorage() throws IOException{
		file = File.createTempFile("levelRecs_", "dat");
		out = new DataOutputStream(new FileOutputStream(file));
	}
	/**
	 * 
	 * @param entry
	 * @throws IOException 
	 */
	protected void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException{
		mapEntryConverter.write(out, entry);
	}
	
	/**
	 * 
	 * @return
	 */
	protected Cursor getLevelIterator(){
		return  new FileInputCursor<MapEntry<Long,DoublePointRectangle>>(mapEntryConverter, file);
	}
	
	
	
	
	
	/**
	 * 
	 * @param data
	 * @param level
	 * @param partitionSize
	 * @param out
	 * @param rtree
	 * @param mapEntryConverter
	 * @param B
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked", "serial" })
	public int writeLevel(Iterator data, 
			final  int level, 
			int partitionSize,
			RTree rtree, 
			Container treeContainer, 
			int B) throws IOException{
		// read  partitions size to a list
		int counter = 0;
		List partition = new LinkedList();
		if (pType ==ProcessingType.SIMPLE){
			int P = (level > 0 ) ?  B_Index : B_Leaf; 
			P = (int) (storageUtil * P);
			partitionSize = P*P;
		}
		
		while(data.hasNext()){
			for(int  i = 0; data.hasNext() && i < partitionSize; i++ ){
				if (level > 0 ){
					MapEntry<Long, DoublePointRectangle> mapEntry = (MapEntry<Long, DoublePointRectangle>) data.next();
					DoublePointRectangle rec = mapEntry.getValue();
					// create index entry 
					IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
					((ORTree.IndexEntry)indexEntry.initialize(mapEntry.getKey())).initialize(rec);
					partition.add(indexEntry);
				}else{ 
					Object rec = data.next();
					partition.add(rec);
				}
			}
			
			if (partition.size() >  B ){
				Function mapping = new AbstractFunction() {
					
					public Object invoke(Object obj ){
						return (level ==  0 )? 
							 (toRectangle.invoke((T)obj) )	: (DoublePointRectangle)((IndexEntry)obj).descriptor(); 
					}
				}; 
				
				final int[] distribution = computeDistribution((Iterator<DoublePointRectangle>)new Mapper(mapping,  partition.iterator() ),  level, partition.size());
				
					
				counter += 	writePartition(distribution, partition.iterator(), level, B,  rtree,  treeContainer);
				if (verbose)
					System.out.println("L: " + level + " " + partition.size() );
			}else{   
				// just allocate one node
				MapEntry<Long, DoublePointRectangle> entry = writeNode(partition, level,  rtree,  treeContainer) ;
				storeTempIndexEntry(entry);
				//mapEntryConverter.write(out, entry );
				counter++;
			}
			partition = new LinkedList();
		}
		return counter;
	}
	
	/**
	 * writes a partition according computed distribution
	 * @param distribution
	 * @param data
	 * @param level
	 * @param out
	 * @return
	 * @throws IOException
	 */
	public int writePartition(int[] distribution, 
			Iterator data, 
			int level, 
			int B,  RTree rtree, Container treeContainer) throws IOException{
		for(int i : distribution){
			List entries = new ArrayList(i);
			for(int k = 0;  data.hasNext() && k < i ; k++){
				DoublePointRectangle rec = null;
				if(level != 0 ){
					IndexEntry indexEntry  = (IndexEntry) data.next();
					entries.add(indexEntry);
				}else{
					Object obj =  data.next();
					entries.add(obj);
				}
			}
			if (i > B ){
				throw new RuntimeException("too many entries per block");
			}		
			MapEntry<Long,DoublePointRectangle> entry = writeNode(entries, level,  rtree,  treeContainer);
			storeTempIndexEntry(entry);
		}
		return distribution.length;
	}
	
	 
	
	
	
	/**
	 * writes node {@link DoublePointRectangle}
	 * @param entries
	 * @param level
	 * @return
	 */
	public MapEntry<Long,DoublePointRectangle> createNode(List entries, int level) {
		DoublePointRectangle descriptor = null;
		for (Object o : entries ){
			DoublePointRectangle rec = (level == 0) ? (DoublePointRectangle)(toRectangle.invoke((T)o)): (DoublePointRectangle)((IndexEntry)o).descriptor();
			if (descriptor == null)
				descriptor = new DoublePointRectangle(rec);
			else 
				descriptor.union(rec);
		}
		final Node node = (Node) rtree.createNode(level);
		Long nodeId = (Long) treeContainer.reserve(new Constant<Node>(node));
		node.initialize(level, entries);
		treeContainer.update(nodeId, node);// I/O 
		return new MapEntry<Long, DoublePointRectangle>(nodeId, descriptor);
	}
	/**
	 * writes node {@link DoublePointRectangle}
	 * @param entries
	 * @param level
	 * @return
	 */
	public MapEntry<Long,DoublePointRectangle> writeNode(List entries, int level, RTree rtree, Container treeContainer) {
		DoublePointRectangle descriptor = null;
		for (Object o : entries ){
			DoublePointRectangle rec = (level == 0) ? (DoublePointRectangle)(toRectangle.invoke((T)o)): (DoublePointRectangle)((IndexEntry)o).descriptor();
			
			if (descriptor == null)
				descriptor = new DoublePointRectangle(rec);
			else 
				descriptor.union(rec);
		}
		final Node node = (Node) rtree.createNode(level);
		Long nodeId = (Long) treeContainer.reserve(new Constant<Node>(node));
		node.initialize(level, entries);
		treeContainer.update(nodeId, node);// I/O 
		return new MapEntry<Long, DoublePointRectangle>(nodeId, descriptor);
	}
	/**
	 * stores tree meta data 
	 * @param path
	 * @param rootNodeId
	 * @param rootEntry
	 * @param rootDescriptor
	 * @throws IOException
	 */
	public static void storeMetaData(String path, Long rootNodeId, IndexEntry rootEntry, Rectangle rootDescriptor) throws IOException{
		DataOutputStream dOut = new DataOutputStream(new FileOutputStream(path + "treeTemp.dat"));
		dOut.writeInt(rootEntry.parentLevel());
		dOut.writeLong(rootNodeId);
		rootDescriptor.write(dOut);
		dOut.close();	
	}
	/*
	 * 
	 */
	protected static DoublePointRectangle[] toArray(Iterator<DoublePointRectangle> iterator, int size){
		DoublePointRectangle[] recs = new DoublePointRectangle[size];
		int i = 0; 
		while(iterator.hasNext()){
			recs[i] = iterator.next();
			i++;
		}
		return recs;
	}
	
	
	
	
	
	
	@SuppressWarnings("unchecked")
	public int[] computeDistribution(Iterator<DoublePointRectangle> iterator,  int level, int size){
		this.arrayProcessor.reset();
		switch(pType){
			case SOPT_F :  {
				int b = (level > 0 ) ?  b_Index : b_Leaf;
				int B = (level > 0 ) ?  B_Index : B_Leaf; 
				int n = (int) (Math.ceil(size/(storageUtil * B)));
				Bucket[][] buckets = GenericPartitioner.computeOPTF(toArray(iterator, size), b, B, n, this.arrayProcessor);	
				// take last bucket
				return GenericPartitioner.getDistribution(buckets[n-1][size-1]);
			}
			case SIMPLE:{
				int B = (level > 0 ) ?  B_Index : B_Leaf; 
				B = (int) (storageUtil * B);
				List<Integer> list = new ArrayList<>();
				int number = size;
				while(number > 0){
					int n = (number - B ) >= 0 ? B : number;  
					list.add(n);
					number -= B;
				}
				int[] distribution = new int[list.size()];
				for(int i = 0; i < distribution.length; i++){
					distribution[i] = list.get(i);
				}
				return distribution;
			}
			default : {
				// GOPT is default
				int b = (level > 0 ) ?  b_Index : b_Leaf;
				int B = (level > 0 ) ?  B_Index : B_Leaf; 
				Bucket[] buckets = GenericPartitioner.computeGOPT(toArray(iterator, size), b, B, this.arrayProcessor);	
				return  GenericPartitioner.getDistribution(buckets[buckets.length-1]);
			}
		}
		
	}

	
	/******************************************************************************************************
	 * Static Classes
	 * 
	 * 
	 * 
	 ******************************************************************************************************/

	/**
	 * 
	 * @author achakeye
	 *
	 */
	public static class MainMemoryCollectionBulkLoader<T> extends RtreeIterativeBulkloader<T> {
		
		
		
		protected List<MapEntry<Long,DoublePointRectangle>> levelList;   	
		
		public MainMemoryCollectionBulkLoader(RTree tree, 
				String path, 
				int dimension,
				int blockSize, double ratio, double storageUtil,
				int partitionSize) {
			super(tree, path, dimension, blockSize, ratio, storageUtil, partitionSize);
		}
		
		
		@Override
		public RtreeIterativeBulkloader<T> initTreeBulkloader(
				CostFunctionArrayProcessor<? extends DoublePointRectangle> arrayProcessor,
				ProcessingType pType,  int dataSize,
				Converter<T> dataConverter,
				UnaryFunction<T, DoublePointRectangle> toRectangle) {
			 super.initTreeBulkloader(arrayProcessor, pType,  dataSize,
					dataConverter, toRectangle);
			 return this;
		}
		
		/*
		 * (non-Javadoc)
		 * @see core.bulkloader.RtreeIterativeBulkloader#reinitTempLevelStorage()
		 */
		protected void reinitTempLevelStorage() throws IOException{
			levelList = new LinkedList<MapEntry<Long,DoublePointRectangle>>();
		}
		/*
		 * (non-Javadoc)
		 * @see core.bulkloader.RtreeIterativeBulkloader#storeTempIndexEntry(xxl.core.collections.MapEntry)
		 */
		protected void storeTempIndexEntry(MapEntry<Long,DoublePointRectangle> entry) throws IOException{
			levelList.add(entry);
		}
		
		/*
		 * (non-Javadoc)
		 * @see core.bulkloader.RtreeIterativeBulkloader#getLevelIterator()
		 */
		protected Cursor getLevelIterator(){
			return Cursors.wrap(levelList.iterator());
		}
		
		@Override
		public int[] computeDistribution(Iterator<DoublePointRectangle> iterator,
			int level, int size) {
		return super.computeDistribution(iterator, level, size);
		}
		
	}
	
	
	
	/**
	 * 
	 * @author achakeye
	 *
	 */
	public static class RtreeIterativeBulkloaderAsynch<T> extends RtreeIterativeBulkloader<T>{
		
		private Future<?> future = null;
		
		private ExecutorService service = null;
		
		public RtreeIterativeBulkloaderAsynch(RTree tree, String path,
				int dimension, int blockSize, double ratio, double storageUtil,
				int partitionSize) {
			super(tree, path, dimension, blockSize, ratio, storageUtil, partitionSize);
			service =  Executors.newSingleThreadExecutor();
		}
		@SuppressWarnings("rawtypes")
		@Override
		public void buildRTree(Iterator rectangles) throws IOException {
			super.buildRTree(rectangles);
			service.shutdown();
		}

		/**
		 * 
		 * @param data
		 * @param level
		 * @param partitionSize
		 * @param out
		 * @param rtree
		 * @param mapEntryConverter
		 * @param B
		 * @return
		 * @throws IOException
		 */
		@SuppressWarnings({ "unchecked", "serial" })
		public int writeLevel(Iterator data, 
				final  int level, 
				int partitionSize,
				RTree rtree, 
				Container treeContainer, 
				int B) throws IOException{
			// read  partitions size to a list
			int counter = 0;
			List partition = new LinkedList();
			if (pType ==ProcessingType.SIMPLE){
				int P = (level > 0 ) ?  B_Index : B_Leaf; 
				P = (int) (storageUtil * P);
				partitionSize = P*P;
			}
			
			while(data.hasNext()){
				for(int  i = 0; data.hasNext() && i < partitionSize; i++ ){
					if (level > 0 ){
						MapEntry<Long, DoublePointRectangle> mapEntry = (MapEntry<Long, DoublePointRectangle>) data.next();
						DoublePointRectangle rec = mapEntry.getValue();
						// create index entry 
						IndexEntry indexEntry = (IndexEntry) rtree.createIndexEntry(level);
						((ORTree.IndexEntry)indexEntry.initialize(mapEntry.getKey())).initialize(rec);
						partition.add(indexEntry);
					}else{ 
						Object rec = data.next();
						partition.add(rec);
					}
				}
				
				if (partition.size() >  B ){
					Function mapping = new AbstractFunction() {
						
						public Object invoke(Object obj ){
							return (level ==  0 )? 
								 (toRectangle.invoke((T)obj) )	: (DoublePointRectangle)((IndexEntry)obj).descriptor(); 
						}
					}; 
					
					final int[] distribution = computeDistribution((Iterator<DoublePointRectangle>)new Mapper(mapping,  partition.iterator() ),  level, partition.size());
					
						
					counter += 	writePartition(distribution, partition.iterator(), level, B,  rtree,  treeContainer);
					if (verbose)
						System.out.println("L: " + level + " " + partition.size() );
				}else{   
					// just allocate one node
					MapEntry<Long, DoublePointRectangle> entry = writeNode(partition, level,  rtree,  treeContainer) ;
					storeTempIndexEntry(entry);
					//mapEntryConverter.write(out, entry );
					counter++;
				}
				partition = new LinkedList();
			}
			if (future!=null){
				try {
					future.get();
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				} catch (ExecutionException e) {
					
					e.printStackTrace();
				}
			}
			return counter;
		}
		/**
		 * writes a partition according computed distribution
		 * @param distribution
		 * @param data
		 * @param level
		 * @param out
		 * @return
		 * @throws IOException
		 */
		public int writePartition(int[] distribution, 
				Iterator data, 
				int level, 
				int B,  RTree rtree, Container treeContainer) throws IOException{
			return  writeFlushModus( distribution,  data, level, B,  rtree, treeContainer);
		}
		/**
		 * 
		 * @param distribution
		 * @param dataIter
		 * @param level
		 * @param B
		 * @param rtree
		 * @param treeContainer
		 * @return
		 * @throws IOException
		 */
		private int writeFlushModus(final int[] distribution, 
				final Iterator dataIter, 
				final int level, 
				final int B, final  RTree rtree,final  Container treeContainer) throws IOException{
		
			
			Runnable writer = new Runnable() {
				
				 List<Object> rList = consume();
				
				
				private List<Object> consume(){
					List<Object> recs = new LinkedList<>();
					while(dataIter.hasNext()){
						recs.add(dataIter.next());
					}
					return recs;
				}
				
				
				@Override
				public void run() {
					Node[] nodes = new Node[distribution.length];
					DoublePointRectangle[] recs = new DoublePointRectangle[distribution.length];
					int j = 0; 
					Iterator data = rList.iterator();
					for(int i : distribution){
						List entries = new ArrayList(i);
						DoublePointRectangle descriptor = null;
						for(int k = 0;  data.hasNext() && k < i ; k++){
							DoublePointRectangle rec = null;
							Object o = data.next();
							if(level != 0 ){
								IndexEntry indexEntry  = (IndexEntry) o;
								entries.add(indexEntry);
							}else{
								Object obj =  o;
								entries.add(obj);
							}
							rec = (level == 0) ? (DoublePointRectangle)(toRectangle.invoke((T)o)): (DoublePointRectangle)((IndexEntry)o).descriptor();
							
							if (descriptor == null)
								descriptor = new DoublePointRectangle(rec);
							else 
								descriptor.union(rec);
						}
						if (i > B ){
							throw new RuntimeException("too many entries per block");
						}		
						final Node node = (Node) rtree.createNode(level);
						node.initialize(level, entries);
						nodes[j] = node;
						recs[j] = descriptor;
						j++;
					}
					
					ConverterContainerRawAccess rawContainer = (ConverterContainerRawAccess)treeContainer; 
					Long[] ids = (Long[]) rawContainer.flushArrayOfBlocks(nodes);
					
					for(int i = 0;  i <ids.length ; i++){
						MapEntry<Long,DoublePointRectangle> entry = new MapEntry<Long, DoublePointRectangle>(ids[i], recs[i]);
						try {
							storeTempIndexEntry(entry);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					
				}
			};
			future = service.submit(writer);
			return distribution.length;
		}
	}
	
}
