package xxl.core.spatial.spatialBPlusTree;

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

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.UnaryFunction;
import xxl.core.functions.Functions;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.indexStructures.rtrees.RtreeIterativeBulkloader;
import xxl.core.io.Buffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.spatialBPlusTree.separators.LongKeyRange;
import xxl.core.spatial.spatialBPlusTree.separators.LongSeparator;

/**
 * This is a bulk loading class for a BPlusTree. Here we use a similar  optimization as in  {@link RtreeIterativeBulkloader}, since we use z-strings our optimization function is a 
 * sum of prefixes of z-strings.   
 * 
 * 
 * @author achakeev
 *
 */
public class SingleLevelwiseOptimizedBulkloader<T> {
	
	/**
	 * 
	 * @author d
	 *
	 */
	public static enum DistributionType{
		DISTRIBUTION_GOPT,
		DISTRIBUTION_OPT,
	}
	
	/**
	 * for writing data 
	 */
	public MeasuredConverter<T> dataConverter; 
	
	/**
	 * for writing temp files
	 */
	public Converter<MapEntry<Long, Long>> mapEntryConverter = new Converter<MapEntry<Long, Long>>(){

		@Override
		public MapEntry<Long, Long> read(DataInput arg0,
				MapEntry<Long, Long> arg1) throws IOException {
			long key = LongConverter.DEFAULT_INSTANCE.readLong(arg0);
			long value = LongConverter.DEFAULT_INSTANCE.readLong(arg0);
			return new MapEntry<Long, Long>(key, value) ;
		}

		@Override
		public void write(DataOutput arg0,
				MapEntry<Long, Long> arg1) throws IOException {
			LongConverter.DEFAULT_INSTANCE.write(arg0, arg1.getKey());
			LongConverter.DEFAULT_INSTANCE.write(arg0, arg1.getValue());
		}
		
	};
	
	/**
	 * 
	 */
	public BPlusTree tree;
	
	/**
	 * 
	 */
	public int partitionSize;
	
	/**
	 * 
	 */
	public int dimension;
	
	/**
	 * 
	 */
	public int blockSize;
	
	/**
	 * 
	 */
	public double ratio;
	
	/**
	 * 
	 */
	public double ratioIndex;
	
	/**
	 * 
	 */
	public int b_Leaf; 
	
	/**
	 * 
	 */
	public int B_Leaf;
	
	/**
	 * 
	 */
	public int b_Index; 
	
	/**
	 * 
	 */
	public int B_Index;
	
	/**
	 * 
	 */
	public int numberOfRectangles;
	
	/**
	 * 
	 */
	public double minMemory;
	
	/**
	 * 
	 */
	public double maxMemory;
	
	/**
	 * 
	 */
	public Container treeContainer; 
	
	/**
	 * 
	 */
	public DistributionType distributionType;
	
	/**
	 * 
	 */
	public double[] a;
	
	/**
	 * 
	 */
	String path;
	
	/**
	 * 
	 */
	final Function<T, Long> getKey; 
	
	/**
	 * 
	 */
	LongKeyRange rootDescriptor;
	
	/**
	 * 
	 * @param partitionSize
	 * @param dimension
	 * @param blockSize
	 * @param ratio
	 * @param memory
	 * @param fileContainer
	 * @param distributionType
	 * @param a
	 * @param path
	 */
	public SingleLevelwiseOptimizedBulkloader(MeasuredConverter<T> dataConverter, int partitionSize, 
			int dimension,
			int blockSize, 
			double ratio, 
			double minMemory, 
			double maxMemory,  
			Container fileContainer,
			DistributionType distributionType, 
			String path, 
			UnaryFunction<T,Long> getKey, 
			Buffer buffer) {
		super();
		this.dataConverter = dataConverter;
		this.getKey = Functions.toFunction(getKey);
		// duplicates enabled
		tree = new BPlusTree(blockSize, true);
		this.partitionSize = partitionSize;
		this.dimension = dimension;
		this.blockSize = blockSize;
		this.ratio = ratio;
		this.treeContainer = new ConverterContainer(fileContainer, tree.nodeConverter());
		if(buffer!=null){
			this.treeContainer = new BufferedContainer(this.treeContainer, buffer); 
		}
		this.distributionType = distributionType;
		this.path = path;
		this.minMemory = minMemory;
		this.maxMemory = maxMemory;
		// level 2 , number 4 , link 8
		int payload = blockSize-2-4-8; 
		b_Leaf = (int)( (payload *  ratio)/ (dimension * 16));
		B_Leaf = payload / (dimension * 16);
		b_Index =  (int)( (payload *  ratio)/ (8+8));
		B_Index = payload / (8+8);
		// hack to initialize detremineCpontainer Function;
		tree.initialize(null, null, 
				this.getKey,
				this.treeContainer, 
				ZBPlusTreeIndexFactrory.longKeyMeasuredConverter, 
				this.dataConverter,
				LongSeparator.FACTORY_FUNCTION,  
				LongKeyRange.FACTORY_FUNCTION);
		// 
	}

	/**
	 * 
	 * @param rectangles
	 * @throws IOException
	 */
	public void buildBPlusTRee(Iterator data) throws IOException{
		Iterator tempIterator = data;
		int level = 0;
		while(tempIterator.hasNext()){
			File file = File.createTempFile("levelKeys_", "dat");
			DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
			int written = writeLevel(tempIterator, level, partitionSize, out);
			level++;
			Cursor levelIterator = new FileInputCursor<MapEntry<Long,Long>>(mapEntryConverter, file);
			tempIterator = levelIterator;
			if (written <= 1){
				break;
			}
			file.deleteOnExit();
		}
		// create rtree
		MapEntry<Long, Long> entry = (MapEntry<Long, Long>)(tempIterator.next());
		Long key = entry.getValue();
		IndexEntry indexEntry = (IndexEntry) tree.createIndexEntry(level);
		LongSeparator rootSep = new LongSeparator(entry.getValue());
		IndexEntry rootEntry = (IndexEntry) ((BPlusTree.IndexEntry)indexEntry).initialize( entry.getKey(), rootSep);
		// hack to initialize detremineCpontainer Function;
		tree.initialize(rootEntry, rootDescriptor,
				getKey,
				this.treeContainer, 
				ZBPlusTreeIndexFactrory.longKeyMeasuredConverter, 
				dataConverter,
				LongSeparator.FACTORY_FUNCTION,  
				LongKeyRange.FACTORY_FUNCTION );
		
	} 
	
	
	/**
	 * 
	 * @param data
	 * @param level
	 * @param partitionSize
	 * @param out
	 * @return
	 * @throws IOException
	 */
	public int writeLevel(Iterator data, final  int level, int partitionSize,final  DataOutput out) throws IOException{
		// read  partitions size to a list
		int counter = 0;
		int k = 0;
		List partition = new LinkedList();
		while(data.hasNext()){
			for(int  i = 0; data.hasNext() && i < partitionSize; i++ ){
				if (level > 0 ){
					MapEntry<Long, Long> mapEntry = (MapEntry<Long, Long>) data.next();
					Long key = mapEntry.getValue();
					// create index entry 
					IndexEntry indexEntry = (IndexEntry) tree.createIndexEntry(level);
					//FIXME change to factory function!!!
					indexEntry.initialize(mapEntry.getKey(), new LongSeparator(key));
					partition.add(indexEntry);
				}else{ 
					DoublePointRectangle rec = (DoublePointRectangle) data.next();
					partition.add(rec);
				}
			}
			
			if (partition.size() > ((level > 0  ) ? B_Index : B_Leaf)  ){
				Function mapping = new AbstractFunction() {
					
					public Object invoke(Object obj ){
						return (level ==  0 )? getKey.invoke((T)obj) : ((IndexEntry)obj).separator.sepValue(); 
					}
				}; 
				final int[] distribution = computeDistribution((Iterator<Long>)new Mapper(mapping,  partition.iterator() ), 
						partition.size(), level);
				counter += 	writePartition(distribution, partition.iterator(),level, out);;
			}else{
				// just allocate one node
				MapEntry<Long, Long> entry = writeNode(partition, level) ;
				mapEntryConverter.write(out, entry );
				counter++;
			}
			partition = new LinkedList();
		}
		return counter;
	}
	
	public int writePartition(int[] distribution, Iterator data, int level, DataOutput out) throws IOException{
		for(int i : distribution){
			List entries = new ArrayList(i);
			for(int k = 0;  data.hasNext() && k < i ; k++){
				DoublePointRectangle rec = null;
				if(level != 0 ){
					IndexEntry indexEntry  = (IndexEntry) data.next();
					entries.add(indexEntry);
				}else{
					rec = (DoublePointRectangle) data.next();
					entries.add(rec);
				}
			}
			MapEntry<Long, Long> entry = writeNode(entries, level);
			mapEntryConverter.write(out, entry );
		}
		return distribution.length;
	}
	
	
	
	public MapEntry<Long, Long> writeNode(final List entries, int level) {
		Long descriptor = null;
		if (level == 0){ 
			descriptor = (Long) getKey.invoke((T)entries.get(entries.size()-1)); 
		}else{
			descriptor = (Long) ((IndexEntry) entries.get(entries.size()-1)).separator().sepValue();
		}
		if(rootDescriptor == null){
			Long smallestVal = (Long) getKey.invoke((T)entries.get(0)); 
			rootDescriptor = new LongKeyRange(smallestVal, smallestVal);
		}else{
			rootDescriptor.union(descriptor);
		}
		final Node node = (Node) tree.createNode(level);
		Long nodeId = (Long) treeContainer.reserve(new Constant<Node>(node));
		node.initialize(level, entries);
		treeContainer.update(nodeId, node);
		return new MapEntry<Long, Long>(nodeId, descriptor);
	}
	

	/**
	 * creates optimal distribution for space and function 
	 * @return
	 */
	protected int[] computeDistribution(Iterator<Long> iterator, int size, int level){
		int b = (level > 0) ? b_Index : b_Leaf;
		int B = (level > 0) ? B_Index : B_Leaf;
		int maxBlocks = (int) (Math.ceil(size/(minMemory * B)));
		int minBlocks = (int) (size/(maxMemory * B));
		switch(distributionType){
		case DISTRIBUTION_OPT : {
			return ZValueDistributionGenerator.computeZKeysDistributionApprox(
					Cursors.toList(iterator,new ArrayList<Long>()), 
					b, B);
		}
		default:	{	
			return 	ZValueDistributionGenerator.computeZKeysDistribution(
					Cursors.toList(iterator,new ArrayList<Long>()), 
					b, B, maxBlocks);
		}
		}
	} 	

}
