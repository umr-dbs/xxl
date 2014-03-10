package xxl.core.indexStructures.mvbts;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.sources.Permutator;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.MVBTree;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.MVBTree.Lifespan;
import xxl.core.indexStructures.MVBTree.MVRegion;
import xxl.core.indexStructures.MVBTree.MVSeparator;
import xxl.core.indexStructures.descriptors.LongMVRegion;
import xxl.core.indexStructures.descriptors.LongMVSeparator;
import xxl.core.indexStructures.descriptors.LongVersion;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.util.Pair;
import xxl.core.util.Triple;
/**
 * This class shows how to initialize and use @see {@link  MVBTree}. 
 * 
 * 
 * 
 */
public class SimpleLoadMVBTree {
	public static final String file = "F:/mvbt_";// change for your needs
	public static final int LRU_SLOTS = 1_00; // LRU buffer slots
	public static final int BLOCK_SIZE = 4096*2; // page size in bytes 
	public static final float D = 0.25f; // minimum number of live elements per node
	public static final float E = 0.5f; //  fraction of live number 
	/**
	 * Type of operation 
	 */
	public static enum OperationType{
		INSERT, 
		UPDATE,
		DELETE,
	}
	/**
	 * type def for log entry: represented as data, time stamp, and operation type (insert|delete|update)
	 */
	public static class LogEntry<T> extends Triple<T,Long, OperationType>{
		
		public LogEntry(T data, Long timeStamp, OperationType opstype) {
			super(data, timeStamp, opstype);
		}
	}

	
	/**
	 *  10% are inserts followed by intermixed sequence of insert and deletes
	 * @param operationsNumber
	 */
	public static List<LogEntry<Long>> generatedDeleteWorkload(int operationsNumber, double deleteRatio, long seed){
		Random random = new Random(seed);
		List<LogEntry<Long>> resultSequence = new LinkedList<LogEntry<Long>>();
		int firstInsertsNumber = operationsNumber /10;
		int remainNumber = operationsNumber -firstInsertsNumber;
		int deleteNumber = (int)((double)remainNumber * deleteRatio); 
		int overallInsertNumber = operationsNumber -deleteNumber;
		int[] deletesInserts = new int[remainNumber];
		for(int i = 0; i < deletesInserts.length; i++){
			if(i <= deleteNumber){
				deletesInserts[i] = 3;
			}else{
				deletesInserts[i] = 1;
			}
		}
		List<Integer> liveSet = new ArrayList<>(remainNumber);
		//populate live set and generate first inserts 
		Permutator permutator = new Permutator(overallInsertNumber, random);
		long ts = 0;
		for(int i = 0; i < firstInsertsNumber && permutator.hasNext(); i++){
			Integer key = permutator.next();
			liveSet.add(key);
			resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.INSERT));
			ts++;
		}
		// generate remain inserts and deletes
		Permutator remainPermutator = new Permutator(deletesInserts, random){
			@Override
			public void open() {
				if (!isOpened)
					isOpened = true;
			}
		};
		List<Integer> deletes = new ArrayList<>();
		while(remainPermutator.hasNext()){
			Integer opsType = remainPermutator.next();
			if(opsType == 3){ //delete
				if(liveSet.isEmpty()){
					deletes.add(opsType);
				}else{
					int index = random.nextInt(liveSet.size());
					Integer key = liveSet.get(index);
					resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.DELETE));
					liveSet.remove(index);
				}
			}else{ // insert
				if(permutator.hasNext()){
					Integer key = permutator.next();
					liveSet.add(key);
					resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.INSERT));
				}
			}
			ts++;
		}
		for(Integer del : deletes){
			int index = random.nextInt(liveSet.size());
			Integer key = liveSet.get(index);
			resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.DELETE));
			liveSet.remove(index);
		}
		return resultSequence;
	}

	/**
	 *  10% are inserts followed by intermixed sequence of insert and deletes
	 * @param operationsNumber
	 */
	public static List<LogEntry<Long>> generatedUpdateWorkload(int operationsNumber, double updateRatio, long seed){
		Random random = new Random(seed);
		List<LogEntry<Long>> resultSequence = new LinkedList<LogEntry<Long>>();
		int firstInsertsNumber = operationsNumber /10;
		int remainNumber = operationsNumber -firstInsertsNumber;
		int updateNumber = (int)((double)remainNumber * updateRatio); 
		int overallInsertNumber = operationsNumber -updateNumber;
		int[] deletesInserts = new int[remainNumber];
		for(int i = 0; i < deletesInserts.length; i++){
			if(i <= updateNumber){
				deletesInserts[i] = 2;
			}else{
				deletesInserts[i] = 1;
			}
		}
		List<Integer> liveSet = new ArrayList<>();
		//populate live set and generate first inserts 
		Permutator permutator = new Permutator(overallInsertNumber, random);
		long ts = 0;
		for(int i = 0; i < firstInsertsNumber && permutator.hasNext(); i++){
			Integer key = permutator.next();
			liveSet.add(key);
			resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.INSERT));
			ts++;
		}
		// generate remain inserts and deletes
		Permutator remainPermutator = new Permutator(deletesInserts, random){	
			@Override
			public void open() {
				if (!isOpened)
					isOpened = true;
			}
		};
		while(remainPermutator.hasNext()){
			Integer opsType = remainPermutator.next();
			if(opsType == 2){ //delete
				int index = random.nextInt(liveSet.size());
				Integer key = liveSet.get(index);
				resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.UPDATE));
			}else{ // insert
				if(permutator.hasNext()){
					Integer key = permutator.next();
					liveSet.add(key);
					resultSequence.add(new LogEntry<Long>(new Long(key), ts, OperationType.INSERT));
				}
			}
			ts++;
		}
		return resultSequence;
	}
	/**
	 * 
	 * @param path
	 * @param dataList
	 * @throws IOException
	 */
	public static void createASCIIFile(String path, List<LogEntry<Long>> dataList) throws IOException{
		PrintStream printStream = new PrintStream(path);
		for(LogEntry<Long> entry : dataList){
			printStream.println((entry.getElement3().ordinal()+1) +" "+entry.getElement1()+" 0 ");
		}
		printStream.close();
	}
	
	/**
	 * 
	 * simple tye def for loading
	 *
	 */
	public static class Element extends Triple<Object, LongVersion, OperationType>{
		
		public Element(Object data, LongVersion  version, OperationType type) {
			super(data, version, type);
		}
	}
	/** 
	 * simple iterator to read ascii files of the following format, sorted acording to time stamps 
	 * 
	 * operationType(1|2|3) key info
	 * 
	 * 
	 * 
	 */
	public static Iterator<Element> getIteratorDataSet(String file) throws IOException{
		final BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
		final String f = reader.readLine();
		return new Iterator<Element>() {
			String line = f;
			int timeStamp = 1;
			@Override
			public boolean hasNext() {
				return line != null;
			}

			@Override
			public Element next() {
				String[] plainrecord = line.split(" ");
				int ops = new Integer(plainrecord[0]);
				OperationType type = null;
				switch(ops){
					case 1 : type = OperationType.INSERT; break;
					case 2 : type = OperationType.UPDATE; break;
					case 3 : type = OperationType.DELETE; break;
					default : throw new IllegalArgumentException(); 
				}
				Long key = new Long(plainrecord[1]);
				Integer info = new Integer(plainrecord[2]);
				Long time = new Long(timeStamp);
				Element record = new Element(new Pair<Long, Integer>(key, info),  new LongVersion(time), type);
				timeStamp++;
				try {
					line =  reader.readLine();
				} catch (IOException e) {
					line = null;
				}
				return record;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
				
			}
			
		};
	}
	
	
	/**
	 * Data to store are key value pairs Long and Integer
	 * 
	 * Serializer for our data objects
	 * 
	 */
	public static Converter<Pair<Long,Integer>> recordEntryConverter = new Converter<Pair<Long,Integer>>(){

		@Override
		public Pair<Long,Integer> read(DataInput dataInput,
				Pair<Long,Integer> object) throws IOException {
			long key = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
			int value = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput); 
			return new Pair<Long,Integer>(key, value);
		}

		@Override
		public void write(DataOutput dataOutput, Pair<Long,Integer> object)
				throws IOException {
			LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, object.getElement1());
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, object.getSecond());	
		}
	};
	
	/**
	 * Function to extract key from the key values pairs
	 */
	public static Function<Object, Long> getKey = new AbstractFunction<Object, Long>() {
		
		public Long invoke(Object argument) {
			return ((Pair<Long, Integer>)argument).getFirst();
		};
		
	}; 

	/**
	 * Serializer, provides additionally the maximal size of the object 
	 */
	public static MeasuredConverter<Pair<Long,Integer>> dataConverter =  
			Converters.createMeasuredConverter(LongConverter.SIZE + IntegerConverter.SIZE, 
					recordEntryConverter);
	/**
	 * Serializer for the keys
	 */
	public static MeasuredConverter<Long> keyConverter = Converters.createMeasuredConverter(LongConverter.DEFAULT_INSTANCE);
	
	/**
	 * This method load MVBT tree with its previous state
	 *  
	 * @param metaInfo
	 */
	public static void reloadMVBT(MVBTree tree, String metaInfo) throws IOException{
		//MVBT previous version
		BPlusTree.IndexEntry rootEntryMVBTree = null;
		Descriptor liveRootDescriptorMVBTree = null; 
		BPlusTree.IndexEntry rootsRootEntryMVBTree = null; 
		Descriptor rootsRootDescriptorMVBTree = null;
		// MVBT VariableLength  info to restore the state  
		Descriptor liveRootDescriptorMVBTreevl = null; 
		Descriptor rootsRootDescriptorMVBTreevl = null;
		DataInputStream dis = new DataInputStream(new FileInputStream(new File(metaInfo)));
		boolean hasRootEntry = BooleanConverter.DEFAULT_INSTANCE.readBoolean(dis);	 // check if the root entry exists
		if (hasRootEntry){
			int level = IntegerConverter.DEFAULT_INSTANCE.readInt(dis); // read level info of root entry
			Object id = LongConverter.DEFAULT_INSTANCE.read(dis); // read container id of the root entry (root node)
			LongVersion minVersionSeparator = LongVersion.VERSION_CONVERTER.read(dis); // min version sep
			LongVersion minVersion = LongVersion.VERSION_CONVERTER.read(dis); // min version region
			Long minKey = LongConverter.DEFAULT_INSTANCE.read(dis); // min key
			Long maxKey = LongConverter.DEFAULT_INSTANCE.read(dis);// max key
			// init index entry
			rootEntryMVBTree = ((BPlusTree.IndexEntry)tree.createIndexEntry(level)).initialize(id, 
					new LongMVSeparator( minVersionSeparator, minKey)); 
			liveRootDescriptorMVBTree = new LongMVRegion(minVersion, null, minKey, maxKey);
			boolean hasRootsTree = BooleanConverter.DEFAULT_INSTANCE.readBoolean(dis);	
			if (hasRootsTree){ // check if roots tree exists 
				int levelRoots = IntegerConverter.DEFAULT_INSTANCE.readInt(dis);
				Object idRoots = LongConverter.DEFAULT_INSTANCE.read(dis);
				LongVersion minVersionRoots = LongVersion.VERSION_CONVERTER.read(dis);
				LongVersion maxVersionRoots = LongVersion.VERSION_CONVERTER.read(dis);
				rootsRootDescriptorMVBTree = new MVBTree.Lifespan(minVersionRoots, maxVersionRoots);
				rootsRootEntryMVBTree =(BPlusTree.IndexEntry)(tree.createIndexEntry(levelRoots)).initialize(idRoots);
			}
		}
		dis.close();
		//
		// container for node serialization reload read the block size from the container meta info
		Container containerMVBT_LRU = new BlockFileContainer(file+ "tree"); 
		// wrapper for i/o counting
		CounterContainer cContainerMVBT_LRU = new CounterContainer(containerMVBT_LRU);
		// LRU buffer
		Container fMVBTContainer_LRU = new BufferedContainer(cContainerMVBT_LRU, new LRUBuffer(LRU_SLOTS));
		CounterContainer cfMVBTContainer_LRU = new CounterContainer(fMVBTContainer_LRU);
		// serializer container for main tree
		Container mvbtStorageContainer_LRU = new ConverterContainer(cfMVBTContainer_LRU, tree.nodeConverter());
		//serializer container for roots tree	
		Container mvbtRootsContainer_LRU = new ConverterContainer(cfMVBTContainer_LRU, tree.rootsTree().nodeConverter());
		tree.initialize(rootEntryMVBTree, // rootEntry
				liveRootDescriptorMVBTree, // Descriptor MVRegion
				rootsRootEntryMVBTree, // roots tree root Entry
				rootsRootDescriptorMVBTree, // descriptro KeyRange
				getKey, // getKey Function
				mvbtRootsContainer_LRU, // container roots tree
				mvbtStorageContainer_LRU, // main container
				LongVersion.VERSION_MEASURED_CONVERTER, // converter for version object 
				keyConverter, // key converter 
				dataConverter, // data converter mapEntry
				LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
				LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
	}
	
	/**
	 * 
	 */
	public static void saveMetaInfo(String path, MVBTree tree) throws IOException{
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(path)));
		IndexEntry rootEntry = (IndexEntry)tree.rootEntry();   
		LongMVRegion rootDescriptor = (LongMVRegion)tree.rootDescriptor(); 
		IndexEntry rootRootEntry = (IndexEntry) tree.roots.rootEntry(); 
		KeyRange rootsRootKeyRange = (KeyRange)tree.roots.rootDescriptor(); 
		//write root entry information
		boolean hasRootEntry = (tree.rootEntry() != null);
		BooleanConverter.DEFAULT_INSTANCE.writeBoolean(dos, hasRootEntry);
		if(hasRootEntry){
		//get liveRoot MetaData
			Tree.IndexEntry liveRoot = tree.rootEntry();
			MVBTree.MVRegion liveRootRegion = (MVBTree.MVRegion)tree.rootDescriptor();
			MVBTree.MVSeparator liveRootSeparator = (MVBTree.MVSeparator)((BPlusTree.IndexEntry) tree.rootEntry()).separator;
			int height = tree.height();
			Object id = liveRoot.id();
			MVBTree.Lifespan liveRootRegionLifeSpan = liveRootRegion.lifespan();
			LongVersion minVersionSeparator = (LongVersion)liveRootSeparator.lifespan().beginVersion();
			LongVersion minVersionDescriptor = (LongVersion)liveRootRegionLifeSpan.beginVersion();
			Long minKey = (Long)liveRootRegion.minBound();
			Long maxKey = (Long)liveRootRegion.maxBound();
			// level
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dos, height);
			// id
			LongConverter.DEFAULT_INSTANCE.write(dos, (Long)id);
			// version of sep and desc
			LongVersion.VERSION_CONVERTER.write(dos, minVersionSeparator);
			LongVersion.VERSION_CONVERTER.write(dos, minVersionDescriptor);
			// key
			LongConverter.DEFAULT_INSTANCE.write(dos,minKey);
			LongConverter.DEFAULT_INSTANCE.write(dos,maxKey);
			// roots tree
			boolean hasRootsTree = (tree.rootsTree().rootEntry() != null);
			BooleanConverter.DEFAULT_INSTANCE.writeBoolean(dos, hasRootsTree);
			if (hasRootsTree){
				Tree.IndexEntry rootsRoot = tree.rootsTree().rootEntry();
				MVBTree.Lifespan rootRootsDescriptor = (MVBTree.Lifespan)tree.rootsTree().rootDescriptor();
				LongVersion minVersionRoots = (LongVersion)rootRootsDescriptor.beginVersion();
				LongVersion maxVersionRoots = (LongVersion)rootRootsDescriptor.endVersion();
				int rootsHeight =  tree.rootsTree().height();
				Object rootsId = rootsRoot.id();
				//Save metadata
				IntegerConverter.DEFAULT_INSTANCE.writeInt(dos, rootsHeight);
				LongConverter.DEFAULT_INSTANCE.write(dos, (Long)rootsId);
				LongVersion.VERSION_CONVERTER.write(dos, minVersionRoots);
				LongVersion.VERSION_CONVERTER.write(dos, maxVersionRoots);
			}	
		}
		dos.flush();
		dos.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		boolean reload = false;
		/*****************************************************************************************
		 * Initialize and insert data
		 ******************************************************************************************/
		int operations = 50_000;
		MVBTree tree = new MVBTree(BLOCK_SIZE, D, E, Long.MIN_VALUE);
		if (!reload){
			// create delete workload consisting of 50_000 elements
			// first 10_000 inserts followed by intermixed sequnce of delets and inserts
			createASCIIFile(file+"data.dat", generatedDeleteWorkload(operations, 0.5d, 42));  
			// container for node serialization
			// on disk
			Container containerMVBT_LRU = new BlockFileContainer(file+ "tree", BLOCK_SIZE); 
			// wrapper for i/o counting
			CounterContainer cContainerMVBT_LRU = new CounterContainer(containerMVBT_LRU);
			// serializer container for main tree
			// important tree.nodeConverter() convertre for MVBT nodes
			Container mvbtStorageContainer_LRU = new ConverterContainer(cContainerMVBT_LRU, tree.nodeConverter());
			//serializer container for roots tree
			// important  tree.rootsTree().nodeConverter() converter for nodes of roots tree
			Container mvbtRootsContainer_LRU = new ConverterContainer(cContainerMVBT_LRU, tree.rootsTree().nodeConverter());
			// LRU buffer
			Buffer LRUBuffer = new LRUBuffer(LRU_SLOTS); 
			Container fMVBTContainer_LRU_main = new BufferedContainer(mvbtStorageContainer_LRU, LRUBuffer);
			Container fMVBTContainer_LRU_roots = new BufferedContainer(mvbtRootsContainer_LRU, LRUBuffer);
			//init MVBT
			tree.initialize(null, // rootEntry
					null, // Descriptor MVRegion
					null, // roots tree root Entry
					null, // descriptro KeyRange
					getKey, // getKey Function
					fMVBTContainer_LRU_roots,  // container roots tree
					fMVBTContainer_LRU_main,  // main container
					LongVersion.VERSION_MEASURED_CONVERTER, // converter for version object 
					keyConverter, // key converter 
					dataConverter, // data converter mapEntry
					LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
					LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
			Iterator<Element> it = getIteratorDataSet(file+"data.dat");
			System.out.println();
			Cursor taker = new Taker(it, operations){
				int k = 0;
				@Override
				public Object next() throws IllegalStateException,
						NoSuchElementException {
					if(k % 5_000 == 0)
						System.out.print(".");
					k++;
					return super.next();
				}
			};
			long time = System.currentTimeMillis();
			// perform operation
			while(taker.hasNext()){
				Element record = (Element) taker.next();
				OperationType ops = record.getElement3();
				LongVersion version = (LongVersion) record.getElement2().clone();
				Pair<Long, Integer> object = (Pair<Long,Integer>)record.getElement1();
				if(ops == OperationType.INSERT){
					tree.insert(version, object);
				}else if (ops == OperationType.DELETE){
					tree.remove(version, object);
				}else if (ops == OperationType.UPDATE){
					tree.update(version, object, object);
				}
			}
			System.out.println();
			System.out.println(System.currentTimeMillis() - time);
			System.out.println(cContainerMVBT_LRU);
			/*******************************************************************************
			 * Key-Time range query 
			 ******************************************************************************/
			Cursor result = tree.rangePeriodQuery(new Long(1000), 
									new Long(1042), new LongVersion(1000), new LongVersion(1042));
			Cursors.println(result);
			//save the state of the tree
			fMVBTContainer_LRU_main.flush();
			fMVBTContainer_LRU_roots.close();
			saveMetaInfo(file + "metainfo.dat", tree);
		}else{
			reloadMVBT(tree, file + "metainfo.dat"); 
			Cursor result = tree.rangePeriodQuery(new Long(1000), 
					new Long(1042), new LongVersion(1000), new LongVersion(1042));
			Cursors.println(result);
		}
		
	}

}
