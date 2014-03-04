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
package xxl.core.indexStructures.mvbts;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
//import java.util.stream.IntStream;


import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.MapContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.containers.io.RawAccessContainer;
import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.io.QueueBuffer;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functional.NullaryFunction;
import xxl.core.indexStructures.MVBT;
import xxl.core.indexStructures.MVBTPlus;
import xxl.core.indexStructures.MVBTree;
import xxl.core.indexStructures.MVBTPlus.Element;
import xxl.core.indexStructures.MVBTPlus.LongVersion;
import xxl.core.indexStructures.MVBTPlus.OperationType;
import xxl.core.indexStructures.MVBTree.LeafEntry;
import xxl.core.indexStructures.MVBTree.MVRegion;
import xxl.core.indexStructures.MVBTree.MVSeparator;
import xxl.core.indexStructures.MVBTree.Version;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RawAccessUtils;
import xxl.core.util.Pair;



/**
 * This class is an example for usage of bulk loading partially persistent Btree {@link MVBTPlus}. 
 * 
 * In this example, we show how to initialize and conduct bulk-loading. 
 * In this example we will use a simple test data generator from {@link SimpleLoadMVBTree#generatedDeleteWorkload(int, double, long)}
 * We will generate 200_000 records; first 10% are inserts followed by intermixed sequence of delete and insert records;
 * We model our records as a tuple. The tuple with a time stamp consist of two values: a key of type long and a information part of type integer. 
 * 
 *  
 * Additionally we show how to use different containers and conduct experiments for different devises.
 * 
 * In this example we execute two naive tuple by tuple bulk-loading approaches for {@link MVBT} and {@link MVBTree}. We also run {@link MVBTPlus} bulk-loading algortihm. 
 * 
 * 
 * 
 * 
 * 
 * 
 * @author achakeye
 *
 */
public class MVBTPlusLoadingExample {
	
	/**
	 * 
	 * This is a MVRegion implementation, NOTE MVBTPlus uses own  {@link LongVersion} as default version implementation 
	 * 
	 * @author d
	 *
	 */
	public static class LongMVRegion extends MVRegion {
		
		public static final Function FACTORY_FUNCTION = new AbstractFunction<Object,Object>() {
			public Object invoke() {throw new UnsupportedOperationException();}
			public Object invoke(Object arg1) {throw new UnsupportedOperationException();}
			public Object invoke(Object arg1,Object arg2) {throw new UnsupportedOperationException();}
			public Object invoke(List<? extends Object> arguments) {
				if(arguments.size()!=4) throw new IllegalArgumentException();
				LongVersion beginVersion=(LongVersion)arguments.get(0);
				LongVersion endVersion=(LongVersion)arguments.get(1);
				Long min= (Long)arguments.get(2);
				Long max= (Long)arguments.get(3);
				return new LongMVRegion(beginVersion, endVersion, min, max);	
			}
		};

		public LongMVRegion(LongVersion beginVersion, LongVersion endVersion, Long min, Long max) {
			super(beginVersion, endVersion, min, max);	
		}
					
		public Object clone() {
			LongVersion begin=(LongVersion)beginVersion().clone();
			LongVersion end=isDead()?(LongVersion)endVersion().clone(): null;
			Long min=new Long(((Long)minBound()).longValue());
			Long max=this.isDefinite()? new Long(((Long)maxBound()).longValue()): null;
			return new LongMVRegion(begin, end, min, max);
		}
	}
	
	/**
	 *  This is a MVSeparator implementation, NOTE MVBTPlus uses own  {@link LongVersion} as default version implementation 
	 * @author d
	 *
	 */
	public static class LongMVSeparator extends MVSeparator{
		
		/**
		 * 
		 */
		public static final Function FACTORY_FUNCTION = new Function<Object,Object>(){
			public Object invoke() { throw new UnsupportedOperationException(); }

			public Object invoke(Object arg1) { throw new UnsupportedOperationException(); }
			
			public Object invoke(Object arg1, Object arg2) { throw new UnsupportedOperationException(); }

			public Object invoke(List<? extends Object> arguments) {
				if(arguments.size() !=3 ) throw new IllegalArgumentException();
				Version insertVersion = (Version)arguments.get(0);
				Version deleteVersion = (Version)arguments.get(1);
				Long min =  new Long((Long)arguments.get(2));
				return new LongMVSeparator(insertVersion, deleteVersion, min);	
			}
		};
		
		
		/**
		 * 
		 * @param insertVersion
		 * @param sepValue
		 */
		public LongMVSeparator(Version insertVersion, Long sepValue) {
			super(insertVersion, sepValue);
		}
		
		/**
		 * 
		 * @param insertVersion
		 * @param deleteVersion
		 * @param sepValue
		 */
		public LongMVSeparator(Version insertVersion, Version deleteVersion, Long sepValue) {
			super(insertVersion, deleteVersion, sepValue);
		}

		
		@Override
		public Object clone() {
			Long copySepValue = new Long((Long)sepValue());
			return new LongMVSeparator((this.insertVersion() != null)?
					(Version)this.insertVersion().clone() : null , 
					(this.deleteVersion() != null)? (Version)this.deleteVersion().clone() : null, 
							copySepValue);
		}

	}
	
	/**
	 * This is a simple wrapper for pair (long, long)
	 * @author d
	 *
	 */
	public static class PayLoadEntry extends Pair<Long,Long>{
		

		public static Converter<PayLoadEntry> DEFAULT_CONVERTER = new Converter<PayLoadEntry>(){

			@Override
			public PayLoadEntry read(DataInput dataInput, PayLoadEntry object)
					throws IOException {
				long f = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
				long s = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
				return new PayLoadEntry(f, s);
			}

			@Override
			public void write(DataOutput dataOutput, PayLoadEntry object)
					throws IOException {
				LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, object.first);
				LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, object.second);
				
			}
			
			
		};
		
		public PayLoadEntry(long f, long s){
			super(f,s);
		}
		
		public PayLoadEntry(Long f, Long s){
			super(f,s);
		}
		
	}
	/**
	 * Path were the trees will be stored
	 * if a persistent container is used
	 * 
	 */
	public static final String TREE_PATH = "F:/mvbt/mvbt_"; // change for your needs
	/**
	 * LRU_SLOTS or AVAILABLE Memory slots
	 */
	public static final int M = 1_00; // LRU_SLOTS or AVAILABLE Memory slots
	/**
	 * Block size 4KB
	 */
	public static final int BLOCK_SIZE = 4096;
	/**
	 * serialized size of a leaf entry in bytes.
	 * leaf nodes of {@link MVBT}, {@link MVBTree} {@link MVBTPlus} store {@link MVBTree.LeafEntry}. 
	 * Leaf entry stores additionally to the data object a time interval. Time interval uses 2 long values and one boolean value. 
	 * Since we manage key value pairs (long, integer)  our leaf entry consists of a three long values one integer and one boolen. 
	 * 
	 */
	public static final int LEAF_ENTRY_SIZE =41;//LongConverter.SIZE * 3  + IntegerConverter.SIZE + BooleanConverter.SIZE; // interval (long + long) + data (pair long + int)
	/**
	 * index entries store additionally node address and two weight counters
	 *  (pointer  + interval + key + weights ) 
	 */
	public static final int INDEX_ENTRY_SIZE = IntegerConverter.SIZE * 2 + LongConverter.SIZE  +  LongConverter.SIZE*2 + LongConverter.SIZE+ BooleanConverter.SIZE;// pointer  + interval + key + weights  
	/**
	 * besides the level information and  number of elements  
	 * each leaf node has two additional  backward pointers ( we actually store two index entries).  
	 */
	public static final int NODE_HEADER_SIZE =  3* IntegerConverter.SIZE+ 2*(45)  ; // 
	/**
	 * number of bytes available for storing entries
	 */
	public static final int NODE_PAYLOAD = BLOCK_SIZE-NODE_HEADER_SIZE; 
	/**
	 * number of entries per leaf node
	 */
	public static final int NODE_B_L =  NODE_PAYLOAD/LEAF_ENTRY_SIZE;
	/**
	 * number of entries per index node
	 */
	public static final int NODE_B_I = NODE_PAYLOAD / INDEX_ENTRY_SIZE;
	/**
	 * fraction of  live part
	 */
	public static final float E = 0.5f;
	/**
	 * fraction that defines the minimal number of entries per node
	 */
	public static final float D = 0.25f;
	/**
	 * branching parameter used for weight balancing
	 */
	public static final int FANOUT_PARAMETER_A = NODE_B_I/4;
	/**
	 * Number of records to insert
	 */
	public static final int OPERATIONS_NUMBER = 200_000;
	/**
	 * Number of blocks if RAW access container is used
	 */
	public static final int NUMBER_OF_BLOCKS_RAW_ACCESS = (OPERATIONS_NUMBER/NODE_B_I)*10;
	/**
	 * Number of blocks for a free list if RAW access container is used
	 */
	public static final int MAX_FREE_LIST = NUMBER_OF_BLOCKS_RAW_ACCESS/(BLOCK_SIZE/8); 
	
	/*****************************************************************************************************
	 * 
	 * 
	 * Converters
	 * 
	 * 
	 * 
	 ****************************************************************************************************/
	
	/**
	 * This is a converter for input objects 
	 */
	public static Converter<Pair<Long, PayLoadEntry>> recordEntryConverter = new Converter<Pair<Long, PayLoadEntry>>(){

		@Override
		public Pair<Long, PayLoadEntry> read(DataInput dataInput,
				Pair<Long, PayLoadEntry> object) throws IOException {
			long key = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
			PayLoadEntry array =  PayLoadEntry.DEFAULT_CONVERTER.read(dataInput);
			return new Pair<Long, PayLoadEntry>(key, array);
		}

		@Override
		public void write(DataOutput dataOutput, Pair<Long, PayLoadEntry> object)
				throws IOException {
			LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, object.getElement1());
			 PayLoadEntry.DEFAULT_CONVERTER.write(dataOutput, object.getSecond());
		}
	};

	
	/**
	 * 
	 * this converter is used for input object serialization in MVBTPlus (MVBT, MVBTree) nodes 
	 */
	public static MeasuredConverter<Pair<Long, PayLoadEntry>> dataConverter =  new MeasuredConverter<Pair<Long, PayLoadEntry>>() {
		 
		public int getMaxObjectSize() {
			 return 24;//;
		 }
		
		 public void write(DataOutput dataOutput, Pair<Long, PayLoadEntry> object) throws IOException {
			 recordEntryConverter.write(dataOutput, object);
		 };
		 
		 public Pair<Long, PayLoadEntry> read(DataInput dataInput, Pair<Long, PayLoadEntry> object) throws IOException {
			 return recordEntryConverter.read(dataInput);
		 
		 };

	};	
	

	
	
	/**
	 * converter for {@link Element}
	 * 
	 * this objects are temporarily stored in buffers of the buffer node (queues) 
	 * 
	 */
	public static MeasuredConverter<Element> recordConverter = new MeasuredConverter<Element>() {
		 
		public int getMaxObjectSize() {
			 return dataConverter.getMaxObjectSize() + LongConverter.SIZE +  IntegerConverter.SIZE;
		 }
	
		public void write(DataOutput dataOutput, Element object) throws IOException {
			recordEntryConverter.write(dataOutput, (Pair<Long,  PayLoadEntry>)object.getElement1());
			LongVersion.VERSION_CONVERTER.write(dataOutput, object.getElement2());
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, object.getElement3().ordinal());
		}
		
		public Element read(DataInput dataInput, Element object) throws IOException {
			Pair<Long,  PayLoadEntry> pair = recordEntryConverter.read(dataInput);
			LongVersion version = LongVersion.VERSION_CONVERTER.read(dataInput);
			int val = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
			OperationType type = OperationType.values()[val];
			return new Element(pair, version, type);
		};
	};
	/**
	 * 
	 */
	public static MeasuredConverter<Long> keyConverter = Converters.createMeasuredConverter(LongConverter.DEFAULT_INSTANCE);
	
	
	/*****************************************************************************************************
	 * 
	 * 
	 * END Converters
	 * 
	 * 
	 * 
	 ****************************************************************************************************/
	
	
	/**
	 * A key is a long value of a triple
	 */
	public static Function<Object, Long> getKey = new AbstractFunction<Object, Long>() {
		
		public Long invoke(Object argument) {
			return ((Pair<Long,  PayLoadEntry>)argument).getFirst();
		};
		
	}; 
	
	
	/**
	 * this method reads data sets store 
	 * 
	 * @param file
	 * @return
	 * @throws IOException
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
				Long time = new Long(timeStamp);
				Element record = new Element(new Pair<Long,  PayLoadEntry>(key, new PayLoadEntry(key.longValue(), key.longValue())),  new LongVersion(time), type);
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
	 * for "raw" we return {@link RawAccessContainer}
	 * for "memory" we return {@link MapContainer}
	 * default we return {@link BlockFileContainer}
	 * 
	 * @param path
	 * @param containerType
	 * @return returns container that manages MVBT nodes
	 * @throws IOException
	 */
	public static Container getContainer(String path, String containerType) throws IOException{
		//create 
		switch(containerType.toLowerCase()){
			case "raw":{
				// we simulate RAWAccess using RAFRawAccess
				// alternatively -> RAWAccess could be used on this case JNI RAW Access should be called and accessible
				// before we can create RAW access, we should create a file or raw devise
				File file = new File(path);
				file.createNewFile();
				RawAccessUtils.createFileForRaw(path, NUMBER_OF_BLOCKS_RAW_ACCESS + NUMBER_OF_BLOCKS_RAW_ACCESS/(NUMBER_OF_BLOCKS_RAW_ACCESS/8), BLOCK_SIZE); 
				return new RawAccessContainer(new RAFRawAccess(path, false, BLOCK_SIZE), MAX_FREE_LIST);
			} 
			case "memory":{
				return new MapContainer(); 
			}
			default:{
				return new BlockFileContainer(path, BLOCK_SIZE);
			}
		}
	}

	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		/*****************************************************************************************
		 * test data generation
		 ******************************************************************************************/
		
		String conatinerType= "raw"; //"memory"
		// create ASCII File with 200_000 operations
		SimpleLoadMVBTree.createASCIIFile(TREE_PATH+"data.dat", SimpleLoadMVBTree.generatedDeleteWorkload(OPERATIONS_NUMBER , 0.5d, 42));  
		/*****************************************************************************************
		 * bulk load MVBTPlus
		 ******************************************************************************************/
		// get iterator for reading entries from file
		Iterator<Element> it = getIteratorDataSet(TREE_PATH+"data.dat");
		//memory expressed in the number of elements
		int memoryCapacity = M*NODE_B_I;
		//we allocate LRU buffer, this represents available memory, all nodes and buffer pages will be managed in this buffer
		int LRU_SLOTS = M;
		LRUBuffer memory = new LRUBuffer<>(LRU_SLOTS);
		// we initialize tree
		MVBTPlus mvbtplus = new MVBTPlus(BLOCK_SIZE, D, E,  Long.MIN_VALUE);
		// now we create a container were we store buffers
		// we will use QueueBuffer alternatively BlockBasedQueue can be used
		final Container bufferStorage =  getContainer(TREE_PATH + "queues.dat", conatinerType);
		final CounterContainer cBufferStorage = new CounterContainer(new ConverterContainer(bufferStorage, QueueBuffer.getPageConverter(recordConverter)));
		//the queue buffer pages are managed by LRUbuffer 
		final CounterContainer memoryQueueStorage  = new CounterContainer(new BufferedContainer(cBufferStorage, memory));
		// this is a factory function that is used for bulk loading
		NullaryFunction<Queue<Element>> queueFunction = new NullaryFunction<Queue<Element>>() {
			@Override
			public Queue<Element> invoke() {
				return new QueueBuffer<>(memoryQueueStorage, recordConverter.getMaxObjectSize(), BLOCK_SIZE);
			}
			
		};
		// now we initialize container for MBVT Nodes and root structure
		CounterContainer cContainerInMVBT = new CounterContainer( getContainer(TREE_PATH + "mvbtplus.dat", conatinerType)); // file conatiner 
		Container ContainerInMVBT = new ConverterContainer(cContainerInMVBT, mvbtplus.nodeConverter()); // MVBT main 
		Container ContainerfMVBT = new ConverterContainer(cContainerInMVBT,  mvbtplus.rootsTree().nodeConverter()); // roots Tree
		//both container get LRU buffer assigned
		//NOTE: that BufferedContainer is a on top of hierarchy and does not need a converter, therefore, we do not need to serialize java objects (map tpo byte arrays)  
		Container fMVBTContainerIn = new BufferedContainer(ContainerInMVBT, memory); // buffer for MBVT Main
		Container fMVBTContainerRoots = new BufferedContainer(ContainerfMVBT, memory); // buffer for MBVT Main
		Container mainTreeContainer =  fMVBTContainerIn;
		Container rootsPlusContainer = fMVBTContainerRoots; 
		// initilaize MVBTPlus
		mvbtplus.initialize(null, // rootEntry
				null, // Descriptor MVRegion
				null, // roots tree root Entry
				null, // descriptro KeyRange
				getKey, // getKey Function
				rootsPlusContainer, // container roots tree
				mainTreeContainer, // main container
				keyConverter, // key converter 
				dataConverter, // data converter mapEntry
				LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
				LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
		// 
		Mapper<Element,Element> mapper = new Mapper<>(new AbstractFunction<Element, Element>() {
			int k = 1;
			  @Override
			public Element invoke(Element argument) {
					if(k % 10_000 == 0 )
						System.out.print(".");
					k++;
				return argument;
			}
			
		}, it);
		System.out.println();
		long time = System.currentTimeMillis();
		// here we provide additional information about how and what kind of queues are used as buffers
		// start bulk loading
		mvbtplus.bulkLoad(mapper, queueFunction,  memoryCapacity);
		System.out.println();
		time = System.currentTimeMillis()-time;
		//print number of I/Os for queues
//		System.out.println(cBufferStorage);
		//print number of I/Os for nodes
//		System.out.println(cContainerInMVBT);
		int queues = cBufferStorage.gets + cBufferStorage.updates + cBufferStorage.inserts;
		int main = cContainerInMVBT.gets +  cContainerInMVBT.updates + cContainerInMVBT.inserts;
		int all = queues+ main;
		System.out.println();
		System.out.printf(Locale.GERMANY, "queue I/O %d ; nodes I/O %d ; overall I/O %d ; time %d ; \n",  queues,  main, all, time);
		System.out.println();
		/*****************************************************************************************
		 * bulk load MVBT naive tuple-by-tuple
		 ******************************************************************************************/
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		// create MVBT
		MVBT tree = new MVBT(BLOCK_SIZE, D, E, Long.MIN_VALUE);
		LRUBuffer memMVBT = new LRUBuffer<>(LRU_SLOTS);
		//1.Init MVBT roots tree and main nodes are in the same container
		CounterContainer cContainerMVBT_LRU = new CounterContainer(getContainer((TREE_PATH + "mvbtLRU.dat"), conatinerType));
		Container containerMVBT_LRU = new ConverterContainer(cContainerMVBT_LRU, tree.nodeConverter()); // mvbt main
		Container cContainerMVBT_LRU_roots = new ConverterContainer(cContainerMVBT_LRU, tree.roots.nodeConverter()); // roots
		Container fMVBTContainer_LRU = new BufferedContainer(containerMVBT_LRU, memMVBT);
		CounterContainer cfMVBTContainer_LRU = new CounterContainer(fMVBTContainer_LRU);
		Container fMVBTContainer_LRU_roots = new BufferedContainer(cContainerMVBT_LRU_roots, memMVBT);
		//CounterContainer cfMVBTContainer = new CounterContainer(containerMVBT);
		Container mvbtStorageContainer_LRU = cfMVBTContainer_LRU;
		Container mvbtRootsContainer_LRU = fMVBTContainer_LRU_roots;
		tree.initialize(null, // rootEntry
				null, // Descriptor MVRegion
				null, // roots tree root Entry
				null, // descriptro KeyRange
				getKey, // getKey Function
				mvbtRootsContainer_LRU, // container roots tree
				mvbtStorageContainer_LRU, // main container
				LongVersion.VERSION_MEASURED_CONVERTER, // converter for version object 
				keyConverter, // key converter 
				dataConverter, // data converter mapEntry
				LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
				LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
		// create iterator for reading dat set
		it = getIteratorDataSet(TREE_PATH+"data.dat");
		System.out.println();
		// 
		mapper = new Mapper<>(new AbstractFunction<Element, Element>() {
					int k = 1;
					  @Override
					public Element invoke(Element argument) {
							if(k % 10_000 == 0 )
								System.out.print(".");
							k++;
						return argument;
					}
					
		}, it);
		time = System.currentTimeMillis();
		int k = 0;
		// insert elements
		while(mapper.hasNext()){
			Element record = (Element) mapper.next();
			OperationType ops = record.getElement3();
			LongVersion version = (LongVersion) record.getElement2().clone();
			Pair<Long, PayLoadEntry> object = (Pair<Long, PayLoadEntry>)record.getElement1();
			if(ops == OperationType.INSERT){
				tree.insert(version, object);
			}else if (ops == OperationType.DELETE){
				tree.remove(version, object);
			}else if (ops == OperationType.UPDATE){
				tree.update(version, object, object);
			}
			k++;
		}
		System.out.println();
		time = System.currentTimeMillis()-time;
		main = cContainerMVBT_LRU.gets +  cContainerMVBT_LRU.updates + cContainerMVBT_LRU.inserts;
		System.out.printf(Locale.GERMANY, "overlall I/O %d ; time %d ; \n",  main, time);
		System.out.println();
		/*****************************************************************************************
		 * bulk load MVBTtree naive tuple-by-tuple
		 ******************************************************************************************/
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		// create MVBT
		MVBTree mvbttree = new MVBTree(BLOCK_SIZE, D, E, Long.MIN_VALUE);
		LRUBuffer memMVBTree = new LRUBuffer<>(LRU_SLOTS);
		//1.Init MVBT roots tree and main nodes are in the same container
		CounterContainer cContainerMVBT_LRUtree = new CounterContainer(getContainer((TREE_PATH + "mvbttreeLRU.dat"), conatinerType));
		Container containerMVBT_LRUtree = new ConverterContainer(cContainerMVBT_LRUtree, mvbttree.nodeConverter()); // mvbt main
		Container cContainerMVBT_LRU_rootstree = new ConverterContainer(cContainerMVBT_LRUtree, mvbttree.roots.nodeConverter()); // roots
		Container fMVBTContainer_LRUtree = new BufferedContainer(containerMVBT_LRUtree, memMVBTree);
		CounterContainer cfMVBTContainer_LRUtree = new CounterContainer(fMVBTContainer_LRUtree);
		Container fMVBTContainer_LRU_rootstree = new BufferedContainer(cContainerMVBT_LRU_rootstree, memMVBTree);
		//CounterContainer cfMVBTContainer = new CounterContainer(containerMVBT);
		Container mvbtStorageContainer_LRUtree = cfMVBTContainer_LRUtree;
		Container mvbtRootsContainer_LRUtree = fMVBTContainer_LRU_rootstree;
		mvbttree.initialize(null, // rootEntry
				null, // Descriptor MVRegion
				null, // roots tree root Entry
				null, // descriptro KeyRange
				getKey, // getKey Function
				mvbtRootsContainer_LRUtree, // container roots tree
				mvbtStorageContainer_LRUtree, // main container
				LongVersion.VERSION_MEASURED_CONVERTER, // converter for version object 
				keyConverter, // key converter 
				dataConverter, // data converter mapEntry
				LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
				LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
		// create iterator for reading dat set
		it = getIteratorDataSet(TREE_PATH+"data.dat");
		System.out.println();
		// 
		mapper = new Mapper<>(new AbstractFunction<Element, Element>() {
					int k = 1;
					  @Override
					public Element invoke(Element argument) {
							if(k % 10_000 == 0 )
								System.out.print(".");
							k++;
						return argument;
					}
					
		}, it);
		time = System.currentTimeMillis();
		k = 0;
		// insert elements
		while(mapper.hasNext()){
			Element record = (Element) mapper.next();
			OperationType ops = record.getElement3();
			LongVersion version = (LongVersion) record.getElement2().clone();
			Pair<Long, PayLoadEntry> object = (Pair<Long, PayLoadEntry>)record.getElement1();
			if(ops == OperationType.INSERT){
				mvbttree.insert(version, object);
			}else if (ops == OperationType.DELETE){
				mvbttree.remove(version, object);
			}else if (ops == OperationType.UPDATE){
				mvbttree.update(version, object, object);
			}
			k++;
		}
		System.out.println();
		time = System.currentTimeMillis()-time;
		main = cContainerMVBT_LRUtree.gets +  cContainerMVBT_LRUtree.updates + cContainerMVBT_LRUtree.inserts;
		System.out.printf(Locale.GERMANY, "overall I/O  %d ; time %d ; \n",  main, time);
		System.out.println();
		
		/*****************************************************************************************
		 * bulk load MVBTPlus naive tuple-by-tuple
		 ******************************************************************************************/
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		// create MVBT
		MVBTPlus mvbttreePlus = new MVBTPlus(BLOCK_SIZE, D, E, Long.MIN_VALUE);
		LRUBuffer memMVBTreePlus = new LRUBuffer<>(LRU_SLOTS);
		//1.Init MVBT roots tree and main nodes are in the same container
		CounterContainer cContainerMVBT_LRUtreePlus = new CounterContainer(getContainer((TREE_PATH + "mvbttreeLRUPlus.dat"), conatinerType));
		Container containerMVBT_LRUtreePlus = new ConverterContainer(cContainerMVBT_LRUtreePlus, mvbttreePlus.nodeConverter()); // mvbt main
		Container cContainerMVBT_LRU_rootstreePlus = new ConverterContainer(cContainerMVBT_LRUtreePlus, mvbttreePlus.roots.nodeConverter()); // roots
		Container fMVBTContainer_LRUtreePlus = new BufferedContainer(containerMVBT_LRUtreePlus, memMVBTreePlus);
		CounterContainer cfMVBTContainer_LRUtreePlus = new CounterContainer(fMVBTContainer_LRUtreePlus);
		Container fMVBTContainer_LRU_rootstreePlus = new BufferedContainer(cContainerMVBT_LRU_rootstreePlus, memMVBTreePlus);
		//CounterContainer cfMVBTContainer = new CounterContainer(containerMVBT);
		Container mvbtStorageContainer_LRUtreePlus = cfMVBTContainer_LRUtreePlus;
		Container mvbtRootsContainer_LRUtreePlus = fMVBTContainer_LRU_rootstreePlus;
		mvbttreePlus.initialize(null, // rootEntry
				null, // Descriptor MVRegion
				null, // roots tree root Entry
				null, // descriptro KeyRange
				getKey, // getKey Function
				mvbtRootsContainer_LRUtreePlus, // container roots tree
				mvbtStorageContainer_LRUtreePlus, // main container
				keyConverter, // key converter 
				dataConverter, // data converter mapEntry
				LongMVSeparator.FACTORY_FUNCTION, // factory function for separator
				LongMVRegion.FACTORY_FUNCTION); // factory function for MultiVersion Regions
		// create iterator for reading dat set
		it = getIteratorDataSet(TREE_PATH+"data.dat");
		System.out.println();
		// 
		mapper = new Mapper<>(new AbstractFunction<Element, Element>() {
					int k = 1;
					  @Override
					public Element invoke(Element argument) {
							if(k % 10_000 == 0 )
								System.out.print(".");
							k++;
						return argument;
					}
					
		}, it);
		time = System.currentTimeMillis();
		k = 0;
		// insert elements
		while(mapper.hasNext()){
			Element record = (Element) mapper.next();
			mvbttreePlus.insert(record);
			k++;
		}
		System.out.println();
		time = System.currentTimeMillis()-time;
		main = cContainerMVBT_LRUtreePlus.gets +  cContainerMVBT_LRUtreePlus.updates + cContainerMVBT_LRUtreePlus.inserts;
		System.out.printf(Locale.GERMANY, "overall I/O  %d ; time %d ; \n",  main, time);
		System.out.println();
	}

}
