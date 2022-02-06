package xxl.core.indexStructures.mvbts;


import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.NoSuchElementException;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.indexStructures.MVBTree;
import xxl.core.indexStructures.MVBTree.Root;
import xxl.core.indexStructures.descriptors.LongMVRegion;
import xxl.core.indexStructures.descriptors.LongMVSeparator;
import xxl.core.indexStructures.descriptors.LongVersion;
import xxl.core.util.Pair;
import xxl.core.util.Triple;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.functions.Function;
import xxl.core.functions.AbstractFunction;


public class MVBTreeDemo {
    public static final String file = "D:/Code/java/xxl/temp/mvb_demo/mvbt_";// change for your needs
	
	public static final int LRU_SLOTS = 1000; // LRU buffer slots
	public static final int BLOCK_SIZE = 512; // page size in bytes 
	public static final float D = 0.20f; // minimum number of live elements per node
	public static final float E = 0.50f; //  fraction of live number 

	public static final int num_operation = 120; //  number of operations 0-14; 15-74; 75-110; 111-


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
	 * Function to extract key from the key values pairs
	 */
	public static Function<Object, Long> getKey = new AbstractFunction<Object, Long>() {
		
		public Long invoke(Object argument) {
			return ((Pair<Long, Integer>)argument).getFirst();
		};
		
	}; 


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
	 * operationType(1|2|3) key info
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
	
	

    public static void main(String[] args) throws IOException {
        MVBTree tree = new MVBTree(BLOCK_SIZE, D, E, Long.MIN_VALUE);

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
            
            System.out.print("finish initialize\n");
            Iterator<Element> it = getIteratorDataSet(file+"data.dat");
            long time = System.currentTimeMillis();
			Cursor taker = new Taker(it, num_operation){
				int k = 0;
				@Override
				public Object next() throws IllegalStateException,
						NoSuchElementException {
                            if(k % 5 == 0)
                                System.out.print(".");
					k++;
					return super.next();
				}
			};

            			
            // perform operation
			while(taker.hasNext()){
				Element record = (Element) taker.next();
				OperationType ops = record.getElement3();
				LongVersion version = (LongVersion) record.getElement2().clone();
				Pair<Long, Integer> object = (Pair<Long,Integer>)record.getElement1();				
				// System.out.print("--> Record: ops "+ ops + " version " + version + " pair "+ object +" \n");
				if(ops == OperationType.INSERT){
					tree.insert(version, object);
				}else if (ops == OperationType.DELETE){
					tree.remove(version, object);
				}else if (ops == OperationType.UPDATE){
					tree.update(version, object, object);
				}
			}


			// check sth
			System.out.println();
			// System.out.println(System.currentTimeMillis() - time);
			// System.out.println(cContainerMVBT_LRU);

			/*******************************************************************************
			 * Key-Time range query 
			 ******************************************************************************/
			// Cursor result = tree.rangePeriodQuery(new Long(0), 
			// 						new Long(200), new LongVersion(0), new LongVersion(20));
			// Cursors.println(result);



			if(num_operation < 15)
			{
				System.out.print("tree.roots " +
						"B_IndexNode " + tree.roots.getIndexNodeB() + " D_IndexNode " + tree.roots.getIndexNodeD() +
						" B_LeafNode " + tree.roots.getLeafNodeB() + " D_LeafNode " + tree.roots.getLeafNodeD() + "\n");
				System.out.println("tree.roots.height " + tree.roots.height());
				System.out.println("tree.roots.rootDescriptor() " + tree.roots.rootDescriptor());

				System.out.print("\ntree " +
						"B_IndexNode " + tree.getIndexNodeB() + " D_IndexNode " + tree.getIndexNodeD() +
						" B_LeafNode " + tree.getLeafNodeB() + " D_LeafNode " + tree.getLeafNodeD() + "\n");
				System.out.println("tree.height " + tree.height());
				System.out.println("tree.rootDescriptor() " + tree.rootDescriptor());
				System.out.println("tree.rootEntry() " + tree.rootEntry());
				System.out.println("tree.rootEntry().get().level() " + tree.rootEntry().get().level());
				System.out.println("tree.rootEntry().get().number() " + tree.rootEntry().get().number());
				System.out.println("tree.rootEntry().get().entries() ");
				Iterator tempIter = tree.rootEntry().get().entries();
				while (tempIter.hasNext()) {
					xxl.core.indexStructures.MVBTree.LeafEntry ee = (xxl.core.indexStructures.MVBTree.LeafEntry) tempIter
							.next();
					System.out.println(ee);
				}
			}
			else if(num_operation < 75)
			{
				System.out.print("tree.roots " +
						"B_IndexNode " + tree.roots.getIndexNodeB() + " D_IndexNode " + tree.roots.getIndexNodeD() +
						" B_LeafNode " + tree.roots.getLeafNodeB() + " D_LeafNode " + tree.roots.getLeafNodeD() + "\n");
				System.out.println("tree.roots.height " + tree.roots.height());
				System.out.println("tree.roots.rootDescriptor() " + tree.roots.rootDescriptor());
				System.out.println("tree.roots.rootEntry() " + tree.roots.rootEntry());
				System.out.println("tree.roots.rootEntry().get() " + tree.roots.rootEntry().get());
				Iterator tempRootIter = tree.roots.rootEntry().get().entries();
				System.out.println("********** ");
				while (tempRootIter.hasNext()) {
					xxl.core.indexStructures.MVBTree.Root ee = (xxl.core.indexStructures.MVBTree.Root) tempRootIter
							.next();
					System.out.println("-- tree.roots.rootEntry().get().entries() " + ee);
					System.out.println("rootentry.getRegion() " + ee.getRegion());
					System.out.println("rootentry.parentLevel() " + ee.parentLevel());
					System.out.println("rootentry.rootNodeId() " + ee.rootNodeId());
					System.out.println("rootentry.toIndexEntry().get().level() " + ee.toIndexEntry().get().level());
					System.out.println("rootentry.toIndexEntry().get().number() " + ee.toIndexEntry().get().number());
					System.out.println("rootentry.toIndexEntry().get().entries() ");
					Iterator tempIterRoot = ee.toIndexEntry().get().entries();
					while (tempIterRoot.hasNext()) {
						xxl.core.indexStructures.MVBTree.LeafEntry re = (xxl.core.indexStructures.MVBTree.LeafEntry) tempIterRoot
								.next();
						System.out.println(re);
					}
				}
				System.out.println("********** ");

				System.out.print("\ntree " +
						"B_IndexNode " + tree.getIndexNodeB() + " D_IndexNode " + tree.getIndexNodeD() +
						" B_LeafNode " + tree.getLeafNodeB() + " D_LeafNode " + tree.getLeafNodeD() + "\n");
				System.out.println("tree.height " + tree.height());
				System.out.println("tree.rootDescriptor() " + tree.rootDescriptor());
				System.out.println("tree.rootEntry() " + tree.rootEntry());
				System.out.println("tree.rootEntry().get() " + tree.rootEntry().get());

				Iterator tempIter = tree.rootEntry().get().entries();
				while (tempIter.hasNext()) {
					xxl.core.indexStructures.Tree.IndexEntry ii = (xxl.core.indexStructures.Tree.IndexEntry) tempIter
							.next();
					System.out.println("=== Middle IndexEntry " + ii);
					System.out.println("Middle Node .level()" + ii.get().level());
					System.out.println("Middle Node .number()" + ii.get().number());
					System.out.println("Middle Node .entries()");
					Iterator tempIterInner = ii.get().entries();
					while (tempIterInner.hasNext()) {
						xxl.core.indexStructures.MVBTree.LeafEntry iii = (xxl.core.indexStructures.MVBTree.LeafEntry) tempIterInner
								.next();
						System.out.println(iii);
					}
				}
			}
			else if(num_operation < 111)
			{
				System.out.print("tree.roots " +
						"B_IndexNode " + tree.roots.getIndexNodeB() + " D_IndexNode " + tree.roots.getIndexNodeD() +
						" B_LeafNode " + tree.roots.getLeafNodeB() + " D_LeafNode " + tree.roots.getLeafNodeD() + "\n");
				System.out.println("tree.roots.height " + tree.roots.height());
				System.out.println("tree.roots.rootDescriptor() " + tree.roots.rootDescriptor());
				System.out.println("tree.roots.rootEntry() " + tree.roots.rootEntry());
				System.out.println("tree.roots.rootEntry().get() " + tree.roots.rootEntry().get());
				Iterator tempRootIter = tree.roots.rootEntry().get().entries();
				System.out.println("********** ");
				while (tempRootIter.hasNext()) {
					xxl.core.indexStructures.MVBTree.Root ee = (xxl.core.indexStructures.MVBTree.Root) tempRootIter
							.next();
					System.out.println("-- tree.roots.rootEntry().get().entries() " + ee);
					System.out.println("rootentry.getRegion() " + ee.getRegion());
					System.out.println("rootentry.parentLevel() " + ee.parentLevel());
					System.out.println("rootentry.rootNodeId() " + ee.rootNodeId());
					System.out.println("rootentry.toIndexEntry().get().level() " + ee.toIndexEntry().get().level());
					System.out.println("rootentry.toIndexEntry().get().number() " + ee.toIndexEntry().get().number());
					System.out.println("rootentry.toIndexEntry().get() " + ee.toIndexEntry().get());
				}
				System.out.println("********** ");

				System.out.print("\ntree " +
						"B_IndexNode " + tree.getIndexNodeB() + " D_IndexNode " + tree.getIndexNodeD() +
						" B_LeafNode " + tree.getLeafNodeB() + " D_LeafNode " + tree.getLeafNodeD() + "\n");
				System.out.println("tree.height " + tree.height());
				System.out.println("tree.rootDescriptor() " + tree.rootDescriptor());
				System.out.println("tree.rootEntry() " + tree.rootEntry());
				System.out.println("tree.rootEntry().get() " + tree.rootEntry().get());

				Iterator tempIter = tree.rootEntry().get().entries();
				while (tempIter.hasNext()) {
					xxl.core.indexStructures.Tree.IndexEntry ii = (xxl.core.indexStructures.Tree.IndexEntry) tempIter
							.next();
					System.out.println("=== Middle IndexEntry " + ii);
					System.out.println("Middle Node .level()" + ii.get().level());
					System.out.println("Middle Node .number()" + ii.get().number());
					System.out.println("Middle Node .entries()");
					Iterator middleIter = ii.get().entries();
					while (middleIter.hasNext()) {
						xxl.core.indexStructures.MVBTree.LeafEntry iii = (xxl.core.indexStructures.MVBTree.LeafEntry) middleIter
								.next();
						System.out.println(iii);
					}
				}

			}
			else
			{
				System.out.print("tree.roots " +
						"B_IndexNode " + tree.roots.getIndexNodeB() + " D_IndexNode " + tree.roots.getIndexNodeD() +
						" B_LeafNode " + tree.roots.getLeafNodeB() + " D_LeafNode " + tree.roots.getLeafNodeD() + "\n");
				System.out.println("tree.roots.height " + tree.roots.height());
				System.out.println("tree.roots.rootDescriptor() " + tree.roots.rootDescriptor());
				System.out.println("tree.roots.rootEntry() " + tree.roots.rootEntry());
				System.out.println("tree.roots.rootEntry().get() " + tree.roots.rootEntry().get());
				Iterator tempRootIter = tree.roots.rootEntry().get().entries();
				System.out.println("********** ");
				while (tempRootIter.hasNext()) {
					xxl.core.indexStructures.MVBTree.Root ee = (xxl.core.indexStructures.MVBTree.Root) tempRootIter
							.next();
					System.out.println("-- tree.roots.rootEntry().get().entries() " + ee);
					System.out.println("rootentry.getRegion() " + ee.getRegion());
					System.out.println("rootentry.parentLevel() " + ee.parentLevel());
					System.out.println("rootentry.rootNodeId() " + ee.rootNodeId());
					System.out.println("rootentry.toIndexEntry().get().level() " + ee.toIndexEntry().get().level());
					System.out.println("rootentry.toIndexEntry().get().number() " + ee.toIndexEntry().get().number());
					System.out.println("rootentry.toIndexEntry().get() " + ee.toIndexEntry().get());
				}
				System.out.println("********** ");

				System.out.print("\ntree " +
						"B_IndexNode " + tree.getIndexNodeB() + " D_IndexNode " + tree.getIndexNodeD() +
						" B_LeafNode " + tree.getLeafNodeB() + " D_LeafNode " + tree.getLeafNodeD() + "\n");
				System.out.println("tree.height " + tree.height());
				System.out.println("tree.rootDescriptor() " + tree.rootDescriptor());
				System.out.println("tree.rootEntry() " + tree.rootEntry());
				System.out.println("tree.rootEntry().get() " + tree.rootEntry().get());

				Iterator tempIter = tree.rootEntry().get().entries();
				while (tempIter.hasNext()) {
					xxl.core.indexStructures.Tree.IndexEntry ii = (xxl.core.indexStructures.Tree.IndexEntry) tempIter
							.next();
					System.out.println("=== Middle IndexEntry " + ii);
					System.out.println("Middle Node .level()" + ii.get().level());
					System.out.println("Middle Node .number()" + ii.get().number());
					System.out.println("Middle Node .entries()");
					Iterator middleIter = ii.get().entries();
					while (middleIter.hasNext()) {
						xxl.core.indexStructures.Tree.IndexEntry ll = (xxl.core.indexStructures.Tree.IndexEntry) middleIter
								.next();
						System.out.println("=== Leaf IndexEntry " + ll);
						System.out.println("Leaf Node .level()" + ll.get().level());
						System.out.println("Leaf Node .number()" + ll.get().number());
						System.out.println("Leaf Node .entries()");
						Iterator leafIter = ll.get().entries();
						while (leafIter.hasNext()) {
							xxl.core.indexStructures.MVBTree.LeafEntry iii = (xxl.core.indexStructures.MVBTree.LeafEntry) leafIter
									.next();
							System.out.println(iii);
						}
					}
				}
			}
    }

}
