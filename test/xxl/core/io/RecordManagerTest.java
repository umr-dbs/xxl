/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
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

package xxl.core.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.RawAccessContainer;
import xxl.core.collections.containers.recordManager.RecordManager;
import xxl.core.collections.containers.recordManager.Strategy;
import xxl.core.collections.containers.recordManager.TId;
import xxl.core.collections.containers.recordManager.TIdManager;
import xxl.core.collections.containers.recordManager.Utility;
import xxl.core.cursors.sorters.ShuffleCursor;
import xxl.core.cursors.sources.DiscreteRandomNumber;
import xxl.core.io.Block;
import xxl.core.io.LRUBuffer;
import xxl.core.io.XXLFilesystem;
import xxl.core.io.raw.DecoratorRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.io.raw.StatisticsRawAccess;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.XXLSystem;
import xxl.core.util.random.JavaDiscreteRandomWrapper;
import xxl.core.util.reflect.TestFramework;

/**
 * A simple test class for the record manager. That includes the
 * test of the Strategy, the Page and the Block Classes.
 */
public class RecordManagerTest {

	/** The number of loops around the whole test. */
	public static int loopCount = 50;
	/** For TestFramework */ public static final String loopCountDescription = "Number of Loops around the whole test"; 

	/** Choice of the test szenario: 0: With a lot of testing, 1: runtime test with insert, update and remove, 2: the same than 1) but update after remove, 3: runtime test only insertion, 4: runtime test insertion and then only updates, 5: runtime test insertion and then insertion and updates */
	public static int testSzenario = 5;
	/** For TestFramework */ public static final String testSzenarioDescription = "Choice of the test szenario: 0: With a lot of testing, 1: runtime test with insert, update and remove, 2: the same than 1) but update after remove, 3: runtime test only insertion, 4: runtime test insertion and then only updates, 5: runtime test insertion and then insertion and updates";
	/** For TestFramework */ public static final int testSzenarioMin = 5;
	/** For TestFramework */ public static final int testSzenarioMax = 5;
	/** For TestFramework */ // public static final int testSzenarioValues[] = new int[]{3}; // 1,2,4};

	/** Name of the raw device or null */
	// public final static String rawDeviceName = "\\\\.\\h:";
	// public final static String rawDeviceName = "\\\\.\\y:";
	// public final static String rawDeviceName = "\\\\.\\z:";
	// public final static String rawDeviceName = null;
	public final static String rawDeviceName = ""; // Use main memory!
	private final static int rawDeviceLength = 10360;

	/** The number of objects used for insertion. */
	public static int numberOfObjects = 1000;
	/** For TestFramework */ public static final String numberOfObjectsDescription = "Number of objects used for insertion";

	/** The size of each page in bytes. */
	public static int pageSize=512;
	/** For TestFramework */ public static final String pageSizeDescription = "Size of each page in bytes";
	/** For TestFramework */ public static final int pageSizeValues[] =  new int[]{512,2048,8192};

	/** The update percentage in test number 5. */
	public static int updatePercentageTest5 = 0;
	/** For TestFramework */ public static int updatePercentageTest5Values[] = new int[]{0, 20, 40, 60, 80, 100};

	/** The minimal size of a generated record */
	public static final int recordSizeMin = 16;

	/** The size of the buffer in percent of the data */
	public static int blockBufferSizePercentage = 0;
	/** For TestFramework */ public static int blockBufferSizePercentageValues[] = new int[]{0,10}; // ,20,50};

	/** Write back mode of the buffer */
	public static boolean writeBack = true;
	/** For TestFramework */ public static boolean writeBackValues[] = new boolean[]{true, false};

	/** 0: OneRecordPerPage, 1: FirstFit, 2: NextFit, 3: AppendOnly, 4: AppendOnly(n), 5: BestFit(bestFitPercentage), 6: BestFitOnNEmptiestPages(n), 7: NextFitWithH(n), 8: NextFitWithHW(n), 9: HybridAONF(n,goalPercentage), 10: HybridBFOENFHStrategy(n), 11: HybridBFOENFHWStrategy(n), 12: LastToFirstFitStrategy, 13: LRUStrategy(n), 14: HybridLRULFStrategy(n) */
	public static int recordManagerStrategy=0;
	/**	For TestFramework */ public static final String recordManagerStrategyDescription = "0: OneRecordPerPage, 1: FirstFit, 2: NextFit, 3: AppendOnly, 4: AppendOnly(n), 5: BestFit(bestFitPercentage), 6: BestFitOnNEmptiestPages(n), 7: NextFitWithH(n), 8: NextFitWithHW(n), 9: HybridAONF(n,goalPercentage), 10: HybridBFOENFHStrategy(n), 11: HybridBFOENFHWStrategy(n), 12: LastToFirstFitStrategy, 13: LRUStrategy(n), 14: HybridLRULFStrategy(n)";
	/**	For TestFramework */ public static final int recordManagerStrategyMin=0;
	/**	For TestFramework */ public static final int recordManagerStrategyMax=14;

	/** A number identifying the TId manager (0: identity TId, 1: map TId).*/
	public static int tidManagerNumber = 0;
	/**	For TestFramework */ public static final String tidManagerNumberDescription = "0: IdentityTId, 1: MapTId";
	/**	For TestFramework */ public static final int tidManagerNumberMin = 0;
	/**	For TestFramework */ public static final int tidManagerNumberMax = 1;

	/** If the best fit strategy finds a page that would have this percentage free after inserting the Record, then the insertion operation is done although there might exist better fitting Pages */
	public static double bestFitPercentFree = 0.05;
	/**	For TestFramework */ public static final String bestFitPercentFreeDescription = "If the best fit strategy finds a page that would have this percentage free after inserting the Record, then the insertion operation is done although there might exist better fitting Pages";

	/** Strategy n for the strategies */
	public static int strategyParameterN = 10;
	/**	For TestFramework */ public static final int strategyParameterNValues[] = new int[] {5, 10, 20};
	/**	For TestFramework */ public static final String strategyParameterNDescription = "Strategy n for the strategies";

	/** Memory utilization factor for hybrid strategy. */
	public static double memoryUtilizationHybrid = 0.85;
	/**	For TestFramework */ public static final String memoryUtilizationHybridDescription = "Memory utilization factor for hybrid strategy";

	/**A boolean flag determining whether the test program should perform verbose output. */
	public static boolean verbose = false;

	/**The number of slots for reserve operations inside each page. */
	public static final int numberOfDirectReserves = 0;
	/**A boolean flag determining whether the record manager should be check for consistency. */
	public static final boolean checkConsistency = false;
	/**A boolean flag determining whether the random number generator should be initialized with the current system time.*/
	public static final boolean initRandomWithMillis = false;

	/** The real buffer size in bytes which is calculated below. */
	private static int blockBufferSize;
	/** The maximal size of a generated record */
	private static int recordSizeMax;
	/** The maximal size difference */
	private static int recordSizeDiff;

	/**
	 * Part of the @link{TestFramework} which returns the relational 
	 * metadata to the meassured values of a test run. These test values
	 * are stored inside a @link{TestFramework.list}.
	 * @return Here, no values are meassured and null is returned.
	 */
	public static ResultSetMetaData getReturnRSMD() {
		return new ColumnMetaDataResultSetMetaData(
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "blockBufferSize", "blockBufferSize", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SpecificTestValue", "SpecificTestValue", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, "TimeForInsertion", "TimeForInsertion", "", 15, 0, "", "", Types.DOUBLE, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SequentialRawAccess", "SequentialRawAccess", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "RandomRawAccess", "RandomRawAccess", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SameSectorRawAccess", "SameSectorRawAccess", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SizeOfAllPages", "SizeOfAllPages", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SizeOfAllRecords", "SizeOfAllRecords", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, "SPAN", "SPAN", "", 15, 0, "", "", Types.DOUBLE, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SizeOfAllRecordsMap",  "SizeOfAllRecordsMap", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "SizeOfAllRecordsRecalc", "SizeOfAllRecordsRecalc", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "NumberOfLinkRecords", "NumberOfLinkRecords", "", 9, 0, "", "", Types.INTEGER, true, false, false)
		);
	}

	/**
	 * Part of the @link{TestFramework} which has to produces test values
	 * for a variable declared above in this class.
	 * @param fieldName Name of a variable from above for which
	 * 	test values are wanted.
	 * @return An Iterator containing appropriate test values.
	 */
	public static Iterator getTestValues(String fieldName) {
		return null;
	}

	/**
	 * A random nunber generator used for the test.
	 */
	private static Random random;

	/**
	 * Manipulating an entry of the map? If this is done, then the
	 * test should fail and throw an exception.
	 */
	public static final boolean MAKE_MANIPULATION = false;

	/**
	 * Performs an insertion, so that numberOfObjects records are
	 * inside the RecordManager after the execution. The
	 * records are of random size around pageSize/4.
	 * @param map Map on which the same operations are performed as on the RecordManager.
	 * @param rm The RecordManager to be tested.
	 */
	public static void insert(Map map, RecordManager rm) {
		Object key;
		Block value;
		byte bytes[];
		long t1=0,t2=0;
		
		if (verbose) {
			System.out.println("Perform "+numberOfObjects+" inserts");
			t1 = System.currentTimeMillis();
		}
		
		int numberOfInsertions = numberOfObjects - map.size();
		for (int i=0;i<numberOfInsertions;i++) {
			int randomSize = random.nextInt(recordSizeDiff+1)+recordSizeMin;
			bytes = new byte[randomSize];
			
			random.nextBytes (bytes);
			value = new Block(bytes);
			try {
				key = rm.insert(value);
			}
			catch (Exception e) {
				System.out.println("Error inserting number "+i);
				System.out.println("Length: "+bytes.length);
				throw new WrappingRuntimeException(e);
			}
			if (verbose) {
				System.out.println("Insertion key: "+key+" size: "+value.size);
				System.out.println("---------------------------------------------------------");
				System.out.println(rm.toStringWithPages());
				System.out.println("---------------------------------------------------------");
				rm.checkConsistency();
			}
			// also insert into the map
			map.put(key, value);
		}
		if (map.size()!=numberOfObjects || rm.size()!=numberOfObjects)
			throw new RuntimeException("Number of objects is not numberOfObjects");
		
		if (verbose) {
			t2 = System.currentTimeMillis();
			System.out.println("Time: "+(t2-t1));
		}
	}

	/**
	 * Performs insertion and updates at a specified rate.
	 * @param map Map on which the same operations are performed as on the RecordManager.
	 * @param rm The RecordManager to be tested.
	 * @return the number of insert operations done
	 */
	public static int insertUpdate(Map map, RecordManager rm) {
		Object key;
		Block value;
		byte bytes[];
		int numberInsertsPerformed = 0;
		
		Object keys[] = map.keySet().toArray();
		
		for (int i=0;i<numberOfObjects;i++) {
			int randomSize = random.nextInt(recordSizeDiff+1)+recordSizeMin;
			bytes = new byte[randomSize];
			
			random.nextBytes (bytes);
			value = new Block(bytes);
			
			if (random.nextInt(100)<updatePercentageTest5) {
				key = keys[random.nextInt(keys.length)];
				rm.update(key, value);
			}
			else {
				key = rm.insert(value);
				numberInsertsPerformed++;
			}
			// also insert into the map
			map.put(key, value);
		}
		if (map.size()!=rm.size())
			throw new RuntimeException("Number of objects is not correct");
		
		return numberInsertsPerformed;
	}

	/**
	 * Performs an insertion, but in contrast to the above insertion does
	 * not fill the arrays (they are 0) and do not insert into a map. 
	 * @param rm The RecordManager to be tested.
	 */
	public static void insertFast(RecordManager rm) {
		Block value;
		byte bytes[] = new byte[pageSize];
		
		for (int i=0;i<numberOfObjects;i++) {
			int randomSize = random.nextInt(recordSizeDiff+1)+recordSizeMin;
			value = new Block(bytes,0,randomSize);
			try {
				rm.insert(value);
			}
			catch (Exception e) {
				System.out.println("Error inserting number "+i);
				System.out.println("Length: "+bytes.length);
				throw new WrappingRuntimeException(e);
			}
		}
	}

	/**
	 * Updates approximatly half of the records in random order.
	 * @param map Map on which the same operations are performed as on the RecordManager.
	 * @param rm The RecordManager to be tested.
	 */
	public static void update(Map map, RecordManager rm) {
		Object key;
		Block value;
		byte bytes[];
		long t1=0,t2=0;
		int zufall;
		
		ShuffleCursor shuffler =
			new ShuffleCursor(
				map.keySet().iterator(),
				new DiscreteRandomNumber(
					new JavaDiscreteRandomWrapper(random)));
		
		if (verbose) {
			System.out.println("Update half of the Records randomly making them larger\n");
			t1 = System.currentTimeMillis();
		}
		while (shuffler.hasNext()) {
			key = shuffler.next();
			zufall = random.nextInt(2);
			if (zufall==0) {
				int randomSize = random.nextInt(recordSizeDiff+1)+recordSizeMin;
				bytes = new byte[randomSize];
				random.nextBytes (bytes);
				value = new Block(bytes);
				if (verbose)
					System.out.println("Update key: "+key+" size: "+value.size);
				rm.update(key,value);
				if (verbose) {
					System.out.println("---------------------------------------------------------");
					System.out.println(rm.toStringWithPages());
					System.out.println("---------------------------------------------------------");
					rm.checkConsistency();
				}
				if (map.put(key, value)==null)
					throw new RuntimeException("object is not in the map");
			}
		}
		shuffler.close();
		if (verbose) {
			t2 = System.currentTimeMillis();
			System.out.println("Time: "+(t2-t1));
		}
	}

	/**
	 * Removes approximatly half of the records in random order.
	 * @param map Map on which the same operations are performed as on the RecordManager.
	 * @param rm The RecordManager to be tested.
	 */
	public static void remove(Map map, RecordManager rm) {
		Object key;
		int zufall;
		
		if (verbose)
			System.out.println("Remove half of the Records randomly\n");
		
		ShuffleCursor shuffler =
			new ShuffleCursor(
				map.keySet().iterator(),
				new DiscreteRandomNumber(
					new JavaDiscreteRandomWrapper(random)));
		
		while (shuffler.hasNext()) {
			key = shuffler.next();
			zufall = random.nextInt(2);
			if (zufall==0) {
				rm.remove(key);
				if (verbose) {
					System.out.println("Remove key: "+key);
					System.out.println("---------------------------------------------------------");
					System.out.println(rm.toStringWithPages());
					System.out.println("---------------------------------------------------------");
					rm.checkConsistency();
				}
				if (map.remove(key)==null)
					throw new RuntimeException("object is not in the map");
			}
		}
		shuffler.close();
	}

	/**
	 * Compares the content of the map with the content of the RecordManager.
	 * @param map Map on which the same operations are performed as on the RecordManager.
	 * @param rm The RecordManager to be tested.
	 */
	public static void comparison(Map map, RecordManager rm) {
		Iterator it;
		Object key;
		Block value,valueFromManager;
		boolean ok = true;
		
		if (verbose)
			System.out.println("Comparison of the map with the record manager...");
		
		it = map.keySet().iterator();
		while (it.hasNext()) {
			key = it.next();
			value = (Block) map.get(key);
			valueFromManager = (Block) rm.get(key);
			//rm.checkConsistency();
			if (valueFromManager==null) {
				ok = false;
				break;
			}
			if (!valueFromManager.equals(value)) {
				ok = false;
				break;			
			}
		}
		
		if (ok) {
			if (verbose)
				System.out.println("Everything ok: map and manager contain the same elements in correct order");
		}
		else {
			System.out.println("ERROR!!!");
			throw new RuntimeException("TestRecordManager: ERROR at comparing elements");
		}
	}

	/**
	 * Tests the TIds inside RecordManager and a map.
	 * @param rm The RecordManager.
	 * @param map The map.
	 * @param useLinks Determines iff the keys are TIds.
	 */
	public static void testTIds(RecordManager rm, Map map, boolean useLinks) {
		if (verbose) {
			System.out.println("Test TIds ...");
			System.out.println("of record manager");
		}
		
		Iterator it;
		Object key, tid;
		int countRM=0, countMap=0;
		
		it = rm.ids();
		while (it.hasNext()) {
			key = it.next();
			countRM++;
			if (key==null)
				throw new RuntimeException("Key is null");
			if (useLinks) {
				tid = (TId)key;
				// Object idcur = tid.getId();
				// short rn = tid.getRecordNr();
				if (verbose)
					System.out.println(tid);
			}
		}
		
		if (verbose)
			System.out.println("of map");
		
		it = map.keySet().iterator();
		while (it.hasNext()) {
			key = it.next();
			countMap++;
			if (key==null)
				throw new RuntimeException("Key is null");
			if (useLinks) {
				tid = (TId)key;
				// Object idcur = tid.getId();
				// short rn = tid.getRecordNr();
			}
		}
		
		if (countRM!=countMap)
			throw new RuntimeException("Number of ids is not correct. Map: "+countMap+", RM: "+countRM);
	}

	private static int getSizesOfAllRecords(RecordManager rm) {
		int size = 0;
		Iterator it = rm.objects();
		while (it.hasNext()) {
			Block b = (Block) it.next();
			size += b.size;
		}
		return size;
	}

	private static int getSizesOfAllRecords(Map map) {
		int size = 0;
		Iterator it = map.values().iterator();
		while (it.hasNext()) {
			Block b = (Block) it.next();
			size += b.size;
		}
		return size;
	}

	private static RawAccess ra;
	private static String path;
	private static String containerName = "rmTest";
	private static Container bfc = null;

	private static Container openContainer(boolean init) {
		if (init)
			bfc = new RawAccessContainer(ra, 1000);
		else
			bfc = new RawAccessContainer(ra);
		Container usedContainer = bfc;
		if (blockBufferSize>0)
			usedContainer = new BufferedContainer(bfc, 
				new LRUBuffer(blockBufferSize/pageSize), writeBack, true);
		/*
		usedContainer = new DecoratorContainer(usedContainer) {
			int mode = 2;
			public Object get (Object id, boolean unfix) throws NoSuchElementException {
				System.out.println("DC: get "+id);
				return super.get(id, unfix);
			}
			public Object get (Object id) throws NoSuchElementException {
				System.out.println("DC: get "+id);
				Object o = super.get(id);
				if (mode==1) {
					Block block = (Block) o;
					Page p = new Page(BLOCKSIZE);
					p.readHeader(block.dataInputStream()); //reconstruct only the page-header
					System.out.println("#links: "+p.getNumberOfLinkRecords());
				}
				return o;
			}
			public Object insert (Object object, boolean unfix) {
				Object o = super.insert(object, unfix);
				System.out.println("DC: insert "+o);
				return o;
			}
			public Object insert (Object object) {
				Object o = super.insert(object);
				System.out.println("DC: insert "+o);
				return o;
			}
			public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
				System.out.println("DC: update "+id);
				super.update(id,object,unfix);
			}
			public void update (Object id, Object object) throws NoSuchElementException {
				System.out.println("DC: update "+id);
				Block block = (Block) object;
				Page p = new Page(BLOCKSIZE);
				p.readHeader(block.dataInputStream()); //reconstruct only the page-header
				int links = p.getNumberOfLinkRecords();
				if (mode==1)
					System.out.println("#links: "+links);
				super.update(id,object);
			}
			public void remove (Object id) throws NoSuchElementException {
				System.out.println("DC: remove "+id);
				container.remove(id);
			}
			public Object reserve (Function getObject) {
				System.out.println("DC: reserve");
				return container.reserve(getObject);
			}
		};
		*/
		return usedContainer;
	}

	/**
	 * Example using the RecordManager.
	 * @param args The command line options. Call the class with help or "?" to
	 * 	get a list of the possible parameters.
	 */
	public static void main(String[] args) {
		
		if (!TestFramework.processParameters("RecordManagerTest\n", RecordManagerTest.class, args, System.out))
			return;
		
/*		if (initRandomWithMillis)
			random = new Random(System.currentTimeMillis());
		else*/
			random = new Random(68); 
		
		TIdManager tidm=null;
		Strategy strategy=null;
		Container usedContainer;

		recordSizeMax = 512/2-recordSizeMin;
		recordSizeDiff = recordSizeMax-recordSizeMin;
		blockBufferSize = numberOfObjects*((recordSizeMax+recordSizeMin)/2)*blockBufferSizePercentage/100;
		
		TestFramework.list.add(new Integer(blockBufferSize));
		
		path = Common.getOutPath();
		StatisticsRawAccess sra = null;
		
		if (rawDeviceName!=null) {
			path = "";
			
			int rdl = rawDeviceLength;
			if (testSzenario==3)
				rdl *= 10;
			else if (testSzenario==5)
				rdl = (28*rdl) / 10;
			
			ra = XXLFilesystem.createRawAccess(false, pageSize, rdl, rawDeviceName, 0, true);

			RawAccess ra2 = ra;
			while (!(ra2 instanceof StatisticsRawAccess))
				ra2 = ((DecoratorRawAccess) ra2).getDecoree();
			sra = (StatisticsRawAccess) ra2;
			sra.resetCounter();
		}
		else
			throw new RuntimeException("Not yet implemented");
		
		usedContainer = openContainer(true);
		
		strategy = Utility.getStrategy(recordManagerStrategy, strategyParameterN, bestFitPercentFree, memoryUtilizationHybrid);
		tidm = Utility.getTIdManager(tidManagerNumber, usedContainer.objectIdConverter());
		
		System.out.println("TIdManager: "+tidm);
		System.out.println("Strategy: "+strategy);
		System.out.println();
		
		RecordManager rm = new RecordManager(usedContainer,pageSize,strategy,tidm,numberOfDirectReserves);
		
		long t1,t2;
		Map map = new HashMap();
		
		////////////////////////////////////////////////////////////////
		// Warmup
		////////////////////////////////////////////////////////////////
		
		if (verbose)
			System.out.println("Insert a lot...");
		insert(map,rm);
		if (verbose)
			System.out.println("...and remove everything");
		rm.clear();
		map.clear();
		
		t1 = System.currentTimeMillis(); 
		
		if (testSzenario==0) {
			Object key;
			// Block value;
			
			// Block valueFromManager;
			Iterator it;
			
			int iterations=0;
			while (iterations<loopCount) {
				if (verbose)
					System.out.println("Iteration "+iterations);
				
				insert(map,rm);
				
				testTIds(rm, map, tidm.useLinks());
				
				if (MAKE_MANIPULATION) {
					it = map.keySet().iterator();
					map.put(it.next(), new Block(("Hallo").getBytes()));
					it = map.keySet().iterator();
				}
				
				comparison(map,rm);
				if (checkConsistency)
					rm.checkConsistency();
				
				if (verbose) {
					System.out.println("---------------------------------------------------------");
					System.out.println(rm);
					System.out.println("---------------------------------------------------------");
				}
				
				update(map,rm);
				
				testTIds(rm, map, tidm.useLinks());
				
				comparison(map,rm);
				if (checkConsistency)
					rm.checkConsistency();
				
				if (verbose) {
					System.out.println("---------------------------------------------------------");
					System.out.println(rm);
					System.out.println("---------------------------------------------------------");
				}
				
				remove(map,rm);
				
				testTIds(rm, map, tidm.useLinks());

				if (verbose) {
					System.out.println("---------------------------------------------------------");
					System.out.println(rm);
					System.out.println("---------------------------------------------------------");
				}
				
				comparison(map,rm);
				if (checkConsistency)
					rm.checkConsistency();
				
				iterations++;
			}
			
			///////////////////////////////////////////////////////////////////////////////77
			testTIds(rm, map, tidm.useLinks());
			///////////////////////////////////////////////////////////////////////////////77
			
			if (verbose) {
				System.out.println("Closing and reopening the RecordManager");
				System.out.println("Write RecordManager cfg-File");
			}

			String cfgFile = XXLSystem.getOutPath(new String[]{"output","applications","io"})
				+File.separator+"RMMetaData.bin";
			try {	
				DataOutputStream output = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(cfgFile)));
				
				rm.getStrategy().write(output);
				rm.getTIdManager().write(output);
				rm.write(output);
				output.close();
			}
			catch (FileNotFoundException e) {
				throw new WrappingRuntimeException(e);
			}
			catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}
			
			rm.close();
			System.out.println("Strategy: "+strategy);

			strategy.close();
			tidm.close();
			usedContainer.close();
			
			rm = null;
			strategy = null;
			tidm = null;
			usedContainer = null;
			
			if (verbose)
				System.out.println("Read RecordManager cfg-File");
			
			try {	
				DataInputStream input = new DataInputStream(
					new BufferedInputStream(new FileInputStream(cfgFile)));
				
				usedContainer = openContainer(false);
				
				strategy = Utility.getStrategy(recordManagerStrategy, strategyParameterN, bestFitPercentFree, memoryUtilizationHybrid);
				tidm = Utility.getTIdManager(tidManagerNumber, usedContainer.objectIdConverter());
	
				strategy.read(input);
				tidm.read(input);
				
				rm = new RecordManager(usedContainer,pageSize,strategy,tidm,numberOfDirectReserves);
				rm.read(input);
				input.close();
			}
			catch (FileNotFoundException e) {
				throw new WrappingRuntimeException(e);
			}
			catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}

			if (verbose) {
				System.out.println("---------------------------------------------------------");
				System.out.println(rm);
				System.out.println("---------------------------------------------------------");
			}
			
			///////////////////////////////////////////////////////////////////////////////77
			testTIds(rm, map, tidm.useLinks());
			///////////////////////////////////////////////////////////////////////////////77

			if (verbose)
				System.out.println("Checking consistency of the reopened RecordManager");
			
			comparison(map,rm);
			if (checkConsistency)
				rm.checkConsistency();
			
			///////////////////////////////////////////////////////////////////////////////77
			
			if (verbose)
				System.out.println("Remove the all Records");
			
			// remove the other records
			it = map.keySet().iterator();
			while (it.hasNext()) {
				key = it.next();
				rm.remove(key);
				if (verbose) {
					System.out.println("Remove key: "+key);
					System.out.println("---------------------------------------------------------");
					System.out.println(rm.toStringWithPages());
					System.out.println("---------------------------------------------------------");
					System.out.println(strategy);
					rm.checkConsistency();
				}
			}
			map.clear();
			
			if (verbose) {
				System.out.println("---------------------------------------------------------");
				System.out.println(rm);
				System.out.println("---------------------------------------------------------");
			}
			TestFramework.list.add(null);
		}
		else if (testSzenario==1) {
			int iterations=0;
			while (iterations<loopCount) {
				// System.out.println(iterations);
				insert(map, rm);
				// System.out.println("After insert: number of records: "+rm.size());
				update(map, rm);
				// System.out.println("After update: number of records: "+rm.size());
				iterations++;
				if (iterations<loopCount) {
					remove(map, rm);
					// System.out.println("After remove: number of records: "+rm.size());
				}
			}
			TestFramework.list.add(null);
		}
		else if (testSzenario==2) {
			int iterations=0;
			while (iterations<loopCount) {
				// System.out.println(iterations);
				insert(map, rm);
				iterations++;
				if (iterations<loopCount) {
					remove(map, rm);
					update(map, rm);
				}
			}
			TestFramework.list.add(null);
		}
		else if (testSzenario==3) {
			int iterations=0;
			int iterationCount = 100;
			while (iterations<iterationCount) {
				// System.out.println(iterations);
				insertFast(rm);
				iterations++;
			}
			TestFramework.list.add(null);
		}
		else if (testSzenario==4) {
			insert(map, rm);
			int iterations=0;
			while (iterations<loopCount) {
				// System.out.println(iterations);
				update(map, rm);
				iterations++;
			}
			TestFramework.list.add(null);
		}
		else if (testSzenario==5) {
			int insertionCounter = 0;
			insert(map,rm);
			int iterations=0;
			while (iterations<loopCount/2) {
				insertionCounter += insertUpdate(map, rm);
				iterations++;
			}
			TestFramework.list.add(new Integer(insertionCounter));
		}
		
		t2 = System.currentTimeMillis();
		System.out.println("Time: "+(t2-t1)+"ms");
		
		TestFramework.list.add(new Double( ((double)(t2-t1)) / 1000));
		if (sra!=null) {
			TestFramework.list.add(new Integer(sra.getSequentialAccessCount()));
			TestFramework.list.add(new Integer(sra.getRandomAccessCount()));
			TestFramework.list.add(new Integer(sra.getSameSectorAccessCount()));
			System.out.println("seq: "+sra.getSequentialAccessCount());
			System.out.println("ran: "+sra.getRandomAccessCount());
			System.out.println("ssc: "+sra.getSameSectorAccessCount());
		}
		else {
			TestFramework.list.add(null);
			TestFramework.list.add(null);
			TestFramework.list.add(null);
		}
		
		int ctrSize = usedContainer.size()*pageSize;
		int sizeOfRecords = rm.sizeOfAllStoredRecords();
		double memUsage = ((double)sizeOfRecords)/ctrSize;
		TestFramework.list.add(new Integer(ctrSize));
		TestFramework.list.add(new Integer(sizeOfRecords));
		TestFramework.list.add(new Double(memUsage));
		
		TestFramework.list.add(new Integer(getSizesOfAllRecords(map)));
		TestFramework.list.add(new Integer(getSizesOfAllRecords(rm)));
		TestFramework.list.add(new Integer(rm.numberOfLinkRecords()));
		rm.checkConsistency();
		
		System.out.println("Container size: "+ctrSize);
		System.out.println("Size of all records: "+sizeOfRecords);
		System.out.println("Memory usage: "+memUsage);
		System.out.println("Strategy: "+strategy);
		rm.close();
		usedContainer.close();
		
		System.out.println();
		System.out.println(TestFramework.getListAsTuple(getReturnRSMD()));
	}
}
