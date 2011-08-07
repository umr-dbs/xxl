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

import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import xxl.core.collections.bags.Bag;
import xxl.core.collections.bags.ListBag;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.MapContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.MultiBlockContainer;
import xxl.core.collections.containers.io.RawAccessContainer;
import xxl.core.collections.containers.recordManager.IdentityTIdManager;
import xxl.core.collections.containers.recordManager.MapTIdManager;
import xxl.core.collections.containers.recordManager.NextFitStrategy;
import xxl.core.collections.containers.recordManager.RecordManager;
import xxl.core.collections.containers.recordManager.Strategy;
import xxl.core.collections.containers.recordManager.TIdManager;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.identities.TeeCursor;
import xxl.core.cursors.sorters.ShuffleCursor;
import xxl.core.cursors.sources.DiscreteRandomNumber;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.io.Block;
import xxl.core.io.LRUBuffer;
import xxl.core.io.raw.RAMRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Equal;
import xxl.core.predicates.LeftBind;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.util.Arrays;
import xxl.core.util.random.JavaDiscreteRandomWrapper;
import xxl.core.util.reflect.TestFramework;
import xxl.core.util.timers.Timer;
import xxl.core.util.timers.TimerUtils;

/**
 * A simple class which tests containers, record managers, and so on.
 * The test includes the test of the Strategy, the Page and the Record classes
 * of the record manager.
 * The objects inside the container have to support the equals operation.
 */
public class ContainerTest {

	/**A description for the field <tt>testCase</tt>;*/
	public static final String testCaseDescription = "1: MapContainer, 2: BlockFileContainer, 3: MultiBlockContainer, 4: RecordManager, 5: RawAccessContainer";
	/**The smallest number identifying a test case.*/
	public static final int testCaseMin = 1;
	/**The greatest number identifying a test case.*/
	public static final int testCaseMax = 5;
	/**A description for the field <tt>numberOfObjects</tt>;*/
	public static final String numberOfObjectsDescription = "it has to hold: numberOfObjects%4 ==0";

	/**A number identifying the test case (1: MapContainer, 2: BlockFileContainer, 3: MultiBlockContainer, 4: RecordManager, 5: RawAccessContainer).*/
	public static int testCase = 4;
	
	public static int tidManager = 0;
	/**The size of a block in this test case.*/
	public static int blockSize = 2048;
	/**The number of objects used for this test case. It has to hold: <tt>numberOfObjects%4==0</tt>.*/
	public static int numberOfObjects = 9000;
	/**A boolean flag determining whether manipulations of the containers should be perform during the test.*/
	public static boolean makeManipulation = false;
	/**A boolean flag determining whether the test should perform verbose output.*/
	public static boolean verbose = false;
	/**A boolean flag determining whether the method calls to the tested container should be counted.*/
	public static boolean activateCounterContainer = true;
	/**A boolean flag determining whether the containers should be buffered during the test case.*/
	public static boolean activateBuffer = true;
	/**A boolean flag determining whether the inserted objects should be cloned by the container.*/
	public static boolean useCloning = true;

	/**The number of slots for reserve operations inside each page.*/
	public static final int numberOfDirectReserves = 0;

	/**
	 * Part of the @link{TestFramework} which returns the relational 
	 * metadata to the meassured values of a test run. These test values
	 * are stored inside a @link{TestFramework.list}.
	 * @return Here, the metadata for TimeForInsertion, TimeForRemoveQuarter
	 * 	and TimeForRemoveAllHalf are returned.
	 */
	public static ResultSetMetaData getReturnRSMD() {
		return 	new ColumnMetaDataResultSetMetaData(
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, "TimeForInsertion", "TimeForInsertion", "", 15, 0, "", "", Types.DOUBLE, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, "TimeForRemoveQuarter", "TimeForRemoveQuarter", "", 15, 0, "", "", Types.DOUBLE, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, "TimeForRemoveAllHalf", "TimeForRemoveAllHalf", "", 15, 0, "", "", Types.DOUBLE, true, false, false)
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
		if (fieldName.equals("testCase"))
			return Arrays.intArrayIterator(new int[]{1,2,3,4});
		else if (fieldName.equals("verbose"))
			return new SingleObjectCursor(Boolean.FALSE);
		else if (fieldName.equals("blockSize"))
			return Arrays.intArrayIterator(new int[]{512,1024,2048,4096,8192});
		else if (fieldName.equals("numberOfObjects"))
			return Arrays.intArrayIterator(new int[]{500,1000,2000});
		else
			return null;
	}

	/**The container to be tested.*/
	private static Container container;
	/**A map that is used to store the elements inserted into the container for checking the result of the test.*/
	private static Map map;
	/**A random number generator used for test cases.*/
	private static Random random;

	/**
	 * Function to map the byte array to a Block.
	 */
	private static Function mapToBlockFunction = new AbstractFunction() {
		public Object invoke(Object o) {
			return new Block((byte[])o);
		}
	};

	/**
	 * For Blocks and Records (because Records are Blocks).
	 */
	private static Function modifyBlockFunction = new AbstractFunction() {
		public Object invoke(Object id) {
			Block b = (Block) container.get(id);
			if (b.size>0)
				b.set(0,(byte) 13);
			return b;
		}
	};

	/**
	 * Creates a new function returning a random size for an object to be
	 * inserted into the container.
	 * 
	 * @param maxValue the maximum size of the object to be inserted.
	 * @return a new function returning a random size for an object to be
	 *         inserted into the container.
	 */
	private static Function getRandomSizeFunction(final int maxValue) {
		return new AbstractFunction() {
			public Object invoke() {
				return new Integer(random.nextInt(maxValue));
			}
		};
	}

	/**
	 * Compares the container to be tested with a map also holding the elements
	 * of the container.
	 * 
	 * @param container the container to be tested.
	 * @param map a map holding the elements of the container.
	 * @return <tt>true</tt> if the given container and the specified map conatin
	 *         exaclty the same id/object pairs, otherwise <tt>false</tt>.
	 */
	private static boolean compareContainerWithMap(Container container, Map map) {
		Iterator it;
		Object key, valueMap, valueContainer;
		
		// test the elements of the map
		it =  map.keySet().iterator();
		while (it.hasNext()) {
			key = it.next();
			valueMap = map.get(key);
			valueContainer = container.get(key);
			if (valueMap==null)
				return false;
			if (valueContainer==null)
				return false;
			if (!valueContainer.equals(valueMap))
				return false;
		}
		
		it =  container.ids();
		while (it.hasNext()) {
			key = it.next();
			valueMap = map.get(key);
			valueContainer = container.get(key);
			if (valueMap==null)
				return false;
			if (valueContainer==null)
				return false;
			if (!valueContainer.equals(valueMap))
				return false;
		}
		
		return true;
	}

	/**
	 * Prints the first 10 elements of the given iteration to the default
	 * output steam. When the verbose flag is set, all elements are printed.
	 *
	 * @param it the iteration which elements should be printed.
	 */
	private static void output10Ids(Iterator it) {
		if (!verbose)
			it = new Taker(it,10);
		Cursors.println(it);
		if (!verbose)
			System.out.println("...");
	}

	/**
	 * Compares the number of the container's objects with an expected one.
	 * 
	 * @param expectedNumberOfObjects the expected number of objects the
	 *        container should hold.
	 */
	private static void compareNumberOfObjects(long expectedNumberOfObjects) {
		if (Cursors.count(container.ids())!=expectedNumberOfObjects)
			throw new RuntimeException("Container does not contain all Ids");
		if (Cursors.count(container.objects())!=expectedNumberOfObjects)
			throw new RuntimeException("Container does not contain all Objects");
		if (container.size()!=expectedNumberOfObjects)
			throw new RuntimeException("Container does not contain all Objects");
	}

	/**
	 * Example using the EXTree.
	 * @param args The command line options. Call the class with help or "?" to
	 * 	get a list of the possible parameters.
	 */
	public static void main(String[] args) {
		
		if (!TestFramework.processParameters("Container Test\n", ContainerTest.class, args, System.out))
			return;
		
		Function mapBeforeInsertFunction = null;
		Function modifyObjectFunction = null;
		Function sizeFunction = null;
		double time;
		
		switch (testCase) {
		case 1:
			System.out.println("Using a MapContainer");
			container = new MapContainer(useCloning);
			
			if (useCloning) {
				mapBeforeInsertFunction = mapToBlockFunction;
				modifyObjectFunction = modifyBlockFunction;
			}
			else {
				mapBeforeInsertFunction = Identity.DEFAULT_INSTANCE;
				modifyObjectFunction = Identity.DEFAULT_INSTANCE; // Maps the Object to its id! (The update-Function is called with the id)
			}
			sizeFunction = getRandomSizeFunction(blockSize);
			break;
		case 2:
			System.out.println("Using a BlockFileContainer");
			container = new BlockFileContainer(Common.getOutPath()+"ContainerTest",blockSize);
			
			mapBeforeInsertFunction = mapToBlockFunction;
			modifyObjectFunction = modifyBlockFunction;
			sizeFunction = new Constant(new Integer(blockSize));
			break;
		case 3:
			System.out.println("Using a MultiBlockContainer");
			container = new MultiBlockContainer(Common.getOutPath()+"ContainerTestMulti",blockSize);
			
			mapBeforeInsertFunction = mapToBlockFunction;
			modifyObjectFunction = modifyBlockFunction;
			sizeFunction = getRandomSizeFunction(3*blockSize);
			break;
		case 4:
			System.out.println("Using a RecordManager");
			Container baseCtr = new BlockFileContainer(Common.getOutPath()+"RecordManagerTest",blockSize);
			Strategy strategy = new NextFitStrategy();
			TIdManager manager;
			if (tidManager==0)
				manager = new IdentityTIdManager(baseCtr.objectIdConverter());
			else
				manager = new MapTIdManager(baseCtr.objectIdConverter());
			container = new RecordManager(baseCtr, blockSize, strategy, manager, numberOfDirectReserves);
			
			mapBeforeInsertFunction = mapToBlockFunction;
			modifyObjectFunction = modifyBlockFunction;
			sizeFunction = getRandomSizeFunction(blockSize-16);
			break;
		case 5:
			System.out.println("Using a RawAccessContainer in main memory");
			RawAccess ra = new RAMRawAccess(10000, blockSize);
			container = new RawAccessContainer(ra, 100);
			
			mapBeforeInsertFunction = mapToBlockFunction;
			modifyObjectFunction = modifyBlockFunction;
			sizeFunction = new Constant(new Integer(blockSize));
			break;
		default:
			System.out.println("This test case is not implemented so far");
			return;
		}

		if (activateBuffer)
			container = new BufferedContainer(container,new LRUBuffer(100),useCloning);
		
		if (activateCounterContainer)
			container = new CounterContainer(container);

		// Reference map. All objects are also inserted into the map.
		map = new HashMap();

		random = new Random(System.currentTimeMillis());
		Timer timer = (Timer) TimerUtils.FACTORY_METHOD.invoke();
		TimerUtils.warmup(timer);
		long tdur;
		
		Cursor cursor;
		Object key,value;
		byte[] bytes;
		Iterator it;

		//////////////////////////////////////////////////////////////////////////
		
		timer.start();
		
		for (int i=0;i<numberOfObjects;i++) {
			bytes = new byte[((Integer) sizeFunction.invoke()).intValue()];
			random.nextBytes(bytes);
			
			if (verbose)
				System.out.println(""+i+": Try to insert a new object (size="+bytes.length+")");
			// fill the byte array
			value = mapBeforeInsertFunction.invoke(bytes);
			key = container.insert(value);
			// also insert it into the map
			map.put(key, value);
			if (verbose)
				System.out.println("Object successfully inserted");
		}
		
		tdur = timer.getDuration();
		time = ((double) tdur/timer.getTicksPerSecond());
		TestFramework.list.add(new Double(time));
		
		System.out.println("Insertion completed");
		System.out.println("Time for insertion: "+time+"s");
		
		compareNumberOfObjects(numberOfObjects);
		
		//////////////////////////////////////////////////////////////////////////
		
		it = map.keySet().iterator();
		
		if (makeManipulation) {
			// perform a manipulation. Hopefully the test will fail at some point afterwards...
			map.put(it.next(), mapBeforeInsertFunction.invoke(("Hello").getBytes()));
			it = map.keySet().iterator();
		}

		//////////////////////////////////////////////////////////////////////////
		// Remove quarter of the elements in random order
		
		System.out.println("Remove quarter of the elements (element per element)");

		cursor =
		 	new Taker(
				new ShuffleCursor(
					map.keySet().iterator(),
					new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(random))
				),
				numberOfObjects/4
			);
	
		timer.start();
		while (cursor.hasNext()) {
			key = cursor.next();
			container.remove(key);
			if (map.remove(key)==null)
				throw new RuntimeException("REMOVE: object was not in the map (error in Java-API?)");
		}
		tdur = timer.getDuration();
		time = ((double) tdur/timer.getTicksPerSecond());
		TestFramework.list.add(new Double(time));
		
		cursor.close();

		compareNumberOfObjects(numberOfObjects*3/4);
		
		//////////////////////////////////////////////////////////////////////////
		// Remove half of the elements in random order using removeAll
		
		System.out.println("Remove half of the elements (using removeAll)");

		TeeCursor ltc =
			new TeeCursor(
			 	new Taker(
					new ShuffleCursor(
						map.keySet().iterator(),
						new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(random))
					),
					numberOfObjects/2
				)
			);

		timer.start();
		
		container.removeAll(ltc);

		tdur = timer.getDuration();
		time = ((double) tdur/timer.getTicksPerSecond());
		TestFramework.list.add(new Double(time));
		
		// The same in the map
		it = ltc.cursor();

		while (it.hasNext()) {
			key = it.next();
			if (map.remove(key)==null)
				throw new RuntimeException("REMOVE: object was not in the map (error in Java-API?)");
		}
		
		compareNumberOfObjects(numberOfObjects/4);

		//////////////////////////////////////////////////////////////////////////
		// Insert another quarter of elements with reserve/update and insert concurrently
		
		System.out.println("Insert half of the elements (using reserve/update and insert concurrently)");

		for (int i=0;i<numberOfObjects/4;i++) {
			bytes = new byte[((Integer) sizeFunction.invoke()).intValue()];
			random.nextBytes(bytes);

			// fill the byte array
			Object val = mapBeforeInsertFunction.invoke(bytes);
			key = container.reserve(new Constant(val));
			
			// also insert it into the map
			map.put(key, val);
			
			if (!container.isUsed(key))
				throw new RuntimeException("Key was not reserved");
//			if (container.contains(key))
//				throw new RuntimeException("Object was inserted by reserve");
			
			container.update(key,val);
		}
		
		compareNumberOfObjects(numberOfObjects/2);
		
		//////////////////////////////////////////////////////////////////////////
		// Reserve the id and remove the element immediately.

		System.out.println("Reserve the id and remove the element immediately");
System.out.println("Untested!!!!");
		
		bytes = new byte[((Integer) sizeFunction.invoke()).intValue()];
		random.nextBytes(bytes);

		// fill the byte array
		key = container.reserve(new Constant(mapBeforeInsertFunction.invoke(bytes)));
		container.remove(key);
		
		compareNumberOfObjects(numberOfObjects/2);

		//////////////////////////////////////////////////////////////////////////
		
		System.out.println("Comparison of the map with the container...");
		
		if (compareContainerWithMap(container,map)) {
			System.out.println("Everything ok: map and container contain the same elements");
		}
		else {
			if (!makeManipulation)
				throw new RuntimeException("ERROR: Container and Map are inconsistent!");				
			else
				System.out.println("Everything ok: manipulation has been detected correctly");
		}

		//////////////////////////////////////////////////////////////////////////
		
		System.out.println("Perform an integrity test inside the container...");
		
		cursor = container.objects();
		it = container.getAll(
			new Filter(
				container.ids(),
				new AbstractPredicate() {
					public boolean invoke(Object id) {
						return container.contains(id);
					}
				}
			)
		);
		
		Bag b = new ListBag(cursor);
		
		// Manipulation for testing the test!
		// cursor = b.cursor();
		// cursor.next();
		// cursor.remove();
		
		Predicate p = Equal.DEFAULT_INSTANCE;
		
		boolean equal =  true;
		
		while (it.hasNext()) {
			Object o = it.next();

			if (!Cursors.removeFirst(b.cursor(),new LeftBind(p,o))) {
				equal = false;
				break;
			}
		}
		
		if (b.size()>0)
			equal = false;
		
		if (!equal)
			throw new RuntimeException("Equal test failed");
		
		//////////////////////////////////////////////////////////////////////////

		// After the update there are no further tests on the objects because
		// they might have changed (and not the objects of the map).
		
		System.out.println("Testing updates");
		container.flush();
		
		container.updateAll(
			container.ids(),
			modifyObjectFunction
		);

// Iterator container.insertAll(Iterator)
// Iterator container.getAll(Iterator)
		if (compareContainerWithMap(container,map)) {
			if (!makeManipulation)
				throw new RuntimeException("ERROR: Container and Map are inconsistent!");				
			else
				System.out.println("Everything ok: manipulation has been detected correctly");
		}
		else {
			System.out.println("Everything ok: map and container contain the same elements");
		}

		//////////////////////////////////////////////////////////////////////////

		if (activateCounterContainer) {
			System.out.println("Statistics");
			System.out.println(container);
		}
		
		//////////////////////////////////////////////////////////////////////////

		System.out.println("Output at most 10 Ids");
		
		System.out.println("of the container:");
		output10Ids(container.ids());
		
		System.out.println("of the map:");
		output10Ids(map.keySet().iterator());

		//////////////////////////////////////////////////////////////////////////

		System.out.println("Remove the rest of the elements");

		container.clear();
		compareNumberOfObjects(0);
		
		//////////////////////////////////////////////////////////////////////////

		System.out.println("Finishing");
		container.flush();
		
		container.close();

		System.out.println("Everything ok");
	}
}
