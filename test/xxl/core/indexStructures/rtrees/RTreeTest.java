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

package xxl.core.indexStructures.rtrees;

import java.awt.Frame;
import java.io.File;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.containers.io.MultiBlockContainer;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.cursors.sources.SingleObjectCursor;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Common;
import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.GreenesRTree;
import xxl.core.indexStructures.LinearRTree;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.QuadraticRTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.ShowCursor;
import xxl.core.indexStructures.SortBasedBulkLoading;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.Tree.Query;
import xxl.core.indexStructures.Tree.Query.Candidate;
import xxl.core.indexStructures.XTree;
import xxl.core.io.Convertable;
import xxl.core.io.FilesystemOperations;
import xxl.core.io.LRUBuffer;
import xxl.core.io.LogFilesystemOperations;
import xxl.core.io.XXLFilesystem;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.spatial.KPE;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.spatial.rectangles.Rectangles;
import xxl.core.util.Arrays;
import xxl.core.util.reflect.TestFramework;

/**
 * Creates a flexible, disk resident <b>R*-Tree</b>, <b>linear R-Tree</b>, 
 * <b>quadratic R-Tree</b>, <b>Greenes R-Tree</b> or <b>X-Tree</b>
 * and performs exact match and range queries on it. Furthermore,
 * a detailed performance
 * evaluation is implemented, so the time for building and filling
 * the complete tree, as well as IO operations to external memory and buffers,
 * as the time for the different query evaluations is determined. <br>
 * <p>
 * <b>Data:</b>
 * The R-Tree indexes 10000 entries of key-pointer elements, a tuple containing a multi 
 * dimensional rectangle (key) and a pointer, which are extracted from the dataset 
 * rr_small.bin lying in the directory xxl\data. rr_small.bin contains a 
 * sample of minimal bounding rectangles (mbr) from
 * railroads in Los Angeles, i.e., the entries are of type 
 * {@link xxl.core.spatial.KPE}. 
 * Another dataset, called st_small.bin, located in the same directory,
 * is used for query evaluation. It also contains <tt>KPE</tt> objects, but
 * they represent mbr's of streets.
 * <p>
 * <b>Insertion:</b>
 * Elements delivered by an input cursor iterating over the external dataset
 * rr_small.bin can be inserted into the tree using two different
 * strategies: <br>
 * <ul>
 * <li> tuple insertion : each element of the input cursor is inserted separately </li>
 * <li> bulk insertion : quantities of elements are inserted at once </li>
 * </ul>
 * When using sort-based bulk insertion the user is able to choose different
 * compare modes: given order, by x-axis, by peano-value or by hilbert-value.
 * <p>
 * <b>Queries:</b>
 * <ul>
 * <li> range queries: <br>
 * 		range queries are performed, taking KPEs from
 *		st_small.bin. These rectangles represent descriptors. The descriptors
 *		are used for the query at any specified target level in the tree.
 * </li>
 * <li> nearest neighbor queries: <br>
 * 		A nearest neighbor query is also executed. Therefore
 * 		the first rectangle from st_small.bin is used as a query object, to which
 * 		the ten nearest neighbors are determined. For this
 *		realization a priority queue (dynamic heap) based on a special comparator
 *		defining an order on the distances of rectangles to
 * 		the query object is made use of.
 * </li>
 * </ul>
 * <p>
 * To get all valid parameters, please call <code>xxl xxl.applications.indexStructures.RTreeTest help</code>.
 * <p>
 * <b>Example usage:</b>
 * <pre>
 * 	xxl xxl.applications.indexStructures.RTreeTest minCap=10 maxCap=25 insertType=bulk_hilbert showData
 * </pre>
 *
 * @see java.util.Iterator
 * @see java.util.Comparator
 * @see xxl.core.collections.containers.Container
 * @see xxl.core.functions.Function
 * @see xxl.core.indexStructures.RTree
 * @see xxl.core.indexStructures.LinearRTree
 * @see xxl.core.indexStructures.QuadraticRTree
 * @see xxl.core.indexStructures.GreenesRTree
 * @see xxl.core.indexStructures.XTree
 * @see xxl.core.indexStructures.ORTree
 * @see xxl.core.indexStructures.Tree
 * @see xxl.core.io.Convertable
 * @see xxl.core.cursors.sources.io.FileInputCursor
 * @see xxl.core.io.LRUBuffer
 * @see xxl.core.spatial.KPE
 * @see xxl.core.spatial.rectangles.Rectangles
 */
public class RTreeTest {
	
	/** Dimension of the data. The data used in this example is 2 dimensional. This field is intended to 
	 * show where the dimensionality influeneces the usage.
	 */
	public static final int dimension = 2;
	
	/** Description for {@link #filenameTree}.
	 */
	public static final String filenameTreeDescription = "name of the file used for the tree";
	
	/** Name of the file used for the tree.
	 */
	public static String filenameTree = "rr_small.bin";
	
	/** Description for {@link #filenameQueries}.
	 */
	public static final String filenameQueriesDescription = "name of the file that contains the queries";
	
	/** Name of the file that contains the queries.
	 */
	public static String filenameQueries = "st_small.bin";
	
	/** Description for {@link #numberOfQueries}.
	 */
	public static final String numberOfQueriesDescription = "number of window-queries (-1 means all)";
	
	/** Number of window-queries (-1 means all).
	 */
	public static int numberOfQueries = -1;

	/** Description for {@link #blockSize}.
	 */
	public static final String blockSizeDescription = "size of a block in bytes";
	
	/** Size of a block in bytes.
	 */
	public static int blockSize = 1536;
		
	/** Description for {@link #minMaxFactor}.
	 */
	public static final String minMaxFactorDescription = "factor which the minimum capacity of nodes is smaller than the maximum capacity";
	
	/** Factor which the minimum capacity of nodes is smaller than the maximum capacity.
	 */
	public static double minMaxFactor = 1.0/3.0;
	
	/** Description for {@link #tree}.
	 */
	public static final String treeDescription = "Linear, Quadratic, Greene, R*, X";
	
	/** Linear, Quadratic, Greene, R*, X.
	 */
	public static String tree = "r*";

	/** Description for {@link #insertType}.
	 */
	public static final String insertTypeDescription = "type of insertion: tuple, bulk, bulk_xsort, bulk_peano, bulk_hilbert";
	
	/** Type of insertion: tuple, bulk, bulk_xsort, bulk_peano, bulk_hilbert.
	 */
	public static String insertType = "tuple";
	
	/** Description for {@link #bufferSize}.
	 */
	public static final String bufferSizeDescription = "buffersize (number of node-objects)";
	
	/** Buffersize (number of node-objects).
	 */
	public static int bufferSize = 100;
	
	/** Description for {@link #targetLevel}.
	 */
	public static final String targetLevelDescription = "number of level for queries";
	
	/** Number of level for queries.
	 */
	public static int targetLevel = 0;
	
	/** Description for {@link #unbufferedWrite}.
	 */
	public static final String unbufferedWriteDescription = "unbuffered write operations. Only with sdk 1.4 and higher";
	
	/** Unbuffered write operations. Only with sdk 1.4 and higher.
	 */
	public static boolean unbufferedWrite = false;
	
	/** Description for {@link #rawDevice}.
	 */
	public static final String rawDeviceDescription = "use raw devices";
	
	/** Use raw devices.
	 */
	public static boolean rawDevice= false;

	/** Description for {@link #showData}.
	 */
	public static final String showDataDescription = "shows the input data of the first relation while inserting";
	
	/** Shows the input data of the first relation while inserting.
	 */
	public static boolean showData = false;
	
	/** Description for {@link #showDataDelay}.
	 */
	public static final String showDataDelayDescription = "delay after each rectangle in ms";
	
	/** Delay after each rectangle in ms.
	 */
	public static int showDataDelay = 1;

	/** Description for {@link #verbose}.
	 */
	public static final String verboseDescription = "shows some more informations on System.out";
	
	/** Shows some more informations on System.out.
	 */
	public static boolean verbose = false;

	/**
	 * Part of the @link{TestFramework} which returns the relational 
	 * metadata to the meassured values of a test run. These test values
	 * are stored inside a @link{TestFramework.list}.
	 * @return Here, the following values are meassured and the appropriate
	 * 	relational meta data is returned: TimeForInsertion, TreeHeight,
	 *  CheckDescriptor, OverAndUnderflows, RangeQueryTime, RangeQueryHits, 
	 *  NearestNeighborTime, RootRangeQueryResults, RemoveTime. 
	 */
	public static ResultSetMetaData getReturnRSMD() {
		return new ColumnMetaDataResultSetMetaData(
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "TimeForInsertion", "TimeForInsertion", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "TreeHeight", "TreeHeight", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, 1, "CheckDescriptor", "CheckDescriptor", "", 1, 0, "", "", Types.BIT,  true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "OverAndUnderflows", "OverAndUnderflows", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "RangeQueryTime", "RangeQueryTime", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "RangeQueryHits", "RangeQueryHits", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "NearestNeighborTime", "NearestNeighborTime", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "RootRangeQueryResults", "RootRangeQueryResults", "", 9, 0, "", "", Types.INTEGER, true, false, false),
			new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, "RemoveTime", "RemoveTime", "", 9, 0, "", "", Types.INTEGER, true, false, false)
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
		if (fieldName.equals("verbose"))
			return new SingleObjectCursor(Boolean.FALSE);
		else if (fieldName.equals("showData"))
			return new SingleObjectCursor(Boolean.FALSE);
		else if (fieldName.equals("showDataDelay"))
			return new SingleObjectCursor(new Integer(1));
		else if (fieldName.equals("rawDevice"))
			return new SingleObjectCursor(Boolean.FALSE);
		else if (fieldName.equals("targetLevel"))
			return new SingleObjectCursor(new Integer(0));
		else if (fieldName.equals("bufferSize"))
			return Arrays.intArrayIterator(new int[]{0,200});
		else if (fieldName.equals("insertType"))
			return Arrays.stringArrayIterator(new String[]{"tuple", "bulk", "bulk_xsort", "bulk_peano", "bulk_hilbert"});
		else if (fieldName.equals("tree"))
			return Arrays.stringArrayIterator(new String[]{"Quadratic", "Greene", "R*", "X", "Linear"});
		else if (fieldName.equals("mincap"))
			return Arrays.intArrayIterator(new int[]{10,20,30,40});
		else if (fieldName.equals("maxcapFactor"))
			return Arrays.doubleArrayIterator(new double[]{3.0});
		else
			return null;
	}

	/**
	 * The Universe containing all used rectangles.
	 */
	protected static Rectangle universe = null;
	
	/**
	 * Precision of the space filling curve.
	 */
	protected static int FILLING_CURVE_PRECISION = 1<<30;
	
	/**
	 * Factory Function to get a leaf entry.
	 */
	static Function<Object, KPE> LEAFENTRY_FACTORY = new AbstractFunction<Object, KPE>() {
		public KPE invoke() {
			return new KPE(new DoublePointRectangle(dimension));
		}
	};
	
	/** 
	 * Function creating a descriptor for a given object. 
	 */
	static Function GET_DESCRIPTOR = new AbstractFunction() {
		public Object invoke (Object o) {
			return ((KPE)o).getData(); 
		}
	};

	/** 
	 * A function that sorts the rectangles according to their lower left border.
	 * This is needed for sort-based bulk insertion.
	 */
	static Comparator COMPARE = new Comparator() {
		public int compare(Object o1, Object o2) {
			return ((Rectangle)(((KPE)o1).getData())).compareTo(((KPE)o2).getData()); 
		}
	};

	/**
	 * A function that sorts the rectangles according to their hilbert value.
	 * This is needed for sort-based bulk insertion.
	 */
	static Comparator COMPARE_HILBERT = new Comparator() {
		protected double uni[];
		protected double uniDeltas[];	
		
		public int compare(Object o1, Object o2) {
			if (dimension!=2) throw new IllegalArgumentException();
			if (uni == null) {
				uni = (double[])universe.getCorner(false).getPoint();
				uniDeltas = universe.deltas();	
			}
			double leftBorders1[] = (double[])((Rectangle)((KPE)o1).getData()).getCorner(false).getPoint();
			double leftBorders2[] = (double[])((Rectangle)((KPE)o2).getData()).getCorner(false).getPoint();
				
			double x1 = (leftBorders1[0]-uni[0])/uniDeltas[0];
			double y1 = (leftBorders1[1]-uni[1])/uniDeltas[1];
			double x2 = (leftBorders2[0]-uni[0])/uniDeltas[0];
			double y2 = (leftBorders2[1]-uni[1])/uniDeltas[1];
			
			long h1 = SpaceFillingCurves.hilbert2d((int) (x1*FILLING_CURVE_PRECISION),(int) (y1*FILLING_CURVE_PRECISION));
			long h2 = SpaceFillingCurves.hilbert2d((int) (x2*FILLING_CURVE_PRECISION),(int) (y2*FILLING_CURVE_PRECISION));
			return (h1<h2)?-1: ((h1==h2)?0:+1);
		}
	};

	/**
	 * A function that sorts the rectangles according to their peano value.
	 * This is needed for sort-based bulk insertion.
	 */
	static Comparator COMPARE_PEANO = new Comparator() {
		protected double uni[];
		protected double uniDeltas[];	
		
		public int compare(Object o1, Object o2) {
			if (dimension!=2) throw new IllegalArgumentException();
			if (uni == null) {
				uni = (double[])universe.getCorner(false).getPoint();
				uniDeltas = universe.deltas();	
			}
			double leftBorders1[] = (double[])((Rectangle)((KPE)o1).getData()).getCorner(false).getPoint();
			double leftBorders2[] = (double[])((Rectangle)((KPE)o2).getData()).getCorner(false).getPoint();
			
			double x1 = (leftBorders1[0]-uni[0])/uniDeltas[0];
			double y1 = (leftBorders1[1]-uni[1])/uniDeltas[1];
			double x2 = (leftBorders2[0]-uni[0])/uniDeltas[0];
			double y2 = (leftBorders2[1]-uni[1])/uniDeltas[1];
			
			long h1 = SpaceFillingCurves.peano2d((int) (x1*FILLING_CURVE_PRECISION),(int) (y1*FILLING_CURVE_PRECISION));
			long h2 = SpaceFillingCurves.peano2d((int) (x2*FILLING_CURVE_PRECISION),(int) (y2*FILLING_CURVE_PRECISION));
			return (h1<h2) ? -1 : ( (h1==h2) ? 0 : 1 );
		}
	};

	/**
	 * Returns a comparator which evaluates the distance of two candidate objects
	 * to the specified <tt>queryObject</tt>. This comparator
	 * is used for nearest neighbor queries and defines an order on candidate-
	 * descriptors. With the help of a priority queue (Min-heap) and this
	 * comparator the nearest neighbor query can be performed.
	 *
	 * @param queryObject a KPE to which the nearest neighbors should be determined
	 * @return a comparator defining an order on candidate objects
	 */
	public static Comparator getDistanceBasedComparator (KPE queryObject) {
		final Rectangle query = (Rectangle) queryObject.getData();
		return new Comparator () {
			public int compare (Object candidate1, Object candidate2) {
				Rectangle r1 = (Rectangle) (((Candidate) candidate1).descriptor()) ;
				Rectangle r2 = (Rectangle) (((Candidate) candidate2).descriptor()) ;
				
				double d1 = query.distance(r1, 2);
				double d2 = query.distance(r2, 2);
				return (d1<d2) ? -1 : ( (d1==d2) ? 0 : 1 );
			}
		};
	}

	/**
	 * Parses the arguments and put them into a property list.
	 * 
	 * @param defaultProps default values for properties
	 * @param args command line parameters
	 * @return property list 
	 */
	public static Properties argsToProperties(Properties defaultProps, String args[]) {
		Properties props = new Properties(defaultProps);
		
		for (int i=0; i<args.length; i++) {
			String prop;
			String val = null;
			int indexEqual = args[i].indexOf('=');
			
			if (indexEqual>0) {
				prop = args[i].substring(0, indexEqual);
				val = args[i].substring(indexEqual+1);
			}
			else {
				prop = args[i];
				val = ""; 
			}
			prop = prop.toLowerCase();
			props.setProperty(prop, val);	
		}
		
		return props;
	}

	/**
	 * Starts the RTreeTest. Start this class with
	 * option "help" in order to get all the command-line options.
	 * 
	 * @param args command line parameters
	 * @throws Exception
	 */
	public static void main (String args[]) throws Exception {
		
		if (!TestFramework.processParameters("RTreeTest : an example using RTree and its variants\n", RTreeTest.class, args, System.out))
			return;
		
		String path = Common.getOutPath();
		
		boolean bulk = insertType.toLowerCase().startsWith("bulk");
		Comparator compare = null;
		if (bulk) {
			if (insertType.equalsIgnoreCase("bulk_peano"))
				compare = COMPARE_PEANO;
			else if (insertType.equalsIgnoreCase("bulk_hilbert"))
				compare = COMPARE_HILBERT;
			else if (insertType.equalsIgnoreCase("bulk_xsort"))
				compare = COMPARE;
		}
				
		int dataSize = dimension*2*8+4; // DoublePointRectangle (2 doubles per dimension) + int
		int descriptorSize = dimension*2*8; // DoublePointRectangle (2 doubles per dimension)
		boolean useMultiBlockContainer = tree.equalsIgnoreCase("x");
				
		// RawDevice internal parameters
		int sizeOfRawDevice = 10000; // only interesting if it is a file
		boolean nativeRawDevice = false;
		String rawDeviceName = path+"RTree";
		
		XXLFilesystem fs;
		
		if (rawDevice) {
			path = "";
			int partitions[];
			String fileNames[];
			
			if (useMultiBlockContainer) {
				int ctrBlocks = (sizeOfRawDevice-2-2-2-10)/2;
				partitions =  new int[]{2, 2, 2, 10, ctrBlocks, 2, 2, 2, 10};
				fileNames = MultiBlockContainer.getFilenamesUsed("RTree");
			}
			else {
				partitions = new int[]{2, 2, 2, 10};
				fileNames = BlockFileContainer.getFilenamesUsed("RTree");
			}
			fs = new XXLFilesystem(
				nativeRawDevice, blockSize,	sizeOfRawDevice, 
				rawDeviceName, partitions, fileNames, 0, false, true, false
			);
		}
		else
			fs = new XXLFilesystem(unbufferedWrite);
		
		FilesystemOperations fso = fs.getFilesystemOperations();
		
		/*********************************************************************/
		/*                          BUILDING TREES                           */
		/*********************************************************************/
		
		Frame f = null;
		RTree rtree=null;
		Container fileContainer=null;
		fso = new LogFilesystemOperations(fso, System.out, true);
		
		if (tree.equalsIgnoreCase("x")) {
			rtree = new XTree();
			fileContainer = new MultiBlockContainer(path+"RTree", blockSize, fso);
		}
		else {
			if (tree.equalsIgnoreCase("r*"))
				rtree = new RTree();
			else if (tree.equalsIgnoreCase("greene"))
				rtree = new GreenesRTree();
			else if (tree.equalsIgnoreCase("linear"))
				rtree = new LinearRTree();
			else if (tree.equalsIgnoreCase("quadratic"))
				rtree = new QuadraticRTree();
			
			fileContainer = new BlockFileContainer(path+"RTree", blockSize, fso);
		}
			
		// an unbuffered container that counts the access to the RTree		
		CounterContainer lowerCounterContainer = new CounterContainer(
			new ConverterContainer(
				fileContainer,
				rtree.nodeConverter(
					new ConvertableConverter(LEAFENTRY_FACTORY), dimension
				)
			)
		);
		
		// a buffered container that count the access to the buffered RTree
		Container bufferedContainer;
		if (bufferSize>0)
			bufferedContainer = new BufferedContainer(lowerCounterContainer, new LRUBuffer(bufferSize), true);
		else
			bufferedContainer = lowerCounterContainer;
		
		CounterContainer upperCounterContainer = new CounterContainer(bufferedContainer);
		
		// the container that stores the content of the RTree
		Container container = upperCounterContainer;
		
		// Read the universe (needed for space filling curves)
		universe = Rectangles.readSingletonRectangle(new File(Common.getDataPath()+filenameTree+".universe"), new DoublePointRectangle(dimension));
		System.out.println("Universe read:\n"+universe+"\n");
		
		// initialize the RTree with the descriptor-factory method, a
		// container for storing the nodes and the minimum and maximum
		// capacity of them
		if (tree.equalsIgnoreCase("x")) {
			int entrySize = Math.max(dataSize, descriptorSize+8);
			int xTreeMaxCap = (blockSize - 6) / entrySize;
			int xTreeMinCap = (int) (xTreeMaxCap * minMaxFactor);
			((XTree) rtree).initialize(GET_DESCRIPTOR, container, xTreeMinCap, xTreeMaxCap, dimension);
		}
		else
			rtree.initialize(GET_DESCRIPTOR, container, blockSize, dataSize, descriptorSize, minMaxFactor);

		Cursor cursor = new FileInputCursor<KPE>(
			new ConvertableConverter<KPE>(LEAFENTRY_FACTORY),
			new File(Common.getDataPath()+filenameTree)
		);

		/*********************************************************************/
		/*                             INSERTION                             */
		/*********************************************************************/

		long t1 = System.currentTimeMillis(), t2;
		
		if (!bulk) {
			// insert an iterator of objects by inserting every single object
			if (showData) {
				ShowCursor rc = new ShowCursor(ShowCursor.KPE_DOUBLE_POINT_RECTANGLE_MODE, cursor, universe, showDataDelay);
				f = rc.createFrame(800, 800);
				cursor = rc;
			}
				
			while (cursor.hasNext()) {
				Convertable c = (Convertable) cursor.next();
				rtree.insert(c);
			}
		}
		else {
			// or by bulk-insertion
			if (compare!=null)
				cursor = new MergeSorter(cursor, compare, dataSize, 4*4096, 4*4096); 
			
			if (showData) {
				ShowCursor rc = new ShowCursor(ShowCursor.KPE_DOUBLE_POINT_RECTANGLE_MODE, cursor, universe, showDataDelay);
				f = rc.createFrame(800, 800);
				cursor = rc;
			}
						
			new SortBasedBulkLoading(rtree, cursor, new Constant(container));
		}
		cursor.close();
		
		t2 = System.currentTimeMillis();
		TestFramework.list.add(new Long(t2-t1));
		TestFramework.list.add(new Integer(rtree.height()));
		
		System.out.println("Time for insertion: "+(t2-t1));
		System.out.println("Insertion complete, height: "+rtree.height()+", universe: \n"+rtree.rootDescriptor()+"\n");
		System.out.println("Accessing the BufferedContainer\n"+upperCounterContainer+"\n");
		System.out.println("Accessing the ConverterContainer and the BlockFileContainer\n"+lowerCounterContainer+"\n");
		
		System.out.println("Reset counters");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();
		
		if (bufferSize>0) {
			System.out.println("Flushing buffers");
			bufferedContainer.flush();
		}
		System.out.println("\nAccessing the BufferedContainer\n"+upperCounterContainer+"\n");
		System.out.println("Accessing the ConverterContainer and the BlockFileContainer\n"+lowerCounterContainer+"\n");

		/*********************************************************************/
		/*                  ADDITIONAL CHECKS FOR CONSISTENCE                */
		/*********************************************************************/

		System.out.print("Checking descriptors... ");
		TestFramework.list.add(new Boolean(rtree.checkDescriptors()));
		System.out.println("done.");

		System.out.print("Checking number of entries (between min and max?)... ");
		TestFramework.list.add(new Integer(rtree.checkNumberOfEntries()));
		System.out.println("done.\n");
		
		System.out.println("Reset counters");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();

		/*********************************************************************/
		/*                             RANGE QUERY                           */
		/*********************************************************************/

		System.out.println("Perform queries");
		
		int hits = 0;
		cursor = new FileInputCursor<KPE>(
			new ConvertableConverter<KPE>(LEAFENTRY_FACTORY),
			new File(Common.getDataPath()+filenameQueries)
		);
		
		if (numberOfQueries>-1)
			cursor = new Taker(cursor,numberOfQueries);

		t1 = System.currentTimeMillis();
		while (cursor.hasNext()) {
			hits += Cursors.count(
				rtree.query( 
					(Descriptor) GET_DESCRIPTOR.invoke(cursor.next()),
					targetLevel
				)
			);
		}
		cursor.close();
		t2 = System.currentTimeMillis();
		TestFramework.list.add(new Long(t2-t1));
		TestFramework.list.add(new Integer(hits));
		
		System.out.println("Time for queries: "+(t2-t1));
		System.out.println("Number of hits: "+hits+"\n");
		System.out.println("Accessing the BufferedContainer\n"+upperCounterContainer+"\n");
		System.out.println("Accessing the ConverterContainer and the BlockFileContainer\n"+lowerCounterContainer+"\n");
		
		/*********************************************************************/
		/*                     NEAREST NEIGHBOR QUERY                        */
		/*********************************************************************/

		System.out.println("Performing a nearest neighbor query against the tree \n"
						 + "determining the 10 nearest neighbor entries at target level \n"
						 + "concerning the input iterator's next element in ascending order: ");

		cursor = new FileInputCursor<KPE>(
				new ConvertableConverter<KPE>(LEAFENTRY_FACTORY),
			new File(Common.getDataPath()+filenameQueries)
		);
		
		cursor.hasNext();
		KPE queryObject = (KPE) cursor.next();
		System.out.println("Query object:"+queryObject);
		
		t1 = System.currentTimeMillis();

		// consuming the fifty nearest elements concerning the query object at the
		// the target level;
		// the order is determined by the comparator given to the dynamic heap
		// structure realizing a priority queue
		cursor = new Taker(
			rtree.query(new DynamicHeap(getDistanceBasedComparator(queryObject)), targetLevel),
			10
		);
		int counter = 0;
		while (cursor.hasNext()) {
			Rectangle next = (Rectangle)((Candidate)cursor.next()).descriptor();
			if (verbose)
				System.out.println("candidate no. "+(++counter)+": "+next);
			// "\t distance to query object: "+queryObject.centerDistance(next));
		}
		cursor.close();
		
		t2 = System.currentTimeMillis();
		TestFramework.list.add(new Long(t2-t1));

		System.out.println("\nTime for query: "+(t2-t1) +" ms.");

		/*********************************************************************/
		/*                   RANGE QUERY WITH ROOT DESCRIPTOR                */
		/*********************************************************************/
		
		System.out.println("Querying root descriptor");
		int numberOfElements = Cursors.count(rtree.query(rtree.rootDescriptor(), targetLevel));
		// equivalent with Cursors.count(rtree.query())
		System.out.println("Number of results: "+numberOfElements);

		TestFramework.list.add(new Integer(numberOfElements));

		/*********************************************************************/
		/*                       REMOVING ALL ELEMENTS                       */
		/*********************************************************************/

		System.out.println("Removing all elements");

		t1 = System.currentTimeMillis();
		
		// Three ways of removing all elements!
		
		/*
		Cursor c; 
		// only one element can be removed with each query (because the state of the cursor 
		// becomes illegal after the removal.
		while (true) {
			c = rtree.query();
			if (c.hasNext()) {
				c.next();
				c.remove();
			}
			else 
				break;
		}*/
		
		cursor = new FileInputCursor<KPE>(
			new ConvertableConverter<KPE>(LEAFENTRY_FACTORY),
			new File(Common.getDataPath()+filenameTree)
		);
		
		while(cursor.hasNext())
			rtree.remove(cursor.next());
		cursor.close();
		
		/*
		while (it.hasNext()) {
			Tree.Descriptor t = (Tree.Descriptor) GET_DESCRIPTOR.invoke(it.next());
			rtree.remove(t,0);
		}*/
		
		t2 = System.currentTimeMillis();
		TestFramework.list.add(new Long(t2-t1));
		System.out.println("Time for removal: "+(t2-t1)+"\n");
		
		numberOfElements = Cursors.count(rtree.query());
		System.out.println("Number of elements in the tree: "+numberOfElements);
		
		System.out.println("Closing application");
		
		container.close();
		
		// delete files
		if (fileContainer instanceof BlockFileContainer)
			((BlockFileContainer) fileContainer).delete();
		else if (fileContainer instanceof MultiBlockContainer)
			((MultiBlockContainer) fileContainer).delete();
		
		fs.close();
		
		if (f!=null) {
			System.out.println("Press RETURN to finish application");
			System.in.read();
			f.dispose();
		}
	}
}
