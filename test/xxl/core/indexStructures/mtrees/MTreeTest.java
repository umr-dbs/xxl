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

package xxl.core.indexStructures.mtrees;

import java.io.File;
import java.util.Comparator;
import java.util.Properties;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Taker;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Common;
import xxl.core.indexStructures.MTree;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.SlimTree;
import xxl.core.indexStructures.SortBasedBulkLoading;
import xxl.core.indexStructures.Sphere;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.Tree.Query;
import xxl.core.indexStructures.Tree.Query.Candidate;
import xxl.core.io.Convertable;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.spatial.KPE;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.cursors.KPEInputCursor;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.points.Point;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.spatial.rectangles.Rectangles;


/**
 * Creates a flexible, disk resident <b>M-Tree</b> or <b>Slim-Tree</b> and performs exact match, range
 * and nearest neighbor queries on it. Furthermore a detailed performance
 * evaluation is implemented, so the time for building and filling
 * the complete M-Tree, as well I/O-operations to external memory and buffers,
 * as the time for the different query evaluations is determined. <br>
 * <p>
 * <b>Data:</b>
 * The M-Tree indexes 10000 entries of type <tt>DoublePoint</tt>, a high dimensional point, which
 * are extracted from the dataset rr_small.bin lying in the directory
 * xxl\data. rr_small.bin contains a sample of minimal bounding rectangles (mbr) from
 * railroads in Los Angeles, i.e., the entries are of type KPE. So all
 * entries have to be converted to DoublePoints at first, using the
 * mapping functions and factory methods provided by this class.
 * Another dataset, called st_small.bin, located in the same directory
 * is used for query evaluation. It also contains KPE objects, but
 * they represent mbr's of streets.
 * <p>
 * <b>Insertion:</b>
 * Elements delivered by input cursor iterating over the external dataset
 * rr_small.bin can be inserted into the M-Tree using two different
 * strategies: <br>
 * <ul>
 * <li> tuple insertion: each element of the input cursor is inserted separately </li>
 * <li> bulk insertion: quantities of elements are inserted at once </li>
 * </ul>
 * When using sort-based bulk insertion the user is able to choose different
 * compare modes: given order, by x-axis, by peano-value or by hilbert-value.
 * <p>
 * <b>Queries:</b>
 * <ul>
 * <li> exact match queries: <br>
 *		1000 exact match queries are performed, taking 1000 KPEs from
 *		st_small.bin, converting them to DoublePoints and querying
 *		target level 0 of the M-Tree, i.e., only leaf nodes will be
 *		returned.
 * </li>
 * <li> range queries: <br>
 * 		1000 range queries are performed, taking 1000 KPEs from
 *		st_small.bin and converting them to Spheres, such that
 *		a sphere covers the rectangle belonging to a KPE, i.e.,
 *		the center of the sphere is set to the center of the rectangle.
 *		These spheres represent descriptors. The descriptors
 *		are used for the query at any specified target level in the M-Tree.
 * </li>
 * <li> nearest neighbor queries: <br>
 * 		A nearest neighbor query is also executed. Therefore a
 * 		sphere resulting by applying a mapping function to
 * 		an input element delivered from the input cursor iterating
 *		over st_small.bin is used as a query object, to which
 * 		the fifty nearest neighbors will be determined. For this
 *		realization a priority queue (dynamic heap) based on a special comparator
 *		defining an order on the distances of arbitrary points to
 * 		the query object is made use of.
 * </li>
 * </ul>
 * <p>
 * <b>Parameters:</b>
 * <ul>
 * <li> 1.) minimum capacity of nodes </li>
 * <li> 2.) maximum capacity of nodes </li>
 * <li> 3.) insertion type: tuple or bulk (different compare modes) </li>
 * <li> 4.) buffersize (number of node-objects)</li>
 * <li> 5.) target level for queries. Default: 0 </li>
 * <li> 6.) split strategy
 * <li> 7.) type of tree: MTree or SlimTree
 * </ul>
 * <p>
 * <b>Example usage:</b>
 * <pre>
 * 	java -Dxxlrootpath=W:\\dev\\xxl.cvs\\xxl -Dxxloutpath=W:\\dev\\xxl.out\\ xxl.applications.indexStructures.MTreeTest minCap=10 maxCap=25 insertType=bulk_hilbert bufSize=256 level=0 slimTree
 *
 * 	or:
 *
 * 	xxl xxl.applications.indexStructures.MTreeTest minCap=10 maxCap=25 insertType=bulk_hilbert bufSize=256 level=0 slimTree
 *
 * </pre>
 * For further parameters and settings of default values type:
 * <pre>
 *	java xxl.applications.indexStructures.MTreeTest /?
 * </pre>
 *
 *
 * @see java.util.Iterator
 * @see java.util.Comparator
 * @see xxl.core.collections.containers.Container
 * @see xxl.core.functions.Function
 * @see xxl.core.indexStructures.MTree
 * @see xxl.core.indexStructures.ORTree
 * @see xxl.core.indexStructures.SlimTree
 * @see xxl.core.indexStructures.Sphere
 * @see xxl.core.indexStructures.Tree
 * @see xxl.core.io.converters.Converter
 * @see xxl.core.io.LRUBuffer
 * @see xxl.core.spatial.points.DoublePoint
 * @see xxl.core.spatial.KPE
 * @see xxl.core.spatial.points.Point
 * @see xxl.core.spatial.rectangles.Rectangle
 */
public class MTreeTest {

	/**
	 *  File containing the railroad input data.
	 */ 
	protected static String FILENAME_TREE = "rr_small.bin";

	/**
	 *  File containing the streets input data.
	 */ 
	protected static String FILENAME_QUERIES = "st_small.bin";

	/**
	 * The precision parameter for the computation of space filling curves.
	 */
	protected static int FILLING_CURVE_PRECISION = 1<<30;
	
	/** 
	 * An rectangle containing all data rectangles. 
	 * Needed for peano-/hilbert value computation.
	 */
	protected static Rectangle universe = Common.getDataPath() != null ? Rectangles.readSingletonRectangle(new File(Common.getDataPath()+FILENAME_TREE+".universe"), new DoublePointRectangle(2)) : null;

	/**
	 * A factory method to be invoked with one parameter of type KPE
	 * and returning an instance of the class DoublePoint, namely
	 * the midpoint of the rectangle belonging to the given KPE.
	 */
	public static Function LEAFENTRY_FACTORY = new AbstractFunction() {
		public Object invoke (Object kpe) {
			return getMidPoint((KPE)kpe);
		}
	};

	/**
	 * A factory method to be invoked with one parameter of type KPE
	 * and returning an instance of the class Sphere covering the
	 * whole rectangle.
	 */
	public static Function SPHERE_COVERING_RECTANGLE_FACTORY = new AbstractFunction() {
		public Object invoke (Object kpe) {
			DoublePoint center = getMidPoint((KPE)kpe);
			return new Sphere (center, center.distanceTo(((Rectangle)((KPE)kpe).getData()).getCorner(true)),
						   centerConverter(center.dimensions()));
		}
	};

	/**
	 * An unary factory method that returns a descriptor for
	 * a given point. That means a new Sphere is generated
	 * containing the given point as its center.
	 */
	public static Function getDescriptor = new AbstractFunction() {
		public Object invoke (Object o) {
			Point point = (Point)o;
			return new Sphere(point, 0.0, centerConverter(point.dimensions()));
		}
	};

	/**
	 * Returns a converter that serializes the center
	 * of sphere objects. In this case the center of a sphere
	 * is an high-dimensional DoublePoint. <br>
	 * This converter is used by the M-Tree to read/write leaf
	 * nodes to external memory.
	 *
	 * @param dimension the dimension of the DoublePoint representing
	 * 		the center of the sphere
	 * @return a converter serializing DoublePoints
	 */
	public static Converter centerConverter (final int dimension) {
		return new ConvertableConverter(
			new AbstractFunction() {
				public Object invoke () {
					return new DoublePoint(dimension);
				}
			}
		);
	}

	/**
	 * Returns a converter that serializes the descriptors of
	 * the M-Tree, i.e., spheres
	 * This converter is used by the M-Tree to read/write index
	 * nodes to external memory
	 *
	 * @param dimension the dimension of the DoublePoint representing
	 * 		the center of the sphere
	 * @return a converter serializing spheres
	 */
	public static Converter descriptorConverter (final int dimension) {
		
		
		return new ConvertableConverter(
			new AbstractFunction() {
				public Object invoke () {
					return new Sphere(new DoublePoint(dimension), 0.0, centerConverter(dimension));
				}
			}
		);
	}

	/**
	 * Determining the midpoint of the rectangle belonging to
	 * the given KPE object.
	 *
	 * @param kpe the KPE object delivering the rectangle
	 * @return the midpoint of the given rectangle
	 */
	public static DoublePoint getMidPoint (KPE kpe) {
		Rectangle rect = (Rectangle)kpe.getData();
		Point ll = rect.getCorner(false);
		Point ur = rect.getCorner(true);
		int dim = ll.dimensions();
		double[] midValues = new double[dim];
		for (int i = 0; i < dim; i++)
			midValues[i] = ll.getValue(i) + (ur.getValue(i) - ll.getValue(i))/2;
		return new DoublePoint(midValues);
	}

	/**
	 * Returns a comparator which evaluates the distance of two candidate objects
	 * to the specified <tt>queryObject</tt>. This comparator
	 * is used for nearest neighbor queries and defines an order on candidate-
	 * descriptors. With the help of a priority queue (Min-heap) and this
	 * comparator the nearest neighbor query can be performed.
	 *
	 * @param queryObject a sphere to which the nearest neighbors should be determined
	 * @return a comparator defining an order on candidate objects
	 */
	public static Comparator getDistanceBasedComparator (final Sphere queryObject) {
		return new Comparator () {
			public int compare (Object candidate1, Object candidate2) {
				double sphereDist1 = queryObject.sphereDistance((Sphere)((Candidate)candidate1).descriptor()),
					   sphereDist2 = queryObject.sphereDistance((Sphere)((Candidate)candidate2).descriptor());
				return sphereDist1 < sphereDist2 ? -1 : sphereDist1 == sphereDist2 ? 0 : 1;
			}
		};
	}

	/**
	 * A factory method returning a Comparator defining an order on points
	 * according to their first dimension. <br>
	 * This is only needed for sort-based bulk insertion.
	 */
	static Comparator COMPARE_X_AXIS = new Comparator() {
		public int compare (Object point1, Object point2) {
			return ((Point)point1).getValue(0) < ((Point)point2).getValue(0) ? -1 :
				   ((Point)point1).getValue(0) == ((Point)point2).getValue(0) ? 0 : 1;
		}
	};

	/**
	 * A function that sorts the Points according to the hilbert value.
	 * This is needed for sort-based bulk insertion.
	 */
	static Comparator COMPARE_HILBERT = new Comparator() {

		protected double delta_x = universe.getCorner(true).getValue(0) - universe.getCorner(false).getValue(0);
		protected double delta_y = universe.getCorner(true).getValue(1) - universe.getCorner(false).getValue(1);

		public int compare(Object point1, Object point2) {
			double x1 = (((Point)point1).getValue(0)-universe.getCorner(false).getValue(0))/delta_x;
			double y1 = (((Point)point1).getValue(1)-universe.getCorner(false).getValue(1))/delta_y;
			double x2 = (((Point)point2).getValue(0)-universe.getCorner(false).getValue(0))/delta_x;
			double y2 = (((Point)point2).getValue(1)-universe.getCorner(false).getValue(1))/delta_y;
	
			long h1 = SpaceFillingCurves.hilbert2d((int) (x1*FILLING_CURVE_PRECISION),(int) (y1*FILLING_CURVE_PRECISION));
			long h2 = SpaceFillingCurves.hilbert2d((int) (x2*FILLING_CURVE_PRECISION),(int) (y2*FILLING_CURVE_PRECISION));
			return (h1<h2)?-1: ((h1==h2)?0:+1);
		}
	};

	/**
	 * A function that sorts the Points according to the peano value.
	 * This is needed for sort-based bulk insertion.
	 */
	static Comparator COMPARE_PEANO = new Comparator() {

		protected double delta_x = universe.getCorner(true).getValue(0) - universe.getCorner(false).getValue(0);
		protected double delta_y = universe.getCorner(true).getValue(1) - universe.getCorner(false).getValue(1);

		public int compare(Object point1, Object point2) {
			double x1 = (((Point)point1).getValue(0)-universe.getCorner(false).getValue(0))/delta_x;
			double y1 = (((Point)point1).getValue(1)-universe.getCorner(false).getValue(1))/delta_y;
			double x2 = (((Point)point2).getValue(0)-universe.getCorner(false).getValue(0))/delta_x;
			double y2 = (((Point)point2).getValue(1)-universe.getCorner(false).getValue(1))/delta_y;

			long h1 = SpaceFillingCurves.peano2d((int) (x1*FILLING_CURVE_PRECISION),(int) (y1*FILLING_CURVE_PRECISION));
			long h2 = SpaceFillingCurves.peano2d((int) (x2*FILLING_CURVE_PRECISION),(int) (y2*FILLING_CURVE_PRECISION));
			return (h1<h2)?-1: ((h1==h2)?0:+1);
		}
	};

	/**
	 * This method is used to set the properties for the application. 
	 *
	 * @param defaultProps default properties
	 * @param args user defined properties
	 * @return new properties for the application
	 */
	public static Properties argsToProperties (Properties defaultProps, String args[]) {
		Properties props = new Properties(defaultProps);

		for (int i = 0; i < args.length ; i++) {
			String prop, val = null;
			int indexEqual = args[i].indexOf('=');

			if (indexEqual > 0) {
				prop = args[i].substring(0, indexEqual);
				val = args[i].substring(indexEqual+1);
			}
			else {
				prop = args[i];
				val = ""; // != null !
			}
			prop = prop.toLowerCase();
			props.setProperty(prop,val);
		}
		return props;
	}

	/**
	 * The main method builds an M-Tree/Slim-Tree and executes various kinds of
	 * queries on it. 
	 * 
	 * @param args command line parameters
	 * @throws Exception
	 */
	public static void main (String [] args) throws Exception {

		System.out.println("MTreeTest: an example using xxl.core.indexStructures.MTree and xxl.core.indexStructures.SlimTree\n");

		// defining default properties
		Properties defaultProps = new Properties();
		defaultProps.setProperty("mincap", "10");
		defaultProps.setProperty("maxcap", "25");
		defaultProps.setProperty("inserttype", "tuple");
		defaultProps.setProperty("bufsize", "100");
		defaultProps.setProperty("level", "0");
		defaultProps.setProperty("splitmode", "hyperplane");

		Properties props = argsToProperties(defaultProps,args);

		// help information for usage
		if ((props.getProperty("help")!=null) || (props.getProperty("h")!=null) || (props.getProperty("/h")!=null) || (props.getProperty("/?")!=null)) {
			System.out.println("This applications can be called with the following parameters:");
			System.out.println();
			System.out.println("minCap=<val>          minimum capacity of nodes. Default: 10");
			System.out.println("maxCap=<val>          maximum capacity of nodes. Default: 25");
			System.out.println("insertType=<val>      type of insertion: tuple, bulk, bulk_xsort, bulk_peano, bulk_hilbert. Default: tuple");
			System.out.println("bufSize=<val>         buffersize (number of node-objects). Default: 100");
			System.out.println("level=<val>           number of level for queries. Default: 0");
			System.out.println("splitMode=<val>       type of split (only for M-Tree): hyperplane, balanced. Default: hyperplane");
			System.out.println("SlimTree              use SlimTree instead of MTree");
			return;
		}

		// evaluate properties
		int minCapacity = Integer.parseInt(props.getProperty("mincap"));
		int maxCapacity = Integer.parseInt(props.getProperty("maxcap"));
		String insertType = props.getProperty("inserttype");
		boolean bulk = insertType.toLowerCase().startsWith("bulk");
		Comparator compare = null;
		if (bulk) {
			if (insertType.equalsIgnoreCase("bulk_peano"))
				compare = COMPARE_PEANO;
			else if (insertType.equalsIgnoreCase("bulk_hilbert"))
				compare = COMPARE_HILBERT;
			else if (insertType.equalsIgnoreCase("bulk_xsort"))
				compare = COMPARE_X_AXIS;
		}
		int bufferSize = Integer.parseInt(props.getProperty("bufsize"));
		int targetLevel = Integer.parseInt(props.getProperty("level"));
		boolean balanced = false;
		String splitMode = props.getProperty("splitmode");
		if (splitMode.equalsIgnoreCase("balanced"))
			balanced = true;
		else
			splitMode = "hyperplane";
		boolean slimTree = (props.getProperty("slimtree")!=null);

		System.out.println("Parameter settings: \n");
		System.out.println("minCapacity = "+minCapacity);
		System.out.println("maxCapacity = "+maxCapacity);
		System.out.println("bulk = "+bulk);
		if (bulk)
			System.out.println("compareMode: " +
				((compare==COMPARE_X_AXIS)?"X-AXIS":
					((compare==COMPARE_PEANO)?"PEANO":
						((compare==null)?"given order":
							"HILBERT"
						)
					)
				)
			);
		System.out.println("bufferSize = "+bufferSize);
		System.out.println("targetLevel = "+targetLevel);
		if (!slimTree)
			System.out.println("splitMode = "+splitMode);
		else
			System.out.println("splitMode = minimum spanning tree");
		System.out.println("SlimTree = "+slimTree);
		System.out.println();

		// Show the universe (needed for Peano/Hilbert curve)
		System.out.println("Universe of input data (MBR):");
		System.out.println(universe);
		System.out.println();


		/*********************************************************************/
		/*                   BUILDING M-TREE or SLIM-TREE                    */
		/*********************************************************************/

		// generating a new instance of the class M-Tree or Slim-Tree
		final MTree mTree;
		if (slimTree) mTree = new SlimTree();
		else		  mTree = new MTree(balanced ? MTree.BALANCED_SPLIT : MTree.HYPERPLANE_SPLIT);

		// an unbuffered container that counts the access to the MTree
		CounterContainer lowerCounterContainer = new CounterContainer(
			new ConverterContainer(
				// leaf nodes are 16+8 Bytes of size.
				// index nodes are (16+8+8)+8 Bytes of size.
				// ==> so take the maximum for the block size!
				// additionally each node consumes further 4 bytes for
				// node number and 2 bytes for level information.
				new BlockFileContainer(Common.getOutPath()+"MTree", 2+4+40*maxCapacity),

				// actually dimension of inserted points is '2'
				mTree.nodeConverter(mTree.leafEntryConverter(centerConverter(2)), mTree.indexEntryConverter(descriptorConverter(2))) // define node converter
			)
		);

		// a buffered container that counts the access to the buffered MTree
		BufferedContainer bufferedContainer = new BufferedContainer(lowerCounterContainer, new LRUBuffer(bufferSize), true);
		CounterContainer upperCounterContainer = new CounterContainer(bufferedContainer);

		// the container that stores the content of the MTree
		Container container = upperCounterContainer;

		// initialize the MTree with the descriptor-factory method, a
		// container for storing the nodes and their minimum and maximum
		// capacity
		mTree.initialize(getDescriptor, container, minCapacity, maxCapacity);

		// accessing input from file and converting KPEs to DoublePoints
		Cursor cursor = new Mapper(LEAFENTRY_FACTORY,
			new KPEInputCursor(
				new File(Common.getDataPath()+FILENAME_TREE),
				4096, // buffer size
				2     // dimension of KPE's
			)
		);

		long t1,t2;
		t1 = System.currentTimeMillis();

		if (!bulk) {
			// inserting an iterator of objects by inserting every single object
			while (cursor.hasNext()) {
				Convertable c = (Convertable) cursor.next();
				mTree.insert(c);
			}
		}
		else {
			// or by bulk-insertion
			if (compare != null) cursor = new MergeSorter(cursor, compare, 12, 4*4096, 4*4096);
			new SortBasedBulkLoading(mTree, cursor, new Constant(container));
		}
		cursor.close();
		
		t2 = System.currentTimeMillis();

		System.out.println("Time for insertion: "+(t2-t1)+" ms.");
		System.out.println("Insertion complete, height: "+mTree.height()+", universe: ");
		System.out.println(mTree.rootDescriptor());
		System.out.println("\nAccessing the BufferedContainer: ");
		System.out.println(upperCounterContainer);
		System.out.println("\nAccessing the ConverterContainer and the BlockFileContainer: ");
		System.out.println(lowerCounterContainer);

		System.out.println("\nReset counters.");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();

		System.out.println("Flushing buffers.");
		bufferedContainer.flush();
		System.out.println("\nAccessing the BufferedContainer: ");
		System.out.println(upperCounterContainer);
		System.out.println("\nAccessing the ConverterContainer and the BlockFileContainer: ");
		System.out.println(lowerCounterContainer);

		System.out.print("\nChecking descriptors... ");
		mTree.checkDescriptors();
		System.out.println("done.\n");


		/*********************************************************************/
		/*                        EXACT MATCH QUERY                          */
		/*********************************************************************/

		// accessing input from file
		Cursor input = new KPEInputCursor(
			new File(Common.getDataPath()+FILENAME_QUERIES),
			4096, // buffer size
			2     // dimension of KPE's
		);

		// preparing to consume 1000 elements applying the mapping function
		// (KPE ==> DoublePoint) to each of them
		cursor = new Taker(
			new Mapper(LEAFENTRY_FACTORY,input),
			1000
		);

		int hits = 0;

		System.out.println("\nPerforming 1000 exact match queries against the M-tree: ");

		t1 = System.currentTimeMillis();
		while (cursor.hasNext()) {
			hits += Cursors.count(
				// querying M-Tree using query(descriptor)
				// targetLevel is 0 due to exact match queries can only
				// be performed on leaf nodes.
				mTree.query(cursor.next())
			);
		}
		t2 = System.currentTimeMillis();

		System.out.println("Time for queries: "+(t2-t1) +" ms.");
		System.out.println("Number of hits: "+hits);
		System.out.println("\nAccessing the BufferedContainer: ");
		System.out.println(upperCounterContainer);
		System.out.println("\nAccessing the ConverterContainer and the BlockFileContainer: ");
		System.out.println(lowerCounterContainer);

		System.out.println("\nReset counters.");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();


		/*********************************************************************/
		/*                             RANGE QUERY                           */
		/*********************************************************************/

		// preparing to consume further 1000 elements applying the mapping function
		// (KPE ==> Sphere) to each of them
		cursor = new Taker(
			new Mapper(SPHERE_COVERING_RECTANGLE_FACTORY,input),
			1000
		);

		hits = 0;
		System.out.println("\nPerforming 1000 range queries against the M-tree: ");

		t1 = System.currentTimeMillis();
		while (cursor.hasNext()) {
			hits += Cursors.count(
				// querying M-Tree using query(descriptor, targetLevel)
				mTree.query((Sphere)cursor.next(), targetLevel)
			);
		}
		t2 = System.currentTimeMillis();

		System.out.println("Time for queries: "+(t2-t1) +" ms.");
		System.out.println("Number of hits: "+hits);
		System.out.println("\nAccessing the BufferedContainer: ");
		System.out.println(upperCounterContainer);
		System.out.println("\nAccessing the ConverterContainer and the BlockFileContainer: ");
		System.out.println(lowerCounterContainer+"\n");

		System.out.println("\nReset counters.");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();


		/*********************************************************************/
		/*                     NEAREST NEIGHBOR QUERY                        */
		/*********************************************************************/

		hits = 0;
		System.out.println("Performing a nearest neighbor query against the M-tree \n"
						 + "determining the 50 nearest neighbor entries at target level \n"
						 + "concerning the input iterator's next element in ascending order: ");

		// consuming one further input element applying the mapping function
		// (KPE ==> Sphere) to it
		Sphere queryObject = (Sphere)SPHERE_COVERING_RECTANGLE_FACTORY.invoke(input.next());

		System.out.println("\nQuery object: " +queryObject);

		t1 = System.currentTimeMillis();

		// consuming the fifty nearest elements concerning the query object at the
		// the target level;
		// the order is determined by the comparator given to the dynamic heap
		// structure realizing a priority queue
		cursor = new Taker(
				mTree.query(new DynamicHeap(getDistanceBasedComparator(queryObject)), targetLevel),
				50
		);
		int counter = 0;
		while (cursor.hasNext()) {
			Sphere next = (Sphere)((Candidate)cursor.next()).descriptor();
			System.out.println("candidate no. "+(++counter)+": "+next
							 + "\t distance to query object: "+queryObject.centerDistance(next));
		}
		cursor.close();
		t2 = System.currentTimeMillis();

		System.out.println("\nTime for query: "+(t2-t1) +" ms.");
		System.out.println("\nAccessing the BufferedContainer: ");
		System.out.println(upperCounterContainer);
		System.out.println("\nAccessing the ConverterContainer and the BlockFileContainer: ");
		System.out.println(lowerCounterContainer);

		System.out.println("\nReset counters.");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();


		/*********************************************************************/
		/*                   RANGE QUERY WITH ROOT DESCRIPTOR                */
		/*********************************************************************/

		System.out.println("\nQuerying root descriptor: ");
		// range query with root descriptor should return all entries of the
		// M-Tree at the specified target level
		hits = Cursors.count(mTree.query(mTree.rootDescriptor(), targetLevel));
		System.out.println("Number of results: "+hits);

		/*********************************************************************/
		/*                  ADDITIONAL CHECKS FOR CONSISTENCE                */
		/*********************************************************************/

		System.out.print("\nChecking descriptors... ");
		mTree.checkDescriptors();

		// check if all 'distance to parent'-attributes are correct
		System.out.println("\nChecking 'distance to parent' entries... ");
		mTree.checkDistanceToParent();

		// verify number of node entries
		System.out.println("\nVerifying number of node entries within range [minCapcity, maxCapacity]... ");
		mTree.checkNumberOfEntries();

		/*********************************************************************/
		/*                       REMOVING ALL ELEMENTS                       */
		/*********************************************************************/

		System.out.println("\nReset counters.");
		upperCounterContainer.reset();
		lowerCounterContainer.reset();

		// get an iterator over all input elements again
		cursor = new Mapper(LEAFENTRY_FACTORY,
			new KPEInputCursor(
				new File(Common.getDataPath()+FILENAME_TREE),
				4096, // buffer size
				2     // dimension of KPE's
			)
		);

		System.out.println("\nRemoving all elements of the M-tree. ");
		t1 = System.currentTimeMillis();
		while (cursor.hasNext())
			mTree.remove(cursor.next()); // remove next element
		cursor.close();
		t2 = System.currentTimeMillis();

		System.out.println("\nTime for searching and removal of all elements: "+(t2-t1) +" ms.");
		System.out.println("\nAccessing the BufferedContainer: ");
		System.out.println(upperCounterContainer);
		System.out.println("\nAccessing the ConverterContainer and the BlockFileContainer: ");
		System.out.println(lowerCounterContainer);
		System.out.println("\nRemaining elements in M-Tree: "+Cursors.count(mTree.query(mTree.rootDescriptor(), targetLevel)));

		System.out.println("Closing application.");
		container.close();
	}
}
