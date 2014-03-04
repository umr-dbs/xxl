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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Taker;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.indexStructures.Tree.Query;
import xxl.core.indexStructures.Tree.Query.Candidate;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;


public class SimpleRTreeTest {
	
	/** Dimension of the data. 
	 */
	public static final int dimension = 2;
			
	/** Size of a block in bytes.
	 */
	public static int blockSize = 1536;
			
	/** Factor which the minimum capacity of nodes is smaller than the maximum capacity.
	 */
	public static double minMaxFactor = 1.0/3.0;
			
	/** Buffersize (number of node-objects).
	 */
	public static int bufferSize = 100;
											
	/**
	 * Factory Function to get a leaf entry.
	 */
	static Function<Object, DoublePoint> LEAFENTRY_FACTORY = new AbstractFunction<Object, DoublePoint>() {
		public DoublePoint invoke() {
			return new DoublePoint(dimension);
		}
	};
	
	/** 
	 * Function creating a descriptor for a given object. 
	 */
	static Function<Object,Object> GET_DESCRIPTOR = new AbstractFunction<Object,Object>() {
		@Override
		public Object invoke (Object o) {
			DoublePoint p = (DoublePoint)o;
			return new DoublePointRectangle(p, p); 
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
	public static Comparator<Object> getDistanceBasedComparator (DoublePoint queryObject) {
		final Rectangle query = new DoublePointRectangle(queryObject, queryObject);
		return new Comparator<Object> () {
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
	 * Starts the SimpleRTreeTest. 
	 * 
	 * @param args command line parameter: Path to RTree file
	 * @throws Exception
	 */
	public static void main (String args[]) throws Exception {
		// check for command line argument
		if (args.length!=1) 
			System.out.println("usage: java SimpleRTreeTest filename");
		// test if RTree exists
		boolean reopen = (new File(args[0]+".ctr")).canRead();		
		// size of a data entry = size of DoublePoint (1 double per dimension)
		int dataSize = dimension*8;
		// size of a decriptor = size of DoublePointRectangle (2 doubles per dimension)
		int descriptorSize = dimension*2*8; 
										
		RTree rtree = new RTree();
		ORTree.IndexEntry rootEntry = null; 
		Container fileContainer;
		if (!reopen) {
			System.out.println("Building new RTree");
			fileContainer = new BlockFileContainer(args[0], blockSize);
		}
		else {
			System.out.println("Using existing RTree");
			fileContainer = new BlockFileContainer(args[0]);
			File parameters = new File(args[0]+"_params.dat");
			DataInputStream dos = new DataInputStream(new FileInputStream(parameters));
			int height = IntegerConverter.DEFAULT_INSTANCE.read(dos);
			long rootPageId = LongConverter.DEFAULT_INSTANCE.read(dos);						
			DoublePointRectangle rootDescriptor = (DoublePointRectangle) ConvertableConverter.DEFAULT_INSTANCE.read(dos, new DoublePointRectangle(dimension));
			rootEntry = (ORTree.IndexEntry) ((ORTree.IndexEntry)rtree.createIndexEntry(height)).initialize(rootDescriptor).initialize(rootPageId);
		}
		// determine Converters and Containers 
		Converter converter = rtree.nodeConverter(new ConvertableConverter(LEAFENTRY_FACTORY), dimension);
		ConverterContainer converterContainer = new ConverterContainer(fileContainer, converter);
		// use buffer
		BufferedContainer bufferedContainer = new BufferedContainer(converterContainer, new LRUBuffer(bufferSize), true);				
		// initialize RTree
		rtree.initialize(rootEntry, GET_DESCRIPTOR, bufferedContainer, blockSize, dataSize, descriptorSize, minMaxFactor);

		/*********************************************************************/
		/*                         INSERT RANDOM DATA                        */
		/*********************************************************************/
		
		if (!reopen) {
			Random random = new Random(42);
			for (int j=0; j<100000; j++) {				
				// create random coordinates
				double [] point = new double[dimension];
				for (int i=0; i<dimension; i++) 
					point[i] = random.nextDouble();
				// insert new point
				rtree.insert(new DoublePoint(point));
				
				if (j%10000==0)
					System.out.print(j/1000+"%, ");
			}
			System.out.println("100%");
			// flush buffers
			bufferedContainer.flush();
		}				
		
		/*********************************************************************/
		/*                             RANGE QUERY                           */
		/*********************************************************************/

		// create query window
		double [] leftCorner = new double[dimension];
		double [] rightCorner = new double[dimension];
		for (int i=0; i<dimension; i++) {
			leftCorner[i] = 0.4975;
			rightCorner[i] = 0.5025;
		}
		DoublePointRectangle queryRange = new DoublePointRectangle(leftCorner, rightCorner);		
		// perform query
		Cursor results = rtree.query(queryRange, 0);
		// show results
		int counter = 0;
		System.out.println("Results for range query ("+queryRange+")");
		while (results.hasNext()) {
			DoublePoint next = (DoublePoint)results.next();
			System.out.println("result no. "+(++counter)+": "+next);
		}
		results.close();
				
		/*********************************************************************/
		/*                     NEAREST NEIGHBOR QUERY                        */
		/*********************************************************************/

		// create query point
		double [] point = new double[dimension];
		for (int i=0; i<dimension; i++) 
			point[i] = 0.5;			
		DoublePoint queryObject = new DoublePoint(point);		
		// lazy Iterator of all nearest neighbors
		Iterator neighbors = rtree.query(new DynamicHeap(getDistanceBasedComparator(queryObject)), 0); 		
		// compute only 5 nearest neigbors
		results = new Taker(neighbors, 5);
		// show results
		counter = 0;
		System.out.println("\nResults for nearest neighbor query ("+queryObject+")");
		while (results.hasNext()) {
			DoublePoint next = (DoublePoint)((Candidate)results.next()).entry();
			System.out.println("candidate no. "+(++counter)+": "+next+" (distance="+next.distanceTo(queryObject)+")");
		}
		results.close();

		// save parameters of RTree
		if (!reopen) {
			File parameters = new File(args[0]+"_params.dat");
			DataOutputStream dos = new DataOutputStream(new FileOutputStream(parameters));
			IntegerConverter.DEFAULT_INSTANCE.write(dos, rtree.height());
			LongConverter.DEFAULT_INSTANCE.write(dos, (Long)rtree.rootEntry().id());						
			ConvertableConverter.DEFAULT_INSTANCE.write(dos, (DoublePointRectangle) rtree.rootDescriptor());
		}
		
		// close container
		bufferedContainer.flush();
		bufferedContainer.close();				
	}
}
