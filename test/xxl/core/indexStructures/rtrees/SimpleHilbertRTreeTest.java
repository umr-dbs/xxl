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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Taker;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree;
import xxl.core.indexStructures.Descriptor;
import xxl.core.indexStructures.HilbertRTree;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.HilbertRTree.ORKeyRange;
import xxl.core.indexStructures.HilbertRTree.ORSeparator;
import xxl.core.indexStructures.Tree.Query;
import xxl.core.indexStructures.Tree.Query.Candidate;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.MeasuredFixedSizeConverter;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;
/**
 * Simple Test of the HilbertRTree with random DoublePoint ({@link DoublePoint})
 */
public class SimpleHilbertRTreeTest {
	/**MinMaxFactor*/
	static public double minMaxFactor = 1d/3d; 
	/**Dimension */
	static public final int dimension = 2;
	/**Block size */
	static public final int blockSize = 1536;
	/**Buffer size*/
	static public final int bufferSize = 20;
	/**Precision of the Hilbert Space filling curve*/
	static public final int FILLING_CURVE_PRECISION = 1<<20;
	/** Universe of the data space 
	 * ( DoublePoints uniformly distributed in rectangle with xmin = 0 , ymin = 0, xmax = 1 , ymax = 1)
	 */
	static public final DoublePointRectangle universe = new DoublePointRectangle(new double[]{0,0},
			new double[]{1.0,1.0});
	/**
	 * Converter for the keys of the data. Keys are the java.long values(hilbert values of the MBRs middle points).   
	 */
	static public MeasuredConverter keyConverter = new MeasuredFixedSizeConverter<Long>(LongConverter.DEFAULT_INSTANCE);
	
	/**
	 * Measured converter for DoublePoints 
	 */
	static public MeasuredConverter dataConverter = new MeasuredConverter(){
		public int getMaxObjectSize() {
			return dimension * 8;
		}
		public Object read(DataInput dataInput, Object object) throws IOException {
			return  ConvertableConverter.DEFAULT_INSTANCE.read(dataInput, new DoublePoint(dimension));
		}
		public void write(DataOutput dataOutput, Object object) throws IOException {
			ConvertableConverter.DEFAULT_INSTANCE.write(dataOutput, (DoublePoint)object);
		}
	};
	/** 
	 * Function creating a MBR ({@link DoublePointRectangle})  for a given doublePoint object . 
	 */
	static Function getEntryMBR  = new AbstractFunction() {
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
	public static Comparator getDistanceBasedComparator (DoublePoint queryObject) {
		final Rectangle query = new DoublePointRectangle(queryObject, queryObject);
		return new Comparator () {
			public int compare (Object candidate1, Object candidate2) {
				Rectangle r1 =( (HilbertRTree.ORSeparator) ((Candidate) candidate1).descriptor() ).getIndexEntryMBR();
				Rectangle r2 = ( (HilbertRTree.ORSeparator) ((Candidate) candidate2).descriptor() ).getIndexEntryMBR();				
				double d1 = query.distance(r1, 2);
				double d2 = query.distance(r2, 2);
				return (d1<d2) ? -1 : ( (d1==d2) ? 0 : 1 );
			}
		};
	}
	
	/**
	 * This function computes hilbert curve value of the MBRs middle point
	 */
	static public Function getHilbertValue = new AbstractFunction(){
		public Object invoke(Object point){
			if (point instanceof DoublePoint){
				DoublePoint middlePoint = (DoublePoint)point;
				double x = middlePoint.getValue(0);
				double y = middlePoint.getValue(1);
				return new Long(SpaceFillingCurves.hilbert2d((int) (x*FILLING_CURVE_PRECISION),(int) (y*FILLING_CURVE_PRECISION)));
			}
			throw new IllegalArgumentException();
		}
	};
	/**
	 * Factory function that creates ORSeparator of the HilbertRTree 
	 */
	public static final Function createORSeparator = 
		new AbstractFunction() {
			public Object invoke(Object key, Object mbr) {
				return new DoublePointRectangleSep((Long)key, (DoublePointRectangle)mbr);
			}
	};
	/**
	 *  Factory function that creates ORKeyRange of the HilbertRTree 
	 */
	public static final Function createORKeyRange = 
		new AbstractFunction() {
		public Object invoke(List arguments) {
			if(arguments.size() !=3 ) throw new IllegalArgumentException();
			Long min = (Long)arguments.get(0);
			Long max = (Long)arguments.get(1);
			DoublePointRectangle entryMBR =  (DoublePointRectangle)arguments.get(2);
			return new LongRange(min, max, entryMBR);	
		}
	};
	/**
	 * This class represents the ORSeparator of the HilbertRTree
	 * 
	 *
	 */
	public static class DoublePointRectangleSep extends HilbertRTree.ORSeparator{	
		public DoublePointRectangleSep(Long separatorValue, DoublePointRectangle entryMBR) {
			super(separatorValue, entryMBR);
		}
		@Override
		public Object clone() {
			return new DoublePointRectangleSep(((Long)this.sepValue()).longValue(),
					(DoublePointRectangle) ((Descriptor) this.entryMBR).clone()) ;
		}
	}
	/**
	 * This class represents the ORKeyRange of the HilbertRTree
	 * 
	 *
	 */
	public static class LongRange extends HilbertRTree.ORKeyRange{
		public LongRange(Long min, Long max, DoublePointRectangle entryMBR) {
			super(min, max, entryMBR);
		}

		@Override
		public Object clone() {
			return new LongRange(((Long)this.sepValue).longValue(), ((Long)this.maxBound).longValue(),
					(DoublePointRectangle) ((Descriptor) this.entryMBR).clone());
		}
	}
	/**
	 * This method saves the parameters of the HilbertRTree, this allows to make the tree persistent
	 * @param tree HilbertRTree
	 * @param pfad
	 * @throws IOException
	 */
	public static void saveParams(HilbertRTree tree, String pfad, Converter keyKonverter) throws IOException{
		Tree.IndexEntry root = tree.rootEntry();
		HilbertRTree.ORKeyRange desc = (HilbertRTree.ORKeyRange)tree.rootDescriptor();
		int height =  tree.height();
		Long id = (Long)root.id();
		Rectangle rec = (Rectangle)desc.getIndexEntryMBR();
		Object minBound = desc.minBound();
		Object maxBound = desc.maxBound();
		DataOutputStream outPut = new DataOutputStream(new FileOutputStream(new File(pfad)));
		LongConverter.DEFAULT_INSTANCE.write(outPut, id);
		IntegerConverter.DEFAULT_INSTANCE.writeInt(outPut, height);
		keyKonverter.write(outPut, minBound);
		keyKonverter.write(outPut, maxBound);
		ConvertableConverter.DEFAULT_INSTANCE.write(outPut, (DoublePointRectangle)rec);
		outPut.close();
		((Container)tree.getContainer.invoke()).flush();
		((Container)tree.getContainer.invoke()).close();
	} 
	/**
	 * Starts simpleHilbertRTree Test 
	 * @param args path To HilbertRTree
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// check for command line argument
		if (args.length!=1) 
			System.out.println("usage: java SimpleRTreeTest filename");
		// test if RTree exists
		boolean reopen = (new File(args[0]+".ctr")).canRead();		
		HilbertRTree tree = new HilbertRTree(blockSize, universe, minMaxFactor);
		Container fileContainer = null;
		BufferedContainer treeContainer = null;
		/*********************************************************************/
		/*                       INIT HILBERT TREE                           */
		/*********************************************************************/
		String treePath = args[0];
		if (!reopen){
			System.out.println("Init new HilbertRTree");
			fileContainer = new CounterContainer( 
					new BlockFileContainer(treePath, blockSize)); 
			treeContainer = new   BufferedContainer(new ConverterContainer
					(fileContainer,   tree.nodeConverter()),
					new LRUBuffer(bufferSize), false);
			tree.initialize(getHilbertValue , getEntryMBR, 
					 	treeContainer, 
						keyConverter, 
						dataConverter,
						createORSeparator, 
						createORKeyRange);
		} else{
			System.out.println("Load and Init persistent HilbertRTree");
			fileContainer = new CounterContainer( 
					new BlockFileContainer(treePath)); 
			treeContainer = new   BufferedContainer(new ConverterContainer
					( fileContainer,   tree.nodeConverter()),
					new LRUBuffer(bufferSize), false);
			File parameters = new File(treePath+"_params.dat");
			DataInputStream dos = new DataInputStream(new FileInputStream(parameters));
			long rootsPageId = LongConverter.DEFAULT_INSTANCE.read(dos);
			int height = IntegerConverter.DEFAULT_INSTANCE.read(dos);
			Long minBound = (Long)keyConverter.read(dos);
			Long maxBound = (Long)keyConverter.read(dos);
			DoublePointRectangle mbr = (DoublePointRectangle)
				ConvertableConverter.DEFAULT_INSTANCE.read(dos, new DoublePointRectangle(dimension));
			BPlusTree.IndexEntry rootEntry = ((BPlusTree.IndexEntry)tree.createIndexEntry(height)).initialize
					(rootsPageId, new DoublePointRectangleSep(minBound, mbr));
			tree.initialize(rootEntry, new LongRange(minBound, maxBound, mbr),
					getHilbertValue, getEntryMBR, treeContainer,
					keyConverter, dataConverter,
					createORSeparator, createORKeyRange);
		}
		/*********************************************************************/
		/*                         INSERT RANDOM DATA                        */
		/*********************************************************************/
		if (!reopen){
		System.out.println("Insert random data: ");
		Random random = new Random(42);
		int dataNumber = 100000;
		for (int j=0; j<dataNumber; j++) {				
			// create random coordinates
			double [] point = new double[dimension];
			for (int i=0; i<dimension; i++) 
				point[i] = random.nextDouble();
			// insert new point
			tree.insert(new DoublePoint(point));
			if (j % (dataNumber/10)==0)
				System.out.print((j * 100)/dataNumber+"%, ");
		}
			System.out.print("100%\n");
		}
		/*********************************************************************/
		/*                             RANGE QUERY                           */
		/*********************************************************************/
		// create query window
		System.out.println("Range Queries");
		double [] leftCorner = new double[dimension];
		double [] rightCorner = new double[dimension];
		for (int i=0; i<dimension; i++) {
			leftCorner[i] = 0.4975;
			rightCorner[i] = 0.5025;
		}
		DoublePointRectangle queryRange = new DoublePointRectangle(leftCorner, rightCorner);		
		// perform  query
		// 
		Cursor results = tree.queryOR(queryRange, 0);
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
		System.out.println("kNN-Query");
		// create query point
		int resultsNumber = 10;
		double [] point = new double[dimension];
		for (int i=0; i<dimension; i++) 
			point[i] = 0.5;			
		DoublePoint queryObject = new DoublePoint(point);		
		// lazy Iterator of all nearest neighbors
		Iterator nnIterator = tree.query(new DynamicHeap(getDistanceBasedComparator(queryObject)), 0); 		
		results = new Taker(nnIterator,resultsNumber );
		// show results
		counter = 0;
		System.out.println("\nResults for nearest neighbor query ("+queryObject+")");
		while (results.hasNext()) {
			DoublePoint next = (DoublePoint)((Candidate)results.next()).entry();
			System.out.println("candidate no. "+(++counter)+": "+next+" (distance="+next.distanceTo(queryObject)+")");
		}
		results.close();
		/*********************************************************************/
		/*                     Save HilbertRTree		                     */
		/*********************************************************************/
		if (!reopen){
			saveParams(tree, args[0]+"_params.dat", keyConverter );
		}
		
		
	}

}
