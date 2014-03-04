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

import java.util.Random;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.ORTree;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.spatial.KPE;
import xxl.core.spatial.rectangles.DoublePointRectangle;


public class SimpleKPERTreeTest {
	
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
	 * Starts the SimpleRTreeTest. 
	 * 
	 * @throws Exception
	 */
	public static void main (String args[]) throws Exception {
		// size of a data entry = (2 doubles per dimension) + int
		int dataSize = dimension*2*8+4; 
		// size of a decriptor = size of DoublePointRectangle (2 doubles per dimension)
		int descriptorSize = dimension*2*8; 
										
		RTree rtree = new RTree();
		ORTree.IndexEntry rootEntry = null; 
		Container fileContainer;
		fileContainer = new BlockFileContainer("C:\\temp\\testTree", blockSize);
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
		
		Random random = new Random(42);
		for (int j=0; j<1000; j++) {				
			// create random coordinates
			double [] left = new double[dimension];
			double [] right = new double[dimension];
			for (int i=0; i<dimension; i++) { 
				left[i] = random.nextDouble();
				right[i] = left[i]+1;
			}
			DoublePointRectangle r = new DoublePointRectangle(left, right);
			// insert new KPE
			KPE e = new KPE(r, new Integer(j), IntegerConverter.DEFAULT_INSTANCE);
			rtree.insert(e);
		}
		// flush buffers
		bufferedContainer.flush();
		
		/*********************************************************************/
		/*                             RANGE QUERY                           */
		/*********************************************************************/

		// create query window
		double [] leftCorner = new double[dimension];
		double [] rightCorner = new double[dimension];
		for (int i=0; i<dimension; i++) {
			leftCorner[i] = 0.01;
			rightCorner[i] = 0.05;
		}
		DoublePointRectangle queryRange = new DoublePointRectangle(leftCorner, rightCorner);		
		// perform query
		Cursor results = rtree.query(queryRange, 0);
		// show results
		int counter = 0;
		System.out.println("Results for range query ("+queryRange+")");
		while (results.hasNext()) {
			KPE next = (KPE)results.next();
			System.out.println("result no. "+(++counter)+": "+next);
		}
		results.close();
						
		// close container
		bufferedContainer.flush();
		bufferedContainer.close();				
	}
}
