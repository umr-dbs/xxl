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

import java.awt.Color;
import java.util.Random;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.RTree;
import xxl.core.indexStructures.ShowCursor;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.RTree.AsymmetricTwoDimensionalSkylineQuery;
import xxl.core.indexStructures.RTree.SymmetricSkylineQuery;
import xxl.core.indexStructures.Tree.Query;
import xxl.core.indexStructures.Tree.Query.Candidate;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;

public class RTreeSkyLineQueryTest extends SimpleRTreeTest {
	
	/**
	 * Starts the SimpleRTreeTest. 
	 * 
	 * @param args command line parameter: Path to RTree file
	 * @throws Exception
	 */
	public static void main (String args[]) throws Exception {
		// check for command line argument
		if (args.length!=1) 
			System.out.println("usage: java RTreeSkyLineQueryTest filename");
		// size of a data entry = size of DoublePoint (1 double per dimension)
		int dataSize = dimension*8;
		// size of a decriptor = size of DoublePointRectangle (2 doubles per dimension)
		int descriptorSize = dimension*2*8; 
										
		System.out.println("Building new RTree");
		RTree rtree = new RTree();
		// determine Converters and Containers 
		Container fileContainer = new BlockFileContainer(args[0], blockSize);
		Converter converter = rtree.nodeConverter(new ConvertableConverter(LEAFENTRY_FACTORY), dimension);
		ConverterContainer converterContainer = new ConverterContainer(fileContainer, converter);
		// use buffer
		BufferedContainer bufferedContainer = new BufferedContainer(converterContainer, new LRUBuffer(bufferSize), true);				
		// initialize RTree
		rtree.initialize(null, GET_DESCRIPTOR, bufferedContainer, blockSize, dataSize, descriptorSize, minMaxFactor);

		/*********************************************************************/
		/*                         INSERT RANDOM DATA                        */
		/*********************************************************************/
		
		Random random = new Random(2);
		int numberOfElements = 100000;
		for (int j=0; j<numberOfElements; j++) {				
			// create random coordinates
			double [] point = new double[dimension];
			for (int i=0; i<dimension; i++) 
				point[i] = random.nextDouble();
			// insert new point
			rtree.insert(new DoublePoint(point));
			
			if (j%(numberOfElements/10)==0)
				System.out.print((j*100/numberOfElements)+"%, ");
		}
		System.out.println("100%");
		// flush buffers
		bufferedContainer.flush();
		
		/*********************************************************************/
		/*                 RANGE QUERY SHOWING THE WHOLE TREE                */
		/*********************************************************************/

		// perform query
		Cursor results = rtree.query();
		// show results
		DoublePointRectangle universe = new DoublePointRectangle(new double[]{0,0}, new double[]{1,1});
		ShowCursor rc = new ShowCursor(ShowCursor.DOUBLE_POINT_MODE, results, universe, 0);
		rc.createFrame(800, 800);
		rc.open();
		while (rc.hasNext())
			rc.next();
		rc.close();
		
		/*********************************************************************/
		/*                         SKYLINE QUERYS                            */
		/*********************************************************************/

		// create query point
		double [] point = new double[dimension];
		for (int i=0; i<dimension; i++) 
			point[i] = 0.5;			
		DoublePoint queryObject = new DoublePoint(point);
		rc.mark(queryObject, Color.red);
		rc.drawQueryPoint(queryObject);

		for (int symmetric=0; symmetric<2; symmetric++) {
			// lazy Iterator of all skyline points
			if (symmetric==0)
				results = rtree.new AsymmetricTwoDimensionalSkylineQuery(new DynamicHeap(getDistanceBasedComparator(queryObject)), queryObject);
			else 
				results = rtree.new SymmetricSkylineQuery(new DynamicHeap(getDistanceBasedComparator(queryObject)), queryObject);
			// show results
			int counter = 0;
			System.out.println("\nResults for "+(symmetric==0?"a":"")+"symmetric skyline query ("+queryObject+")");
			while (results.hasNext()) {
				DoublePoint next = (DoublePoint)((Candidate)results.next()).entry();
				System.out.println("candidate no. "+(++counter)+": "+next);
				rc.mark(next, symmetric==0 ? Color.blue : Color.green);
			}
			System.out.println("\nTotal of "+counter+" skyline points found\n");
			rc.flush();
			results.close();
		}
		
		// close container
		bufferedContainer.flush();
		bufferedContainer.close();			
		
	}
}
