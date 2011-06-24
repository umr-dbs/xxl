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

package xxl.core.spatial.cursors;

import java.util.Iterator;

import xxl.core.collections.queues.Queue;
import xxl.core.collections.queues.Queues;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.sorters.MergeSorter;
import xxl.core.cursors.unions.Merger;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.cursors.wrappers.QueueCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.KPEzCode;
import xxl.core.spatial.points.FloatPoint;
import xxl.core.util.BitSet;

/**
 *	This class provides the <bold>MSJ</bold> (Multidimensional Spatial Join) similarity-join algorithm
 *	proposed by Koudas and Sevick in "[KS 98] Nick Koudas, Kenneth C.
 *	Sevcik: High Dimensional Similarity Joins: Algorithms and Performance
 *	Evaluation. ICDE 1998: 466-475" which is the multi-dimensional
 *	extension of <bold>S3J</bold> (Size Separation Spatial Join) proposed by the same
 *	authors in "Nick Koudas, Kenneth C. Sevcik: Size Separation Spatial Join.
 *	SIGMOD Conference 1997: 324-335". (This implementation corresponds to 
 *	the S3J-algorithm if you set the dimensionality to 2).
 *	<br><br>
 *	MSJ/S3J performs similar to Orenstein's algorithm with two main differences: First,
 *	replication is not allowed and second, an I/O strategy based on
 *	so-called <it>level-files</it> is employed. Moreover, an n-ary recursive
 *	partitioning is used where n = 2^d (quadtree-partitioning).
 *
 *	<br><br>
 *	The algorithm starts by partitioning the hypercubes of the input
 *	relations into level-files according to their levels. Hence, a 
 *	hypercube of level l is kept in the l-th level-file. Then, the level-files are sorted w.r.t.
 *	the code of the hypercubes. Finally, the Merge algorithm of
 *	Orenstein is called.
 *
 *	<br><br>
 *	Deficiencies of this method for high-dimensional intersection
 *	joins are that a high fraction of the input relation will be in 
 *	level 0. The hypercubes in level 0, however, need to  
 *	be tested against the entire input relation in a nested-loop
 *	manner. Moreover, [Dittrich and Seeger, ICDE 2000]  showed for two dimensions that a modest rate of
 *	replication considerably speeds up the overall execution time of MSJ.
 *
 *	<br><br>
 *	(See "[DS 01] GESS: a Scalable Algorithm for Mining Large Datasets in High Dimensional Spaces"
 *        by Jens-Peter Dittrich and Bernhard Seeger, ACM SIGKDD 2001. pages 47-56. for a
 *	review of MSJ).
 *	<br><br>
 *
 *	@see xxl.core.spatial.cursors.Orenstein
 *	@see xxl.core.spatial.cursors.GESS
 *  @see xxl.core.spatial.cursors.Mappers
 */
public class MSJ extends Orenstein {
	
	/** This class provides the I/O-strategy of MSJ
	 */
	public static class MSJSorter extends AbstractCursor {

		/**
		 * The input iteration holding the data to be sorted.
		 */
		private Cursor cursor;
		
		/**
		 * Sorts the input data for the multidimensional spatial join.
		 * @param input the input iterator
		 * @param maxLevel the maximum level of the grid
		 * @param dim the dimension of the objects
		 * @param newQueue a functional factory for generating queues
		 * @param mem the available size in main memory
		 */
		public MSJSorter(Iterator input, int maxLevel, final int dim, Function newQueue, final int mem){
			Queue[] queues = new Queue[maxLevel+1];
			int objectSize = 0;
			try{
				objectSize = xxl.core.util.XXLSystem.getObjectSize(new KPEzCode(new FloatPoint(dim), new BitSet(32)));
			}	
			catch (Exception e){System.out.println(e);}

			Function inputBufferSize =  new Constant((int)(0.2*mem));		//determine buffer-size for reading data from disk
			Function outputBufferSize = new Constant(mem/queues.length);		//determine buffer-size for writing data to disk

			for(int i=0; i<=maxLevel; i++)						//initialize output-queues
				(queues[i] = (Queue) newQueue.invoke( inputBufferSize, outputBufferSize )).open();

			while(input.hasNext()){							//write data into "level-files" (see paper of Koudas and Sevcik!)
				KPEzCode next = (KPEzCode) input.next();
				int level = Math.min(next.getzCode().precision()/dim,maxLevel);	//determine queue
				queues[ level ].enqueue(next);					//insert object into queue
			}

			Iterator[] iterators = new Iterator[maxLevel+1];

			for(int i=0; i<=maxLevel; i++){						//for each level-file: sort w.r.t. space-filling curve
				Queue tmp  = (Queue) newQueue.invoke( inputBufferSize, outputBufferSize );	//get new Queue for this level-file
				tmp.open();
				//sort level-file and materialize result into queue
				Queues.enqueueAll(
					tmp, 
					new MergeSorter(
						new QueueCursor(queues[i]),
						new ComparableComparator(),
						objectSize,
						(int)(mem*0.8),
						mem/(maxLevel+1),
						newQueue,
						false
					)
				);
				iterators[i] = new QueueCursor(tmp);
			}

			cursor = new Merger(new ComparableComparator(), iterators);						//merge sorted streams
		}
		/**
		 * @return true if there is another object available
		 */
		public boolean hasNextObject() {
			return cursor.hasNext();
		}
		
		/**
		 * @return the next object
		 */
		public Object nextObject() {
			return cursor.next();
		}
	}

	
	 
	/**
	 * The top-level constructor for MSJ
	 * @param input0 ths first input
	 * @param input1 the second input
	 * @param predicate the join predicate
	 * @param newResult a function for mapping the output to an object
	 * @param initialCapacity the initial capacity of a bucket
	 * @param maxLevel the maximum level of the grid
	 * @param dim the dimension of the objects
	 * @param newQueue a functional factory for creating queues
	 * @param mem the available main memory
	 */
	public MSJ(Cursor input0, Cursor input1, Predicate predicate, Function newResult, final int initialCapacity, final int maxLevel, final int dim, final Function newQueue, final int mem){
		super(
			input0,
			input1,
			predicate,
			new AbstractFunction(){
        public Object invoke(Object input){
		        return new MSJSorter((Iterator)input, maxLevel, dim, newQueue, mem);
        }
      },
			newResult,
			initialCapacity
		);
	}

	/** top-level constructor for a self-join
	*/
/*	
	public MSJ(Cursor input, Predicate predicate, Function newResult, final int initialCapacity, final int type, final int maxLevel, final int dim, final Function newQueue, final int mem){
		super( input, predicate,
			new AbstractFunction(){
                public Object invoke(Object input){
                        return new MSJSorter((Iterator)input, maxLevel, dim, newQueue, mem);
                }
            },
			newResult, initialCapacity, type
		);
	}
*/

	/**
	 * The top-level constructor for MSJ
	 * @param input0 ths first input
	 * @param input1 the second input
	 * @param predicate the join predicate
	 * @param newResult a function for mapping the output to an object
	 * @param initialCapacity 
	 * @param maxLevel the maximum level of the grid
	 * @param dim the dimension of the objects
	 * @param newQueue a functional factory for creating queues
	 * @param mem the available main memory
	 */
	public MSJ(Iterator input0, Iterator input1, Predicate predicate, Function newResult, final int initialCapacity, final int maxLevel, final int dim, final Function newQueue, final int mem){
		this( new IteratorCursor(input0), new IteratorCursor(input1), predicate, newResult, initialCapacity, maxLevel, dim, newQueue, mem);
	}

	/** constructor for a self-join
	*/
//	public MSJ(Iterator input, Predicate predicate, Function newResult, final int initialCapacity, final int maxLevel, final int dim, final Function newQueue, final int mem){
//		this( new BufferedCursor(input), predicate, newResult, initialCapacity, THETA_JOIN, maxLevel, dim, newQueue, mem);
//	}
	
}

