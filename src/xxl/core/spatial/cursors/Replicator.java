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

import xxl.core.collections.queues.ListQueue;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.KPEzCode;
import xxl.core.spatial.SpaceFillingCurves;
import xxl.core.spatial.rectangles.FixedPointRectangle;

/**
 *	The Replicator provides the replication engine of GESS.
 *	The Replicator maps each input-Object (of arbitrary type) to an Iterator of KPEzCode-Objects.
 *	For a detailed description see
 *	"GESS: a Scalable Similarity-Join Algorithm for Mining Large Data Sets in High Dimensional Spaces
 *	by Jens-Peter Dittrich and Bernhard Seeger, ACM SIGKDD 2001. pages: 47-56."
 *
 *	@see xxl.core.spatial.cursors.GESS
 *	@see xxl.core.spatial.cursors.GESS.ReferencePointMethod
 *	@see xxl.core.spatial.points.FixedPoint
 *	@see xxl.core.spatial.rectangles.FixedPointRectangle
 *	@see xxl.core.spatial.KPEzCode
 *
 */
public class Replicator extends IteratorCursor {


	//Split-Predicate class/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/** Abstract Predicate used to decide whether further splits are allowed.
	 *
	 *	@see xxl.core.predicates.Predicate
	 */
	public static abstract class Split extends AbstractPredicate {

		/** The bound that is checked by the predicate
		 */
		protected int bound;

		/** Creates a new Split-Predicate.
		 * 	@param bound is the boundary used for splitting
		 */
		public Split(int bound){
			this.bound = bound;
		}
	}

	/** Allows at most <code>bound</code> generations (2^(generation) replicates are created).
	 */
	public static class MaxGeneration extends Split{

		/** Creates a new MaxGeneration-Predicate.
		 * @param bound is the boundary of the predicate
		 */
		public MaxGeneration(int bound){
			super(bound);
		}

		/**
		 * @param object the object (of class QueueEl) where a split might be performed
		 * @return true if a split is preformed
		 */
		public boolean invoke(Object object){
			return ((QueueEl)object).splitGeneration < bound; 	//split at most <bound> times
		}
	}

	/** Allows at most <code>bound</code> splits per level.
	*/
	public static class MaxSplitsPerLevel extends Split{

		/** Creates a new MaxSplitsPerLevel-Predicate.
		 * @param bound limits the number of splits per level
		*/
		public MaxSplitsPerLevel(int bound){
			super(bound);
		}

		/**
		 * @param object The object for which a split might be performed
		 * @return true if the object is split
		 */
		public boolean invoke(Object object){
			QueueEl q = (QueueEl)object;

			if ( (q.flag == -1) || (q.dim == 0))
				q.flag = q.replicate.numberOfSplitsPerLevel(q.bitIndex); 	
				//split at most <bound> times per level/**/
				//use flag to store whether splits are allowed for this level
				//(the flag is like a cookie: we have to switch from local to global split decision here)
			return q.flag <= bound;
		}
	}

	/** Allows splits if the actual bitIndex is greater or equal than a given bound.
	*/
	public static class MaxSplitBit extends Split{

		/** Creates a new MaxSplitBit-Predicate.
			@param bound max-split-bit for which a split is allowed, bound in [62,...,1]
		*/
		public MaxSplitBit(int bound){
			super(bound);
		}

		/**
		 * @param object The object for which a split might be performed
		 * @return true if a split is performed
		 */
		public boolean invoke(Object object){
			return ((QueueEl)object).bitIndex >= bound; 	//split if bitIndex >= bound
		}
	}

	/** Allows splits if the level is smaller or equal than a given bitIndex.
		The bound is reversed, i.e. this class passes <code>62-level</code>
		to its super-constructor.
	*/
	public static class MaxSplitLevel extends MaxSplitBit{

		/** 
		 * @param level denotes the maximum level where splits are still allowed
		*/
		public MaxSplitLevel(int level){
			super(62-level);
		}
	}

	/** Constrains replication to 'bound' replicates per input-element.
	*/
	public static class MaxReplicates extends Split{

		/** Creates a new MaxReplicates-Predicate.
		 * @param bound the maximum number of replicas per input element
		*/
		public MaxReplicates(int bound){
			super(bound);
		}

		/** This method throws an UnsupportedOperationException
		 * 
		 * @param object
		 * @return true if a split is preformed
		 */
		public boolean invoke(Object object) {
			throw new UnsupportedOperationException();
		}
	}


	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//static-fields/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/** table used to set all bits > bitIndex = split-bit to true
	*/
	public static final long[] m1 = new long[64];

	/** table used to select the bit-prefix, i.e. all bits <= bitIndex = split-bit
	*/
	public static final long[] prefix = new long[64];

	/** table used to select the bit at position 'index'
	*/
	public static final long[] bit = new long[64];

	/** initializer for static protected fields
	*/
	static{
		long bitMask = 0x1L << 63;				//first bit true
		long prefixMask = bitMask;				//first bit true
		for(int i=63; i>=0; i--){
			prefix[i] = prefixMask;
			m1[i] = ~prefixMask;
			bit[i] = bitMask;
			bitMask >>>= 1;
			prefixMask += bitMask;
		}
	}

	/** split until sons
	*/
	public static final Split SPLIT_ONCE			= new MaxGeneration(1);

	/** split until grandsons
	*/
	public static final Split SPLIT_TWICE			= new MaxGeneration(2);

	/** split until grandgrandsons
	*/
	public static final Split SPLIT_THRICE			= new MaxGeneration(3);

	/** split once per level
	*/
	public static final Split SPLIT_ONCE_PER_LEVEL		= new MaxSplitsPerLevel(1);

	/** split twice per level
	*/
	public static final Split SPLIT_TWICE_PER_LEVEL		= new MaxSplitsPerLevel(2);

	/** split thrice per level
	*/
	public static final Split SPLIT_THRICE_PER_LEVEL	= new MaxSplitsPerLevel(3);

	/** Counter used to count elements that are created by the replicator
		(that are passed "out" to the next operator)
	*/
	public static long EL_OUT = 0;

	/** Counter used to count elements that are passed to the replicator
		(that are passed "in" to this operator)
	*/
	public static long EL_IN = 0;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//QueueEl class:////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/** 
		A Wrapper-class needed to unwind the recursive split. We do not use "real" recursion
		here for reasons of flexibility: the type of Queue used determines the traversing-
		strategy of recursive splits.
	*/
	protected static class QueueEl{

		/** The source-object, needed for result-tuples
		*/
		protected Object source;

		/** split (replicate) rectangle
		*/
		protected FixedPointRectangle replicate;

		/** the bit actually treated (start=62)
		*/
		protected int bitIndex;

		/** the dimension actually treated
		*/
		protected int dim;

		/** internal flag needed to precompute the number of possible splits for a sequence of levels
			default-value: -1
		*/
		protected int flag;

		/** The generation of the split:
			0: no split occured for this El, 1: one split, etc.
		*/
		protected short splitGeneration;

		/** Top-level constructor, creates a new QueueEl.

			@param source the source data
			@param replicate the replicate-rectangle used internally to compute the replicate-set
			@param bitIndex	the bit-position actually treated
			@param dim the dimension of the objects
			@param splitGeneration the generation of the split
			@param flag internal flag (should be set to -1, used by MaxSplitsPerLevel-predicate)
		*/
		public QueueEl(Object source, FixedPointRectangle replicate, int bitIndex, int dim, short splitGeneration, int flag){
			this.source = source;
			this.replicate = replicate;
			this.bitIndex = bitIndex;
			this.dim = dim;
			this.splitGeneration = splitGeneration;
			this.flag = flag;
		}

		/** Creates a new QueueEl.

			@param source the source data
			@param replicate the replicate-rectangle used internally to compute the replicate-set
		*/
		public QueueEl(Object source, FixedPointRectangle replicate){
			this(source, replicate, 62, 0, (short)0, -1);
		}

		/** Clones a new QueueEl from an existing queueEl.

			@param q the QueueEl to be cloned
		*/
		public QueueEl(QueueEl q){
			this(q.source, (FixedPointRectangle)q.replicate.clone(), q.bitIndex, q.dim, q.splitGeneration, q.flag);
		}

		/** Splits this element.
		 * @return the right replicate. 'this' QueueEl is the left replicate.
		*/
		public QueueEl split(){
			QueueEl qRight = new QueueEl(this);	//clone this QueueEl
			((long[])qRight.replicate.getCorner(false).getPoint())[dim] = ((long[])replicate.getCorner(true).getPoint())[dim] & prefix[bitIndex] ;		//keep prefix of the bits seen so far and set remaining bits to "false"
			((long[])this.replicate.getCorner(true).getPoint())[dim] = (((long[])this.replicate.getCorner(false).getPoint())[dim] & prefix[bitIndex]) | m1[bitIndex];	//keep prefix of the bits seen so far and set remaining bits to "true"

			qRight.dim = ++dim;				//increment dim-counter for next assessment
			qRight.splitGeneration = ++splitGeneration;	//increment split-generation
			return qRight;					//return right replicate
		}

		/** Checks whether we got a split here.

			@return true if the bits of component <dim> at bit-index <bitIndex> are different (i.e. we are facing a split at the current posiiton)
		*/
		public boolean checkForSplit(){
			return replicate.bitsDiffer(dim,bitIndex);
		}
	}


	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//Replicator: constructor///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/** The top-level constructor for this class.
	 *
	 *	@param inputMapping Mapper for the input to the Replicator (maps incoming object of arbitrary type to FixedPointRectangle)
	 *	@param input the input to be processed by this class
	 *	@param newResult a Function that maps the internally used QueueEl to the Object to be returned by the replication process
	 *	@param dimensions the dimensionality of the data
	 *	@param queue the queue used to process replicates
	 *	@param splitAllowed the Predicate used to determine whether a split is allowed
	 *	@param minBitIndex the minimal bit-index to be considered for the replication-process ( 0 <= bitIndex <= 62 )
	 */
	public Replicator(final Function inputMapping, final Iterator input, final Function newResult, final int dimensions, final Queue queue, final Predicate splitAllowed, final int minBitIndex){
		super(
			new Mapper(															//maps QueueEl-Objects to newObject (e.g. KPEzCode)
				newResult,	//map internally used QueueEl-Objects to user-determined Object
					new Sequentializer(
					new Mapper(
						new AbstractFunction() {												//maps input-Points to Iterator of QueueEl-Objects (i.e. computes replicates using assessment)
	
							/** Assesses a given QueueEl.
							@return resulting queue
							**/
							protected QueueEl assess(Object queueEl){
								QueueEl h = (QueueEl)queueEl;
								for(; h.bitIndex > minBitIndex; h.bitIndex--){		//repeat outer loop if the actual bit-index is greater minimal bit-index
									for(; h.dim < dimensions; h.dim++){				//inner loop, considers each dimension of the input vectors
										if( h.checkForSplit() ){					//check whether h hits a split line for the actual bitIndex and dimension
											if( splitAllowed.invoke(h) ){			//check whether it is allowed to perform a split for element h
												queue.enqueue(h.split());			//split h and insert right replicate into queue
												queue.enqueue(h);					//insert left replicate into queue
												return assess(queue.dequeue());		//recurse: access next-element of the queue
											}
											else									//i.e. split is not allowed
												return h;							//h is a result of the replication-process and can be returned by the Replicator, 
																					//i.e. <bitIndex> and <dim> point to a bit that hit a split-line
										}
									}
									h.dim = 0;										//reset h.dim to 0 for next iteration of outer loop
								}
								h.bitIndex = minBitIndex;							//minBitIndex was reached, i.e. h passes all split-lines until minBitIndex
								return h;											//return h;
							}
	
							/** Precondition: the argument object is of type xxl.core.spatial.points.Point, the point must be inside [0;1)^dim (i.e. NOT 1)
							*/
							public Object invoke(final Object object){				//Function used to map incoming objects to (Iterator of QueueEl)
								return new AbstractCursor(){
									{
										EL_IN++;									//increment counter used for counting size of the input
										queue.enqueue(new QueueEl( object, (FixedPointRectangle) inputMapping.invoke(object)) );	//unwind recursion
									}
	
									public boolean hasNextObject(){
										return !queue.isEmpty();				//Precondition: queue not empty
									}
									
									public Object nextObject() {
										return queue.dequeue(); //call assessment which computes next replicate
									}
								};
							}
						}
						, input)
				)	
			)
		);
	}

	/** Creates a new Replicator. This constructors passes a ListQueue to the parameter 'queue' of the top-level constructor.
	 *
	 *	@param inputMapping Mapper for the input to the Replicator (maps incoming object of arbitrary type to FixedPointRectangle)
	 *	@param input the input to be processed by this class
	 *	@param newResult a Function that maps the internally used QueueEl to the Object to be returned by the replication process
	 *	@param dimensions the dimensionality of the data
	 *	@param splitAllowed the Predicate used to determine whether a split is allowed
	 *	@param minBitIndex the minimal bit-index to be considered for the replication-process ( 0 <= bitIndex <= 62 )
	 *
	 *	@see xxl.core.collections.queues.ListQueue
	 */
	public Replicator(final Function inputMapping, final Iterator input, final Function newResult, final int dimensions, final Predicate splitAllowed, final int minBitIndex){
		this(inputMapping, input, newResult, dimensions, new ListQueue(), splitAllowed, minBitIndex);
	}

	/** Creates a new Replicator. This constructors provides an implementation of the parameter-Function 'newResult'.
	 *	
	 *	@param inputMapping Mapper for the input to the Replicator (maps incoming object of arbitrary type to FixedPointRectangle)
	 *	@param input the input to be processed by this class
	 *	@param dimensions the dimensionality of the data
	 *	@param splitAllowed the Predicate used to determine whether a split is allowed
	 *	@param minBitIndex the minimal bit-index to be considered for the replication-process ( 0 <= bitIndex <= 62 )
	 *	@param considerAdditionalBits determines whether kd-splitting should be applied (otherwise quadtree-splitting is performed)
	 */
	public Replicator(final Function inputMapping, final Iterator input, final int dimensions, final Predicate splitAllowed, final int minBitIndex, final boolean considerAdditionalBits){
		this(inputMapping, input,
			new AbstractFunction(){																		//= newResult-Function
				public Object invoke(Object queueEl){
					QueueEl q = (QueueEl) queueEl;
					final int additionalBits = considerAdditionalBits ? q.dim : 0;				//determines the additional bits to be considered for computing the z-code
					final int componentPrecision = Math.min(62-q.bitIndex, 62-minBitIndex);		//computes the number of bits to be considered for computing the z-code
					EL_OUT++;																	//increment counter used for counting size of the output
					return new KPEzCode( 
						q.source, 																//insert source point into KPEzCode
						SpaceFillingCurves.zCode( ((long[])q.replicate.getCorner(false).getPoint()), componentPrecision, additionalBits), 
						q.splitGeneration > 0													//if > 0 holds, this element is a replicate
					);	//computes zCode and returns KPEzCode containg original input-data and z-code
				}
			},
			dimensions, splitAllowed, minBitIndex
		);
	}

	/** Creates a new Replicator.
	 *
	 *	@param inputMapping Mapper for the input to the Replicator (maps incoming object of arbitrary type to FixedPointRectangle)
	 *	@param input the input to be processed by this class
	 *	@param dimensions the dimensionality of the data
	 *	@param splitAllowed the Predicate used to determine whether a split is allowed
	 *	@param minBitIndex the minimal bit-index to be considered for the replication-process ( 0 <= bitIndex <= 62 )
	 */
	public Replicator(final Function inputMapping, final Iterator input, final int dimensions, final Predicate splitAllowed, final int minBitIndex){
		this(inputMapping, input, dimensions, splitAllowed, minBitIndex, true);
	}
}
