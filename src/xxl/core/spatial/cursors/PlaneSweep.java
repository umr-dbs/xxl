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

import java.util.Comparator;
import java.util.Iterator;

import xxl.core.collections.sweepAreas.AbstractSAImplementor;
import xxl.core.collections.sweepAreas.ImplementorBasedSweepArea;
import xxl.core.collections.sweepAreas.SweepArea;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.joins.SortMergeJoin;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.Function;
import xxl.core.functions.Tuplify;
import xxl.core.predicates.Predicates;
import xxl.core.spatial.KPE;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;

//import xxl.core.predicates.Predicate;

/**
 *	Plane Sweep for two-dimensional data. The Sweepline is hash-based.
 *	See Lars Arge et.al. VLDB 1998.
 *
 *	(prerelease for prerelease of XXL)
 *
 */
public class PlaneSweep extends SortMergeJoin {
	
	/**
	 *	Compares two Objects with respect to the first dimension of ther left borders in ascending order.
	 *	This is useful for a computational geometry plane sweep. The input data is assumed to be of type
	 *  KPE.
	 *
	 *  @see xxl.core.spatial.KPE
	 *	@see xxl.core.spatial.cursors.PlaneSweep
	 *
	 */
	public static class KPEPlaneSweepComparator implements Comparator<KPE>{
	
		/** dimensionality used for comparisons.
		*/
		final protected int dim;
	
		/** Point used for comparisons (false: lower-left, true: upper-right).
		*/
		protected boolean upper;
	
		/** Default instance of this class. Sets the sweeping dimension to 0.
		*/
		public static final KPEPlaneSweepComparator DEFAULT_INSTANCE = new KPEPlaneSweepComparator(0);
	
		/** Creates a new KPEPlaneSweepComparator.
		 *
		 *	@param upper which point to use for comparisons (false: lower-left, true: upper-right)
		 *	@param dim dimensionality used for comparisons.
		 */
		public KPEPlaneSweepComparator(boolean upper, int dim){
			this.upper = upper;
			this.dim = dim;
		}
	
		/** Creates a new KPEPlaneSweepComparator.
		 *
		 *	@param dim dimensionality used for comparisons.
		 */
		public KPEPlaneSweepComparator(int dim){
			this(false,dim);
		}
	
		/** Compares its two arguments for order w.r.t. to the specified dimension of this object.
		 * @param object1 the first object
		 * @param object2 the second object
		 * @return an integer as it is defined for a comparator
		*/
		public int compare(KPE object1, KPE object2){
			double left  =  ((Rectangle)(object1).getData()).getCorner(upper).getValue(dim);
			double right =  ((Rectangle)(object2).getData()).getCorner(upper).getValue(dim);
			return left < right ? -1 : (left > right ? +1 : 0);
		}
	}
	
		
	/**
	 *	Compares two Objects with respect to a user specified dimension 
	 *  of their left or right borders in ascending order.
	 *	This is useful for a computational geometry plane sweep.
	 *
	 *  @see xxl.core.spatial.cursors.PlaneSweep
	 *  @see xxl.core.spatial.rectangles.Rectangle
	 */
	public static class PlaneSweepComparator implements Comparator {
	
		/** Dimensionality used for comparisons.
		 */
		protected int dim;
	
		/** Point used for comparisons.
		 */
		protected boolean upper;
	
		/** Default instance of this class. Uses dimension 0 as the sweep dimension.
		 */
		public static final PlaneSweepComparator DEFAULT_INSTANCE = new PlaneSweepComparator(0);
	
		/** Creates a new PlaneSweepComparator.
		 *
		 *	@param upper which point to use for comparisons (false: lower-left, true: upper-right)
		 *	@param dim dimensionality used for comparisons.
		 */
		public PlaneSweepComparator(boolean upper, int dim){
			this.upper = upper;
			this.dim = dim;
		}
	
		/** Creates a new PlaneSweepComparator.
		 *
		 *	@param dim dimensionality used for comparisons.
		 */
		public PlaneSweepComparator(int dim){
			this(false,dim);
		}
	
		/** Compares its two arguments for order w.r.t. to the specified dimension of this object.
		 * @param object1 the first object
		 * @param object2 the second object
		 * @return an integer as it is defined for a comparator
		*/
		public int compare(Object object1, Object object2){
			double left = ((Rectangle) object1).getCorner(upper).getValue(dim);
			double right = ((Rectangle) object2).getCorner(upper).getValue(dim);
			return left < right ? -1 : (left == right ? 0 : +1);
		}
	}
		
	/** A SweepArea based on hash-buckets
	 */
	public static class PlaneSA extends ImplementorBasedSweepArea {

		/**
		 * A SweepArea implementor based on hash-buckets.
		 */
		static class PlaneSAImplementor extends AbstractSAImplementor {
			
			/** A bucket of the sweeparea.
			 */
			class HashBucket {
	
				/** Number of item contained in the sweep area.
				*/
				protected int size = 0;
	
				/** The data held by this sweep area.
				*/
				protected KPE[] data;
	
				/** The y-extension of the sweep area
				*/
				protected Rectangle descriptor1D;
	
				/** Creates a new HashBucket.
				 *	@param initialSize initial size of the array usedfor storing elements
				 *	@param descriptor1D the y-extension of the sweep area
				 */
				public HashBucket(int initialSize, Rectangle descriptor1D){
					data = new KPE[initialSize];
					this.descriptor1D = descriptor1D;
				}
	
				/** Inserts the given argument object into the hash bucket
				 * @param o the object
				*/
				public void insert(final Object o){
					_size++;
					data[size++] = (KPE) o;
				}
	
				/** Queries the hash bucket using the given argument object.
				 * 	@param o the object
				 *	@return a cursor containing all elements that overlap the given query object
				 * @throws IllegalArgumentException Throws an IllegalArgumentException
				 * 		if something goes wrong due to the passed arguments during retrieval.
				 */
				public Iterator query(final Object o) {
					return new AbstractCursor(){
						protected int t = -1;
						final protected KPE k = (KPE) o;
						final protected Rectangle r1 = (Rectangle) k.getData();
						private Object next;
	
						public boolean hasNextObject() {
							for(t++; t<size; t++){
	
								//while ( (rightCorner, first dimension) smaller (leftCorner, first dimension) )
								for( final double value = r1.getCorner(false).getValue(0); ((Rectangle) data[t].getData()).getCorner(true).getValue(0) < value; ){ //remove old elements
									if(t < --size	){				//t is not the last element in the array
										data[t] = data[size];	//remove element
										_size--;
									}
									else
										return false;					//return since end of array reached
								}
	
								Rectangle r0 = (Rectangle) data[t].getData();
	
								//check element at position t for overlap with element <o>
								if	(( r0.getCorner(false).getValue(1) <= r1.getCorner(true).getValue(1) )
											&&
									( r1.getCorner(false).getValue(1) <= r0.getCorner(true).getValue(1) ) ){
	
									//Reference Point Method (RPM), i.e. do not report possible duplicates
									double min = Math.max( r0.getCorner(false).getValue(1), r1.getCorner(false).getValue(1));
									if( (descriptor1D.getCorner(false).getValue(0) < min) && (min <= descriptor1D.getCorner(true).getValue(0)) ){
										next = new KPE[]{ data[t], k };
										return true;
									}
								}
							}
							return false;
						}
						
						public Object nextObject() {
							return next;
						}
						
					};
				}
			}
			
			/** The data space (universe) rectangle which contains all data objects.
			*/
			protected Rectangle universe;
			
			/** The initial bucket size of the hash buckets
			 */
			protected int initialBucketSize;
			
			/** The lower y-boreder of the sweep area
			*/
			protected double yoffset;
	
			/** The extension of a hash-bucket in y-dimension.
			*/
			protected double ydelta;
			
			/** The size of the sweep area
			*/
			protected int _size;		
			
			/**
			 * This is a constructor for the class
			 * @param noOfBuckets the number of buckets of the hashtable
			 * @param initialBucketSize the initial size of the buckets of the hashtable
			 * @param universe the bounding box of all objects
			 */
			public PlaneSAImplementor(final int noOfBuckets, final int initialBucketSize, final Rectangle universe) {
				double[] ll = (double[]) universe.getCorner(false).getPoint();
				double[] ur = (double[]) universe.getCorner(true).getPoint();
				ll[1] -= 0.0001; ur[1] += 0.0001;			//enlarge universe slightly
				DoublePoint llp = new DoublePoint(ll);
				DoublePoint urp = new DoublePoint(ur);
				this.universe = new DoublePointRectangle(llp,urp);	//create universe
				this.initialBucketSize = initialBucketSize;
				
				this.buckets = new HashBucket[noOfBuckets];		//array initialisieren
	
				double start = yoffset = this.universe.getCorner(false).getValue(1);	//yoffset setzen;
				double delta = ydelta = (this.universe.getCorner(true).getValue(1) - this.universe.getCorner(false).getValue(1)) / noOfBuckets;		//y-extension for a single bucket
	
				for(int i=0; i<buckets.length;i++){					//initialize HashBuckets
					buckets[i] = new HashBucket(initialBucketSize,
							new DoublePointRectangle(new DoublePoint(new double[]{start}), new DoublePoint(new double[]{start+=delta})));
				}
			}
			
			/** An array containing the hash buckets.
			 */
			protected HashBucket[] buckets;
			
			/**
			 * Inserts the object into the sweep area.
			 * @param o The object is assumed being from class KPE (@see KPE) and is inserted into the hashtable.
			 */
			public void insert(Object o) /* throws IllegalArgumentException */ {
				KPE k = (KPE)o;
				int start = (int)( (((Rectangle)k.getData()).getCorner(false).getValue(1) - yoffset) / ydelta );
				int end =   (int)( (((Rectangle)k.getData()).getCorner(true ).getValue(1) - yoffset) / ydelta );
	
				//insert object into all those hash-buckets that have some overlap:
				while(start<=end)
					buckets[start++].insert(o);
			}
	
			/** This operation throws an UnSupportedOperationException
			 * @param o
			 * @return
			 */
			public boolean remove(Object o) /* throws IllegalArgumentException */ {
				throw new UnsupportedOperationException();
			}
			
			/** This operation throws an UnSupportedOperationException
			 * @param o1
			 * @param o2
			 * @return
			 */
			public Object update(Object o1, Object o2) /* throws IllegalArgumentException, UnsupportedOperationException */ {
				throw new UnsupportedOperationException();
			}
			
			/**
			 * This method initializes the buckets of the hashtable.
			 */
			public void clear() {
				double start = yoffset = this.universe.getCorner(false).getValue(1);	//yoffset setzen;
				double delta = ydelta = (this.universe.getCorner(true).getValue(1) - this.universe.getCorner(false).getValue(1)) / buckets.length;		//y-extension for a single bucket
	
				for(int i=0; i<buckets.length;i++){					//initialize HashBuckets
					buckets[i] = new HashBucket(initialBucketSize,
							new DoublePointRectangle(new DoublePoint(new double[]{start}), new DoublePoint(new double[]{start+=delta})));
				}
			}
			
			/**
			 * The hashtable is removed (and the occupied storage is returned to the system)
			 */
			public void close() {
				buckets = null;	
			}
			
			/**
			 * @return size of the SweepArea (in the number of elements)
			 */
			public int size() {
				return _size;
			}
			
			/** This operation throws an UnSupportedOperationException
			 * @return
			 */
			public Iterator iterator() {
				throw new UnsupportedOperationException();
			}
			
			/** Queries the sweep area using the given object.
			 *
			 *  @param o the object (of class KPE)
			 * 	@param ID currently not used in the implementation of the method
			 *	@return an iterator containing all elements that overlap the given query object
			 */
			public Iterator query(final Object o, int ID) /* throws IllegalArgumentException */ {
				return new AbstractCursor(){
					protected KPE k = (KPE)o;
					protected int start = (int)( ( ((Rectangle)k.getData()).getCorner(false).getValue(1) - yoffset) / ydelta);
					protected int end =   (int)( ( ((Rectangle)k.getData()).getCorner(true ).getValue(1) - yoffset) / ydelta);
					private Iterator it = EmptyCursor.DEFAULT_INSTANCE;
	
					public boolean hasNextObject() {
						if (it.hasNext()) return true;
						while(start <= end){
							Iterator result = buckets[start++].query(k);
							if(result.hasNext()){
								it = result;
								return true;
							}
						}
						return false;
					}
					
					public Object nextObject() {
						return it.next();
					}
				};
			}
		}
		
		/**
		 * A constructor for the class PlaneSA
		 * @param noOfBuckets the number of buckets of the hashtable
		 * @param initialBucketSize the initial size of the buckets
		 * @param universe is a bounding box of the data space
		 */
		public PlaneSA(int noOfBuckets, int initialBucketSize, Rectangle universe) {
			super(new PlaneSAImplementor(noOfBuckets, initialBucketSize, universe), 0, false, Predicates.FALSE, 2);
		}
		
		/**
		 * Reorganization is performed during the query-phase. Hence, 
		 * the implementation of this method is empty.
		 * 
		 * @param currentStatus The object containing the necessary information
		 * 		  to perform the reorganization step.
		 * @param ID An ID determining from which input this reorganization step
		 * 		   is triggered.
		 * @throws UnsupportedOperationException An UnsupportedOperationException is thrown, if
		 * 		   is method is not supported by this SweepArea.
		 * @throws IllegalStateException Throws an IllegalStateException if
		 * 		   this method is called at an invalid state.
		 */
		public void reorganize(Object currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException {
		
		}
		
	}
	
	/**
	 * This is the top-level constructor for the class PlaneSweep.two inputs, does not require the input to be sorted (two inputs).
	 * @param input0 the first input	
	 * @param input1 the second input
	 * @param newSorter0 a function that returns a sorting method for the first input
	 * @param newSorter1 a function that returns a sorting method for the second input
	 * @param sweepArea0 the SweepArea of the first input
	 * @param sweepArea1 the SweepArea of the second input
	 * @param comparator the Comparator for comparing two elements from the inputs
	 * @param newResult a function that creates an object from the output of the join
	 */
	public PlaneSweep (Cursor input0, Cursor input1, Function newSorter0, Function newSorter1,
						SweepArea sweepArea0, SweepArea sweepArea1, Comparator comparator, Function newResult) {
		super( (Cursor)newSorter0.invoke(input0), (Cursor)newSorter1.invoke(input1), sweepArea0, sweepArea1, comparator, newResult);
	}

	/**
	 * This is another top-level constructir of the class PlaneSweep
	 * @param input0 the first input	
	 * @param input1 the second input
	 * @param newSorter0 a function that returns a sorting method for the first input
	 * @param newSorter1 a function that returns a sorting method for the second input
	 * @param comparator the Comparator for comparing two elements from the inputs
	 * @param universe a bounding box of the data space
	 * @param noOfBuckets the number of buckets of the hashtable
	 * @param initialBucketSize the initial size of the buckets of the hashtable
	 */
	public PlaneSweep(Cursor input0, Cursor input1, Function newSorter0, Function newSorter1, 
		Comparator comparator, final Rectangle universe,
		final int noOfBuckets, final int initialBucketSize){

		this(input0, input1, newSorter0, newSorter1,
			new PlaneSA(noOfBuckets,initialBucketSize, universe),
			new PlaneSA(noOfBuckets,initialBucketSize, universe),
			comparator, Tuplify.DEFAULT_INSTANCE
		);
	}
}
