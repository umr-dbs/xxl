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

import xxl.core.collections.bags.ArrayBag;
import xxl.core.collections.bags.LIFOBag;
import xxl.core.collections.sweepAreas.BagSAImplementor;
import xxl.core.collections.sweepAreas.ImplementorBasedSweepArea;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.joins.SortMergeJoin;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.Function;
import xxl.core.functions.Tuplify;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.KPEzCode;
import xxl.core.spatial.predicates.OverlapsPredicate;
import xxl.core.util.BitSet;

/**
 * The spatial join algorithm based on space-filling curves proposed by Jack
 * Orenstein. See: [Ore 91] Jack A. Orenstein: An Algorithm for Computing the
 * Overlay of k-Dimensional Spaces. SSD 1991: 381-400 for a detailed
 * explanation. See: [DS 01]: Jens-Peter Dittrich, Bernhard Seeger: GESS: a
 * Scalable Similarity-Join Algorithm for Mining Large Data Sets in High
 * Dimensional Spaces. ACM SIGKDD-2001. for a review on Orensteins algorithm.
 * <br>
 * <br>
 * Orensteins algorithm is based on a binary recursive partitioning, where the
 * binary code represents the so-called Z-ordering (z-codes). <br>
 * <br>
 * Orensteins algorithm (ORE) assigns each hypercube of the input relations to
 * disjoint subspaces of the recursive partitioning whose union entirely covers
 * the hypercube. ORE sorts the two sets of hypercubes derived from the input
 * relations (including the possible replicates) w.r.t. the lexicographical
 * ordering of its binary code. After that, the relations are merged using two
 * main-memory stacks Stack_R and Stack_S. It is guaranteed that for two
 * adjacent hypercubes in the stack, the prefix property is satisfied for their
 * associated codes. Only those hypercubes are joined that have the same prefix
 * code. <br>
 * <br>
 * A deficiency of ORE is that the different assignment strategies examined in
 * [Ore91] cause substantial replication rates. This results in an increase of
 * the problem space and hence, sorting will be very expensive. Furthermore, ORE
 * has not addressed the problem of eliminating duplicates in the result set.
 * <br>
 * <br>
 * Note that the method <code>reorganize(final Object
 *	currentStatus)</code>
 * could actually be implemented with only 1 LOC. For efficiency reasons we use
 * a somewhat longer version of the method here. <br>
 * <br>
 * Use-case: <br>
 * The main-method of this class contains the complete code to compute a
 * similarity join of two sets of points using Orensteins algorithm.
 * 
 * @see xxl.core.cursors.joins.SortMergeJoin
 * @see xxl.core.spatial.cursors.Mappers
 * @see xxl.core.spatial.cursors.GESS
 * @see xxl.core.spatial.cursors.Replicator
 *  
 */
public class Orenstein extends SortMergeJoin {
	/**
	 * The sweep area used by the Orenstein algorithm.
	 */
	public static class OrensteinSA<T> extends ImplementorBasedSweepArea<T> {
		/**
		 * internal cursor to the bag (for reasons of efficiency)
		 */
		protected LIFOBag<T> bag;
		/**
		 * Creates a new Orenstein SweepArea
		 * 
		 * @param ID
		 *            ID of the SweepArea
		 * @param lifoBag
		 *            the lifobag for organizing the SweepArea
		 * @param joinPredicate
		 *            the predicate of the join
		 */
		public OrensteinSA(int ID, LIFOBag<T> lifoBag, Predicate<? super T> joinPredicate) {
			super(new BagSAImplementor<T>(lifoBag), ID, false, joinPredicate, 2);
			this.bag = lifoBag;
		}
		/**
		 * Creates a new OrensteinSweepArea. Uses an ArrayBag to store elements.
		 * 
		 * @param ID
		 *            ID of the SweepArea
		 * @param initialCapacity
		 *            the initial capacity of the ArrayBag which is used for
		 *            organizing the SweepArea
		 * @param joinPredicate
		 *            the predicate of the join
		 */
		public OrensteinSA(int ID, int initialCapacity, Predicate<? super T> joinPredicate) {
			this(ID, new ArrayBag<T>(initialCapacity), joinPredicate);
		}
		
		/**
		 * In contrast to the method {@link #expire(Object, int)}, this method removes
		 * all expired elements from a SweepArea without returning them. 
		 * The default implementation removes all elements returned by a call to 
		 * {@link #expire(Object, int)}.<BR>
		 * In order to perform a more efficient removal, this method should
		 * be overwritten, e.g., by implementing a bulk deletion. 
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
		// fourth version of this method: low level implementation of delete
		// (fastest version)
		public void reorganize(final T currentStatus, int ID)
				throws IllegalStateException { //check nesting-condition
			for (Cursor<T> cursor = bag.lifoCursor(); cursor.hasNext();) {
				BitSet top = ((KPEzCode) cursor.next()).getzCode();
				BitSet query = ((KPEzCode) currentStatus).getzCode();
				if ((query.precision() < top.precision())
						|| (query.compare(top) != 0))
					cursor.remove();
				else
					break;
			}
		}
		/**
		 * This method counts the number of comparisons required for processing
		 * the query.
		 * 
		 * @see xxl.core.collections.sweepAreas.SweepArea#query(java.lang.Object,
		 *      int)
		 * @param o
		 *            The query object. This object is typically probed against
		 *            the elements contained in this SweepArea.
		 * @param ID
		 *            An ID determining from which input this method is called.
		 * @return All matching elements of this SweepArea are returned as an
		 *         iterator.
		 */
		public Iterator<T> query(T o, int ID) {
			comparisons.counter += impl.size();
			return super.query(o, ID);
		}
		
		/**
		 * Inserts the given element into this SweepArea. The default implementation
		 * simply forwards this call to the underlying implementor. Thus,
		 * it calls <code>impl.insert(o)</code>.
		 * 
		 * @param object The object to be inserted.
		 * @throws IllegalArgumentException Throws an IllegalArgumentException
		 * 		if something goes wrong with the insertion due to the passed argument.
		 */
		public void insert(T object) {
			super.insert(object);
			MAX_SWEEPAREA_SIZE = Math.max(MAX_SWEEPAREA_SIZE, size()); 
			//determine maximum size of the sweep area
		}
	}
	
	/**
	 * A class for counting
	 */
	public static class Counter {
		/**
		 * Internal counter
		 */
		public long counter = 0;
	}
	/**
	 * Counter for coomparison operations
	 */
	public static final Counter comparisons = new Counter();
	/**
	 * Maximum size of the sweep area (number of elements)
	 */
	public static int MAX_SWEEPAREA_SIZE = 0;
	/**
	 * Constructs an object of the class Orenstain:
	 * 
	 * @param input0
	 *            the first input cursor
	 * @param input1
	 *            the second input cursor
	 * @param joinPredicate
	 *            the join predicate
	 * @param newSorter
	 *            provides a function that returns sorted inputs
	 * @param newResult
	 *            is a function for creating the final result object
	 * @param initialCapacity
	 *            the initial capacity of the ArrayBag that is used for
	 *            organiting the SweepAreas
	 */
	public Orenstein(Cursor input0, Cursor input1, Predicate joinPredicate,
			Function newSorter, Function newResult, final int initialCapacity) {
		super(input0, input1, newSorter, newSorter, new OrensteinSA(0,
				initialCapacity, joinPredicate), new OrensteinSA(1,
				initialCapacity, joinPredicate),
				new ComparableComparator(), newResult);
	}
	/*
	 * top-level constructor for a self-join
	 */
	/*
	 * public Orenstein(Cursor input, Predicate joinPredicate, Function
	 * newSorter, Function newResult, final int initialCapacity, final int
	 * type){ super(input, newSorter, joinPredicate, new
	 * OrensteinSweepArea(initialCapacity), newResult, type ); }
	 */
	/**
	 * Constructs an object of the class Orenstain that wraps input iterators as
	 * cursors.
	 * 
	 * @param input0
	 *            the first input iterator
	 * @param input1
	 *            the second input iterator
	 * @param joinPredicate
	 *            the join predicate
	 * @param newSorter
	 *            provides a function that returns sorted inputs
	 * @param newResult
	 *            is a function for creating the final result object
	 * @param initialCapacity
	 *            the initial capacity of the ArrayBag that is used for
	 *            organiting the SweepAreas
	 */
	public Orenstein(Iterator input0, Iterator input1, Predicate joinPredicate,
			Function newSorter, Function newResult, final int initialCapacity) {
		this(new IteratorCursor(input0), new IteratorCursor(input1),
				joinPredicate, newSorter, newResult, initialCapacity);
	}
	/**
	 * Constructs an object of the class Orenstain that wraps input iterators as
	 * cursors. The join predicate test for overlaps. The method does not apply
	 * a function for creating the final object.
	 * 
	 * @param input0
	 *            the first input iterator
	 * @param input1
	 *            the second input iterator
	 * @param newSorter
	 *            provides a function that returns sorted inputs
	 * @param initialCapacity
	 *            the initial capacity of the ArrayBag that is used for
	 *            organiting the SweepAreas
	 */
	public Orenstein(Iterator input0, Iterator input1, Function newSorter,
			final int initialCapacity) {
		this(input0, input1, OverlapsPredicate.DEFAULT_INSTANCE, newSorter,
				Tuplify.DEFAULT_INSTANCE, initialCapacity);
	}
	/**
	 * constructor for a self-join
	 */
//	public Orenstein(Iterator input, Predicate joinPredicate, Function newSorter, Function newResult, final int initialCapacity) {
//		this(new BufferedCursor(input), joinPredicate, newSorter, newResult, initialCapacity, SortMergeJoin.THETA_JOIN);
//	}
	
}
