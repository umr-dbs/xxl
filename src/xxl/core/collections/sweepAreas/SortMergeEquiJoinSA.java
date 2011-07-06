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

package xxl.core.collections.sweepAreas;

import java.util.Iterator;

import xxl.core.cursors.filters.Remover;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;

/**
 * A SweepArea for sort-based equijoins. In this special case of a join, the retrieval as well as the 
 * reorganization and expiration, respectively, can be improved with regard to efficiency. 
 * Querying a SortMergeEquiJoinSA simply returns an iterator over the complete 
 * SweepArea, whereas expiration and reorganization benefits from the fact that all elements 
 * not equal to the passed argument <code>currentStatus</code> can be removed from the SortMergeEquiJoinSA 
 * instantaneously.
 *  
 * @see xxl.core.cursors.joins.SortMergeEquivalenceJoin
 * @see xxl.core.predicates.Predicate
 * @see xxl.core.predicates.Equal
 */
public class SortMergeEquiJoinSA<E> extends ImplementorBasedSweepArea<E> {

	/**
	 * Binary predicate that decides if its
	 * two arguments are equal.
	 */
	protected Predicate<? super E> equals;

	/**
	 * Constructs a new SortMergeEquiJoinSA with the specified query-predicate <code>equals</code>.
	 * SortMergeEquiJoinSAs permit self-reorganization.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SortMergeEquiJoinSA.
	 * @param dim The dimensionality of this SweepArea, i.e., the number of possible inputs or in other words
	 * 		  the number of different IDs that can be passed to method calls of this SortMergeEquiJoinSA. 
	 * @param equals Binary predicate that decides if its two arguments are equal.
	 */
	public SortMergeEquiJoinSA(SweepAreaImplementor<E> impl, int ID, int dim, Predicate<? super E> equals) {
		super(impl, ID, true, (Predicate<E>)null, (Predicate<E>)null, dim);
		this.equals = equals;
	}

	/**
	 * Constructs a new SortMergeEquiJoinSA using {@link xxl.core.predicates.Equal#DEFAULT_INSTANCE} as
	 * query-predicate. SortMergeEquiJoinSAs permit self-reorganization.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SortMergeEquiJoinSA.
	 * @param dim The dimensionality of this SweepArea, i.e., the number of possible inputs or in other words
	 * 		  the number of different IDs that can be passed to method calls of this SortMergeEquiJoinSA. 
	 */
	public SortMergeEquiJoinSA(SweepAreaImplementor<E> impl, int ID, int dim) {
		this(impl, ID, dim, new Equal<E>());
	}

	/**
	 * Returns an iterator over the complete SortMergeEquiJoinSA by
	 * calling the method {@link #iterator()}.
	 * 
	 * @param o The query object. This object is typically probed against
	 * 		the elements contained in this SortMergeEquiJoinSA.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return All elements of this SortMergeEquiJoinSA are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public Iterator<E> query(E o, int ID) throws IllegalArgumentException {
		return new RemovingIterator(impl.iterator());
	}

	/**
	 * Returns an iterator over the complete SortMergeEquiJoinSA by
	 * calling the method {@link #iterator()}. This version of 
	 * query additionally allows to use partially
	 * filled arrays and specifies how many entries of such a partially
	 * filled array are valid.
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this SortMergeEquiJoinSA.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @param valid Determines how many entries at the beginning of
	 *        <tt>os</tt> and <tt>IDs</tt> are valid and therefore taken into account.
	 * @return All elements of this SortMergeEquiJoinSA are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public Iterator<E> query(E[] os, int[] IDs, int valid) throws IllegalArgumentException {
		return new RemovingIterator(impl.iterator());
	}

	/**
	 * Returns an iterator over the complete SortMergeEquiJoinSA by
	 * calling the method {@link #iterator()}.  
	 *  
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this SortMergeEquiJoinSA.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @return All elements of this SortMergeEquiJoinSA are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public Iterator<E> query(E[] os, int[] IDs) throws IllegalArgumentException {
		return new RemovingIterator(impl.iterator());
	}

	/**
	 * In an SortMergeEquiJoin all elements that are not equal
	 * to <code>currentStatus</code> can be removed from this
	 * SortMergeEquiJoinSA. Therefore, this method
	 * checks if this SortMergeEquiJoinSA contains any
	 * element or the first element is equal to <code>currentStatus</code>.
	 * In both cases, an empty cursor is returned. Otherwise,
	 * all elements of this SortMergeEquiJoinSA are returned
	 * as a cursor and removed during traversal.
	 * 
	 * @param currentStatus The object containing the necessary information
	 * 		to detect expired elements.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return an iteration over the elements which expire with respect to the
	 *         object <tt>currentStatus</tt> and an <tt>ID</tt>.
	 * @throws IllegalStateException Throws an IllegalStateException if
	 * 		   this method is called at an invalid state.
	 */
	public Iterator<E> expire (E currentStatus, int ID) throws IllegalStateException {
		Iterator<? extends E> it = impl.iterator();
		if (!it.hasNext() || equals.invoke(it.next(), currentStatus)) 
			return new EmptyCursor<E>();
		return new Remover<E>(iterator());
	}
	
	/**
	 * In contrast to the method {@link #expire(Object, int)}, this method 
	 * directly removes all expired elements from a SweepArea 
	 * without returning them. Consequently, this SortMergeEquiJoinSA
	 * is cleared whenever <code>currentStatus</code> is not 
	 * equal to the first element of this SortMergeEquiJoinSA.
	 * 
	 * @param currentStatus The object containing the necessary information
	 * 		  to perform the reorganization step.
	 * @param ID An ID determining from which input this reorganization step
	 * 		   is triggered.
	 * @throws IllegalStateException Throws an IllegalStateException if
	 * 		   this method is called at an invalid state.
	 */
	public void reorganize (E currentStatus, int ID) throws IllegalStateException {
		Iterator<? extends E> it = impl.iterator();
		if (!it.hasNext() || equals.invoke(it.next(), currentStatus)) 
			return;
		clear();
	}

}
