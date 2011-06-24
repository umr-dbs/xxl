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

import xxl.core.util.memory.MemoryMonitorable;
import xxl.core.util.metaData.MetaDataManageable;

/**
 * A SweepArea is a highly dynamic datastructure with flexible
 * insertion, retrieval and reorganization capabilities. It is 
 * utilized to remain the state of an operation.
 * 
 * @see ImplementorBasedSweepArea
 * @see xxl.core.cursors.joins.SortMergeJoin
 * @see xxl.core.cursors.joins.MultiWaySortMergeJoin
 * @see java.util.Iterator
 */
public interface SweepArea<I,E> extends MemoryMonitorable, MetaDataManageable<Object,Object>, Iterable<E> {
    
    /**
	 * A default ID for a SweepArea. This is typically
	 * used in unary operations.
	 */
	public static final short DEFAULT_ID = 0;
	
	public static final String SWEEPAREA_PERIODIC_TRIGGER = "SWEEPAREA_PERIODIC_TRIGGER";
	public static final String SWEEPAREA_COSTS = "SWEEPAREA_COSTS";

	/**
	 * Inserts the given element into this SweepArea.
	 * 
	 * @param o The object to be inserted.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the insertion due to the passed argument.
	 */
	public abstract void insert(I o) throws IllegalArgumentException;
	
	/**
	 * Clears this SweepArea. <br>
	 * This method should remove all elements of a 
	 * SweepArea, but holds its allocated resources.
	 */
	public abstract void clear();
	
	/**
	 * Closes this SweepArea. <br>
	 * This method should release all allocated resources
	 * of a SweepArea.
	 */
	public abstract void close();
	
	/**
	 * Returns the size of this SweepArea.
	 * 
	 * @return The size of this SweepArea.
	 */
	public abstract int size();
	
	/**
	 * Returns an iterator over this SweepArea.
	 * 
	 * @return An iterator over this SweepArea.
	 * @throws UnsupportedOperationException if this SweepArea is not able
	 *         to provide an iteration over its elements.
	 */
	public abstract Iterator<E> iterator() throws UnsupportedOperationException;
	
	/**
	 * Queries this SweepArea with the help of the
	 * specified query object <code>o</code>. Returns all 
	 * matching elements as an iterator. <br>
	 * <i>Note:</i>
	 * This iterator should not be used to remove any elements of a
	 * SweepArea!
	 * 
	 * @param o The query object. This object is typically probed against
	 * 		the elements contained in this SweepArea.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public abstract Iterator<E> query(I o, int ID) throws IllegalArgumentException;

	/**
	 * Queries this SweepArea with the help of the
	 * specified query objects <code>os</code>. Returns all matching elements
	 * as an iterator. This version of query additionally allows to use partially
	 * filled arrays and specifies how many entries of such a partially
	 * filled array are valid.<br> 
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this SweepArea.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @param valid Determines how many entries at the beginning of
	 *        <tt>os</tt> and <tt>IDs</tt> are valid and therefore taken into account.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public abstract Iterator<E> query(I[] os, int [] IDs, int valid) throws IllegalArgumentException;

	/**
	 * Queries this SweepArea with the help of the
	 * specified query objects <code>os</code>. Returns all matching elements
	 * as an iterator. 
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this SweepArea.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public abstract Iterator<E> query(I[] os, int [] IDs) throws IllegalArgumentException;
	
	/**
	 * Determines which elements in this SweepArea expire with respect to the object
	 * <tt>currentStatus</tt> and an <tt>ID</tt>. The latter is commonly used
	 * to differ by which input this reorganization step is initiated.<br>
	 * If no elements qualify for removal, an empty cursor is returned and all 
	 * elements are remained. <br>
	 * In order to remove the expired elements, either the returned iterator has to 
	 * support and execute the remove operation for each expired element during traversal
	 * or the {@link #reorganize(Object, int)} has to be overwritten to perform 
	 * the final removal. <p>
	 * Hence, specialized SweepAreas should overwrite this method to gain a more
	 * efficient reorganization.
	 * 
	 * @param currentStatus The object containing the necessary information
	 * 		to detect expired elements.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return an iteration over the elements which expire with respect to the
	 *         object <tt>currentStatus</tt> and an <tt>ID</tt>.
	 * @throws UnsupportedOperationException An UnsupportedOperationException is thrown, if
	 * 		this method is not supported by this SweepArea.
	 * @throws IllegalStateException Throws an IllegalStateException if
	 * 		   this method is called at an invalid state.
	 */
	public abstract Iterator<E> expire(I currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException;
	
	/**
	 * In contrast to the method {@link #expire(Object, int)}, this method removes
	 * all expired elements from a SweepArea without returning them. 
	 * <BR>
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
	public abstract void reorganize(I currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException;
	
}
