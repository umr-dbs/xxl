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

import xxl.core.predicates.Predicate;
import xxl.core.util.metaData.MetaDataManageable;

/**
 * This interface provides the core functionality
 * for an implementor of a SweepArea. To put it more
 * simply, an implementor acts as the underlying datastructure
 * of a SweepArea. <br>
 * For a better understanding how a SweepArea interacts with its 
 * implementor, take a look at the design pattern <i>Bridge</i>
 * whose intention is to "decouple an abstraction from its 
 * implementation so that the two can vary independently". 
 * For further information see: "Gamma et al.: <i>DesignPatterns.
 * Elements of Reusable Object-Oriented Software.</i> Addision 
 * Wesley 1998."
 * 
 * @see SweepArea
 * @see AbstractSAImplementor
 * @see ListSAImplementor
 * @see HashSAImplementor
 * @see xxl.core.predicates.Predicate
 * @see java.util.Iterator
 */
public interface SweepAreaImplementor<E> extends MetaDataManageable<Object,Object> {
	
	/**
	 * This method is used to initialize the implementor
	 * of a SweepArea. Commonly, it is invoked in the 
	 * constructor of a SweepArea to inform the underlying
	 * implementor about the ID and the query-predicates
	 * of the corresponding SweepArea.
	 * 
	 * @param ID The ID of the corresponding SweepArea.
	 * @param predicates The query-predicates of the corresponding SweepArea.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 */
	public void initialize(int ID, Predicate<? super E>[] predicates, Predicate<? super E> equals);
	
	/**
	 * Inserts the given element.
	 * 
	 * @param o The object to be inserted.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the insertion due to the passed argument.
	 */
	public void insert(E o) throws IllegalArgumentException;
	
	/**
	 * Removes the specified element.
	 * 
	 * @param o The object to be removed.
	 * @return <tt>True</tt> if the removal has been successful, otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the removal due to the passed argument.
	 */
	public boolean remove(E o)throws IllegalArgumentException;
	
	/**
	 * Checks if element <tt>o1</tt> is contained and 
	 * if <tt>true</tt> updates it with </tt>o2</tt>. 
	 * 
	 * @param o1 The object to be replaced.
	 * @param o2 The new object.
	 * @return The updated object is returned.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the update operation due to the passed arguments.
	 * @throws UnsupportedOperationException Throws an UnsupportedOperationException
	 * 		if this method is not supported.
	 */
	public E update(E o1, E o2) throws IllegalArgumentException, UnsupportedOperationException;
	
	/**
	 * Clears this implementor. Removes all its
	 * elements, but holds its allocated resources.
	 */
	public void clear();
	
	/**
	 * Closes this implementor and 
	 * releases all its allocated resources.
	 */
	public void close();
	
	/**
	 * Returns the size of this implementor.
	 * 
	 * @return The size.
	 */
	public int size();
	
	/**
	 * Returns an iterator over the elements of this
	 * implementor.
	 * 
	 * @return An iterator over the elements of this SweepAreaImplementor.
	 * @throws UnsupportedOperationException If this operation is not supported.
	 */
	public Iterator<E> iterator() throws UnsupportedOperationException;
	
	/**
	 * Queries this implementor with the help of the
	 * specified query object <code>o</code> and the query-predicates
	 * set during initialization, see method {@link #initialize(int, Predicate[])}. 
	 * Returns all matching elements as an iterator. <br>
	 * <i>Note:</i>
	 * The returned iterator should not be used to remove any elements from
	 * this implementor!
	 * 
	 * @param o The query object. This object is typically probed against
	 * 		the elements contained in this implementor.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return All matching elements of this implementor are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public Iterator<E> query(E o, int ID) throws IllegalArgumentException;

	/**
	 * Queries this SweepArea with the help of the
	 * specified query objects <code>os</code> and the query-predicates
	 * set during initialization, see method {@link #initialize(int, Predicate[])}. 
	 * Returns all matching elements as an iterator.
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this implementor.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @return All matching elements of this implementor are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public Iterator<E> query(E[] os, int[] IDs) throws IllegalArgumentException;

	/**
	 * Queries this implementor with the help of the
	 * specified query objects <code>os</code> and the query-predicates
	 * set during initialization, see method {@link #initialize(int, Predicate[])}. 
	 * Returns all matching elements as an iterator. This version of query 
	 * additionally allows to use partially
	 * filled arrays and specifies how many entries of such a partially
	 * filled array are valid.
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this implementor.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @param valid Determines how many entries at the beginning of
	 *        <tt>os</tt> and <tt>IDs</tt> are valid and therefore taken into account.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	public Iterator<E> query(E[] os, int[] IDs, int valid) throws IllegalArgumentException;

	
}
