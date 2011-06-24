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

import xxl.core.collections.bags.Bag;
import xxl.core.collections.bags.ListBag;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.predicates.LeftBind;

/**
 * An implementation of the interface {@link SweepAreaImplementor}
 * based on the interface {@link xxl.core.collections.bags.Bag Bag}.
 * 
 * @see SweepAreaImplementor
 * @see xxl.core.collections.bags.Bag 
 */
public class BagSAImplementor<E> extends AbstractSAImplementor<E> {

	protected boolean countBagOperations = false;
	
	public class BagSAImplementorMetaDataManagement extends AbstractSAImplementorMetaDataManagement {
		
		public static final String BAG_OPERATION = "BAG_OPERATION";
		
		protected long bagOperationCounter;
		
		public BagSAImplementorMetaDataManagement() {		
			super();
			this.bagOperationCounter = 0;
		}

		@Override
		protected boolean addMetaData(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(BAG_OPERATION)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Long>() {					
					@Override
					public Long invoke() {
						long res = bagOperationCounter;
						bagOperationCounter = 0;
						return res;
					}
				});
				return true;
			}
			return false;
		}

		@Override
		protected boolean removeMetaData(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(BAG_OPERATION)) {
				countBagOperations = false;
				return true;
			}			
			return false;
		}

		
	}
	
	/**
	 * The bag storing the elements.
	 */
	protected Bag<E> bag;
		
	/**
	 * Constructs a new BagSAImplementor.
	 * 
	 * @param bag The underlying bag.
	 */
	public BagSAImplementor(Bag<E> bag) {
		this.bag = bag;
	}

	/**
	 * Constructs a new BagSAImplementor
	 * based on a {@link xxl.core.collections.bags.ListBag ListBag}.
	 */
	public BagSAImplementor() {
		this(new ListBag<E>());
	}
	
	/**
	 * Inserts the given element into the bag.
	 * 
	 * @param o The object to be inserted.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the insertion due to the passed argument.
	 */
	public void insert(E o) throws IllegalArgumentException {
		bag.insert(o);
		if (countBagOperations) {
			synchronized (metaDataManagement) {
				((BagSAImplementorMetaDataManagement)metaDataManagement).bagOperationCounter++;
			}
		}
	}
	
	/**
	 * Removes the specified element from the bag.
	 * This is achieved by querying the bag for <code>o</code>
	 * and calling <tt>remove()</tt> if the cursor returned
	 * by the query contains an element. Otherwise, <tt>false</tt>
	 * is returned. 
	 * 
	 * @param o The object to be removed.
	 * @return <tt>True</tt> if the removal has been successful, otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the removal due to the passed argument.
	 */
	public boolean remove(E o) throws IllegalArgumentException {
		Iterator<E> it = bag.query(new LeftBind<E>(equals, o));
		if (it.hasNext()) {
			it.next();
			it.remove();
			if (countBagOperations) {
				synchronized (metaDataManagement) {
					((BagSAImplementorMetaDataManagement)metaDataManagement).bagOperationCounter++;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Returns the size of this implementor, i.e.,
	 * the size of the bag.
	 * 
	 * @return The size.
	 */
	public int size () {
		return bag.size();
	}

	/**
	 * Clears this implementor, i.e., 
	 * clears the bag.
	 */
	public void clear() {
		bag.clear();
	}

	/**
	 * Closes this implementor, i.e.,
	 * closes the bag.
	 */
	public void close() {
		bag.close();
	}

	/**
	 * Returns a cursor over the elements of the bag.
	 * 
	 * @return A cursor over the elements of the bag.
	 */
	public Iterator<E> iterator() {
		return bag.cursor();
	}
	
	/**
	 * Queries this implementor with the help of the
	 * specified query object <code>o</code> and the query-predicates
	 * set during initialization, see method
	 * {@link #initialize(int, xxl.core.predicates.Predicate[])}. 
	 * If the bag is empty, an empty cursor is returned. Otherwise,
	 * the bag is filtered by calling 
	 * <code>filter(bag.cursor(), o, ID)</code> which produces
	 * a cursor delivering all matching elements.
	 *  <br>
	 * <i>Note:</i>
	 * The returned iterator should not be used to remove any elements 
	 * from this implementor!
	 * 
	 * @param o The query object. This object is typically probed against
	 * 		the elements contained in this implementor.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return All matching elements of this implementor are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 * @see #filter(Iterator, Object, int)
	 */
	public Iterator<E> query (E o, int ID) {
		if (bag.size()==0) return new EmptyCursor<E>();
		return filter(bag.cursor(), o, ID);
	}

	public void createMetaDataManagement() {
		if (metaDataManagement != null)
			throw new IllegalStateException("An instance of MetaDataManagement already exists.");
		metaDataManagement = new BagSAImplementorMetaDataManagement();
	}

}
