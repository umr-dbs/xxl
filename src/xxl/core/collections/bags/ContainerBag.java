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

package xxl.core.collections.bags;

import java.util.Iterator;
import java.util.List;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.MapContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides an implementation of the Bag interface that
 * internally uses a container to store its elements. <p>
 *
 * Because this bag implementation only wraps a container and redirects
 * every method call to a corresponding method of the underlying
 * container, its performance and cursor behavior depends on the
 * container's functionality and cursor behavior.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new container bag
 *
 *     ContainerBag bag = new ContainerBag();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Iterator iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     bag.insertAll(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     Cursor cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     ((Cursor)iterator).close();
 *     cursor.close();
 *     bag.close();
 * </pre>
 *
 * Usage example (2).
 * <pre>
 *     // create a new container
 *
 *     Container container = new MapContainer();
 *
 *     // create an iteration over the Integer between 0 and 19
 *
 *     iterator = new Enumerator(20);
 *
 *     // insert all elements of the iterator into the container
 *
 *     Cursors.consume(container.insertAll(iterator));
 *
 *     // create a new container bag that contains all elements of the container
 *
 *     bag = new ContainerBag(container);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // remove every even Integer from the cursor (and the underlying container bag)
 *
 *     while (cursor.hasNext()) {
 *         int i = ((Integer)cursor.next()).intValue();
 *         if (i%2 == 0)
 *             cursor.remove();
 *     }
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     ((Cursor)iterator).close();
 *     cursor.close();
 *     bag.close();
 * </pre>
 *
 * @see Cursor
 * @see Cursors
 * @see Iterator
 * @see Function
 */
public class ContainerBag extends AbstractBag {

	/**
	 * A factory method to create a new ContainerBag (see contract for
	 * {@link Bag#FACTORY_METHOD FACTORY_METHOD} in interface Bag). It
	 * may be invoked with a <i>parameter list</i> (for further
	 * details see Function) of containers, an iterator or without any
	 * parameters. A <i>parameter list</i> of containers will be
	 * used to initialize the internally used container with the
	 * container at index 0 and an iterator will be used to insert the
	 * contained elements into the new ContainerBag.
	 *
	 * @see Function
	 */
	public static final Function<Object,ContainerBag> FACTORY_METHOD = new AbstractFunction<Object,ContainerBag> () {
		public ContainerBag invoke () {
			return new ContainerBag();
		}

		public ContainerBag invoke(Object iterator) {
			return new ContainerBag((Iterator) iterator);
		}

		public ContainerBag invoke (List<? extends Object> containerList) {
			return new ContainerBag((Container)containerList.get(0));
		}
	};

	/**
	 * The container is internally used to store the elements of the bag.
	 */
	protected Container container;

	/**
	 * Constructs a bag containing the elements of the container. The
	 * specified container is internally used to store the elements of
	 * the bag.
	 *
	 * @param container the container that is used to initialize the
	 *        internally used container.
	 */
	public ContainerBag (Container container) {
		this.container = container;
	}

	/**
	 * Constructs an empty bag. This bag instantiates a new MapContainer
	 * in order to store its elements. This constructor is equivalent to
	 * the call of <code>ContainerBag(new MapContainer())</code>.
	 */
	public ContainerBag () {
		this(new MapContainer());
	}

	/**
	 * Constructs a bag containing the elements of the container and the
	 * specified iterator. For the most cases the container is used to
	 * initialize the internally used container in order to guartantee a
	 * desired performance. This constructor calls the constructor with
	 * the specified container and uses the insertAll method to insert
	 * the elements of the specified iterator.
	 *
	 * @param container the container that is used to initialize the
	 *        internally used container.
	 * @param iterator the iterator whose elements are to be placed into
	 *        this bag.
	 */
	public ContainerBag(Container container, Iterator iterator){
		this(container);
		insertAll(iterator);
	}

	/**
	 * Constructs a bag containing the elements of the specified iterator.
	 * This bag instantiates a new MapContainer in order to store its
	 * elements. This constructor is equivalent to the call of
	 * <code>ContainerBag(new MapContainer(), iterator)</code>.
	 *
	 * @param iterator the iterator whose elements are to be placed into
	 *        this bag.
	 */
	public ContainerBag(Iterator iterator){
		this(new MapContainer(), iterator);
	}

	/**
	 * Removes all of the elements from this bag. The bag will be empty
	 * after this call so that <tt>size() == 0</tt>.<br>
	 * This implementation simply calls the <tt>clear</tt> method of the
	 * wrapped container.
	 */
	public void clear () {
		container.clear();
	}

	/**
	 * Closes this bag and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method take the same effect as a single call. When needed, a
	 * closed bag can be implicitly reopened by a consecutive call to one of
	 * its methods. Because of having an unspecified behavior when this
	 * bag is closed every cursor iterating over the elements of this bag
	 * must be closed.<br>
	 * This implementation simply calls the <tt>close</tt> method of the
	 * wrapped container.
	 */
	public void close () {
		container.close();
	}

	/**
	 * Returns a cursor to iterate over the elements in this bag without
	 * any predefined sequence. The cursor is specifying a <i>view</i> on
	 * the elements of this bag so that closing the cursor takes no
	 * effect on the bag (e.g., not closing the bag). The behavior
	 * of the cursor is unspecified if this bag is modified while the
	 * cursor is in progress in any way other than by calling the methods
	 * of the cursor.<br>
	 * This implementation simply calls the <tt>objects</tt> method of
	 * the wrapped container.
	 *
	 * @return a cursor to iterate over the elements in this bag without
	 *         any predefined sequence.
	 */
	public Cursor cursor () {
		return container.objects();
	}

	/**
	 * Adds the specified element to this bag. This method does not
	 * perform any kind of <i>duplicate detection</i>.<br>
	 * This implementation simply calls the <tt>insert</tt> method of the
	 * wrapped container.
	 *
	 * @param object element to be added to this bag.
	 */
	public void insert (Object object) {
		container.insert(object);
	}

	/**
	 * Adds all of the elements in the specified iterator to this bag.
	 * This method does not perform any kind of <i>duplicate
	 * detection.</i> The behavior of this operation is unspecified if
	 * the specified iterator is modified while the operation is in
	 * progress.<br>
	 * This implementation simply consumes the iterator returned by the
	 * wrapped container's <tt>insertAll</tt> method.
	 *
	 * @param objects iterator whose elements are to be added to this bag.
	 */
	public void insertAll (Iterator objects) {
		Cursors.consume(container.insertAll(objects));
	}

	/**
	 * Returns the number of elements in this bag (its cardinality). If
	 * this bag contains more than <tt>Integer.MAX_VALUE</tt> elements,
	 * <tt>Integer.MAX_VALUE</tt> is returned.<br>
	 * This implementation simply calls the <tt>size</tt> method of the
	 * wrapped container.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	public int size () {
		return container.size();
	}
}
