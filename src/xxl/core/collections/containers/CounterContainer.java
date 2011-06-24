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

package xxl.core.collections.containers;

import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class decorates a container with a counter functionality and
 * counts every call of an insert, get, update and remove method. But the
 * counter only works correctly when a container is decorated that
 * insertAll, getAll, updateAll and removeAll methods are based the
 * <i>counted</i> insert, get, update and remove methods. This must also
 * hold for the iterators ids() and objects(). This constrained decorator
 * is interesting for statistical use or the performance analysis of
 * containers.<p>
 *
 * The numbers of calls are stored in public fields and can be reset by
 * the reset method.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new counter container decorating an empty map container
 *
 *     CounterContainer container = new CounterContainer(new MapContainer());
 *
 *     // reset the counter container
 *
 *     container.reset();
 *
 *     // insert 20 elements and print the counters
 *
 *     for (int i = 0; i < 20; i++)
 *         container.insert(new Integer(i));
 *     System.out.println(container);
 *     System.out.println();
 *
 *     // create an iteration over the ids of the counter container
 *
 *     java.util.Iterator iterator = container.ids();
 *
 *     // get 10 elements and print the counters
 *
 *     for (int i = 0; i < 10; i++)
 *         if (iterator.hasNext())
 *             container.get(iterator.next());
 *     System.out.println(container);
 *     System.out.println();
 *
 *     // update 5 elements and print the counters
 *
 *     for (int i = 0; i < 5; i++)
 *         if (iterator.hasNext())
 *             container.update(iterator.next(), new Integer(i));
 *     System.out.println(container);
 *     System.out.println();
 *
 *     // remove an element and print the counters
 *
 *     if (iterator.hasNext())
 *         container.remove(iterator.next());
 *     System.out.println(container);
 *     System.out.println();
 *
 *     // close the open container after use
 *
 *     container.close();
 * </pre>
 *
 * @see Function
 * @see NoSuchElementException
 */
public class CounterContainer extends ConstrainedDecoratorContainer {

	/**
	 * A factory method to create a new CounterContainer. It may
	 * only be invoked with a <i>parameter list</i> (for further
	 * details see Function) of containers. The <i>parameter
	 * list</i> of containers will be used to initialize the decorated
	 * container by calling the constructor
	 * <code>CounterContainer(list.get(0))</code>.
	 *
	 * @see Function
	 */
	public static final Function<Container,CounterContainer> FACTORY_METHOD = new AbstractFunction<Container,CounterContainer>() {
		public CounterContainer invoke (List<? extends Container> list) {
			return new CounterContainer(list.get(0));
		}
	};

	/**
	 * The number of times an insert method is called on this container
	 * since the last reset. This number is identical to the number of
	 * elements that are inserted since the last reset because the
	 * insertAll method calls for every element the insert method.
	 */
	public int inserts = 0;

	/**
	 * The number of times a get method is called on this container since
	 * the last reset. This number is identical to the number of elements
	 * that are got since the last reset because the getAll method calls
	 * for every element the get method.
	 */
	public int gets = 0;

	/**
	 * The number of times an update method is called on this container
	 * since the last reset. This number is identical to the number of
	 * elements that are updated since the last reset because the
	 * updateAll method calls for every element the update method.
	 */
	public int updates = 0;

	/**
	 * The number of times a remove method is called on this container
	 * since the last reset. This number is identical to the number of
	 * elements that are removed since the last reset because the
	 * removeAll method calls for every element the remove method.
	 */
	public int removes = 0;

	/**
	 * The number of times a reserve method is called on this container
	 * since the last reset.
	 */
	public int reserves = 0;

	/**
	 * Constructes a new CounterContainer that decorates the specified
	 * container.
	 *
	 * @param container the container to be decorated with the counter.
	 */
	public CounterContainer (Container container) {
		super(container);
	}

	/**
	 * Resets the counters for insert, get, update and remove methods.
	 * A call of this method sets every counter to 0.
	 */
	public void reset () {
		inserts = gets = updates = removes = reserves = 0;
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown if there is not object stored with this
	 * <tt>id</tt>. If unfix is set to true, the object can be removed
	 * from the underlying buffer. Otherwise (!unfix), the object has
	 * to be kept in the buffer.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		Object object = super.get(id, unfix);

		gets++;
		return object;
	}

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * The identifier can be reused again when the object is deleted from
	 * the buffer. If unfixed, the object can be removed from the buffer.
	 * Otherwise, it has to be kept in the buffer until an
	 * <tt>unfix()</tt> is called.<br>
	 * After an insertion all the iterators operating on the container can
	 * be in an invalid state.<br>
	 * This method also allows an insertion of a null object. In the
	 * application would really like to have such objects in the
	 * container, some methods have to be modified.
	 *
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		Object id = super.insert(object, unfix);

		inserts++;
		return id;
	}

	/**
	 * Removes the object with identifier <tt>id</tt>. An exception is
	 * thrown when an object with an identifier <tt>id</tt> is not in the
	 * container.<br>
	 * After a call of <tt>remove()</tt> all the iterators (and cursors)
	 * can be in an invalid state.
	 *
	 * @param id an identifier of an object.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	public void remove (Object id) throws NoSuchElementException {
		super.remove(id);
		removes++;
	}

	/**
	 * Reserves an id for subsequent use. The container may or may not
	 * need an object to be able to reserve an id, depending on the
	 * implementation. If so, it will call the parameterless function
	 * provided by the parameter <tt>getObject</tt>.
	 *
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved.
	 * @return the reserved id.
	*/
	public Object reserve (Function getObject) {
		Object id = super.reserve(getObject);

		reserves++;
		return id;
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container.
	 *
	 * @param id identifier of the element.
	 * @param object the new object that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
		super.update(id, object, unfix);
		updates++;
	}

	/**
	 * Outputs the collected statistic to a String.
	 * @return a String representation of the collected statistics. 
	 */
	public String toString() {
		return "inserts = "+inserts + ", " +
		       "gets = "+gets + ", " +
		       "updates = "+updates + ", " +
		       "removes = "+removes + ", " +
		       "reserves = "+reserves;
	}
}
