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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.util.XXLSystem;

/**
 * This class provides an implementation of the Container interface that
 * internally uses a map to store the (id,*)-elements. Most methods
 * directly call the methods of the internally used map.
 * <p>
 * This class supports a clone mode that means every Object which
 * is inserted into/retrieved from the map is cloned. This clone
 * method must use the reflection API because clone is protected inside
 * the class Object. If you do not clone the objects, then a change of
 * the inserted Object also changes the Object inside the Container. So
 * you might have side effects which certainly will cause a different
 * behaviour when you switch to an external Container (which cannot
 * have side effects).
 * <p>
 * If you want to develop an application which works on external memory,
 * then test your application  with a MapContainer without clone mode first,
 * then with the clone mode and then switch to the external Container.
 * <p>
 * The iterators returned by this class' <tt>ids</tt> and
 * <tt>objects</tt> methods are fail-fast: if the container is modified at
 * any time after the iterator is created, in any way except through the
 * iterator's own remove method, the iterator throws a
 * ConcurrentModificationException. Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than
 * risking arbitrary, non-deterministic behavior at an undetermined time
 * in the future.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new map container (that uses a hash map to store its elements per default)
 *
 *     MapContainer container = new MapContainer();
 *
 *     // create an iteration over the Integer between 0 and 19
 *
 *     Iterator iterator = new Enumerator(20);
 *
 *     // insert all elements of the given iterator and save the returned iterator of the ids
 *
 *     iterator = container.insertAll(iterator);
 *
 *     // print all elements of the container
 *
 *     while (iterator.hasNext())
 *         System.out.println(container.get(iterator.next()));
 *
 *     // close the open container after use
 *
 *     container.close();
 * </pre>
 *
 * Usage example (2).
 * <pre>
 *     // create a new map container that uses a tree map to store its elements
 *
 *     container = new MapContainer(new java.util.TreeMap());
 *
 *     // create an iteration over the Integer between 0 and 19
 *
 *     iterator = new Enumerator(20);
 *
 *     // insert all elements of the given iterator
 *
 *     iterator = container.insertAll(iterator);
 *
 *     // consume iterator in order to insert elements (lazy evaluation)
 *
 *     while (iterator.hasNext())
 *         iterator.next();
 *
 *     // generate an iteration over the ids of the container
 *
 *     iterator = container.ids();
 *
 *     // look at every id and ...
 *
 *     while (iterator.hasNext()) {
 *         Object id = iterator.next();
 *         int i = ((Integer)container.get(id)).intValue();
 *
 *         // remove every Integer that is smaller than 5
 *
 *         if (i < 5) {
 *             container.remove(id);
 *
 *             // uddate the iteration over the ids because the queue has been modified
 *
 *             iterator = container.ids();
 *         }
 *         else
 *
 *             // update every odd Integer by multiplying it with 10
 *
 *             if (i%2 != 0)
 *                 container.update(id, new Integer(i*10));
 *     }
 *
 *     // generate an iteration over the objects of the container
 *
 *     iterator = container.objects();
 *
 *     // print all elements of the container
 *
 *     while (iterator.hasNext())
 *         System.out.println(iterator.next());
 *
 *     // close the open container after use
 *
 *     container.close();
 * </pre>
 *
 * @see xxl.core.io.converters.Converter
 * @see Function
 * @see HashMap
 * @see Iterator
 * @see LongConverter
 * @see Map
 * @see java.util.Map.Entry
 * @see xxl.core.cursors.mappers.Mapper
 * @see NoSuchElementException
 */
public class MapContainer extends AbstractContainer {

	/**
	 * A factory method to create a new MapContainer. It may be invoked 
	 * with a <i>parameter list</i> (for further details see Function)
	 * of maps, an iterator or without any parameters. A 
	 * <i>parameter list</i> of maps will be used to initialize the 
	 * internally used map with the map at index 0 and an iterator will 
	 * be used to insert the contained elements into the new MapContainer.
	 *
	 * @see Function
	 */
	public static final Function<Object,MapContainer> FACTORY_METHOD = new AbstractFunction<Object,MapContainer> () {
		public MapContainer invoke () {
			return new MapContainer();
		}

		public MapContainer invoke (List<? extends Object> list) {
			return new MapContainer((Map) list.get(0));
		}
	};

	/**
	 * A counter that is used to create unique ids. Everytime an object is
	 * inserted into the container the counter is increased and a
	 * <tt>Long</tt> object with the actual value of the counter is
	 * returned as id.
	 */
	protected long counter = 0;

	/**
	 * The map is internally used to store the (id,*)-elements of the
	 * container.
	 */
	protected Map map;
	
	/**
	 * Flag which determines if every object is cloned before storing
	 * and before returning it.
	 */
	protected boolean cloneObjects;

	/**
	 * A unique object used to identify mappings where no object has been
	 * assigned to so far.
	 */
	protected static final Object empty = new Object();

	/**
	 * Constructs a container containing the (id,*)-elements of the map.
	 * The specified map is internally used to store the entries of the
	 * container.
	 *
	 * @param map the map that is used to initialize the internally used
	 *	map.
	 * @param cloneObjects determines if every object is cloned before storing
	 *	and before returning it. The clone mode is a lot slower, because
	 *	reflection is needed to call the protected clone method.
	 */
	public MapContainer(Map map, boolean cloneObjects) {
		this.map = map;
		this.cloneObjects = cloneObjects;
	}

	/**
	 * Constructs a container containing the (id,*)-elements of the map.
	 * The specified map is internally used to store the entries of the
	 * container. Objects are not cloned before insertion and before retrieval. 
	 * So beware of side effects.
	 *
	 * @param map the map that is used to initialize the internally used
	 *	map.
	 */
	public MapContainer(Map map) {
		this(map,false);
	}

	/**
	 * Constructs an empty container. This container instantiates a new
	 * HashMap in order to store its (id,*)-elements.
	 * @param cloneObjects determines if every object is cloned before storing
	 *	and before returning it. The clone mode is a lot slower, because
	 *	reflection is needed to call the protected clone Method.
	 */
	public MapContainer(boolean cloneObjects){
		this(new HashMap(),cloneObjects);
	}

	/**
	 * Constructs an empty container. This container instantiates a new
	 * HashMap in order to store its (id,*)-elements. Objects are not
	 * cloned before insertion and before retrieval. So beware of side
	 * effects.
	 */
	public MapContainer(){
		this(new HashMap());
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * Because this container is returning <tt>Long</tt> objects as ids
	 * the converter <code>LongConverter.DEFAULT_INSTANCE</code> is
	 * returned.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 * @see LongConverter#DEFAULT_INSTANCE
	 */
	public FixedSizeConverter objectIdConverter () {
		return LongConverter.DEFAULT_INSTANCE;
	}

	/**
	 * Returns the size of the ids generated by this container in bytes,
	 * which is 8.
	 * @return 8
	 */
	public int getIdSize() {
		return LongConverter.SIZE;
	}

	/**
	 * Removes all elements from this container. After a call of this
	 * method, <tt>size()</tt> will return 0.
	 */
	public void clear () {
		map.clear();
	}

	/**
	 * Returns <tt>true</tt> if there is an object stored within the container
	 * having the identifier <tt>id</tt>.
	 *
	 * @param id identifier of the object.
	 * @return true if the container contains an object for the specified
	 *         identifier.
	 */
	public boolean contains (Object id) {
		return map.containsKey(id) && map.get(id)!=empty;
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown if there is not object stored with this
	 * <tt>id</tt>. The parameter <tt>unfix</tt> has no function because this
	 * container is unbuffered.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		if (!contains(id))
			throw new NoSuchElementException();
		if (cloneObjects)
			return XXLSystem.cloneObject(map.get(id));
		else
			return map.get(id);
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	public Iterator ids () {
		return map.keySet().iterator();
		/* ??? return new Mapper(map.entrySet().iterator(),
			new AbstractFunction() {
				public Object invoke (Object object) {
					return ((Entry)object).getKey();
				}
			}
		);*/
	}

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * This container uses a counter to generate an unique id. Everytime
	 * an object is inserted into the container the counter is increased
	 * and a <tt>Long</tt> object with the actual value of the counter is
	 * returned as id. So the identifier will not be reused again when the
	 * object is deleted from the container. The parameter <tt>unfix</tt> 
	 * has no function because this container is unbuffered.
	 *
	 * @param object is the new object.
	 * @param unfix signals a buffered container whether the object can
	 *        be removed from the underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		Object id = new Long(counter++);
		if (cloneObjects)
			map.put(id, XXLSystem.cloneObject(object));
		else
			map.put(id, object);
		return id;
	}

	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to insert or reserve and hasn't been removed so far.
	 *
	 * @param id the id to be checked.
	 * @return true exactly if the <tt>id</tt> is still in use.
	 */
	public boolean isUsed (Object id) {
		return map.containsKey(id);
	}

	/**
	 * Removes the object with identifier <tt>id</tt>. An exception is
	 * thrown when an object with an identifier <tt>id</tt> is not in the
	 * container.
	 *
	 * @param id an identifier of an object.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	public void remove (Object id) throws NoSuchElementException {
		if (isUsed(id))
			map.remove(id);
		else
			throw new NoSuchElementException();
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
		Object id = new Long(counter++);
		// do not use insert because cloning of empty must not be done.
		map.put(id, empty);
		return id;
	}

	/**
	 * Returns the number of elements of the container.
	 *
	 * @return the number of elements.
	 */
	public int size () {
		return map.size();
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container.
	 *
	 * @param id identifier of the element.
	 * @param object the new object that should be associated to
	 *        <tt>id</tt>.
	 * @param unfix signals a buffered container whether the object can
	 *        be removed from the underlying buffer.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {
		if (isUsed(id)) {
			if (cloneObjects)
				map.put(id, XXLSystem.cloneObject(object));
			else
				map.put(id, object);
		}
		else
			throw new NoSuchElementException();
	}
}
