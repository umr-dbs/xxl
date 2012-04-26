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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.predicates.AbstractPredicate;

/**
 * An AbstractContainer implements all methods of the Container interface
 * except the central methods get, isUsed, ids, objectIdConverter, remove, 
 * reserve, size and update which are abstract.<p>
 * 
 * It can be much more efficient to implement more methods inside a
 * real container, especially implementation of insert is strongly recommended.<p>
 *
 * Subclasses of AbstractContainer are available which manage the data in main
 * memory (e.g. {@link MapContainer MapContainer}) and external storage.
 * The later case will be more common. In order to reduce the number of
 * accesses to external memory, a buffer can be used to keep "interesting"
 * data in main memory. This aspect has some impact on the Container
 * class. Many of the methods below offer a parameter <tt>unfix</tt>. If
 * true, the method automatically has to unfix the object at the end of the
 * operation, i.e. an underlying buffer is allowed to remove the object
 * from memory (probably having it written back to external memory before).
 * Otherwise (<tt>unfix == false</tt>), the object will remain fixed, i.e.
 * the object can only be removed from memory if the method <tt>unfix()</tt>
 * has been called explicitly before. When a container uses a buffer,
 * the method <tt>flush()</tt> has to be implemented. The method writes
 * back the modified objects from the buffer to the container.
 *
 * @see Container
 */
public abstract class AbstractContainer implements Container {

	/**
	 * A factory method to create a default Container. Each
	 * implementation of Container should have a function FACTORY_METHOD
	 * that implements two variants of <code>invoke()</code><br>
	 * <ul>
	 * <dl>
	 * <dt><li><code>Object invoke()</code>:</dt>
	 * <dd>returns <code>new Container()</code>.</dd>
	 * <dt><li><code>Object invoke(Object[] internalDataStructure)</code>:</dt>
	 * <dd>returns <code>new Container((&lt;<i>InternalDataStructure&gt;</i>)internalDataStructure[0])</code>.</dd>
	 * </dl>
	 * </ul>
	 * This field is set to
	 * <code>{@link MapContainer#FACTORY_METHOD MapContainer.FACTORY_METHOD}</code>.
	 *
	 * @see Function#invoke()
	 * @see Function#invoke(List)
	 */
	public static final Function FACTORY_METHOD = MapContainer.FACTORY_METHOD;

	/**
	 * Sole constructor. (For invocation by subclass constructors,
	 * typically implicit.)
	 */
	public AbstractContainer() {
	}

	/**
	 * Removes all elements from the Container. After a call of this
	 * method, <tt>size()</tt> will return 0.<br>
	 * Note, that the implementation of this method relies on the remove
	 * operation of the iterator returned by the method <tt>ids()</tt>.
	 */
	public void clear () {
		Cursors.removeAll(ids());
	}

	/**
	 * Closes the Container and releases all sources. For external
	 * containers, this method closes the files immediately. MOREOVER, all
	 * iterators operating on the container can be in illegal states.
	 * Close can be called a second time without any impact. The default
	 * implementation of close is empty (which is OK for a MapContainer).<br>
	 */
	public void close () {
	}

	/**
	 * Returns true if there is an object stored within the container
	 * having the identifier <tt>id</tt>.
	 * This implementation uses the get method to make the decision.
	 *
	 * @param id identifier of the object.
	 * @return true if the container contains an object for the specified
	 *         identifier.
	 */
	public boolean contains (Object id) {
		try {
			get(id);

			return true;
		}
		catch (NoSuchElementException nsee) {
			return false;
		}
	}

	/**
	 * Flushes all modified elements from the buffer into the container.
	 * After this call the buffer and the container are synchronized.
	 * The default implementation of flush is empty which is OK for an
	 * unbuffered container.
	 */
	public void flush () {
	}

	/**
	 * Flushes the object with identifier <tt>id</tt> from the buffer into
	 * the container. The default implementation of flush is empty which is
	 * OK for an unbuffered container.
	 *
	 * @param id identifier of the object that should be written back.
	 */
	public void flush (Object id) {
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
	public abstract Object get (Object id, boolean unfix) throws NoSuchElementException;

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. It is
	 * equivalent to get(id, true).
	 *
	 * @param id identifier of the object.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id) {
		return get(id, true);
	}

	/**
	 * Returns an iterator referring to the objects of the identifiers
	 * which are delivered by the iterator <tt>ids</tt>s. The parameter
	 * <tt>unfix</tt> specifies whether the object can be removed from the
	 * buffer, see also the discussion of the method <tt>get()</tt>.<br>
	 * The default implementation of this method is lazy, i. e. when the
	 * output iterator should deliver the next element, next is called for
	 * the input iterator.
	 *
	 * @param ids an iterator of identifiers.
	 * @param unfix signals whether the objects of the iterator can be
	 *        removed from the underlying buffer.
	 * @return an iterator of objects.
	 * @throws NoSuchElementException if one of the desired objects is not
	 *         found.
	 */
	public Iterator getAll (Iterator ids, final boolean unfix) {
		return new Mapper(
			new AbstractFunction () {
				public Object invoke (Object id) {
					return get(id, unfix);
				}
			},
			ids
		);
	}

	/**
	 * Returns an iterator pointing to the Object of the given ids. It
	 * behaves like a call of <tt>getAll(ids, true)</tt>.
	 *
	 * @param ids an iterator of object identifiers.
	 * @return an iterator of objects.
	 * @throws NoSuchElementException if one of the desired objects is not
	 *         found.
	 */
	public Iterator getAll (Iterator ids) {
		return getAll(ids, true);
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	public abstract Iterator ids ();

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * The identifier can be reused again when the object is deleted from
	 * the container. If unfixed, the object can be removed from the
	 * buffer. Otherwise, it has to be kept in the buffer until an
	 * <tt>unfix()</tt> is called.<br>
	 * After an insertion all the iterators operating on the container can
	 * be in an invalid state.<br>
	 * This method also allows an insertion of a null object. In the
	 * application would really like to have such objects in the
	 * container, some methods have to be modified.
	 * This implementation first reserves an id and then updates the id
	 * with the given object.
	 *
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		Object id = reserve(new Constant(object));

		update(id, object, unfix);
		return id;
	}

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier. This methods behaves like
	 * <tt>insert(object, true)</tt>.
	 *
	 * @param object is the new object.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object) {
		return insert(object, true);
	}

	/**
	 * Inserts all objects of a given Iterator into the container. It
	 * returns an iterator that contains the identifiers of the new
	 * objects.<br>
	 * Note, that the order of the identifiers corresponds to the order of
	 * their objects. The meaning of unfix is the same as for the method
	 * insert. The default implementation is based on the functional
	 * concept (see class {@link Function Function}) and processes the
	 * input in a lazy fashion. An insertion of an object is performed
	 * when the application program operates on the output iterator!
	 *
	 * @param objects an iterator of objects that should be inserted.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return an iterator containing the identifiers of the objects of
	 *         the input iterator.
	 */
	public Iterator insertAll (Iterator objects, final boolean unfix) {
		return new Mapper(
			new AbstractFunction () {
				public Object invoke (Object object) {
					return insert(object, unfix);
				}
			},
			objects
		);
	}

	/**
	 * Inserts all objects of a given Iterator into the container. This
	 * method behaves like <tt>insertAll(objects, true)</tt>.
	 *
	 * @param objects an iterator of objects that should be inserted.
	 * @return an iterator containing the identifiers of the objects of
	 *         the input iterator.
	 */
	public Iterator insertAll (Iterator objects) {
		return insertAll(objects, true);
	}

	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to insert or reserve and hasn't been removed so far.
	 *
	 * @param id the id to be checked.
	 * @return true exactly if the <tt>id</tt> is still in use.
	 */
	public abstract boolean isUsed (Object id);

	/**
	 * Returns a cursor containing all objects of the container. The order
	 * in which the cursor delivers the objects is not specified.<br>
	 * The default implementation filters the iterator that is returned
	 * by the method <tt>ids()</tt> by dropping all ids that doesn't
	 * contain an object. The objects are retrieved from the container
	 * by using the method <tt>get()</tt>.
	 *
	 * @return a cursor containing all objects of the container.
	 */
	public Cursor objects () {
		return new AbstractCursor() {
			Iterator ids = new Filter(ids(),
				new AbstractPredicate() {
					public boolean invoke(Object id) {
						return AbstractContainer.this.contains(id);
					}
				}
			);
			Object id;
		
			public boolean hasNextObject() {
				return ids.hasNext();
			}

			public Object nextObject() {
				id = ids.next();
				return AbstractContainer.this.get(id);
			}

			public void remove () throws IllegalStateException {
				super.remove();
				ids.remove();
			}
			
			public boolean supportsRemove() {
				return true;
			}

			public void reset () {
				super.reset();
				ids = ids();
			}
			
			public boolean supportsReset() {
				return true;
			}

			public void update (Object object) throws IllegalStateException {
				super.update(object);
				AbstractContainer.this.update(id, object);
			}
			
			public boolean supportsUpdate() {
				return true;
			}
			
		};
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * Since the identifier may have an arbitrary type (which has to be
	 * known in the container), the container has to provide such a method
	 * when the data is not stored in main memory.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 */
	public abstract FixedSizeConverter objectIdConverter ();

	/**
	 * Returns the size of the ids generated by this container in bytes.
	 * Each id must have the same size.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return objectIdConverter().getSerializedSize();
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
	public abstract void remove (Object id) throws NoSuchElementException;

	/**
	 * Removes the objects with the identifiers given by the iterator
	 * <tt>ids</tt>.<br>
	 * The default implementation calls <tt>remove(id)</tt> for each
	 * identifier of <tt>ids</tt>.
	 *
	 * @param ids an iterator containing identifiers of objects.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> is not in the container.
	 */
	public void removeAll (Iterator ids) {
		while (ids.hasNext())
			remove(ids.next());
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
	public abstract Object reserve (Function getObject);

	/**
	 * Returns the number of elements of the container.
	 *
	 * @return the number of elements.
	 */
	public abstract int size ();

	/**
	 * Unfixes the Object with identifier <tt>id</tt>. This method throws
	 * an exception when the identifier <tt>id</tt> isn't used by
	 * the container. After one call of unfix the buffer is allowed to
	 * remove the object (although the objects have been fixed more than
	 * once).<br>
	 * The default implementation only checks whether an object with
	 * identifier <tt>id</tt> is in the buffer.
	 *
	 * @param id identifier of an object that should be unfixed in the
	 *        buffer.
	 * @throws NoSuchElementException if the identifier <tt>id</tt>
	 *         isn't used by the container.
	 */
	public void unfix (Object id) throws NoSuchElementException {
		if (!isUsed(id))
			throw new NoSuchElementException();
	}

	/**
	 * Unfixes the objects with identifiers given by iterator
	 * <tt>ids</tt>. All the objects are unfixed which belong to one of
	 * the identifiers of ids.<br>
	 * The default implementation calls the method <tt>unfix()</tt> for
	 * each identifier of <tt>ids</tt>.
	 *
	 * @param ids an iterator of identifiers.
	 * @throws NoSuchElementException if the identifier <tt>id</tt>
	 *         isn't used by the container.
	 */
	public void unfixAll (Iterator ids) {
		while (ids.hasNext())
			unfix(ids.next());
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
	public abstract void update (Object id, Object object, boolean unfix) throws NoSuchElementException;

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container. The default implementation only
	 * consists of a call of <tt>update(id, object, true)</tt>.
	 *
	 * @param id identifier of the element.
	 * @param object the new object that should be associated to
	 *        <tt>id</tt>.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	public void update (Object id, Object object) {
		update(id, object, true);
	}

	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. For each identifier of <tt>ids</tt>, this
	 * method  calls <tt>update(id, function.invoke(id))</tt>.
	 *
	 * @param ids an iterator of identifiers.
	 * @param function a function that computes the object which should be
	 *        associated to an identifier.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	public void updateAll (Iterator ids, final Function function, boolean unfix) {
		while (ids.hasNext()) {
			Object id = ids.next();
			update(id, function.invoke(id), unfix);
		}
	}

	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. This method is equivalent to
	 * <tt>updateAll(ids, function, true)</tt>.
	 *
	 * @param ids an iterator of identifiers.
	 * @param function a function that computes the object which should be
	 *        associated to an identifier.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	public void updateAll (Iterator ids, Function function) {
		updateAll(ids, function, true);
	}

	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. The objects that have to be associated to
	 * the elements are also given as an iterator.
	 *
	 * @param ids an iterator of identifiers
	 * @param objects an iterator of objects
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	public void updateAll (Iterator ids, Iterator objects, boolean unfix) {
		while (ids.hasNext() && objects.hasNext())
			update(ids.next(), objects.next(), unfix);
	}

	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. This method is equivalent to
	 * <tt>updateAll(ids, objects, true)</tt>.
	 *
	 * @param ids an iterator of identifiers
	 * @param objects an iterator of objects
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	public void updateAll (Iterator ids, Iterator objects) {
		updateAll(ids, objects, true);
	}
	
	@Override
	public Object[] batchInsert(Object[] blocks) {
		throw new UnsupportedOperationException();
	}
}
