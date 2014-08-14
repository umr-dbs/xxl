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
import java.util.NoSuchElementException;

import xxl.core.cursors.Cursor;
import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;

/**
 * A Container is the interface implemented by all classes that deal with the
 * management of sets of objects.<p>
 *
 * A Container represents a set of items, known as its elements. An
 * element is a tuple consisting of an identifier <tt>id</tt> and an
 * <tt>object</tt>. An object in a container can only be retrieved when
 * the corresponding identifier is given. When inserting an object, or
 * when reserving space for an object, the container creates a new
 * <tt>id</tt> for this object and returns it to the caller. When an
 * object is deleted from a container, its <tt>id</tt> can be reused again.<p>
 *
 * An iterator is frequently used to retrieve the ids of a container, see
 * the method <tt>ids()</tt>. Moreover, a cursor is already implemented
 * that supports the direct access to the objects, see the method
 * <tt>objects()</tt>. When a collection is closed (see method
 * <tt>close()</tt>) the iterators and cursors can be in illegal states.
 * It might be that an iterator will still deliver data although the
 * corresponding container is already closed.<p>
 *
 * When an element is directly deleted from a container via
 * <tt>remove()</tt> or an element is inserted into a container via
 * <tt>insert()</tt>, the iterators and cursors can be in an illegal
 * state, see also the discussion in the previous paragraph. Note that
 * iterators and cursors may also have an operation <tt>remove()</tt>. In
 * particular, the <tt>remove()</tt> operation of an iterator should not
 * rely on the <tt>remove()</tt> operation of Container. When
 * <tt>remove()</tt> of an iterator is called, the iterator will still be
 * in a valid state. HOWEVER, the other iterators of the corresponding
 * container can be in illegal states. A similar problem occurs for
 * updates. When a cursor updates an element of the container, the other
 * iterators (and cursors) can be in illegal states. When a user of this
 * class is interested in a more restrictive semantic, he/she is advised
 * to implement the abstract methods <tt>ids()</tt> and <tt>objects()</tt>
 * adequately. Note, that these are the only methods for generating
 * iterators operating directly on a container.<p>
 *
 * Implementations of Container are available which manage the data in main
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
 * @see AbstractContainer
 * @see xxl.core.io.converters.Converter
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.Cursors
 * @see xxl.core.io.converters.FixedSizeConverter
 * @see Function
 * @see Iterator
 * @see xxl.core.cursors.mappers.Mapper
 * @see NoSuchElementException
 */
public interface Container {
	/**
	 * Removes all elements from the Container. After a call of this
	 * method, <tt>size()</tt> will return 0.<br>
	 */
	void clear();
	
	/**
	 * Closes the Container and releases all sources. For external
	 * containers, this method closes the files immediately. MOREOVER, all
	 * iterators operating on the container can be in illegal states.
	 * Close can be called a second time without any impact. 
	 * Note, that it would be desirable that the finalize-mechanism of
	 * Java would already offer the functionality of close. However,
	 * finalize does not release the sources immediately! Consequently,
	 * the corresponding file of a "closed" Container may be opened and
	 * some of the data is still not written back. This is a problem when
	 * for example the JVM stops running (because of a system error).
	 */
	void close();
	
	/**
	 * Returns <tt>true</tt> if there is an object stored within the container
	 * having the identifier <tt>id</tt>.
	 *
	 * @param id identifier of the object.
	 * @return true if the container contains an object for the specified
	 *         identifier.
	 */
	boolean contains(Object id);
	
	/**
	 * Flushes all modified elements from the buffer into the container.
	 * After this call the buffer and the container are synchronized.
	 */
	void flush();
	
	/**
	 * Flushes the object with identifier <tt>id</tt> from the buffer into
	 * the container. 
	 *
	 * @param id identifier of the object that should be written back.
	 */
	void flush(Object id);
	
	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown if there is not object stored with this
	 * <tt>id</tt>. If unfix is set to <tt>true</tt>, the object can be removed
	 * from the underlying buffer. Otherwise (<tt>!unfix</tt>), the object has
	 * to be kept in the buffer.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	Object get(Object id, boolean unfix) throws NoSuchElementException;
	
	/**
	 * Returns the object associated to the identifier <tt>id</tt>. It is
	 * equivalent to <tt>get(id, true)</tt>.
	 *
	 * @param id identifier of the object.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	Object get(Object id) throws NoSuchElementException;
	
	/**
	 * Returns an iterator referring to the objects of the identifiers
	 * which are delivered by the iterator <tt>ids</tt>. The parameter
	 * <tt>unfix</tt> specifies whether the object can be removed from the
	 * buffer, see also the discussion of the method <tt>get()</tt>.<br>
	 *
	 * @param ids an iterator of identifiers.
	 * @param unfix signals whether the objects of the iterator can be
	 *        removed from the underlying buffer.
	 * @return an iterator of objects.
	 * @throws NoSuchElementException if one of the desired objects is not
	 *         found.
	 */
	Iterator getAll(Iterator ids, final boolean unfix) throws NoSuchElementException;
	
	/**
	 * Returns an iterator pointing to the Object of the given ids. It
	 * should behave like a call of <tt>getAll(ids, true)</tt>.
	 *
	 * @param ids an iterator of object identifiers.
	 * @return an iterator of objects.
	 * @throws NoSuchElementException if one of the desired objects is not
	 *         found.
	 */
	Iterator getAll(Iterator ids) throws NoSuchElementException;
	
	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	Iterator ids();
	
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
	 *
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	Object insert(Object object, boolean unfix);
	
	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier. This methods should behave like
	 * <tt>insert(object, true)</tt>.
	 *
	 * @param object is the new object.
	 * @return the identifier of the object.
	 */
	Object insert(Object object);
	
	/**
	 * Inserts all objects of a given Iterator into the container. It
	 * returns an iterator that contains the identifiers of the new
	 * objects.<br>
	 * Note, that the order of the identifiers corresponds to the order of
	 * their objects. The meaning of <tt>unfix</tt> is the same as for the method
	 * insert.
	 *
	 * @param objects an iterator of objects that should be inserted.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return an iterator containing the identifiers of the objects of
	 *         the input iterator.
	 */
	Iterator insertAll(Iterator objects, final boolean unfix);
	
	/**
	 * Inserts all objects of a given Iterator into the container. This
	 * method should behave like <tt>insertAll(objects, true)</tt>.
	 *
	 * @param objects an iterator of objects that should be inserted.
	 * @return an iterator containing the identifiers of the objects of
	 *         the input iterator.
	 */
	Iterator insertAll(Iterator objects);
	
	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to insert or reserve and hasn't been removed so far.
	 *
	 * @param id the id to be checked.
	 * @return true exactly if the <tt>id</tt> is still in use.
	 */
	boolean isUsed(Object id);
	
	/**
	 * Returns a cursor containing all objects of the container. The order
	 * in which the cursor delivers the objects is not specified.<br>
	 *
	 * @return a cursor containing all objects of the container.
	 */
	Cursor objects();
	
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
	FixedSizeConverter objectIdConverter();
	
	/**
	 * Returns the size of the ids generated by this container in bytes.
	 * Each id must have the same size.
	 * @return the size in bytes of each id.
	 */
	int getIdSize();
	
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
	void remove(Object id) throws NoSuchElementException;
	
	/**
	 * Removes the objects with the identifiers given by the iterator
	 * <tt>ids</tt>.<br>
	 *
	 * @param ids an iterator containing identifiers of objects.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> is not in the container.
	 */
	void removeAll(Iterator ids) throws NoSuchElementException;
	
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
	Object reserve(Function getObject);
	
	/**
	 * Returns the number of elements of the container.
	 *
	 * @return the number of elements.
	 */
	int size();
	
	/**
	 * Unfixes the Object with identifier <tt>id</tt>. This method throws
	 * an exception when the identifier <tt>id</tt> isn't used by
	 * the container. After one call of <tt>unfix</tt> the buffer is allowed to
	 * remove the object (although the objects have been fixed more than
	 * once).<br>
	 *
	 * @param id identifier of an object that should be unfixed in the
	 *        buffer.
	 * @throws NoSuchElementException if the identifier <tt>id</tt>
	 *         isn't used by the container.
	 */
	void unfix(Object id) throws NoSuchElementException;
	
	/**
	 * Unfixes the objects with identifiers given by iterator
	 * <tt>ids</tt>. All the objects are unfixed which belong to one of
	 * the identifiers of ids.<br>
	 *
	 * @param ids an iterator of identifiers.
	 * @throws NoSuchElementException if the identifier <tt>id</tt>
	 *         isn't used by the container.
	 */
	void unfixAll(Iterator ids) throws NoSuchElementException;
	
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
	void update(Object id, Object object, boolean unfix) throws NoSuchElementException;
	
	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container. 
	 *
	 * @param id identifier of the element.
	 * @param object the new object that should be associated to
	 *        <tt>id</tt>.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> does not exist in the container.
	 */
	void update(Object id, Object object) throws NoSuchElementException;
	
	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. 
	 *
	 * @param ids an iterator of identifiers.
	 * @param function a function that computes the object which should be
	 *        associated to an identifier.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	void updateAll(Iterator ids, final Function function, boolean unfix) throws NoSuchElementException;
	
	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. This method should be equivalent to
	 * <tt>updateAll(ids, function, true)</tt>.
	 *
	 * @param ids an iterator of identifiers.
	 * @param function a function that computes the object which should be
	 *        associated to an identifier.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	void updateAll(Iterator ids, Function function) throws NoSuchElementException;
	
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
	void updateAll(Iterator ids, Iterator objects, boolean unfix) throws NoSuchElementException;
	
	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>. This method should be equivalent to
	 * <tt>updateAll(ids, objects, true)</tt>.
	 *
	 * @param ids an iterator of identifiers
	 * @param objects an iterator of objects
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	void updateAll(Iterator ids, Iterator objects) throws NoSuchElementException;

    /**
     * Inserts the given array of elements in a bulk into a container.
     *
     * @param blocks an array of elements to be inserted
     * @return an array of <tt>ids</tt> referring to the inserted elements
     */
	public Object[] batchInsert(Object[] blocks);
}
