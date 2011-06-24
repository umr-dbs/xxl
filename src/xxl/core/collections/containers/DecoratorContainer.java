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
import xxl.core.util.Decorator;

/**
 * The class provides a decorator for a container that follows the
 * <i>Decorator Design Pattern</i> (for further details see Structural
 * Patterns, Decorator in <i>Design Patterns: Elements of Reusable
 * Object-Oriented Software</i> by Erich Gamma, Richard Helm, Ralph
 * Johnson, and John Vlissides). It provides a more flexible way to add
 * functionality to a container or one of its subclasses as by
 * inheritance.<p>
 *
 * To provide this functionality the class contains a reference to the
 * container to be decorated. This reference is used to redirect method
 * calls to the <i>original</i> container. This class is an abstract class
 * although it provides no abstract methods. That's so because it does not
 * make any sense to instanciate this class which redirects every method
 * call to the corresponding method of the decorated container without
 * adding any functionality.<p>
 *
 * <b>Note</b> that the decorator redirects <b>every</b> method call to
 * the underlying container. For this reason, the decorator container
 * provides non of the default implementations of the container class.
 * E.g., when decorating only the method <tt>insert(Object, boolean)</tt>, 
 * a call to this method will be redirected to the underlying container. 
 * When this underlying container uses the default implementations of the
 * container class, this implementation will call the undecorated insert
 * method of the underlying container and not the decorated one of the
 * decorator.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new decorated container that adds functionality to the insert method and leaves
 *     // the other methods untouched
 *
 *     DecoratorContainer container = new DecoratorContainer(new MapContainer()) {
 *         public Object insert (Object object, boolean unfix) {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the insertion of the specified object!");
 *             Object object = super.insert(object, unfix);
 *             System.out.println("After the insertion of the specified object!");
 *             return object;
 *         }
 *
 *         public Object insert (Object object) {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the insertion of the specified object!");
 *             Object object = super.insert(object);
 *             System.out.println("After the insertion of the specified object!");
 *             return object;
 *         }
 *
 *         public Iterator insertAll (Iterator objects, boolean unfix) {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the insertion of the specified objects!");
 *             Iterator objects = super.insertAll(objects, unfix);
 *             System.out.println("After the insertion of the specified objects!");
 *             return objects;
 *         }
 *
 *         public Iterator insertAll (Iterator objects) {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the insertion of the specified objects!");
 *             Iterator objects = super.insertAll(objects);
 *             System.out.println("After the insertion of the specified objects!");
 *             return objects;
 *         }
 *     }
 * </pre>
 *
 * @see xxl.core.io.converters.Converter
 * @see Cursor
 * @see Function
 * @see Iterator
 * @see NoSuchElementException
 */
public abstract class DecoratorContainer implements Container, Decorator<Container> {

	/**
	 * A factory method to create a new DecoratorContainer. It may
	 * only be invoked with an array (<i>parameter list</i>) (for further
	 * details see Function) of containers. The array (<i>parameter
	 * list</i>) will be used to initialize the decorated container. This
	 * field is set to
	 * <code>{@link UnmodifiableContainer#FACTORY_METHOD UnmodifiableContainer.FACTORY_METHOD}</code>.
	 *
	 * @see Function
	 */
	public static final Function FACTORY_METHOD = UnmodifiableContainer.FACTORY_METHOD;

	/**
	 * A reference to the container to be decorated. This reference is
	 * used to perform method calls on the <i>original</i> container.
	 */
	protected Container container;

	/**
	 * Constructs a new DecoratorContainer that decorates the specified
	 * container.
	 *
	 * @param container the container to be decorated.
	 */
	public DecoratorContainer (Container container) {
		this.container = container;
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
	public FixedSizeConverter objectIdConverter () {
		return container.objectIdConverter();
	}

	/**
	 * Returns the size of the ids generated by this container in bytes.
	 * Each id must have the same size.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return container.getIdSize();
	}

	/**
	 * Removes all elements from the Container. After a call of this
	 * method, <tt>size()</tt> will return 0.<br>
	 * Note, that the implementation of this method relies on the remove
	 * operation of the iterator returned by the method <tt>ids()</tt>.
	 */
	public void clear () {
		container.clear();
	}

	/**
	 * Closes the Container and releases all sources. For external
	 * containers, this method closes the files immediately. MOREOVER, all
	 * iterators operating on the container can be in illegal states.
	 * Close can be called a second time without any impact.<br>
	 * Note, that it would be desirable that the finalize-mechanism of
	 * Java would already offer the functionality of close. However,
	 * finalize does not release the sources immediately! Consequently,
	 * the corresponding file of a "closed" Container may be opened and
	 * some of the data is still not written back. This is a problem when
	 * for example the JVM stops running (because of a system error).
	 */
	public void close () {
		container.close();
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
		return container.contains(id);
	}

	/**
	 * Flushes all modified elements from the buffer into the container.
	 * After this call the buffer and the container are synchronized.
	 */
	public void flush () {
		container.flush();
	}

	/**
	 * Flushes the object with identifier <tt>id</tt> from the buffer into
	 * the container.
	 *
	 * @param id identifier of the object that should be written back.
	 */
	public void flush (Object id) {
		container.flush(id);
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown if there is not object stored with this
	 * <tt>id</tt>. If <tt>unfix</tt> is set to <tt>true</tt>, the object can 
	 * be removed from the underlying buffer. Otherwise (<tt>!unfix</tt>), 
	 * the object has to be kept in the buffer.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		return container.get(id, unfix);
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>.
	 *
	 * @param id identifier of the object.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id) {
		return container.get(id);
	}

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
	public Iterator getAll (Iterator ids, final boolean unfix) {
		return container.getAll(ids, unfix);
	}

	/**
	 * Returns an iterator pointing to the Object of the given ids.
	 *
	 * @param ids an iterator of object identifiers.
	 * @return an iterator of objects.
	 * @throws NoSuchElementException if one of the desired objects is not
	 *         found.
	 */
	public Iterator getAll (Iterator ids) {
		return container.getAll(ids);
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 *
	 * @return an iterator of all identifiers used by this container.
	 */
	public Iterator ids () {
		return container.ids();
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
		return container.insert(object, unfix);
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
		return container.insert(object);
	}

	/**
	 * Inserts all objects of a given Iterator into the container. It
	 * returns an iterator that contains the identifiers of the new
	 * objects.<br>
	 * Note, that the order of the identifiers corresponds to the order of
	 * their objects. The meaning of unfix is the same as for the method
	 * insert.
	 *
	 * @param objects an iterator of objects that should be inserted.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return an iterator containing the identifiers of the objects of
	 *         the input iterator.
	 */
	public Iterator insertAll (Iterator objects, boolean unfix) {
		return container.insertAll(objects, unfix);
	}

	/**
	 * Inserts all objects of a given Iterator into the container.
	 *
	 * @param objects an iterator of objects that should be inserted.
	 * @return an iterator containing the identifiers of the objects of
	 *         the input iterator.
	 */
	public Iterator insertAll (Iterator objects) {
		return container.insertAll(objects);
	}

	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to <tt>insert</tt> or <tt>reserve</tt> and hasn't been removed so far.
	 *
	 * @param id the id to be checked.
	 * @return true exactly if the <tt>id</tt> is still in use.
	 */
	public boolean isUsed (Object id) {
		return container.isUsed(id);
	}

	/**
	 * Returns a cursor containing all objects of the container. The order
	 * in which the cursor delivers the objects is not specified.<br>
	 *
	 * @return a cursor containing all objects of the container.
	 */
	public Cursor objects () {
		return container.objects();
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
		container.remove(id);
	}

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
		container.removeAll(ids);
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
		return container.reserve(getObject);
	}

	/**
	 * Returns the number of elements of the container.
	 *
	 * @return the number of elements.
	 */
	public int size () {
		return container.size();
	}

	/**
	 * Unfixes the Object with identifier <tt>id</tt>. This method throws
	 * an exception when no element with an identifier <tt>id</tt> is in
	 * the container. After one call of <tt>unfix()</tt> the buffer is allowed to
	 * remove the object (although the objects have been fixed more than
	 * once).<br>
	 * The default implementation only checks whether an object with
	 * identifier <tt>id</tt> is in the buffer.
	 *
	 * @param id identifier of an object that should be unfixed in the
	 *        buffer.
	 * @throws NoSuchElementException if no element with an identifier
	 *         <tt>id</tt> is in the container.
	 */
	public void unfix (Object id) throws NoSuchElementException {
		container.unfix(id);
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
		container.unfixAll(ids);
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
		container.update(id, object, unfix);
	}

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
		container.update(id, object);
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
		container.updateAll(ids, function, unfix);
	}

	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>.
	 *
	 * @param ids an iterator of identifiers.
	 * @param function a function that computes the object which should be
	 *        associated to an identifier.
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	public void updateAll (Iterator ids, Function function) {
		container.updateAll(ids, function);
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
		container.updateAll(ids, objects, unfix);
	}

	/**
	 * Overwrites the elements of a container whose identifiers are given
	 * by iterator <tt>ids</tt>.
	 *
	 * @param ids an iterator of identifiers
	 * @param objects an iterator of objects
	 * @throws NoSuchElementException if an object with an identifier
	 *         of <tt>ids</tt> does not exist in the container.
	 */
	public void updateAll (Iterator ids, Iterator objects) {
		container.updateAll(ids, objects);
	}
	
	@Override
	public Container getDecoree() {
		return container;
	}
	
}
