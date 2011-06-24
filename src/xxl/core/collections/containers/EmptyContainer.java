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

import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;

/**
 * This class provides an empty container. In other words, this container
 * is not able to contain an element. Therefore, every method that inserts
 * elements into the container always throw an
 * <tt>UnsupportedOperationException</tt>. The methods that accesses
 * elements of the container always throw a
 * <tt>NoSuchElementException</tt>. This class is useful when you have to
 * return a container of some kind, but do not have any elements the
 * container points to.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new empty container
 *
 *     EmptyContainer container = EmptyContainer.DEFAULT_INSTANCE;
 *
 *     // println the number of elements contained by the container
 *
 *     System.out.println(container.size());
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Cursor cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // catch UnsupportedOperationExceptions
 *
 *     try {
 *
 *         // insert all elements of the given iterator
 *
 *         while (cursor.hasNext())
 *             container.insert(cursor.next());
 *     }
 *     catch (UnsupportedOperationException uoe) {
 *         System.out.println(uoe.getMessage());
 *     }
 *
 *     // println the number of elements contained by the container
 *
 *     System.out.println(container.size());
 *
 *     // close the open bag and cursor after use
 *
 *     container.close();
 *     cursor.close();
 * </pre>
 *
 * @see xxl.core.io.converters.Converter
 * @see xxl.core.io.converters.ConvertableConverter
 * @see Cursor
 * @see EmptyCursor
 * @see Function
 * @see Iterator
 * @see NoSuchElementException
 */
public class EmptyContainer extends AbstractContainer {

	/**
	 * A factory method to create a new EmptyContainer. It may be invoked
	 * with a<i>parameter list</i> (for further details see Function)
	 * of objects, an iterator or without any parameters. The factory method 
	 * always returns a default instance of this class.
	 *
	 * @see Function
	 */
	public static final Function<Object,EmptyContainer> FACTORY_METHOD = new AbstractFunction<Object,EmptyContainer> () {
		public EmptyContainer invoke (List<? extends Object> list) {
			return DEFAULT_INSTANCE;
		}
	};

	/**
	 * This instance can be used for getting a default instance of
	 * EmptyContainer. It is similar to the <i>Singleton Design
	 * Pattern</i> (for further details see Creational Patterns, Prototype
	 * in <i>Design Patterns: Elements of Reusable Object-Oriented
	 * Software</i> by Erich Gamma, Richard Helm, Ralph Johnson, and John
	 * Vlissides) except that there are no mechanisms to avoid the
	 * creation of other instances of EmptyContainer.
	 */
	public static final EmptyContainer DEFAULT_INSTANCE = new EmptyContainer();

	/**
	 * Constructs a new empty container that is not able to contain any
	 * elements.
	 */
	public EmptyContainer() {
	}

	/**
	 * Returns true if there is an object stored within the container
	 * having the identifier <tt>id</tt>.
	 * This implementation always returns false.
	 * 
	 * @param id identifier of the object.
	 * @return true if the container contains an object for the specified
	 *         identifier.
	 */
	public boolean contains (Object id) {
		return false;
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown if there is not object stored with this
	 * <tt>id</tt>. If unfix is set to true, the object can be removed
	 * from the underlying buffer. Otherwise (!unfix), the object has
	 * to be kept in the buffer.
	 * This implementation always throws an <tt>UnsupportedOperationException</tt>.
	 * 
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		throw new NoSuchElementException();
	}

	/**
	 * Returns an iterator that delivers all the identifiers of
	 * the container that are in use.
	 * This implementation always returns an <tt>EmptyCursor</tt>.
	 * 
	 * @return an iterator of all identifiers used by this container.
	 */
	public Iterator ids () {
		return EmptyCursor.DEFAULT_INSTANCE;
	}

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
	 * This implementation always throws an
	 * <tt>UnsupportedOperationException</tt>.
	 * 
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 * @throws UnsupportedOperationException if the <tt>insert</tt> operation
	 *         is not supported by this container.
	 */
	public Object insert (Object object, boolean unfix) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("No modification allowed.");
	}

	/**
	 * Checks whether the <tt>id</tt> has been returned previously by a
	 * call to insert or reserve and hasn't been removed so far.
	 * This implementation always returns false.
	 * 
	 * @param id the id to be checked.
	 * @return true exactly if the <tt>id</tt> is still in use.
	 */
	public boolean isUsed (Object id) {
		return false;
	}

	/**
	 * Returns a cursor containing all objects of the container. The order
	 * in which the cursor delivers the objects is not specified.<br>
	 * This implementation always returns an <tt>EmptyCursor</tt>.
	 *
	 * @return a cursor containing all objects of the container.
	 */
	public Cursor objects () {
		return EmptyCursor.DEFAULT_INSTANCE;
	}

	/**
	 * Returns a converter for the ids generated by this container. A
	 * converter transforms an object to its byte representation and vice
	 * versa - also known as serialization in Java.<br>
	 * This implementation always returns a <tt>ConvertableConverter</tt>.
	 *
	 * @return a converter for serializing the identifiers of the
	 *         container.
	 */
	public FixedSizeConverter objectIdConverter () {
		return LongConverter.DEFAULT_INSTANCE;
	}

	/**
	 * Returns the size of the ids generated by this container in bytes which is 0
	 * @return 0
	 */
	public int getIdSize() {
		return 0;
	}
	
	/**
	 * Removes the object with identifier <tt>id</tt>. An exception is
	 * thrown when an object with an identifier <tt>id</tt> is not in the
	 * container.<br>
	 * This implementation always throws a <tt>NoSuchElementException</tt>
	 * because the container is empty.
	 *
	 * @param id an identifier of an object.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	public void remove (Object id) throws NoSuchElementException {
		throw new NoSuchElementException();
	}

	/**
	 * Reserves an id for subsequent use. The container may or may not
	 * need an object to be able to reserve an id, depending on the
	 * implementation. If so, it will call the parameterless function
	 * provided by the parameter <tt>getObject</tt>.
	 * This implementation always throws an <tt>UnsupportedOperationException</tt>.
	 *  
	 * @param getObject A parameterless function providing the object for
	 * 			that an id should be reserved.
	 * @return the reserved id.
	*/
	public Object reserve (Function getObject) {
		throw new UnsupportedOperationException("No modification allowed.");
	}

	/**
	 * Returns the number of elements of the container.<br>
	 * This implementation always returns <tt>0</tt>.
	 *
	 * @return the number of elements.
	 */
	public int size () {
		return 0;
	}

	/**
	 * Unfixes the Object with identifier <tt>id</tt>. This method throws
	 * an exception when the identifier <tt>id</tt> isn't used by
	 * the container. After one call of unfix the buffer is allowed to
	 * remove the object (although the objects have been fixed more than
	 * once).<br>
	 * This implementation always throws a <tt>NoSuchElementException</tt>
	 * because the container is empty.
	 *
	 * @param id identifier of an object that should be unfixed in the
	 *        buffer.
	 * @throws NoSuchElementException if no element with an identifier
	 *         <tt>id</tt> is in the container.
	 */
	public void unfix (Object id) throws NoSuchElementException {
		throw new NoSuchElementException();
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container.<br>
	 * This implementation always throws a <tt>NoSuchElementException</tt>
	 * because the container is empty.
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
		throw new NoSuchElementException();
	}
}
