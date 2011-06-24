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

package xxl.core.collections.containers.io;

import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.collections.containers.ConstrainedDecoratorContainer;
import xxl.core.collections.containers.Container;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;
import xxl.core.io.Buffer;
import xxl.core.util.XXLSystem;

/**
 * This class wraps a container by buffering its elements in a given
 * buffer. There are only little differences between the buffered
 * container and the wrapped container:<br>
 * every time elements of the buffered container are accessed, the buffer
 * is check first, whether it contains the desired elements. When the
 * desired elements are not buffered, the elements are taken from the
 * wrapped container.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new buffered container with ...
 *
 *     BufferedContainer container = new BufferedContainer(
 *
 *         // a map container that stores the elements
 *
 *         new MapContainer(),
 *
 *         // a LRU buffer with 5 slots
 *
 *         new LRUBuffer(5)
 *     );
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Iterator iterator = new Enumerator(20);
 *
 *     // insert all elements of the given iterator
 *
 *     iterator = container.insertAll(iterator);
 *
 *     // print all elements of the queue
 *
 *     while (iterator.hasNext())
 *         System.out.println(container.get(iterator.next()));
 *
 *     // get the ids of the elements of the container
 *
 *     iterator = container.ids();
 *
 *     // remove 5 elements
 *
 *     for (int i = 0; i < 5 && iterator.hasNext(); i++) {
 *         container.remove(iterator.next());
 *
 *         // refresh the iterator (cause it can be in an invalid state)
 *
 *         iterator = container.ids();
 *     }
 *
 *     // fix 5 elements in the buffer
 *
 *     for (int i = 0; i < 5 && iterator.hasNext(); i++)
 *         container.get(iterator.next(), false);
 *
 *     // try to access another element (the buffer will overrun)
 *
 *     try {
 *         container.get(iterator.next());
 *     }
 *     catch (Exception e) {
 *         System.out.println(e);
 *     }
 *
 *     // flushes the whole buffer
 *
 *     container.flush();
 *
 *     // get the ids of the elements of the container
 *
 *     iterator = container.ids();
 *
 *     // print all elements of the queue
 *
 *     while (iterator.hasNext())
 *         System.out.println(container.get(iterator.next()));
 *
 *     // close the open queue after use
 *
 *     container.close();
 * </pre>
 *
 * @see Constant
 * @see Container
 * @see ConstrainedDecoratorContainer
 * @see Function
 * @see Iterator
 * @see NoSuchElementException
 */
public class BufferedContainer extends ConstrainedDecoratorContainer {

	/**
	 * The buffer that is used for buffering the elements of the container.
	 */
	protected Buffer buffer;

	/**
	 * A flag that determines whether the buffered elements should be
	 * written back to the container, when they are flushed (true) or if 
	 * they already should be written back after each update operation 
	 * (false).
	 */
	protected boolean writeBack;

	/**
	 * Flag which determines if every object is cloned before storing
	 * and before returning it.
	 */
	protected boolean cloneObjects;

	/**
	 * Constructs a new buffered container that uses the specified buffer
	 * for buffering the elements of the given container. The flag
	 * <tt>writeBack</tt> determines whether the buffered elements should be
	 * written back to the container, when they are flushed (true) or if 
	 * they already should be written back after each update operation 
	 * (false).
	 *
	 * @param container the container to be buffered.
	 * @param buffer the buffer used for buffering the specified
	 *		container.
	 * @param writeBack Signals that the buffered elements should be
	 * 	written back to the container, when they are flushed (true) or if 
	 * 	they already should be written back after each update operation 
	 * 	(false). <b>Use writeBack with care.</b> The inserted/updated object
	 * 	must stay unchanged until the buffer writes back the object to
	 * 	the underlying container.
	 * @param cloneObjects determines if every object is cloned before storing
	 *		and before returning it. The clone mode is a lot slower, because
	 *		reflection is needed to call the protected clone Method.
	 */
	public BufferedContainer (Container container, Buffer buffer, boolean writeBack, boolean cloneObjects) {
		super(container);
		this.buffer = buffer;
		this.writeBack = writeBack;
		this.cloneObjects = cloneObjects;
	}

	/**
	 * Constructs a new buffered container that uses the specified buffer
	 * for buffering the elements of the given container. The flag
	 * <tt>writeBack</tt> determines whether the buffered elements should be
	 * written back to the container, when they are flushed (true) or if 
	 * they already should be written back after each update operation 
	 * (false).
	 *
	 * @param container the container to be buffered.
	 * @param buffer the buffer used for buffering the specified
	 *		container.
	 * @param writeBack Signals that the buffered elements should be
	 * 	written back to the container, when they are flushed (true) or if 
	 * 	they already should be written back after each update operation 
	 * 	(false). <b>Use writeBack with care.</b> The inserted/updated object
	 * 	must stay unchanged until the buffer writes back the object to
	 * 	the underlying container.
	 */
	public BufferedContainer (Container container, Buffer buffer, boolean writeBack) {
		this(container, buffer, writeBack, false);
	}

	/**
	 * Constructs a new buffered container that uses the specified buffer
	 * for buffering the elements of the given container. When flushing a
	 * buffered element, it will be written back to the container. This
	 * constructor is equivalent to the call of
	 * <code>BufferedContainer(container, buffer, true)</code>.
	 * Look at the remarks for writeBack!
	 * 
	 * @param container the container to be buffered.
	 * @param buffer the buffer used for buffering the specified
	 *        container.
	 */
	public BufferedContainer (Container container, Buffer buffer) {
		this(container, buffer, true);
	}

	/**
	 * Removes all elements from the container. After a call of this
	 * method, <tt>size()</tt> will return <tt>0</tt>.<br>
	 * This implementation removes all buffered elements out of the buffer
	 * and clears the wrapped container thereafter.
	 */
	public void clear () {
		buffer.removeAll(this);
		super.clear();
	}

	/**
	 * Closes the Container and releases all sources. For external
	 * containers, this method closes the files immediately. MOREOVER, all
	 * iterators operating on the container can be in illegal states.
	 * Close can be called a second time without any impact.<br>
	 * This implementation flushes all buffered elements and removes them
	 * out of the buffer. Thereafter the wrapped container is closed.
	 */
	public void close () {
		buffer.flushAll(this);
		buffer.removeAll(this);
		super.close();
	}

	/**
	 * Returns <tt>true</tt> if the container contains an object for the
	 * identifier <tt>id</tt>. <br>
	 * First, this implementation checks the buffer, if it contains an
	 * object for id. Thereafter the wrapped container is checked.
	 *
	 * @param id identifier of the object.
	 * @return <tt>true</tt> if the container contains an object for the
	 *         specified identifier.
	 */
	public boolean contains (Object id) {
		return buffer.contains(this, id) || super.contains(id);
	}

	/**
	 * Flushes all modified elements from the buffer into the container.
	 * After this call the buffer and the container are synchronized.<br>
	 * This implementation flushes all buffered elements first. Thereafter
	 * the elements of the wrapped container are flushed (in order to
	 * support a second buffer etc.).
	 */
	public void flush () {
		buffer.flushAll(this);
		super.flush();
	}

	/**
	 * Flushes the object with identifier <tt>id</tt> from the buffer into
	 * the container. <br>
	 * This implementation flushes the buffered element with the given id
	 * first. Thereafter the element of the wrapped container is flushed
	 * (in order to support a second buffer etc.).
	 *
	 * @param id identifier of the object that should be written back.
	 */
	public void flush (Object id) {
		buffer.flush(this, id);
		super.flush(id);
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown when the desired object is not found. If <tt>unfix</tt>,
	 * the object can be removed from the underlying buffer. Otherwise
	 * (<tt>!unfix</tt>), the object has to be kept in the buffer.<br>
	 * This implementation calls the get method of the buffer with a
	 * function that returns the desired object of the container, when it
	 * is invoked.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, final boolean unfix) throws NoSuchElementException {
		Object object = buffer.get(this, id,
			new AbstractFunction () {
				public Object invoke (Object id) {
					return BufferedContainer.super.get(id, unfix);
				}
			},
			unfix
		);
		if (cloneObjects)
			return XXLSystem.cloneObject(object);
		else
			return object;
	}

	/**
	 * Returns an iterator that delivers the identifiers of all objects of
	 * the container.
	 *
	 * @return an iterator of object identifiers.
	 */
	public Iterator ids () {
		return new Iterator () {
			Iterator ids = BufferedContainer.super.ids();
			Object id;

			public boolean hasNext () {
				return ids.hasNext();
			}

			public Object next () throws NoSuchElementException {
				return id = ids.next();
			}

			public void remove () throws IllegalStateException {
				ids.remove();
				buffer.remove(BufferedContainer.this, id);
			}
		};
	}

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * If unfixed, the object can be removed from the buffer. Otherwise,
	 * it has to be kept in the buffer until an <tt>unfix()</tt> is
	 * called.<br>
	 * After an insertion all the iterators operating on the container can
	 * be in an invalid state.<br>
	 * This implementation inserts the object into the wrapped container.
	 * Thereafter it is inserted into the buffer by calling the buffer's
	 * <tt>get</tt> method.
	 *
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		if (cloneObjects)
			object = XXLSystem.cloneObject(object);
		
		Object id = reserve(new Constant(object));

		update(id, object, unfix);
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
		return super.isUsed(id);
	}

	/**
	 * Removes the object with identifier <tt>id</tt>. An exception is
	 * thrown when an object with an identifier <tt>id</tt> is not in the
	 * container.<br>
	 * After a call of <tt>remove()</tt> all the iterators (and cursors)
	 * can be in an invalid state.<br>
	 * First, this implementation removes the object from the buffer.
	 * Thereafter it is removed from the wrapped container.
	 *
	 * @param id an identifier of an object.
	 * @throws NoSuchElementException if an object with an identifier
	 *         <tt>id</tt> is not in the container.
	 */
	public void remove (Object id) throws NoSuchElementException {
		buffer.remove(this, id);
		super.remove(id);
	}

	/**
	 * Unfixes the object with identifier <tt>id</tt>. This method throws
	 * an exception when no element with an identifier <tt>id</tt> is in
	 * the container. After one call of unfix the buffer is allowed to
	 * remove the object (although the objects have been fixed more than
	 * once).<br>
	 * This implementation unfixes the buffered object first. Thereafter
	 * the object of the wrapped container is unfixed (in order to support
	 * a second buffer etc.).
	 *
	 * @param id identifier of an object that should be unfixed in the
	 *        buffer.
	 * @throws NoSuchElementException if no element with an identifier
	 *         <tt>id</tt> is in the container.
	 */
	public void unfix (Object id) throws NoSuchElementException {
		buffer.unfix(this, id);
		super.unfix(id);
	}

	/**
	 * Overwrites an existing (id,*)-element by (id, object). This method
	 * throws an exception if an object with an identifier <tt>id</tt>
	 * does not exist in the container.<br>
	 * This implementation calls update method of the buffer. When the
	 * buffered elements should be written back to the buffer, the update
	 * method is called with a flush function, that updates the element of
	 * the wrapped container with the buffered element. Otherwise, the
	 * element of the wrapped container is updated once after calling the
	 * buffer's update method.
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
		if (cloneObjects)
			object = XXLSystem.cloneObject(object);
		
		buffer.update(this, id, object,
			!writeBack ?
				null :
				new AbstractFunction () {
					public Object invoke (Object id, Object object) {
						BufferedContainer.super.update(id, object, !buffer.isFixed(BufferedContainer.this, id));
						return null;
					}
				},
			unfix
		);
		
		// no cloning needed here, because the called container is responsible for that.
		if (!writeBack)
			super.update(id, object, unfix);
	}
	
	/**
	 * Returns the number of fixed elements in the container's buffer.
	 */
	public int fixedElements() {
		return buffer.fixedSlots();
	}
}
