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

package xxl.core.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import xxl.core.functions.Constant;
import xxl.core.functions.Function;

/**
 * This class provides a buffer for buffering I/Os in order to increase
 * performance.
 * 
 * <p>It does not define any displacement strategy. When implementing a buffer
 * by extending this class, the user only has to implement the victim method,
 * that determines the <i>next</i> slot to displace in the buffer. Objects in
 * the buffer are identified by their owner and an id that is unique at the
 * owner. Additional, the buffer stored a map that contains the owners and
 * their slots in this buffer. The owners are mapped to maps that contain the
 * owner's slots. These slot maps identify the slots by their ids (for further
 * detail see field {@link Slot#members members} in <code>Buffer.Slot</code>).
 * 
 * <p>Important: every class used as identifyers has to implement the hashCode
 * method of {@link java.lang.Object}.</p>
 *  
 * <p>The buffer supports the access, update and removal of slots (the objects
 * contained by the slots), that identified by their id and owner, and the the
 * removal of all slot owned by a specified owner. Slots of the buffer can also
 * be fixed (in order to avoid the removal of them) and unfixed. In order to
 * guarantee highest flexibility, the way of flushing an object in the buffer
 * must be implemented by a function. Therefore an owner can specify an own
 * flush function for every object in the buffer.</p>
 *
 * <p>Objects can be inserted into the buffer by calling the fix or get method
 * with an id that is new at the owner and a function, that returns the object
 * to insert, when it is invoked with the specified id. The update method can
 * also be used for inserting an object, when it is called with a new id and
 * the object to insert and a flush function for it.</p>
 *
 * @param <O> the type of the objects specifing the owner of a buffer's slot.
 * @param <I> the type of the identifiers used for identifing the buffer's
 *        slots.
 * @param <E> the type of the elements stored by this buffer.
 * @see Constant
 * @see Function
 * @see HashMap
 * @see Iterator
 * @see Map
 * @see java.util.Map.Entry
 */
public abstract class Buffer<O, I, E> {

	/**
	 * This class provides a single slot in a buffer.
	 * 
	 * <p>The slots are indexed according to their position in the buffer
	 * beginning at index <code>0</code>. Every slot is able to contain a
	 * single object. In addition to the object itself, the slot stores an id
	 * for the object, the owner of the object and a map of all slots that are
	 * owned by the owner of the object. The id AND the owner of an object are
	 * stored in order to ease the identification of elements in the buffer.
	 * Therefore the id of an object must only be an unique id at the owner but
	 * not a global unique id. A slot can be fixed, so that its object can not
	 * be removed from it. In order to guarantee highest flexibility, the way
	 * of flushing a slot must be implemented by a function. Therefore an owner
	 * can specify an own flush function for every object in the buffer.</p>
	 *
	 * @see Function
	 * @see HashMap
	 * @see Map
	 */
	protected class Slot {

		/**
		 * The index of the slot in the buffer. The slots are indexed according
		 * to their position in the buffer beginning at index <code>0</code>.
		 */
		protected int index;

		/**
		 * A map that contains all slots that are owned by the owner of this
		 * slot. The slots are identified by their ids. Therefore the id must
		 * only be an unique id at the owner but not a global unique id. Every
		 * class of identifyers has to implement the hashCode method.
		 */
		protected Map<I, Slot> members;

		/**
		 * An owner of this slot (the object contained by this slot).
		 */
		protected O owner;

		/**
		 * An id that identifies this slot (the object contained by this slot).
		 */
		protected I id;

		/**
		 * The object that is contained by this slot.
		 */
		protected E object = null;

		/**
		 * A flag that determines whether this slot is fixed or not. The object
		 * contained by a fixed slot and the information belonging to it cannot
		 * be removed.
		 */
		protected boolean isFixed = false;

		/**
		 * A function that implements the functionality of flushing a slot.
		 * When this slot should be flushed, the function is called* with its
		 * id and object. Therefore the function must implement all the
		 * functionality that is needed to flush a slot.
		 */
		protected Function<Object, ?> flush = null;

		/**
		 * Number of bytes which are inside the slot.
		 */
		protected int sizeBytes = 0;

		/**
		 * Constructs a new empty slot with the specified index. The new slot
		 * contains no object and is not fixed.
		 *
		 * @param index the index of the new slot.
		 */
		protected Slot(int index) {
			this.index = index;
		}

		/**
		 * Returns the id of this slot.
		 *
		 * @return the id of this slot.
		 */
		protected I id() {
			return id;
		}

		/**
		 * Returns the object that is contained by this slot.
		 *
		 * @return the object of this slot.
		 */
		protected E get() {
			return object;
		}

		/**
		 * Returns whether this slot is dirty or not. In other words, returns
		 * <code>true</code> if <code>flush&nbsp;!=&nbsp;null</code> (the
		 * object is not yet flushed), else returns <code>false</code>.
		 *
		 * @return <code>true</code> if this slot is dirty, else returns
		 *         <code>false</code>.
		 */
		protected boolean isDirty() {
			return flush!=null;
		}

		/**
		 * Returns whether this slot is fixed or not. In other words, returns
		 * <code>true</code> if <code>isFixed&nbsp;==&nbsp;true</code>, else
		 * returns <code>false</code>.
		 *
		 * @return <code>true</code> if this slot is fixed, else returns
		 *         <code>false</code>.
		 */
		protected boolean isFixed() {
			return isFixed;
		}

		/**
		 * Fixes this slot so that the object contained by it cannot be removed
		 * out of the buffer.
		 */
		protected void fix() {
			if (!isFixed())
				fixedSlots++;
			isFixed = true;
		}

		/**
		 * Unfixes this slot so that the object contained by it can be removed
		 * out of the buffer.
		 */
		protected void unfix() {
			if (isFixed())
				fixedSlots--;
			isFixed = false;
		}

		/**
		 * Flushes this slot by calling its flush function with its id and
		 * object. This implementation checks first whether the slot is dirty.
		 * When it is dirty, the flush function is called and set to
		 * <code>null</code>, i.e. the slot is not any longer dirty after
		 * calling this method.
		 */
		protected void flush() {
			if (isDirty()) {
				flush.invoke(id, object);
				flush = null;
			}
		}

		/**
		 * Updates the object and flush function of this slot. The object and
		 * flush function are replaced by the specified object and function,
		 * i.e. the slot is dirty after calling this method.
		 *
		 * @param object the new object of this slot.
		 * @param flush the new flush function of this slot.
		 */
		protected void update(E object, Function<Object, ?> flush) {
			this.object = object;
			this.flush = flush;
			if (Buffer.this.capacityBytes != Integer.MAX_VALUE)
				this.sizeBytes = ((SizeAware)object).getMemSize(); 
		}

		/**
		 * Inserts the specified object with the specified id and owner in this
		 * slot. Sets the object, id and owner of this slot to the specified
		 * objects and updates member map of this slot and the owner map of the
		 * buffer.
		 *
		 * @param owner the new owner of this slot.
		 * @param id the new id of this slot.
		 * @param object the new object of this slot.
		 */
		protected void insert(O owner, I id, E object) {
			this.owner = owner;
			this.id = id;
			this.object = object;
			if (Buffer.this.capacityBytes != Integer.MAX_VALUE)
				this.sizeBytes = ((SizeAware)object).getMemSize(); 
			if ((members = owners.get(owner)) == null)
				owners.put(owner, members = new HashMap<I, Slot>());
			members.put(id, this);
			size++;
		}

		/**
		 * Removes the object and any information belonging to it from this
		 * slot so that it is empty thereafter. This implementation swaps this
		 * slot and the occupied slot with the highest index. Then their
		 * indices, this slot's member map and the owner map of the buffer are
		 * updated. At last, the attributes of this slot are reset.
		 */
		protected void remove() {
			if (index < size) {
				Slot slot = slots.get(--size);
				
				slots.set(slot.index = index, slot);
				slots.set(index = size, this);
				if (members.containsKey(id)) {
					members.remove(id);
					if (members.isEmpty())
						owners.remove(owner);
					members = null;
				}
				owner = null;
				if (isFixed())
					fixedSlots--;
				isFixed = false;
				flush = null;
				object = null;
			}
		}

		/**
		 * Displaces this slot by flushing it and removing the object and any
		 * information belonging to it from it.
		 */
		protected void displace () {
			flush();
			remove();
		}
	}

	/**
	 * The number of fixed slots in this buffer.
	 */
	protected int fixedSlots = 0;

	/**
	 * The number of slots in this buffer that contain an object.
	 */
	protected int size = 0;

	/**
	 * The number of bytes currently inside the buffer.
	 */
	protected int sizeBytes = 0;

	/**
	 * The number of bytes which will be buffered given at most.
	 */
	protected int capacityBytes;

	/**
	 * An array containing all the slots of this buffer.
	 */
	protected List<Slot> slots;

	/**
	 * A map that contains the owners and their slots in this buffer. The
	 * owners are mapped to maps that contain the owner's slots. These slot
	 * maps identify the slots by their ids (for further detail see field
	 * {@link Slot#members members} in <code>Buffer.Slot</code>).
	 */
	protected Map<O, Map<I, Slot>> owners = new HashMap<O, Map<I, Slot>>();

	/**
	 * Constructs a new empty buffer with a number of slots specified by the
	 * given capacity.
	 * 
	 * @param capacity the number of slots in the new buffer.
	 * @param capacityBytes the capacity of the buffer in bytes. If this is
	 *        &gt;&nbsp;-1, then the buffered objects have to efficiently
	 *        implement the interface SizeAware, so that the buffer can
	 *        determine the correct number of bytes used. 
	 */
	public Buffer(int capacity, int capacityBytes) {
		this.capacityBytes = capacityBytes;
		
		this.slots = new ArrayList<Slot>(capacity);
		for (int i = 0; i < capacity; i++)
			slots.add(newSlot(i));
	}

	/**
	 * Constructs a new empty buffer with a number of slots specified by the
	 * given capacity.
	 *
	 * @param capacity the number of slots in the new buffer.
	 */
	public Buffer(int capacity) {
		this(capacity, Integer.MAX_VALUE);
	}

	/**
	 * Returns the <i>next</i> slot to displace in this buffer. This method is
	 * called every time a slot should be displaced and must implement the
	 * displacement strategy of this buffer.
	 *
	 * @return the <i>next</i> slot to displace in this buffer.
	 */
	protected abstract Slot victim();

	/**
	 * Creates a new empty slot with the specified index. This factory method
	 * simply calls the constructor of <code>Slot</code>. Every subclass of
	 * <code>Buffer</code> that extends the inner class <code>Slot</code> must
	 * overwrite this method by defining the method
	 * <code><pre>
	 *   protected Buffer.Slot newSlot(int index) {
	 *       return new Slot(index);
	 *   }
	 * </pre></code>
	 * This guarantees that every call of the newSlot method creates the
	 * correct corresponding <code>Slot</code> object of the subclass.
	 *
	 * @param index the index of the new slot.
	 * @return a new empty slot with the specified index.
	 */
	protected Slot newSlot(int index) {
		return new Slot(index);
	}

	/**
	 * Returns the number of slots in this buffer that contain an object.
	 *
	 * @return the number of occupied slots in this buffer.
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns the capacity of this buffer.
	 *
	 * @return the maximal number of slots this buffer can contain.
	 */
	public int capacity() {
		return slots.size();
	}

	/**
	 * Returns the number bytes used in this buffer.
	 *
	 * @return the number of occupied space in bytes in this buffer.
	 */
	public int bytesUsed() {
		return sizeBytes;
	}

	/**
	 * Returns the slot with the given id owned by the specified owner. This
	 * implementation maps owner to the map that contains the owner's slots by
	 * using the map <code>owners</code>. Thereafter the slot map is used for
	 * mapping the id to the slot identified by the id. When there is no such
	 * slot <code>null</code> is returned.
	 *
	 * @param owner the owner of the slot to return.
	 * @param id the id of the slot to return.
	 * @return the slot with the given id owned by the specified owner or
	 *         <code>null</code> if no such slot exists.
	 */
	protected Slot lookUp(O owner, I id) {
		Map<I, Slot> members = owners.get(owner);
		
		return members == null ? null : members.get(id);
	}

	/**
	 * Handles the situation iff the buffer contains more bytes than the
	 * capacity says. Some slots are removed in this case.
	 */
	protected final void handleSizeOverflow() {
		// fix the size condition
		// The biggest element has to fit into the buffer (alone)!
		while (sizeBytes > capacityBytes) {
			if (fixedSlots == size())
				throw new IllegalStateException("Buffer overflow. Too many slots fixed.");
			Slot vic = victim();
			sizeBytes -= vic.sizeBytes;
			vic.displace();
			// checkBuffer();
		}
	}

	/**
	 * Fixes the slot with the given id owned by the specified owner and
	 * returns it. When no such slot exists, a new object is created by calling
	 * the given function obtain with the specified id and this object is
	 * inserted into the buffer. When the buffer overflows (it is full and all
	 * slots are fixed), an <code>IllegalStateException</code> will be thrown.
	 * Otherwise the <i>next</i> slot to displace will be determined by calling
	 * the victim method and its object will be replaced by the new object.
	 *
	 * @param owner the owner of the slot to fix.
	 * @param id the id of the slot to fix.
 	 * @param obtain a function for getting the object, when there is no slot
 	 *        the given id owned by the specified owner.
	 * @return the fixed slot with the given id owned by the specified owner.
	 * @throws IllegalStateException when the buffer overflows.
	 */
	protected Slot fix(O owner, I id, Function<? super I, ? extends E> obtain) throws IllegalStateException {
		Slot slot = lookUp(owner, id);
		
		if (slot == null) {
			if (fixedSlots == slots.size())
				throw new IllegalStateException("Buffer overflow. Too many slots fixed.");
			// Make space for one new object
			if (size() == slots.size()) {
				Slot vic = victim();
				sizeBytes -= vic.sizeBytes;  
				vic.displace();
			}
			// checkBuffer();
			
			// insert the object
			(slot = slots.get(size())).insert(owner, id, obtain.invoke(id));
			slot.fix();
			sizeBytes += slot.sizeBytes;
			// checkBuffer();
			
			handleSizeOverflow();
		}
		else
			slot.fix();
		
		// checkBuffer();
		return slot;
	}

	/**
	 * Unfixes the slot with the given id owned by the specified owner. The
	 * desired slot is determined by calling the lookUp method. When such a
	 * slot exists, its unfix method is called.
	 *
	 * @param owner the owner of the slot to unfix.
	 * @param id the id of the slot to unfix.
	 */
	public void unfix(O owner, I id) {
		Slot slot = lookUp(owner, id);
		
		if (slot != null)
			slot.unfix();
	}

	/**
	 * Returns whether this buffer contains a slot with the given id owned by
	 * the specified owner. In other words, returns <code>true</code> if
	 * <code>lookUp(owner,id)&bnsp;!=&bnsp;null</code>, else returns
	 * <code>false</code>.
	 *
	 * @param owner the owner of the desired slot.
	 * @param id the id of the desired slot.
	 * @return <code>true</code> if this buffer contains a slot with the given
	 *         id owned by the specified owner, else returns
	 *         <code>false</code>.
	 */
	public boolean contains(O owner, I id) {
		return lookUp(owner, id) != null;
	}

	/**
	 * Returns whether the slot with the given id owned by the specified owner
	 * is fixed or not. This implementation checks whether such a slot exists
	 * and, when it exists, whether it is fixed.
	 *
	 * @param owner the owner of the desired slot.
	 * @param id the id of the desired slot.
	 * @return <code>true</code> if the slot with the given id owned by the
	 *         specified owner is fixed, else returns <code>false</code>.
	 */
	public boolean isFixed(O owner, I id) {
		Slot slot = lookUp(owner, id);
		
		return slot != null && slot.isFixed();
	}

	/**
	 * Flushes the slot with the given id owned by the specified owner. This
	 * implementation checks whether such a slot exists and flushes it, when it
	 * exists.
	 *
	 * @param owner the owner of the slot to flush.
	 * @param id the id of the slot to flush.
	 */
	public void flush(O owner, I id) {
		Slot slot = lookUp(owner, id);
		
		if (slot != null)
			slot.flush();
	}

	/**
	 * Flushes all slots in this buffer that are owned by the specified owner.
	 * This implementation gets all slot owned by the specified owner by using
	 * the map <code>owners</code> and flushes these slot thereafter.
	 *
	 * @param owner the owner of the slots to flush.
	 */
	public void flushAll(O owner) {
		Map<I, Slot> members = owners.get(owner);
		
		if (members != null)
			for (Slot slot : members.values())
				slot.flush();
	}

	/**
	 * Returns the object contained by the slot with the given id owned by the
	 * specified owner. When no such slot exists, a new object is created by
	 * calling the given function obtain with the specified id and this object
	 * is inserted into the buffer. When the buffer overflows (it is full and
	 * all slots are fixed), an <code>IllegalStateException</code> will be
	 * thrown. Otherwise the <i>next</i> slot to displace will be determined by
	 * calling the victim method and its object will be replaced by the new
	 * object. When <code>unfix&nbsp;==&nbsp;true</code> the slot containing
	 * the desired object is unfixed at last.
	 * 
	 * <p>This implementation fixes the desired slot by calling this buffer's
	 * fix method with the specified owner, id and obtain function and calls
	 * the slot's get method thereafter. When
	 * <code>unfix&nbsp;==&nbsp;true</code>, its unfix method is called at
	 * last.</p>
	 *
	 * @param owner the owner of the slot containing the object to get.
	 * @param id the id of the slot containing the object to get.
	 * @param obtain a function for creating a new object, when there is no
	 *        slot the the given id owned by the specified owner.
	 * @param unfix a flag that determines whether the desired slot should be
	 *        unfixed after getting its object or not.
	 * @return the object contained by the slot with the given id owned by the
	 *         specified owner.
	 * @throws IllegalStateException when the buffer overflows.
	 */
	public E get(O owner, I id, Function<? super I, ? extends E> obtain, boolean unfix) throws IllegalStateException {
		Slot slot = fix(owner, id, obtain);
		E object = slot.get();
		
		if (unfix)
			slot.unfix();
		// checkBuffer();
		return object;
	}

	/**
	 * Updates the slot with the given id owned by the specified owner with the
	 * specifed object and flush function. When no such slot exists, the given
	 * object is inserted into the buffer. When the buffer overflows (it is
	 * full and all slots are fixed), an <code>IllegalStateException</code>
	 * will be thrown. Otherwise the <i>next</i> slot to displace will be
	 * determined by calling the victim method and its object and flush
	 * function will be replaced by the given object and flush function. When
	 * <code>unfix&nbsp;==&nbsp;true</code> the slot containing the desired
	 * object is unfixed at last.
	 * 
	 * <p>This implementation fixes the desired slot by calling this buffer's
	 * fix method with the specified owner, id and a constant function that
	 * always returns the given object, when it is invoked. Thereafter the
	 * slot's update method is called with the specified object and flush
	 * function. When <code>unfix&nbsp;==&nbsp;true</code>, its unfix method is
	 * called at last.</p>
	 * 
	 * @param owner the owner of the slot to update.
	 * @param id the id of the slot to update.
	 * @param object the object that replaces the object contained by the
	 *        desired slot.
	 * @param flush the function that replaces the flush function of the
	 *        desired slot.
	 * @param unfix a flag that determines whether the desired slot should be
	 *        unfixed after updating it or not.
	 * @throws IllegalStateException when the buffer overflows.
	 */
	public void update(O owner, I id, E object, Function<Object, ?> flush, boolean unfix) throws IllegalStateException {
		// checkBuffer();
		Slot slot = fix(owner, id, new Constant<E>(object));
		
		// remark: The object must not already be inside the slot,
		// but it can be!
		if (capacityBytes < Integer.MAX_VALUE) {
			sizeBytes -= slot.sizeBytes;
			sizeBytes += ((SizeAware)object).getMemSize();
		}
		slot.update(object, flush);
		
		handleSizeOverflow();
		
		// checkBuffer();
		if (unfix)
			slot.unfix();
	}

	/**
	 * Removes the object and any information belonging to it from the slot
	 * with the given id owned by the specified owner. This implementation
	 * checks whether such a slot exists and calls its remove method, when it
	 * exists.
	 *
	 * @param owner the owner of the slot to remove.
	 * @param id the id of the slot to remove.
	 */
	public void remove(O owner, I id) {
		Slot slot = lookUp(owner, id);
		
		if (slot != null) {
			sizeBytes -= slot.sizeBytes;
			slot.remove();
			// checkBuffer();
		}
	}

	/**
	 * Removes the objects and any information belonging to them from all slots
	 * in this buffer that are owned by the specified owner. This
	 * implementation gets all slot owned by the specified owner by using the
	 * map <code>owners</code> and calls the remove methods of these slots
	 * thereafter.
	 *
	 * @param owner the owner of the slots to remove.
	 */
	public void removeAll(O owner) {
		Map<I, Slot> members = owners.get(owner);
		
		if (members != null)
			for (Iterator<Entry<I, Slot>> entries = members.entrySet().iterator(); entries.hasNext();) {
				Slot slot = entries.next().getValue();
				sizeBytes -= slot.sizeBytes;
				entries.remove();
				slot.remove();
			}
	}

	/**
	 * Checks wheather aggregated values are still correct inside the
	 * structures of the buffer (sizes).
	 */
	public void checkBuffer() {
		int currentSize = 0;
		
		for (Map<I, Slot> map : owners.values())
			for (Slot slot : map.values())
				currentSize += slot.sizeBytes; 
		
		if (currentSize != this.sizeBytes)
			throw new RuntimeException("The size was not counted correctly (" + currentSize + " instead " + this.sizeBytes + ")");
	}
	
	/**
	 * Returns the number of fixed slots in this buffer.
	 */
	public int fixedSlots() {
		return fixedSlots;
	}
}
