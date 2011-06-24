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

package xxl.core.collections.queues;

public abstract class AbstractMemoryManageableQueue<E> extends DecoratorQueue<E> implements MemoryManageableQueue<E> {

    protected final int objectSize;
    protected int assignedMemSize;
    
    public AbstractMemoryManageableQueue(Queue<E> queue, int objectSize) {
        super(queue);
        this.objectSize = objectSize;
    }
    
    /**
	 * Appends the specified element to the <i>end</i> of this queue. The
	 * <i>end</i> of the queue is given by its <i>strategy</i>.
	 * 
	 * @param object element to be appended at the <i>end</i> of this
	 *        queue.
	 * @throws IllegalStateException if the queue is already closed when this
	 *         method is called.
	 */
	public void enqueue(E object) throws IllegalStateException {
		super.enqueue(object);
		if (getCurrentMemUsage()>assignedMemSize)
			handleOverflow();
	}
    
    /**
	 * Returns the estimated size of the objects which are stored
	 * in the memory of this memory manageable object.
	 * This method can be called from the strategy of the memory manager
	 * to obtain useful information for distributing main memory among the
	 * memory using objects.
	 * 
	 * @return Returns the size of the objects in this SweepArea (in bytes).
	 */
	public int getObjectSize() {
	    return objectSize;
	}

	/**
	 * Returns the amount of memory which is needed by this object
	 * for an acceptable performance.
	 * This method can be called from the strategy of the memory manager
	 * to obtain useful information for distributing main memory among the
	 * memory using objects.
	 * 
	 * @return Returns the preferred amount of memory (in bytes).
 	 */
	public int getPreferredMemSize() {
	    return MAXIMUM;
	}
	
	/**
	 * Returns the amount of memory, which is actually assigned to this
	 * object by the memory manager.
	 * 
	 * @return Returns the assigned amount of memory (in bytes).
 	 */
	public int getAssignedMemSize() {
	    return assignedMemSize;
	}
	
	/**
	 * Assigns a special amount of memory to this object.
	 * 
	 * @param newMemSize The amount of memory to be assigned to this object
	 *                   (in bytes).
 	 */
	public void assignMemSize(int newMemSize) {
	    this.assignedMemSize = newMemSize;
	    if (getCurrentMemUsage() > assignedMemSize)
	        handleOverflow();
	}

	/**
	 * Returns the amount of memory, which is actually used by this object.
	 * This method can be called from the strategy of the memory manager to
	 * obtain useful information for distributing main memory among the
	 * memory using objects.
	 * 
	 * @return Returns the amount of memory actually used by this object
	 *         (in bytes).
 	 */
	public int getCurrentMemUsage() {
	    return objectSize * queue.size();
	}
    
	/**
	 * This method is called, if an overflow has occured.
	 * Implementors of subclasses have to specify
	 * how to handle the overflow.
	 */
	public abstract void handleOverflow();
	
}
