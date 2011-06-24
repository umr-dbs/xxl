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

import java.util.Comparator;

import xxl.core.util.ArrayResizer;

/**
 * This class provides a dynamical resizable-array implementation of the
 * heap datastructure. In addition to the advantages of a heap this class
 * uses an ArrayResizer to automatically adjust the size of the internally 
 * used array to the size of the heap. This means that it is not
 * necessary to specify the maximum capacity of this heap in advance.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create an array of objects to store in the heap
 *
 *     Object[] array = new Object[] {
 *          new Object[] {
 *              new Integer(1),
 *              new String("first")
 *          },
 *          new Object[] {
 *              new Integer(2),
 *              new String("first")
 *          },
 *          new Object[] {
 *              new Integer(1),
 *              new String("second")
 *          },
 *          new Object[] {
 *              new Integer(3),
 *              new String("first")
 *          },
 *          new Object[] {
 *              new Integer(1),
 *              new String("third")
 *          }
 *    };
 *
 *     // create a comparator that compares the objects by comparing
 *     // their Integers
 *
 *     Comparator&lt;Object&gt; comparator = new Comparator&lt;Object&gt;() {
 *          public int compare(Object o1, Object o2) {
 *             return ((Integer)((Object[])o1)[0]).intValue() - 
 *                    ((Integer)((Object[])o2)[0]).intValue();
 *          }
 *     };
 *
 *     // create a heap that is initialized with the array and that uses
 *     // the given comparator
 *
 *     DynamicHeap&lt;Object&gt; heap = new DynamicHeap&lt;Object&gt;(array, comparator);
 *
 *     // open the heap
 *     
 *     heap.open();
 *
 *     // insert two elements
 *
 *     heap.enqueue(new Object[] {
 *          new Integer(4),
 *          new String("first")
 *     });
 *     heap.enqueue(new Object[] {
 *          new Integer(1),
 *          new String("fourth")
 *     });
 *
 *     // print the elements of the heap
 *
 *     while (!heap.isEmpty()) {
 *           Object[] o = (Object[])heap.dequeue();
 *           System.out.println("Integer = "+o[0]+" & String = "+o[1]);
 *     }
 *     System.out.println();
 *
 *     // close the open heap after use
 *
 *     heap.close();
 * </pre>
 *
 * Usage example (2).
 * <pre>
 *     // refresh the array (it was internally used into the heap and has changed)
 *
 *     array = new Object[] {
 *         new Object[] {
 *             new Integer(1),
 *             new String("first")
 *         },
 *         new Object[] {
 *             new Integer(2),
 *             new String("first")
 *         },
 *         new Object[] {
 *             new Integer(1),
 *             new String("second")
 *         },
 *         new Object[] {
 *             new Integer(3),
 *             new String("first")
 *         },
 *         new Object[] {
 *             new Integer(1),
 *             new String("third")
 *         }
 *     };
 *
 *     // create an empty heap that uses the given comparator
 *
 *     heap = new DynamicHeap&lt;Object&gt;(comparator);
 *
 *     // open the heap
 *
 *     heap.open();
 *
 *     // generate an iteration over the elements of the given array
 *
 *      Iterator&lt;Object&gt; iterator = new ArrayCursor&lt;Object&gt;(array);
 * 
 *      // insert all elements of the given iterator
 * 
 *      for (; iterator.hasNext(); heap.enqueue(iterator.next()));
 *
 *      // print the elements of the heap
 *    
 *      while (!heap.isEmpty()) {
 *         Object[] o = (Object[])heap.dequeue();
 *         System.out.println("Integer = "+o[0]+" & String = "+o[1]);
 *      }
 *      System.out.println();
 *
 *      // close the open heap after use
 *
 *      heap.close();
 * </pre>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.Heap
 * @see xxl.core.util.ArrayResizer
 */
public class DynamicHeap<E> extends Heap<E> {

	/**
	 * An ArrayResizer managing the growth policy for internally used
	 * array. The ArrayResizer decides before an insertion or after an
	 * removal of an element whether the array is to be resized or not.
	 *
	 * @see ArrayResizer
	 */
	protected ArrayResizer resizer;

	/**
	 * Constructs a dynamic heap containing the elements of the specified
	 * array that returns them according to the order induced by the
	 * specified comparator. The heap is initialized by the call of
	 * <code>new Heap(array, size, comparator)</code> and the growth
	 * policy is managed by the specified ArrayResizer.
	 *
	 * @param array the object array that is used to store the heap and
	 *        initialize the internally used array.
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the heap.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @param resizer an ArrayResizer managing the growth policy for the
	 *        internally used array.
	 */
	public DynamicHeap(E[] array, int size, Comparator<? super E> comparator, ArrayResizer resizer) {
		super(array, size, comparator);
		this.resizer = resizer;
	}

	/**
	 * Constructs a dynamic heap containing the elements of the specified
	 * array that returns them according to the order induced by the
	 * specified comparator. This constructor is equivalent to the call of
	 * <code>new DynamicHeap(array, array.length, comparator, resizer)</code>.
	 *
	 * @param array the object array that is used to store the heap and
	 *        initialize the internally used array.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @param resizer an ArrayResizer managing the growth policy for the
	 *        internally used array.
	 */
	public DynamicHeap(E[] array, Comparator<? super E> comparator, ArrayResizer resizer) {
		this(array, array.length, comparator, resizer);
	}

	/**
	 * Constructs an empty dynamic heap and uses the specified comparator
	 * to order elements when inserted. This constructor is equivalent to
	 * the call of
	 * <code>new DynamicHeap(new Object[0], 0, comparator, resizer)</code>.
	 *
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @param resizer an ArrayResizer managing the growth policy for the
	 *        internally used array.
	 */
	public DynamicHeap(Comparator<? super E> comparator, ArrayResizer resizer) {
		super(0, comparator);
		this.resizer = resizer;
	}

	/**
	 * Constructs a dynamic heap containing the elements of the specified
	 * array that returns them according to the order induced by the
	 * specified comparator. The growth policy is managed by
	 * <tt>ArrayResizer.DEFAULT_INSTANCE</tt>. This constructor is
	 * equivalent to the call of
	 * <code>new DynamicHeap(array, size, comparator, ArrayResizer.DEFAULT_INSTANCE)</code>.
	 *
	 * @param array the object array that is used to store the heap and
	 *        initialize the internally used array.
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the heap.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @see ArrayResizer#DEFAULT_INSTANCE
	 */
	public DynamicHeap(E[] array, int size, Comparator<? super E> comparator) {
		this(array, size, comparator, ArrayResizer.DEFAULT_INSTANCE);
	}

	/**
	 * Constructs a dynamic heap containing the elements of the specified
	 * array that returns them according to the order induced by the
	 * specified comparator. The growth policy is managed by
	 * <tt>ArrayResizer.DEFAULT_INSTANCE</tt>. This constructor is
	 * equivalent to the call of
	 * <code>new DynamicHeap(array, array.length, comparator, ArrayResizer.DEFAULT_INSTANCE)</code>.
	 *
	 * @param array the object array that is used to store the heap and
	 *        initialize the internally used array.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @see ArrayResizer#DEFAULT_INSTANCE
	 */
	public DynamicHeap(E[] array, Comparator<? super E> comparator) {
		this(array, array.length, comparator);
	}

	/**
	 * Constructs an empty dynamic heap and uses the specified comparator
	 * to order elements when inserted. The growth policy is managed by
	 * <tt>ArrayResizer.DEFAULT_INSTANCE</tt>. This constructor is
	 * equivalent to the call of
	 * <code>new DynamicHeap(new Object[0], 0, comparator, ArrayResizer.DEFAULT_INSTANCE)</code>.
	 *
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @see ArrayResizer#DEFAULT_INSTANCE
	 */
	public DynamicHeap(Comparator<? super E> comparator) {
		this(comparator, ArrayResizer.DEFAULT_INSTANCE);
	}

	/**
	 * Removes all elements from this heap. The heap will be empty
	 * after this call returns so that <tt>size() == 0</tt>.
	 */
	public void clear() {
		array = resizer.resize(array, 0);
		super.clear();
	}

	/**
	 * Inserts the specified element into this heap and restores the
	 * structure of the heap if necessary.
	 *
	 * @param object element to be inserted into this heap.
	 */
	protected void enqueueObject(E object) {
		if (resizer != null)
			array = resizer.resize(array, size()+1);
		super.enqueueObject(object);
	}

	/**
	 * Returns the <i>next</i> element in the heap and <i>removes</i> it. 
	 * The <i>next</i> element of the heap is determined by the comparator.
	 *
	 * @return the <i>next</i> element in the heap.
	 */
	protected E dequeueObject() {
		E minimum = super.dequeueObject();
		array = resizer.resize(array, size());
		return minimum;
	}
}
