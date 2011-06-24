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
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

/**
 * This class provides the heap datastructure. The <tt>clear</tt>,
 * <tt>size</tt>, <tt>peek</tt> operations and the constructors that create 
 * an empty heap run in constant time. The <tt>heapify</tt> operation 
 * and the constructors that initialize the heap with an array 
 * heap with an array of objects run in amortized constant time, that is, 
 * adding n elements requires O(n) time.
 *
 * <p>The heap is stored in an internally used array that is never resized by
 * methods of this class. This class provides no methods to change the
 * maximum capacity of a heap. So remember that the internally used array
 * must be able to contain all the elements the heap should be able to
 * contain when creating a new heap. There are two way of ensuring the
 * capacity of a heap when constructing it
 * <ul>
 * <li>Constructing an empty heap with the constructor
 * <tt>Heap(int size)</tt> or
 * <tt>Heap(int size, Comparator comparator)</tt>. The internally used
 * array is set to <tt>new Object[size]</tt>.</li>
 * <li>Constructing a heap containing the elements of a specified array
 * with one of the other constructors. The internally used array is set to
 * the specified array. In this case the size of the specified array is
 * set to the maximal number of elements the heap is able to contain.</li>
 * </ul></p>
 *
 * <p>The order of the heap is determined by a comparator. More exactly,
 * there can be three different cases when two elements <tt>o1</tt> and
 * <tt>o2</tt> are inserted into the heap
 * <ul>
 * <dl>
 * <dt><li><tt>comparator.compare(o1, o2) &lt; 0</tt> :</li></dt>
 * <dd>the heap returns <tt>o1</tt> prior to returning <tt>o2</tt>.</dd>
 * <dt><li><tt>comparator.compare(o1, o2) == 0</tt> :</li></dt>
 * <dd>when inserting equal elements (determined by the used comparator),
 * there is no guarantee which one will be returned first.
 * <dt><li><tt>comparator.compare(o1, o2) &gt; 0</tt> :</li></dt>
 * <dd>the heap returns <tt>o2</tt> prior to returning <tt>o1</tt>.</dd>
 * </dl>
 * </ul></p>
 *
 * <p><b>Note:</b> This heap implementation has an anomaly: the tree has a
 * special root node.<br />
 * This does not have any impact on working with this
 * heap class but the implementation is quite different from common heap 
 * implementations.
 * <pre>
 *                                     [0]   <----- !!!
 *                                      |
 *                                     [1]
 *                                    /   \
 *                                 [2]     [3]
 *                                 / \     / \
 *                                .   .   .   .
 *                               .     . .     .
 * </pre></p>
 * 
 * <p>Usage example (1).
 * <pre>
 *     // create an array of objects to store in the heap
 *
 *     Object[] array = new Object[] {
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
 *     // create a comparator that compares the objects by comparing their 
 *     // Integers
 *
 *     Comparator<Object> comparator = new Comparator<Object>() {
 *         public int compare(Object o1, Object o2) {
 *             return ((Integer)((Object[])o1)[0]).intValue()
 *                    -((Integer)((Object[])o2)[0]).intValue();
 *         }
 *     };
 *
 *     // create a heap that is initialized with the array (the heap has maximum capacity of
 *     // 5 elements (array.length) and contains 5 elements) and that uses the given comparator
 *
 *     Heap<Object> heap = new Heap<Object>(array, comparator);
 *
 *     // open the heap
 *
 *     heap.open();
 *
 *     // print the elements of the heap
 *
 *     while (!heap.isEmpty()) {
 *          Object[] o = (Object[])heap.dequeue();
 *          System.out.println("Integer = "+o[0]+" & String = "+o[1]);
 *     }
 *     System.out.println();
 *
 *     // close the open heap after use
 *
 *     heap.close();
 * </pre></p>
 *
 * <p>Usage example (2).
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
 *     // create a heap that is initialized with the first three elements of the array (the heap
 *     // has a maximum capacity of 5 elements (array.length) and contains 3 elements) and that
 *     // uses the given comparator
 *
 *     heap = new Heap<Object>(array, 3, comparator);
 *
 *     // open the heap
 *
 *     heap.open();
 *
 *     // print the elements of the heap
 *
 *     while (!heap.isEmpty()) {
 *         Object[] o = (Object[])heap.dequeue();
 *         System.out.println("Integer = "+o[0]+" & String = "+o[1]);
 *     }
 *     System.out.println();
 *
 *     // close the open heap after use
 *
 *     heap.close();
 * </pre></p>
 *
 * <p>Usage example (3).
 * <pre>
 *     // refresh the array (it was internally used into the heap and has changed
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
 *     // create an empty heap with a maximum capacity of 5 elements and that uses the given
 *     // comparator
 *
 *     heap = new Heap<Object>(5, comparator);
 *
 *     // open the heap
 *
 *      heap.open();
 *
 *      // generate an iteration over the elements of the given array
 *
 *      Cursor<Object> cursor = new ArrayCursor<Object>(array);
 *
 *      // insert all elements of the given iterator
 *
 *      for (; cursor.hasNext(); heap.enqueue(cursor.next()));
 *
 *      // print the elements of the heap
 *      while (!heap.isEmpty()) {
 *          Object[] o = (Object[])heap.dequeue();
 *          System.out.println("Integer = "+o[0]+" & String = "+o[1]);
 *      }
 *      System.out.println();
 *
 *      // close the open heap and cursor after use
 *  
 *      heap.close();
 *      cursor.close();
 * </pre></p>
 *
 * @param <E> the type of the elements of this queue.
 * @see xxl.core.collections.queues.Queue
 * @see xxl.core.collections.queues.AbstractQueue
 */
public class Heap<E> extends AbstractQueue<E> {

	/**
	 * The array is internally used to store the elements of the heap. The
	 * size of this array defines the maximum capacity of the heap.
	 */
	protected Object[] array;

	/**
	 * Stores the current version of the heap. After each enqueue/dequeue
	 * operation, the version counter is incremented.
	 */
	protected long version;
	
	/**
	 * The comparator to determine the order of the heap. More exactly,
	 * there can be three different cases when two elements <tt>o1</tt>
	 * and <tt>o2</tt> are inserted into the heap
	 * <ul>
	 * <dl>
	 * <dt><li><tt>comparator.compare(o1, o2) < 0</tt> :</dt>
	 * <dd>the heap returns <tt>o1</tt> prior to returning <tt>o2</tt>.</dd>
	 * <dt><li><tt>comparator.compare(o1, o2) == 0</tt> :</dt>
	 * <dd>when inserting equal elements (determined by the used
	 * comparator), there is no guarantee which one will be returned
	 * first.
	 * <dt><li><tt>comparator.compare(o1, o2) > 0</tt> :</dt>
	 * <dd>the heap returns <tt>o2</tt> prior to returning <tt>o1</tt>.</dd>
	 * </dl>
	 * </ul>
	 */
	protected Comparator<? super E> comparator;

	/**
	 * Constructs a heap containing the elements of the specified array
	 * that returns them according to the order induced by the specified
	 * comparator. The specified array has two different functions. First, 
	 * the heap depends on this array and is not able to contain more
	 * elements than the array is able to. Second, it is used to initialize
	 * the heap. The field <tt>array</tt> is set to the specified array,
	 * the field <tt>last</tt> is set to the specified size - 1 and the
	 * field <tt>comparator</tt> is set to the specified comparator. After
	 * initializing the fields the heapify method is called.
	 *
	 * @param array the object array that is used to store the heap and
	 *        initialize the internally used array.
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the heap.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 */
	public Heap(E[] array, int size, Comparator<? super E> comparator) throws IllegalArgumentException {
		if (array.length < size || size < 0)
			throw new IllegalArgumentException();
		this.array = array;
		this.size = size;
		this.comparator = comparator;
		
		version = 0;
	}

	/**
	 * Constructs a heap containing the elements of the specified array
	 * that returns them according to the order induced by the specified
	 * comparator. This constructor is equivalent to the call of
	 * <code>Heap(array, array.length, comparator)</code>.
	 *
	 * @param array the object array that is used to store the heap and
	 *        initialize the internally used array.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 */
	public Heap(E[] array, Comparator<? super E> comparator) {
		this(array, array.length, comparator);
	}

	/**
	 * Constructs an empty heap with a capacity of <tt>size</tt> elements and 
	 * uses the specified comparator to order elements when inserted. This
	 * constructor is equivalent to the call of
	 * <code>Heap(new Object[size], 0, comparator)</code>.
	 *
	 * @param size the maximal number of elements the heap is able to
	 *        contain.
	 * @param comparator the comparator to determine the order of the
	 *        heap.
	 */
	public Heap(int size, Comparator<? super E> comparator) {
		this.array = new Object[size];
		this.size = 0;
		this.comparator = comparator;
		
		version = 0;
	}

	/**
	 * Computes the heap for the first (<tt>size</tt>) elements of the
	 * internally used array in O(n) time.
	 */
	protected void heapify() {
		version++;
		if (size > 1) {
			for (int i = size/2-1; i > 0; i--) {
				Object top = array[i/2];
				bubbleUp((E)array[i], sinkIn(i));
				array[i/2] = top;
			}
			enqueue(replace((E)array[--size]));
		}
	}

	/**
	 * Inserts the specified object into the heap and overwrites the
	 * element at the index <tt>i</tt> of the internally used array without
	 * damaging the structure of the heap. The specified object is
	 * inserted into the path from the root of the heap to the element
	 * with the index <tt>i</tt> and the whole path beyond the inserted element 
	 * is <i>shifted</i> one level down. This method only works fine when the
	 * following prerequisites are valid
	 * <ul>
	 * <li><tt>0 &le; i &le; last</tt></li>
	 * <li><tt>comparator.compare(array[0], object) &le; 0</tt></li>
	 * </ul>
	 *
	 * @param object the object to insert into the heap.
	 * @param i an index of the internally used array. The specified
	 *        object is inserted into the path from the root of the heap
	 *        to the element with the index i.
	 */
	protected void bubbleUp(E object, int i) {
		// prerequisite: 0 <= i <= last && array[0] <= object
		while (comparator.compare(object, (E)array[i/2]) < 0) {
			array[i] = array[i /= 2];
			if (i == 0) break;
		}
		array[i] = object;
	}

	/**
	 * Removes the element at the index i/2 of the internally used array
	 * without damaging the structure of the heap. The whole path from the
	 * element at the index i of the internally used array to the bottom
	 * level of the heap is shifted one level up. This method only works
	 * fine when the following prerequisite is valid
	 * <ul>
	 * <li><tt>1 &le; i &le; last</tt></li>
	 * </ul>
	 *
	 * @param i an index of the internally used array. The object at the
	 *        index i/2 of the internally used array is removed.
	 * @return the index of the last element in the path that is shifted
	 *         one level up.
	 */
	protected int sinkIn(int i) {
		// prerequisite: 1 <= i <= last
		array[i/2] = array[i];
		while ((i *= 2) < size-1)
			array[(comparator.compare((E)array[i], (E)array[i+1]) < 0 ? i : i++)/2] = array[i];
		return i/2;
	}

	/**
	 * Opens the heap, i.e., signals the heap to reserve resources and
	 * initializes itself. Before a queue has been opened calls to methods like 
	 * <tt>peek</tt> are not guaranteed to yield proper results.
	 * Therefore <tt>open</tt> must be called before a queue's data
	 * can be processed. Multiple calls to <tt>open</tt> do not have any effect,
	 * i.e., if <tt>open</tt> was called the queue remains in the state
	 * <i>opened</i> until its <tt>close</tt> method is called.
	 * 
	 * <p>Note, that a call to the <tt>open</tt> method of a closed queue
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (isOpened) return;
		super.open();
		heapify();
	}

	/**
	 * Removes all elements from this heap. The heap will be empty
	 * after this call returns so that <tt>size() == 0</tt>.
	 */
	public void clear() {
		size = 0;
		version++;
	}

	/**
	 * Inserts the specified element into this heap and restores the
	 * structure of the heap if necessary.
	 *
	 * @param object element to be inserted into this heap.
	 */
	protected void enqueueObject(E object) {
		version++;
		if (size > 0) {
			if (comparator.compare(object, (E)array[0]) < 0) {
				E top  = (E)array[0];
				array[0] = object;
				object = top;
			}
			bubbleUp(object, size);
		}
		else
			array[0] = object;
	}

	/**
	 * Returns the <i>next</i> element in the heap <i>without</i> removing it.
	 * The <i>next</i> element of the heap is determined by the
	 * comparator.
	 *
	 * @return the <i>next</i> element in the heap.
	 */
	protected E peekObject() {
		return (E)array[0];
	}

	/**
	 * Returns the <i>next</i> element in the heap and <i>removes</i> it. 
	 * The <i>next</i> element of the heap is determined by the comparator.
	 *
	 * @return the <i>next</i> element in the heap.
	 */
	protected E dequeueObject() {
		version++;
		E minimum = (E)array[0];
		if (size > 1)
			bubbleUp((E)array[size-1], sinkIn(1));
		return minimum;
	}

	/**
	 * Replaces the next element in the heap with the specified object and
	 * restore the structure of the heap if necessary. More exactly, the
	 * specified object is inserted into the heap and the next element of
	 * the heap is overwritten. Thereafter the structure of the heap is
	 * restored and the overwritten element is returned. This method only
	 * works fine when the following prerequisite is valid
	 * <ul>
	 * <li><tt>comparator.compare(array[0], object) &le; 0</tt></li>
	 * </ul>
	 *
	 * @param object element to be inserted into this heap.
	 * @return the <i>next</i> element in the heap.
	 * @throws NoSuchElementException queue has no more elements.
	 */
	public E replace(E object) throws NoSuchElementException {
		version++;
		// prerequisite: array[0] <= object
		if (isClosed)
			throw new IllegalStateException();	
		if (!isOpened)
			open();
		if (size <= 0)
			throw new NoSuchElementException();
		E minimum = (E)array[0];
		if (size > 1 && comparator.compare(object, (E)array[1]) > 0) {
			int up = sinkIn(1);
			if (2*up == size-1)
				array[up] = array[up = size-1];
			bubbleUp(object, up);
		}
		else
			array[0] = object;
		computedNext = false;
		return minimum;
	}

	/**
	 * If an object is updated at position index, the heap 
	 * condition may be violated. This methode bubbles up the
	 * updated element or lets it sink down, until the condition
	 * is satisfied.
	 * @param index The index inside the array where an update occured.
	 */
	protected void update(int index) {
		computedNext = false;
		bubbleUp((E)array[index], index);
		// sink without removal!
		int i=index;
		if (i==0) {
			if (comparator.compare((E)array[0], (E)array[1]) > 0 ) {
				Object tmp=array[0];
				array[0] = array[1];
				array[1] = tmp;
				i=1;
			}
		}
		if (i!=0) {
			while (2*i < size) {
				int smallerIndex=2*i;
				if (smallerIndex+1<size)
					if (comparator.compare((E)array[smallerIndex], (E)array[smallerIndex+1]) > 0 )
						smallerIndex++;
				if (comparator.compare((E)array[smallerIndex], (E)array[i]) < 0 ) {
					Object tmp=array[i];
					array[i] = array[smallerIndex];
					array[smallerIndex] = tmp;
					i=smallerIndex;
				}
				else
					break;
			}
		}
	}

	/**
	 * Returns a Cursor which supports reset, update and remove.
	 * After an update or remove operation, the cursor is in an
	 * invalid state. The only way to reuse the cursor then, is
	 * to call reset.
	 * @return A Cursor iterating over the elements of the heap.
	 */
	public Cursor<E> cursor() {
		return new AbstractCursor<E>() {
			private long myVersion=version;
			private int index=-1;
			protected boolean hasNextObject() {
				if (myVersion!=version)
					throw new ConcurrentModificationException();
				return index+1<size;
			}
			protected E nextObject() {
				index++;
				return (E)array[index];
			}
			public void remove() {
				if (size>1) {
					// array[size-1]>=array[0]
					array[index]=array[size-1];
					size--;
					Heap.this.update(index);
				}
				else
					size=0;
				version++;
			}
			public boolean supportsRemove() {
				return true;
			}
			public void update(E object) {
				array[index]=object;
				Heap.this.update(index);
				version++;
			}
			public boolean supportsUpdate() {
				return true;
			}
			public void reset() {
				index=-1;
				myVersion = version;
			}
			public boolean supportsReset() {
				return true;
			}
		};
	}

	/**
	 * @return Returns a string representation of the heap.
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < size; i++) {			
			sb.append(((E)array[i]).toString()+ (i==size-1 ? "" : ", "));
		}			
		sb.append("]");
		return sb.toString();
	}
}
