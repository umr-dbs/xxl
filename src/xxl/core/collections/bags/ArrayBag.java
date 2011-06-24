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

package xxl.core.collections.bags;

import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides an implementation of the Bag interface that internally
 * uses an array with fixed size to store its elements.
 * 
 * <p>Each ArrayBag instance has a fixed capacity. The capacity is the size of
 * the array used to store the elements in the bag. It is always at least as
 * large as the bag size.</p>
 *
 * <p>The cursors returned by this class' <code>cursor</code>,
 * <code>fifoCursor</code>, <code>lifoCursor</code> and <code>query</code>
 * methods are <i>fail-fast</i>: if the bag is structurally modified at any
 * time after the cursor is created, in any way except through the cursor's own
 * methods, the cursor will throw a ConcurrentModificationException. Thus, in
 * the face of concurrent modification, the cursor fails quickly and cleanly,
 * rather than risking arbitrary, non-deterministic behavior at an undetermined
 * time in the future.</p>
 *
 * <p>Usage example (1).
 * <code><pre>
 *     // create a new array bag
 *
 *     ArrayBag&lt;Integer&gt; bag = new ArrayBag&lt;Integer&gt;();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Cursor&lt;Integer&gt; iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     bag.insertAll(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     Cursor&lt;Integer&gt; cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     iterator.close();
 *     cursor.close();
 *     bag.close();
 * </pre></code>
 *
 * Usage example (2).
 * <code><pre>
 *     // create an iteration over the Integer between 0 and 19
 *
 *     iterator = new Enumerator(20);
 *
 *     // create a new array bag that contains all elements of the given iterator
 *     
 *     bag = new ArrayBag&lt;Integer&gt;();
 *     
 *     // add all elements
 *     
 *     bag.insertAll(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // remove every even Integer from the cursor (and the underlying array bag)
 *
 *     while (cursor.hasNext()) {
 *         int i = cursor.next();
 *         if (i%2 == 0)
 *             cursor.remove();
 *     }
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     iterator.close();
 *     cursor.close();
 *     bag.close();
 * </pre></code>
 *
 * @param <E> the type of the elements this bag is able to store.
 * @see AbstractCursor
 * @see Cursor
 * @see Function
 */
public class ArrayBag<E> extends AbstractBag<E> implements FIFOBag<E>, LIFOBag<E> {

	/**
	 * A factory method to create a new ArrayBag (see contract for
	 * {@link Bag#FACTORY_METHOD FACTORY_METHOD} in interface Bag). In
	 * contradiction to the contract in Bag it may only be invoked with a
	 * <i>parameter list</i> (for further details see Function) of object
	 * arrays. The <i>parameter list</i> of object arrays will be used to
	 * initialize the internally used array with the object array at index 0.
	 *
	 * @see xxl.core.functions.Function
	 */
	public static final Function<Object[],ArrayBag<Object>> FACTORY_METHOD = new AbstractFunction<Object[],ArrayBag<Object>> () {
		@Override
		public ArrayBag<Object> invoke(List<? extends Object[]> list) {
			return new ArrayBag<Object>(list.get(0));
		}
	};

	/**
	 * The array is internally used to store the elements of the bag. Before an
	 * insertion and after the removal of an element this array is
	 * automatically resized by an ArrayResizer.
	 */
	protected Object[] array;

	/**
	 * An <code>int</code> field storing the number of elements in this bag.
	 */
	protected int last = -1;

	/**
	 * Constructs an array bag containing the elements of the specified array.
	 * The specified array has two different functions. First the bag depends
	 * on this array and is not able to contain more elements than the array is
	 * able to. Second it is used to initialize the bag. The size arguments
	 * only specifies the number of elements of the specified array which
	 * should be used to initialize the bag. The field <code>array</code> is
	 * set to the specified array and the field <code>last</code> is set to the
	 * specified size - 1.
	 *
	 * @param size the number of elements of the specified array which should
	 *        be used to initialize the bag.
	 * @param array the object array that is used to store the bag and
	 *        initialize the internally used array.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the specified
	 *         array.
	 */
	public ArrayBag(int size, E[] array) {
		if (array.length < size || size < 0)
			throw new IllegalArgumentException();
		this.array = array;
		last = size-1;
	}
	
	/**
	 * Constructs an array bag containing the elements of the specified array.
	 * This constructor is equivalent to the call of
	 * <code>ArrayBag(array, array.length)</code>.
	 *
	 * @param array the object array that is used to store the bag and
	 *        initialize the internally used array.
	 */
	public ArrayBag(E[] array) {
		this(array.length, array);
	}

	/**
	 * Constructs an empty array bag with a capacity of <code>size</code>
	 * elements. This constructor is equivalent to the call of
	 * <code>ArrayBag(0, new Object[size])</code>.
	 *
	 * @param size the maximal number of elements the bag is able to contain.
	 */
	public ArrayBag(int size) {
		if (size < 0)
			throw new IllegalArgumentException();
		this.array = new Object[size];
		last = -1;
	}

	/**
	 * Returns a cursor to iterate over the elements in this bag without any
	 * predefined sequence. The cursor is specifying a <i>view</i> on the
	 * elements of this bag so that closing the cursor takes no effect on the
	 * bag (e.g., not closing the bag). The behavior of the cursor is
	 * unspecified if this bag is modified while the cursor is in progress in
	 * any way other than by calling the methods of the cursor. So, when the
	 * implementation of this cursor cannot guarantee that the cursor is in a
	 * valid state after modifing the underlying bag every method of the cursor
	 * except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor to iterate over the elements in this bag without any
	 *         predefined sequence.
	 */
	@Override
	public Cursor<E> cursor() {
		return new AbstractCursor<E>() {
			int pos;

			{
				reset();
			}

			@Override
			public void reset(){
				super.reset();
				pos = 0;
			}
			
			@Override
			public boolean supportsReset() {
				return true;
			}

			@Override
			public boolean hasNextObject() {
				return pos <= last;
			}

			@Override
			@SuppressWarnings("unchecked") // only objects of type E are stored inside the Object array
			public E nextObject() {
				return (E)array[pos++];
			}

			@Override
			public void remove() throws IllegalStateException {
				super.remove();
				System.arraycopy(array, pos, array, pos-1, (last--)+1-(pos--));
			}
			
			@Override
			public boolean supportsRemove() {
				return true;
			}

			@Override
			public void update(E object) throws IllegalStateException {
				super.update(object);
				array[pos-1] = object;
			}
			
			@Override
			public boolean supportsUpdate() {
				return true;
			}
		};
	}

	/**
	 * Returns a cursor representing a FIFO (<i>first in, first out</i>)
	 * iteration over the elements in this bag. The cursor is specifying a
	 * <i>view</i> on the elements of this bag so that closing the cursor takes
	 * no effect on the bag (e.g., not closing the bag). The behavior of the
	 * cursor is unspecified if this bag is modified while the cursor is in
	 * progress in any way other than by calling the methods of the cursor. So,
	 * when the implementation of this cursor cannot guarantee that the cursor
	 * is in a valid state after modifing the underlying bag every method of
	 * the cursor except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor representing a FIFO (<i>first in, first out</i>)
	 *         iteration over the elements in this bag.
	 */
	public Cursor<E> fifoCursor () {
		return cursor();
	}

	/**
	 * Returns a cursor representing a LIFO (<i>last in, first out</i>)
	 * iteration over the elements in this bag. The cursor is specifying a
	 * <i>view</i> on the elements of this bag so that closing the cursor takes
	 * no effect on the bag (e.g., not closing the bag). The behavior of the
	 * cursor is unspecified if this bag is modified while the cursor is in
	 * progress in any way other than by calling the methods of the cursor. So,
	 * when the implementation of this cursor cannot guarantee that the cursor
	 * is in a valid state after modifing the underlying bag every method of
	 * the cursor except <code>close()</code> should throw a
	 * <code>ConcurrentModificationException</code>.
	 *
	 * @return a cursor representing a LIFO (<i>last in, first out</i>)
	 *         iteration over the elements in this bag.
	 */
	public Cursor<E> lifoCursor () {
		return new AbstractCursor<E>() {
			int pos;
			
			{
				reset();
			}

			@Override
			public void reset(){
				super.reset();
				pos = last;
			}
			
			@Override
			public boolean supportsReset() {
				return true;
			}

			@Override
			public boolean hasNextObject() {
				return pos >= 0;
			}
			
			@Override
			@SuppressWarnings("unchecked") // only objects of type E are stored inside the Object array
			public E nextObject() {
				return (E)array[pos--];
			}

			@Override
			public void remove() throws IllegalStateException {
				super.remove();
				//System.arraycopy(array, pos+2, array, pos+1, (--last)-pos); //DEBUG
				last--; //DEBUG: NEW: System.arraycopy ist sehr teuer!
			}
			
			@Override
			public boolean supportsRemove() {
				return true;
			}

			@Override
			public void update(E object) throws IllegalStateException {
				super.update(object);
				array[pos+1] = object;
			}
			
			@Override
			public boolean supportsUpdate() {
				return true;
			}
		};
	}

	/**
	 * Adds the specified element to this bag. This method does not perform any
	 * kind of <i>duplicate detection</i>.
	 *
	 * @param object element to be added to this bag.
	 */
	@Override
	public void insert(E object) {
		array[++last] = object;
	}

	/**
	 * Returns the number of elements in this bag (its cardinality). If this
	 * bag contains more than <code>Integer.MAX_VALUE</code> elements,
	 * <code>Integer.MAX_VALUE</code> is returned.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	@Override
	public int size() {
		return last+1;
	}

	/**
	 * Removes all of the elements from this bag. The bag will be empty after
	 * this call so that <code>size() == 0</code>.
	 * 
	 * <p>This implementation creates a cursor by calling the cursor method and
	 * iterates over it, removing each element using the
	 * {@link Cursor#remove() Cursor.remove} operation. Most implementations
	 * will probably choose to override this method for efficiency.</p>
	 */
	@Override
	public void clear() {
		last = -1;
	}
}
