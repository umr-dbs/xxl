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

import java.util.Iterator;
import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.util.ArrayResizer;

/**
 * This class provides an implementation of the Bag interface that
 * internally uses a resizable array to store its elements. <p>
 *
 * Each DynamicArrayBag instance has a capacity. The capacity is the size
 * of the array used to store the elements in the bag. It is always at
 * least as large as the bag size. As elements are inserted into a
 * DynamicArrayBag, its capacity may grow automatically. The details of
 * the growth policy are specified by the ArrayResizer.<p>
 *
 * The cursors returned by this class' <tt>cursor</tt>,
 * <tt>fifoCursor</tt>, <tt>lifoCursor</tt> and <tt>query</tt> methods are
 * <i>fail-fast</i>: if the bag is structurally modified at any time after
 * the cursor is created, in any way except through the cursor's own
 * methods, the cursor will throw a ConcurrentModificationException. Thus,
 * in the face of concurrent modification, the cursor fails quickly and
 * cleanly, rather than risking arbitrary, non-deterministic behavior at
 * an undetermined time in the future.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new dynamic array bag
 *
 *     DynamicArrayBag bag = new DynamicArrayBag();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Iterator iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     bag.insertAll(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     Cursor cursor = bag.cursor();
 *
 *     // print all elements of the cursor (bag)
 *
 *     while (cursor.hasNext())
 *         System.out.println(cursor.next());
 *
 *     // close the open iterator, cursor and bag after use
 *
 *     ((Cursor)iterator).close();
 *     cursor.close();
 *     bag.close();
 * </pre>
 *
 * Usage example (2).
 * <pre>
 *     // create an iteration over the Integer between 0 and 19
 *
 *     iterator = new Enumerator(20);
 *
 *     // create a new dynamic array bag that contains all elements of the given iterator
 *
 *     bag = new DynamicArrayBag(iterator);
 *
 *     // create a cursor that iterates over the elements of the bag
 *
 *     cursor = bag.cursor();
 *
 *     // remove every even Integer from the cursor (and the underlying dynamic array bag)
 *
 *     while (cursor.hasNext()) {
 *         int i = ((Integer)cursor.next()).intValue();
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
 *     ((Cursor)iterator).close();
 *     cursor.close();
 *     bag.close();
 * </pre>
 *
 * @see AbstractCursor
 * @see ArrayResizer
 * @see Cursor
 * @see Function
 * @see Iterator
 */
public class DynamicArrayBag extends ArrayBag {

	/**
	 * A factory method to create a new DynamicArrayBag (see contract for
	 * {@link Bag#FACTORY_METHOD FACTORY_METHOD} in interface Bag). It may
	 * be invoked with a <i>parameter list</i> (for further
	 * details see Function) of object arrays, an iterator or without any
	 * parameters. A <i>parameter list</i> of object arrays will
	 * be used to initialize the internally used array with the object
	 * array at index 0 and an iterator will be used to insert the
	 * contained elements into the new DynamicArrayBag.
	 *
	 * @see Function
	 */
	public static final Function<Object,DynamicArrayBag> FACTORY_METHOD = new AbstractFunction<Object,DynamicArrayBag> () {
		public DynamicArrayBag invoke () {
			return new DynamicArrayBag();
		}

		public DynamicArrayBag invoke (Object iterator) {
			return new DynamicArrayBag((Iterator) iterator);
		}

		public DynamicArrayBag invoke (List<? extends Object> list) {
			return new DynamicArrayBag((Object[])list.get(0));
		}
	};

	/**
	 * An ArrayResizer managing the growth policy for internally used
	 * array. The ArrayResizer decides before an insertion or after an
	 * removal of an element whether the array is to be resized or not.
	 *
	 * @see ArrayResizer
	 */
	protected ArrayResizer resizer;

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified array resizer.
	 * The field <tt>array</tt> is set to the specified array, the field
	 * <tt>last</tt> is set to the specified size - 1 and the field
	 * <tt>resizer</tt> (growth policy) is set to the specified resizer.
	 *
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the internally used array.
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param resizer an ArrayResizer managing the growth policy for the
	 *        internally used array.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 */
	public DynamicArrayBag (int size, Object [] array, ArrayResizer resizer) throws IllegalArgumentException {
		super(size, array);
		this.resizer = resizer;
	}
	
	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified array resizer.
	 * This constructor is equivalent to the call of
	 * <code>DynamicArrayBag(array.length, array, resizer)</code>.
	 *
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param resizer an ArrayResizer managing the growth policy for the
	 *        internally used array.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 */
	public DynamicArrayBag (Object [] array, ArrayResizer resizer) throws IllegalArgumentException {
		this(array.length, array, resizer);
	}
	
	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified <tt>double</tt>
	 * parameters. This constructor is equivalent to the call of
	 * <code>DynamicArrayBag(size, array, new ArrayResizer(fmin, fover, funder))</code>.
	 *
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the internally used array.
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param fmin a control parameter for the growth policy.
	 * @param fover a control parameter for the growth policy.
	 * @param funder a control parameter for the growth policy.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 * @see ArrayResizer#ArrayResizer(double, double, double)
	 */
	public DynamicArrayBag (int size, Object [] array, double fmin, double fover, double funder) throws IllegalArgumentException {
		this(size, array, new ArrayResizer(fmin, fover, funder));
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified <tt>double</tt>
	 * parameters. This constructor is equivalent to the call of
	 * <code>DynamicArrayBag(array.length, array, new ArrayResizer(fmin, fover, funder))</code>.
	 *
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param fmin a control parameter for the growth policy.
	 * @param fover a control parameter for the growth policy.
	 * @param funder a control parameter for the growth policy.
	 * @see ArrayResizer#ArrayResizer(double, double, double)
	 */
	public DynamicArrayBag (Object [] array, double fmin, double fover, double funder) {
		this(array.length, array, fmin, fover, funder);
	}

	/**
	 * Constructs an empty bag with a growth polity that depends on the
	 * specified <tt>double</tt> parameters. This constructor is equivalent
	 * to the call of
	 * <code>DynamicArrayBag(0, new Object[0], new ArrayResizer(fmin, fover, funder))</code>.
	 *
	 * @param fmin a control parameter for the growth policy.
	 * @param fover a control parameter for the growth policy.
	 * @param funder a control parameter for the growth policy.
	 * @see ArrayResizer#ArrayResizer(double, double, double)
	 */
	public DynamicArrayBag (double fmin, double fover, double funder) {
		this(new Object[0], fmin, fover, funder);
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified <tt>double</tt>
	 * parameters. This constructor is equivalent to the call of
	 * <code>DyanmicArrayBag(size, array, new ArrayResizer(fmin, f))</code>.
	 *
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the internally used array.
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param fmin a control parameter for the growth policy.
	 * @param f a control parameter for the growth policy.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 * @see ArrayResizer#ArrayResizer(double, double)
	 */
	public DynamicArrayBag (int size, Object [] array, double fmin, double f) throws IllegalArgumentException {
		this(size, array, new ArrayResizer(fmin, f));
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified <tt>double</tt>
	 * parameters. This constructor is equivalent to the call of
	 * <code>DynamicArrayBag(array.length, array, new ArrayResizer(fmin, f))</code>.
	 *
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param fmin a control parameter for the growth policy.
	 * @param f a control parameter for the growth policy.
	 * @see ArrayResizer#ArrayResizer(double, double)
	 */
	public DynamicArrayBag (Object [] array, double fmin, double f) {
		this(array.length, array, fmin, f);
	}

	/**
	 * Constructs an empty bag with a growth polity that depends on the
	 * specified <tt>double</tt> parameters. This constructor is
	 * equivalent to the call of
	 * <code>DynamicArrayBag(0, new Object[0], new ArrayResizer(fmin, f))</code>.
	 *
	 * @param fmin a control parameter for the growth policy.
	 * @param f a control parameter for the growth policy.
	 * @see ArrayResizer#ArrayResizer(double, double)
	 */
	public DynamicArrayBag (double fmin, double f) {
		this(new Object[0], fmin, f);
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified <tt>double</tt>
	 * parameter. This constructor is equivalent to the call of
	 * <code>DynamicArrayResizer(size, array, new ArrayResizer(fmin))</code>.
	 *
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the internally used array.
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param fmin a control parameter for the growth policy.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 * @see ArrayResizer#ArrayResizer(double)
	 */
	public DynamicArrayBag (int size, Object [] array, double fmin) throws IllegalArgumentException {
		this(size, array, new ArrayResizer(fmin));
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a growth polity that depends on the specified <tt>double</tt>
	 * parameter. This constructor is equivalent to the call of
	 * <code>DynamicArrayBag(array.length, array, new ArrayResizer(fmin))</code>.
	 *
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @param fmin a control parameter for the growth policy.
	 * @see ArrayResizer#ArrayResizer(double)
	 */
	public DynamicArrayBag (Object [] array, double fmin) {
		this(array.length, array, fmin);
	}

	/**
	 * Constructs an empty bag with a growth polity that depends on the
	 * specified <tt>double</tt> parameter. This constructor is equivalent
	 * to the call of
	 * <code>DynamicArrayBag(0, new Object[0], new ArrayResizer(fmin))</code>.
	 *
	 * @param fmin a control parameter for the growth policy.
	 * @see ArrayResizer#ArrayResizer(double)
	 */
	public DynamicArrayBag (double fmin) {
		this(new Object[0], fmin);
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a default growth polity. This constructor is equivalent to the
	 * call of
	 * <code>DynamicArrayBag(size, array, new ArrayResizer())</code>.
	 *
	 * @param size the number of elements of the specified array which
	 *        should be used to initialize the internally used array.
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @throws IllegalArgumentException if the specified size argument is
	 *         negative, or if it is greater than the length of the
	 *         specified array.
	 * @see ArrayResizer#ArrayResizer()
	 */
	public DynamicArrayBag (int size, Object [] array) throws IllegalArgumentException {
		this(size, array, new ArrayResizer());
	}

	/**
	 * Constructs a bag containing the elements of the specified array
	 * with a default growth polity. This constructor is equivalent to the
	 * call of
	 * <code>DynamicArrayBag(array.length, array, new ArrayResizer())</code>.
	 *
	 * @param array the object array that is used to initialize the
	 *        internally used array.
	 * @see ArrayResizer#ArrayResizer()
	 */
	public DynamicArrayBag (Object [] array) {
		this(array.length, array);
	}

	/**
	 * Constructs an empty bag with a default growth polity. This
	 * constructor is equivalent to the call of
	 * <code>DynamicArrayBag(0, new Object[0], new ArrayResizer())</code>.
	 * @see ArrayResizer#ArrayResizer()
	 */
	public DynamicArrayBag () {
		this(new Object[0]);
	}

	/**
	 * Constructs a bag with a default growth polity containing the
	 * elements of the specified iterator. This constructor calls the void
	 * constructor and uses the insertAll method to insert the elements of
	 * the specified iterator.
	 *
	 * @param iterator the iterator whose elements are to be placed into
	 *        this queue.
	 */
	public DynamicArrayBag (Iterator iterator) {
		this();
		super.insertAll(iterator);
	}

	/**
	 * Returns a cursor to iterate over the elements in this bag without
	 * any predefined sequence. The cursor is specifying a <i>view</i> on
	 * the elements of this bag so that closing the cursor takes no
	 * effect on the bag (e.g., not closing the bag). The behavior
	 * of the cursor is unspecified if this bag is modified while the
	 * cursor is in progress in any way other than by calling the methods
	 * of the cursor. So, when the implementation of this cursor cannot
	 * guarantee that the cursor is in a valid state after modifing the
	 * underlying bag every method of the cursor except <tt>close()</tt>
	 * should throw a <tt>ConcurrentModificationException</tt>.
	 *
	 * @return a cursor to iterate over the elements in this bag without
	 *         any predefined sequence.
	 */
	public Cursor cursor () {
		return new AbstractCursor() {
			int pos = 0;

			public boolean hasNextObject() {
				return pos <= last;
			}

			public Object nextObject() {
				return array[pos++];
			}

			public void remove () throws IllegalStateException {
				super.remove();
				System.arraycopy(array, pos, array, pos-1, last+1-(pos--));
				array = resizer.resize(array, last--);
			}
			
			public boolean supportsRemove() {
				return true;
			}

			public void update (Object object) throws IllegalStateException {
				super.update(object);
				array[pos-1] = object;
			}
			
			public boolean supportsUpdate() {
				return true;
			}
		};
	}

	/**
	 * Returns a cursor representing a LIFO (<i>last in, first out</i>)
	 * iteration over the elements in this bag. The cursor is specifying
	 * a <i>view</i> on the elements of this bag so that closing the
	 * cursor takes no effect on the bag (e.g., not closing the bag). The
	 * behavior of the cursor is unspecified if this bag is modified while
	 * the cursor is in progress in any way other than by calling the
	 * methods of the cursor. So, when the implementation of this cursor
	 * cannot guarantee that the cursor is in a valid state after modifing
	 * the underlying bag every method of the cursor except
	 * <tt>close()</tt> should throw a
	 * <tt>ConcurrentModificationException</tt>.
	 *
	 * @return a cursor representing a LIFO (<i>last in, first out</i>)
	 *         iteration over the elements in this bag.
	 */
	public Cursor lifoCursor () {
		return new AbstractCursor() {
			int pos = last;

			public boolean hasNextObject() {
				return pos >= 0;
			}

			public Object nextObject() {
				return array[pos--];
			}

			public void remove () throws IllegalStateException {
				super.remove();
				System.arraycopy(array, pos+2, array, pos+1, last-pos-1);
				array = resizer.resize(array, last--);
			}
			
			public boolean supportsRemove() {
				return true;
			}

			public void update (Object object) throws IllegalStateException {
				super.update(object);
				array[pos+1] = object;
			}
			
			public boolean supportsUpdate() {
				return true;
			}
			
		};
	}

	/**
	 * Adds the specified element to this bag. This method does not
	 * perform any kind of <i>duplicate detection</i>.
	 *
	 * @param object element to be added to this bag.
	 */
	public void insert (Object object) {
		array = resizer.resize(array, (++last)+1);
		array[last] = object;
	}

	/**
	 * Removes all of the elements from this bag. The bag will be empty
	 * after this call so that <tt>size() == 0</tt>.<br>
	 * This implementation creates a cursor by calling the cursor method
	 * and iterates over it, removing each element using the
	 * {@link Cursor#remove() Cursor.remove} operation. Most
	 * implementations will probably choose to override this method for
	 * efficiency.
	 */
	public void clear () {
		array = new Object[0];
		last = -1;
	}
}
