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

import xxl.core.cursors.Cursor;
import xxl.core.cursors.identities.UnmodifiableCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;

/**
 * This class provides a decorator for a given bag that cannot be
 * modified. The methods of this class call the corresponding methods of
 * the internally stored bag except the methods that modify the bag. These
 * methods (<tt>insert</tt>, <tt>insertAll</tt> and <tt>clear</tt>) throws
 * an <tt>UnsupportedOperationException</tt>.<p>
 *
 * Note that the <tt>cursor</tt> method returns an UnmodifiableCursor.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new bag
 *
 *     ListBag inputBag = new ListBag();
 *
 *     // create an iteration over 20 random Integers (between 0 and 100)
 *
 *     Iterator iterator = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 20);
 *
 *     // insert all elements of the given iterator
 *
 *     inputBag.insertAll(iterator);
 *
 *     // create a new unmodifiable bag with the given bag
 *
 *     UnmodifiableBag bag = new UnmodifiableBag(inputBag);
 *
 *     // generate a cursor that iterates over all elements of the bag
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
 * @see Cursor
 * @see Function
 * @see Iterator
 * @see UnmodifiableCursor
 */
public class UnmodifiableBag extends DecoratorBag {

	/**
	 * A factory method to create a new unmodifiable bag (see contract for
	 * {@link Bag#FACTORY_METHOD FACTORY_METHOD} in interface Bag). In
	 * contradiction to the contract in Bag it may only be invoked with a
	 * <i>parameter list</i> (for further details see Function) of
	 * bags. The <i>parameter list</i> will be used to initialize
	 * the decorated bag by calling the constructor
	 * <code>UnmodifiableBag((Bag) list.get(0))</code>.
	 *
	 * @see Function
	 */
	public static final Function<Bag,UnmodifiableBag> FACTORY_METHOD = new AbstractFunction<Bag,UnmodifiableBag>() {
		public UnmodifiableBag invoke (List<? extends Bag> list) {
			return new UnmodifiableBag(list.get(0));
		}
	};

	/**
	 * Constructs a new unmodifiable bag that decorates the specified bag.
	 *
	 * @param bag the bag to be decorated.
	 */
	public UnmodifiableBag(Bag bag) {
		super(bag);
	}

	/**
	 * Removes all elements from this bag. This implementation
	 * always throws an <tt>UnsupportedOperationException</tt>.
	 *
	 * @throws UnsupportedOperationException when the method is not
	 *         supported.
	 */
	public void clear () throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns an unmodifiable cursor to iterate over the elements in this
	 * bag without any predefined sequence. The cursor is specifying a
	 * <i>view</i> on the elements of this bag so that closing the cursor
	 * takes no effect on the bag (e.g., not closing the bag).
	 *
	 * @return a unmodifiable cursor to iterate over the elements in this
	 *         bag without any predefined sequence.
	 * @see UnmodifiableCursor
	 */
	public Cursor cursor () {
		return new UnmodifiableCursor(bag.cursor());
	}

	/**
	 * Adds the specified element to this bag. This method does not
	 * perform any kind of <i>duplicate detection</i>. This implementation
	 * always throws an <tt>UnsupportedOperationException</tt>.
	 *
	 * @param object element to be added to this bag.
	 * @throws UnsupportedOperationException when the method is not
	 *         supported.
	 */
	public void insert (Object object) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Adds all of the elements in the specified iterator to this bag.
	 * This method does not perform any kind of <i>duplicate
	 * detection.</i> This implementation always throws an
	 * <tt>UnsupportedOperationException</tt>.
	 *
	 * @param objects iterator whose elements are to be added to this bag.
	 * @throws UnsupportedOperationException when the method is not
	 *         supported.
	 */
	public void insertAll (Iterator objects) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns a cursor to iterate over all elements in this bag for which
	 * the given predicate returns <tt>true</tt>. This method is very
	 * similar to the cursor method except that its result is determined
	 * by a predicate.
	 *
	 * @param predicate a predicate that determines whether an element of
	 *        this bag should be returned or not.
	 * @return a cursor to iterate over all elements in this bag for which
	 *         the given predicate returns <tt>true</tt>.
	 */
	public Cursor query(Predicate predicate){
		return new UnmodifiableCursor(bag.query(predicate));
	}
}
