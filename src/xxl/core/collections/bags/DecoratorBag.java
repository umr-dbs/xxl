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

import xxl.core.cursors.Cursor;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.util.Decorator;

/**
 * The class provides a decorator for a bag that follows the <i>Decorator
 * Design Pattern</i> (for further details see Structural Patterns,
 * Decorator in <i>Design Patterns: Elements of Reusable Object-Oriented
 * Software</i> by Erich Gamma, Richard Helm, Ralph Johnson, and John
 * Vlissides). It provides a more flexible way to add functionality to a
 * bag or one of its subclasses as by inheritance.<p>
 *
 * To provide this functionality the class contains a reference to the bag
 * to be decorated. This reference is used to redirect method calls to the
 * <i>original</i> bag. This class is an abstract class although it
 * provides no abstract methods. That's so because it does not make any
 * sense to instanciate this class which redirects every method call to
 * the corresponding method of the decorated container without adding any
 * functionality.<p>
 *
 * Usage example (1).
 * <pre>
 *     // create a new decorated bag that adds functionality to the insert method and leaves the
 *     // other methods untouched
 *
 *     DecoratorBag bag = new DecoratorBag(new ListBag()) {
 *         public void insert (Object object) {
 *
 *             // every desired functionality can be added to this method
 *
 *             System.out.println("Before the insertion of the specified object!");
 *             super.insert(object);
 *             System.out.println("After the insertion of the specified object!");
 *         }
 *     }
 * </pre>
 * 
 * @param <E> the type of the decorated bag's elements.
 *
 * @see Cursor
 * @see Function
 * @see Iterator
 */
public abstract class DecoratorBag<E> implements Bag<E>, Decorator<Bag<E>> {

	/**
	 * A reference to the bag to be decorated. This reference is used to
	 * perform method calls on the <i>original</i> bag.
	 */
	protected Bag<E> bag;

	/**
	 * Constructs a new DecoratorBag that decorates the specified bag.
	 *
	 * @param bag the bag to be decorated.
	 */
	public DecoratorBag(Bag<E> bag) {
		this.bag = bag;
	}

	/**
	 * Removes all elements from this bag. The bag will be empty
	 * after this call so that <tt>size() == 0</tt>.
	 */
	public void clear() {
		bag.clear();
	}

	/**
	 * Closes this bag and releases any system resources associated with
	 * it. This operation is idempotent, i.e., multiple calls of this
	 * method take the same effect as a single call. When needed, a
	 * closed bag can be implicitly reopened by a consecutive call to one of
	 * its methods. Because of having an unspecified behavior when this
	 * bag is closed every cursor iterating over the elements of this bag
	 * must be closed.<br>
	 * <b>Note:</b> This method is very important for bags using external
	 * resources like files or JDBC resources.
	 */
	public void close() {
		bag.close();
	}

	/**
	 * Returns a cursor to iterate over the elements in this bag without
	 * any predefined sequence. The cursor is specifying a <i>view</i> on
	 * the elements of this bag so that closing the cursor takes no
	 * effect on the bag (e.g., not closing the bag). The behavior
	 * of the cursor is unspecified if this bag is modified while the
	 * cursor is in progress in any way other than by calling the methods
	 * of the cursor.
	 *
	 * @return a cursor to iterate over the elements in this bag without
	 *         any predefined sequence.
	 */
	public Cursor<E> cursor() {
		return bag.cursor();
	}

	/**
	 * Adds the specified element to this bag. This method does not
	 * perform any kind of <i>duplicate detection</i>.
	 *
	 * @param object element to be added to this bag.
	 */
	public void insert(E object) {
		bag.insert(object);
	}

	/**
	 * Adds all of the elements in the specified iterator to this bag.
	 * This method does not perform any kind of <i>duplicate
	 * detection.</i> The behavior of this operation is unspecified if the
	 * specified iterator is modified while the operation is in progress.
	 *
	 * @param objects iterator whose elements are to be added to this bag.
	 */
	public void insertAll(Iterator<? extends E> objects) {
		bag.insertAll(objects);
	}

	/**
	 * Returns the number of elements in this bag (its cardinality). If
	 * this bag contains more than <tt>Integer.MAX_VALUE</tt> elements,
	 * <tt>Integer.MAX_VALUE</tt> is returned.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	public int size() {
		return bag.size();
	}

	/**
	 * Returns a cursor to iterate over all elements in this bag for which
	 * the given predicate returns <tt>true</tt>. This method is very
	 * similar to the cursor method except that its result is determined
	 * by a predicate. Like the cursor returned by the cursor method, this
	 * cursor's behavior is unspecified if this bag is modified while the
	 * cursor is in progress in any way other than by calling the methods
	 * of the cursor.
	 *
	 * @param predicate a predicate that determines whether an element of
	 *        this bag should be returned or not.
	 * @return a cursor to iterate over all elements in this bag for which
	 *         the given predicate returns <tt>true</tt>.
	 */
	public Cursor<E> query(Predicate<? super E> predicate){
		return bag.query(predicate);
	}
	
	@Override
	public Bag<E> getDecoree() {
		return bag;
	}
	
}
