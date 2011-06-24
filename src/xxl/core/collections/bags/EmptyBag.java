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

import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;

/**
 * This class provides an empty bag. In other words, this bag is not able
 * to contain an element. Therefore, every method that inserts elements
 * into the bag does nothing. This class is useful when you have to return
 * a bag of some kind, but do not have any elements the bag points to.<p>
 *
 * Example usage (1).
 * <pre>
 *     // create a new empty bag
 *
 *     EmptyBag bag = EmptyBag.DEFAULT_INSTANCE;
 *
 *     // println the number of elements contained by the bag
 *
 *     System.out.println(bag.size());
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
 *         bag.insertAll(cursor);
 *     }
 *     catch (UnsupportedOperationException uoe) {
 *         System.out.println(uoe.getMessage());
 *     }
 *
 *     // println the number of elements contained by the bag
 *
 *     System.out.println(bag.size());
 *
 *     // close the open bag and cursor after use
 *
 *     bag.close();
 *     cursor.close();
 * </pre>
 *
 * @see Cursor
 * @see EmptyCursor
 * @see Function
 */
public class EmptyBag extends AbstractBag {

	/** 
	 * Constant specifying that exceptions should <b>not</b> be thrown if 
	 * an element is inserted into the EmptyBag.
	 */
	public static boolean NO_EXCEPTIONS=false;

	/** 
	 * Constant specifying that exceptions should be thrown if 
	 * an element is inserted into the EmptyBag.
	 */
	public static boolean THROW_EXCEPTIONS=true;
	
	/**
	 * Variable determining if exceptions are thrown.
	 */
	protected boolean throwExceptions=THROW_EXCEPTIONS;

	/**
	 * A factory method to create a new EmptyBag (see contract for
	 * {@link Bag#FACTORY_METHOD FACTORY_METHOD} in interface Bag). It may
	 * be invoked with a <i>parameter list</i>(for further
	 * details see Function) of objects, an iterator or without any
	 * parameters. The factory method always returns a default instance of
	 * this class.
	 *
	 * @see Function
	 */
	public static final Function<Object,EmptyBag> FACTORY_METHOD = new AbstractFunction<Object,EmptyBag>() {
		public EmptyBag invoke (List<? extends Object> list) {
			return DEFAULT_INSTANCE;
		}
	};

	/**
	 * This instance can be used for getting a default instance of
	 * EmptyBag. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by
	 * Erich Gamma, Richard Helm, Ralph Johnson, and John Vlissides)
	 * except that there are no mechanisms to avoid the creation of other
	 * instances of EmptyBag.
	 */
	public static final EmptyBag DEFAULT_INSTANCE = new EmptyBag();

	/**
	 * Constructs a new empty bag that is not able to contain any
	 * elements.
	 *
	 * @param throwExceptions Determining if exceptions are thrown when
	 *	inserting elements. Possible values: NO_EXCEPTIONS, THROW_EXCEPTIONS.
	 */
	public EmptyBag(boolean throwExceptions)
	{
		this.throwExceptions = throwExceptions;
	}

	/**
	 * Constructs a new empty bag that is not able to contain any
	 * elements. Exceptions are thrown if elements are inserted.
	 */
	public EmptyBag()
	{
		this(THROW_EXCEPTIONS);
	}

	/**
	 * Returns a cursor to iterate over the elements in this bag without
	 * any predefined sequence. The cursor is specifying a <i>view</i> on
	 * the elements of this bag so that closing the cursor takes no
	 * effect on the bag (e.g., not closing the bag). The behavior
	 * of the cursor is unspecified if this bag is modified while the
	 * cursor is in progress in any way other than by calling the methods
	 * of the cursor.<br>
	 * This implementation always returns an <tt>EmptyCursor</tt> because
	 * the bag is empty.
	 *
	 * @return a cursor to iterate over the elements in this bag without
	 *         any predefined sequence.
	 */
	public Cursor cursor () {
		return EmptyCursor.DEFAULT_INSTANCE;
	}

	/**
	 * Adds the specified element to this bag. This method does not
	 * perform any kind of <i>duplicate detection</i>.<br>
	 * This implementation always throws an
	 * <tt>UnsupportedOperationException</tt>.
	 *
	 * @param object element to be added to this bag.
	 * @throws UnsupportedOperationException when an object should be
	 *        inserted into this bag.
	 */
	public void insert (Object object) throws UnsupportedOperationException {
		if (throwExceptions == THROW_EXCEPTIONS)
			throw new UnsupportedOperationException("No modification allowed.");
	}

	/**
	 * Returns the number of elements in this bag (its cardinality). If
	 * this bag contains more than <tt>Integer.MAX_VALUE</tt> elements,
	 * <tt>Integer.MAX_VALUE</tt> is returned.<br>
	 * This implementation always returns <tt>0</tt> because the bag is
	 * empty.
	 *
	 * @return the number of elements in this bag (its cardinality).
	 */
	public int size () {
		return 0;
	}

	/**
	 * Returns a cursor to iterate over all elements in this bag for which
	 * the given predicate returns <tt>true</tt>. This method is very
	 * similar to the cursor method except that its result is determined
	 * by a predicate. Like the cursor returned by the cursor method, this
	 * cursor's behavior is unspecified if this bag is modified while the
	 * cursor is in progress in any way other than by calling the methods
	 * of the cursor.<br>
	 * This implementation always returns an <tt>EmptyCursor</tt> because
	 * the bag is empty.
	 *
	 * @param predicate a predicate that determines whether an element of
	 *        this bag should be returned or not.
	 * @return a cursor to iterate over all elements in this bag for which
	 *         the given predicate returns <tt>true</tt>.
	 */
	public Cursor query (Predicate predicate) {
		return EmptyCursor.DEFAULT_INSTANCE;
	}
}
