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

package	xxl.core.cursors.sources;

import xxl.core.cursors.AbstractCursor;

/**
 * An empty cursor contains no elements, i.e., every call to its
 * <code>hasNext</code> method will return <code>false</code>. This class is
 * useful when you have to return an iteration of some kind, but there are no
 * elements the iteration points to. The class contains a static field, called
 * <code>DEFAULT_INSTANCE</code>, which is similar to the design pattern, named
 * <i>Singleton</i>, except that there are no mechanisms to avoid the creation
 * of other instances. The intent of the design pattern is to ensure this class
 * only has one instance, and provide a global point of access to it. For
 * further information see: "Gamma et al.: <i>DesignPatterns. Elements of
 * Reusable Object-Oriented Software.</i> Addision Wesley 1998."
 * 
 * <p><b>Example usage:</b>
 * <code><pre>
 *     EmptyCursor&lt;Integer&gt; emptyCursor = new EmptyCursor&lt;Integer&gt;();
 *     
 *     emptyCursor.open();
 * 
 *     System.out.println("Is a next element available? " + emptyCursor.hasNext());
 * 
 *     emptyCursor.close();
 * 
 *     System.out.println("Is a next element available? " + EmptyCursor.DEFAULT_INSTANCE.hasNext());
 * </pre></code>
 * This example shows in two different ways that an empty cursor contains no
 * elements:
 * <ul>
 *     <li>
 *         A new instance of this class is directly created and the
 *         <code>hasNext</code> method is called.
 *     </li>
 *     <li>
 *         The static field <code>DEFAULT_INSTANCE</code>, which returns an
 *         instance of this class, is used instead and then the
 *         <code>hasNext</code> method is called.
 *     </li>
 * </ul></p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class EmptyCursor<E> extends AbstractCursor<E> {

	/**
	 * This instance can be used for getting a default instance of an empty
	 * cursor. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of this
	 * empty cursor.
	 */
	public static final EmptyCursor<Object> DEFAULT_INSTANCE = new EmptyCursor<Object>();

	/**
	 * Constructs a new empty cursor that contains no elements.
	 */
	public EmptyCursor() {}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * @return <code>true</code> if the empty cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return false;
	}
	
	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the empty cursor's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return null;
	}

	/**
	 * Closes the cursor, i.e., signals the cursor to clean up resources, close
	 * files, etc. When a cursor has been closed calls to methods like
	 * <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Multiple calls to <code>close</code> do not have any
	 * effect, i.e., if <code>close</code> was called the cursor remains in the
	 * state <i>closed</i>.
	 * 
	 * <p>Note, that this implementation is empty. So <code>close</code> does
	 * not have any effect. This is necessary to allow multiple references to
	 * <code>EmptyCursor.DEFAULT_INSTANCE<code>.</p>
	 */
	public void close() {}

	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the empty cursor. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the empty cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return true;
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the empty cursor. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the empty cursor, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return true;
	}

	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the empty cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the empty cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return true;
	}
}
