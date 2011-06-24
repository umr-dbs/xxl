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

package xxl.core.collections;

import java.util.AbstractList;
import java.util.List;

import xxl.core.functions.Function;

/**
 * The class provides a mapping of a list with a given function. The list to
 * map and the mapping function are internally stored and the indexed access
 * methods <code>get</code> and <code>remove</code> are implemented. Every
 * method that adds or sets elements throws an UnsupportedOperationException
 * (the element to insert cannot be <i>back-mapped</i>). Everytime one of the
 * implemented methods is called on this list the mapping function is invoked
 * with the corresponding element of the internally used list. The iterators
 * returned by the <code>iterator</code> and <code>listIterator</code> methods
 * are also mapped.
 * 
 * <p>The performance of the mapped list depends on the internally stored list
 * that is specified when the mapped list is constructed.</p>
 *
 * <p>Usage example (1).
 * <code><pre>
 *   // create a new list
 *   
 *   List&lt;Integer&gt; list = new ArrayList&lt;Integer&gt;();
 *   
 *   // insert the Integers between 0 and 19 into the list
 *   
 *   for (int i = 0; i < 20; i++)
 *       list.add(i);
 *   
 *   // create a function that multplies every odd Integer with 10 and divides every even
 *   // Integer by 2
 *   
 *   Function&lt;Integer, Integer&gt; function = new Function&lt;Integer, Iteger&gt;() {
 *       public Integer invoke(Integer o) {
 *           return o%2 != 0 ? o*10 : o/2;
 *       }
 *   };
 *   
 *   // create a new mapped list that maps the given list with the given function
 *   
 *   MappedList&lt;Integer, Integer&gt; mappedList = new MappedList&lt;Integer, Integer&gt;(list, function);
 *   
 *   // print all elements of the mapped list
 *   
 *   for (int i = 0; i < mappedList.size(); i++)
 *       System.out.println(mappedList.get(i));
 * </pre></code></p>
 *
 * @param <I> the type of the elements stored by the underlying list.
 * @param <O> the type of the elements returned by the methods of this list.
 * @see AbstractList
 * @see Function
 * @see List
 */
public class MappedList<I, O> extends AbstractList<O> {

	/**
	 * This field is used to store the list to map internally.
	 */
	protected List<? extends I> list;

	/**
	 * This field is used to store the mapping function.
	 */
	protected Function<? super I, ? extends O> function;

	/**
	 * Constructs a new mapped list that maps the specified list with the
	 * specified function.
	 *
	 * @param list the list to map.
	 * @param function the mapping function.
	 */
	public MappedList(List<? extends I> list, Function<? super I, ? extends O> function) {
		this.list = list;
		this.function = function;
	}

	
	/**
     * This implementation always throws an UnsupportedOperationException.
     *
     * @param element element to be inserted.
	 * @return 
     * @throws UnsupportedOperationException if the <tt>add</tt> method is not
     *		  supported by this list.
     * @throws ClassCastException if the class of the specified element
     * 		  prevents it from being added to this list.
     * @throws IllegalArgumentException if some aspect of the specified
     *		  element prevents it from being added to this list.
     * @throws IndexOutOfBoundsException index is out of range (<tt>index &lt;
     *		  0 || index &gt; size()</tt>).
     */
//	@Override
	public boolean add(O element) {
    	throw new UnsupportedOperationException("element " + element + " cannot be added to a mapped list.");
    }
	
	public void add(int index, O element) {
		throw new UnsupportedOperationException("element " + element + " cannot be added to a mapped list.");
	};
	
	/**
	 * Returns the number of elements in this list.
	 *
	 * @return the number of elements in this list.
	 */
	public int size() {
		return list.size();
	}

	/**
	 * Returns the mapped element at the specified position in this list. This
	 * method is equivalent to the call of
	 * <code>function.invoke(list.get(index))</code>.
	 *
	 * @param index index of element to return.
	 * @return the mapped element at the specified position in this list.
	 * @throws UnsupportedOperationException if the remove method is not
	 *         supported by this list.
	 */
	public O get(int index) throws UnsupportedOperationException {
		return function.invoke(list.get(index));
	}

	/**
	 * Removes the element at the specified position in this list. Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 * Returns the element that was removed from the list. This method is
	 * equivalent to the call of
	 * <code>function.invoke(list.remove(index))</code>.
	 *
	 * @param index the index of the element to remove.
	 * @return the mapped element previously at the specified position.
	 * @throws IndexOutOfBoundsException if the <code>index</code> is out of
	 *         range (<code>index < 0 || index >= size()</code>).
	 */
	public O remove (int index) throws IndexOutOfBoundsException {
		return function.invoke(list.remove(index));
	}
}
