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

package xxl.core.comparators;

import java.io.Serializable;
import java.util.Comparator;

import xxl.core.functions.Function;

/**
 * The FeatureComparator compares the 'feature's of two given objects.
 * The feature of each object is extracted with a mapping function, i.e.,
 * this classs implements the <code>compare</code> method as follows:
 * <code><pre>
 *   public int compare(T o1, T o2) {
 *       return comparator.compare(function1.invoke(o1), function2.invoke(o2));
 *   }
 * </pre></code>
 * 
 * <p>If the two objects are of the same type it is more convenient to
 * use hand over only one mapping function.</p>
 * 
 * @param <S> the type of the objects the underlying comparator is able to
 *        compare.
 * @param <T> the type of the objects this comparator should be able to
 *        compare.
 * @see java.util.Comparator
 * @see xxl.core.functions.Function
 */
public class FeatureComparator<S, T> implements Comparator<T>, Serializable {

	/**
	 * The function that maps the first object to a new object that is
	 * compared.
	 */
	protected Function<? super T, ? extends S> function1;
	
	/**
	 * The function that maps the second object to a new object that is
	 * compared.
	 */
	protected Function<? super T, ? extends S> function2;
	
	/**
	 * The comparator that is used for comparison.
	 */
	protected Comparator<? super S> comparator;

	/** 
	 * Constructs a new FeatureComparator.
	 * 
	 * @param comparator the comparator that is used for comparison.
	 * @param function1 the function that maps the first object to a new object
	 *        that is compared.
	 * @param function2 the function that maps the second object to a new
	 *        object that is compared.
	 */
	public FeatureComparator(Comparator<? super S> comparator, Function<? super T, ? extends S> function1, Function<? super T, ? extends S> function2) {
		this.function1 = function1;
		this.function2 = function2;
		this.comparator = comparator;
	}

	/** 
	 * Constructs a new FeatureComparator.
	 * 
	 * @param comparator the comparator that is used for comparison.
	 * @param function the function that maps each object to a new object that
	 *        is compared.
	 */
	public FeatureComparator(Comparator<? super S> comparator, Function<? super T, ? extends S> function) {
		this(comparator, function, function);
	}

	/**
	 * Compares two objects.
	 *
	 * @param o1 first object.
	 * @param o2 second object.
	 * @return integer (0 if the objects are equal, -1 if o1<o2, +1 if o1>o2).
	 */
	public int compare(T o1, T o2) {
		return comparator.compare(function1.invoke(o1), function2.invoke(o2));
	}
}
