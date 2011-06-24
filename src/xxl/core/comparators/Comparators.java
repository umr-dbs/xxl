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

import java.util.Comparator;

/** 
 * This class provides some useful static methods for dealing with comparators.
 */
public class Comparators {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Comparators() {
		// private access in order to ensure non-instantiability
	}

	/**
	 * Returns a comparator that is able to compare objects based on the given
	 * comparator. Internally the returned comparator casted the objects to be
	 * compared to the type expected by the given comparator. Therefore the
	 * returned comparator will cause a <code>ClasscastException</code> when
	 * its methods are called with object that do not have the expected type.
	 * 
	 * @param <T> the type of the objects to be compared.
	 * @param comparator the comparator that is used to compare the objects
	 *        internally.
	 * @return a comparator that is able to compare objects based on the given
	 *         comparator.
	 */
	public static <T> Comparator<Object> getObjectComparator(final Comparator<T> comparator) {
		return new Comparator<Object>() {
			public int compare(Object o1, Object o2) {
				return comparator.compare((T)o1, (T)o2);
			}
			
		};
	}
	
	/** 
	 * Returns a {@link java.util.Comparator comparator} able to handle
	 * <code>null</code> values. A flag controls the position of null values
	 * concerning the induced ordering of the given comparator.
	 * If the flag is true the null values will be positioned before all other
	 * values and vice versa.
	 * 
	 * @param <T> the type of the object to be compared.
	 * @param comparator internally used
	 *        {@link java.util.Comparator comparator} for objects that are not
	 *        <code>null</code>.
	 * @param flag determines the position of null values.
	 * @return a {@link java.util.Comparator comparator} able to handle null
	 *         values.
	 */
	public static <T> Comparator<T> newNullSensitiveComparator(final Comparator<T> comparator, boolean flag) {
		return flag ?
			new Comparator<T>(){
				public int compare(T o1, T o2) {
					return o1 == null && o2 == null ?
						0 :
						o1 == null ?
							-1 :
							o2 == null ?
								1 :
								comparator.compare(o1, o2);
				}
			}
		:
			new Comparator<T>(){
				public int compare(T o1, T o2) {
					return o1 == null && o2 == null ?
						0 :
						o1 == null ?
							1 :
							o2 == null ?
								-1 :
								comparator.compare(o1, o2);
				}
			}
		;
	}

	/** 
	 * Returns a {@link java.util.Comparator comparator} able to handle
	 * <code>null</code> values. Null values will be positioned before all
	 * other values.
	 * 
	 * @param <T> the type of the object to be compared.
	 * @param c internally used {@link java.util.Comparator comparator} for
	 *        objects that are not <code>null</code>
	 * @return a {@link java.util.Comparator comparator} able to handle null
	 *         values.
	 */
	public static <T> Comparator<T> newNullSensitiveComparator(Comparator<T> c){
		return newNullSensitiveComparator(c, true);
	}
}
