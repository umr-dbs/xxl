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

/**
 * The InverseComparator inverts the ordering of a given Comparator.
 * 
 * @param <T> the type of the objects to be compared.
 * @see java.util.Comparator
 */
public class InverseComparator<T> implements Comparator<T>, Serializable {

	/** 
	 * The Comparator to be inverted.
	 */
	protected Comparator<? super T> comparator;

	/** Creates a new InverseComparator.
		@param comparator the Comparator to be inverted
	 */
	public InverseComparator(Comparator<? super T> comparator) {
		this.comparator = comparator;
	}

	/** 
	 * Compares its two arguments for order.
	 * @param object1 the first object to be compared
	 * @param object2 the second object to be compared
	 * @return the comparison result
	 */
	public int compare(T object1, T object2) {
		return comparator.compare(object2, object1);
	}
}
