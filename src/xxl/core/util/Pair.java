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

package xxl.core.util;

/**
 * A common task is to return two values from a function or keep two values
 * stored together, because they are closely related. This class serves just
 * this purpose. Both values may be of different types.
 * 
 * @param <T>
 *            First value's type
 * @param <U>
 *            Second value's type
 */
public class Pair<T, U> {

	/** First object */
	private T first;
	/** Second object */
	private U second;

	/**
	 * Creates an empty pair
	 */
	public Pair() {
	}

	/**
	 * Creates a pair with the given values
	 * 
	 * @param first
	 *            first value
	 * @param second
	 *            second value
	 */
	public Pair(T first, U second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * Returns the first value
	 * 
	 * @return first value
	 */
	public T getFirst() {
		return first;
	}

	/**
	 * Sets the first value
	 * 
	 * @param first
	 *            first value
	 */
	public void setFirst(T first) {
		this.first = first;
	}

	/**
	 * Returns the second value
	 * 
	 * @return second value
	 */
	public U getSecond() {
		return second;
	}

	/**
	 * Sets the second value
	 * 
	 * @param second
	 *            second value
	 */
	public void setSecond(U second) {
		this.second = second;
	}

	@Override
	public String toString() {
		return "[" + first + ", " + second + "]";
	}

}
