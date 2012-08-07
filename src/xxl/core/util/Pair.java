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

import java.io.Serializable;
import java.util.Map;

import xxl.core.functions.Functional.UnaryFunction;

/**
 * A common task is to return two values from a function or keep two values
 * stored together, because they are closely related. This class serves just
 * this purpose. Both values may be of different types.
 * 
 * @param <E1>
 *            First value's type
 * @param <E2>
 *            Second value's type
 */
public class Pair<E1, E2> implements Serializable {

	private static final long serialVersionUID = -8611145329658977040L;

	/** First object */
	protected E1 first;
	/** Second object */
	protected E2 second;

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
	public Pair(E1 first, E2 second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * Creates a pair from a given Map.Entry
	 * 
	 * @param mapEntry
	 *            Entry of a Java Map
	 */
	public Pair(Map.Entry<E1, E2> mapEntry) {
		this(mapEntry.getKey(), mapEntry.getValue());
	}

	/**
	 * Returns the first value
	 * 
	 * @return first value
	 */
	public E1 getFirst() {
		return first;
	}

	/**
	 * Returns the first value
	 * 
	 * @return first value
	 */
	public E1 getElement1() {
		return first;
	}

	/**
	 * Sets the first value
	 * 
	 * @param first
	 *            first value
	 */
	public void setFirst(E1 first) {
		this.first = first;
	}

	/**
	 * Sets the first value
	 * 
	 * @param element
	 *            first value
	 */
	public void setElement1(E1 element) {
		this.first = element;
	}

	/**
	 * Returns the second value
	 * 
	 * @return second value
	 */
	public E2 getSecond() {
		return second;
	}

	/**
	 * Returns the second value
	 * 
	 * @return second value
	 */
	public E2 getElement2() {
		return second;
	}

	/**
	 * Sets the second value
	 * 
	 * @param second
	 *            second value
	 */
	public void setSecond(E2 second) {
		this.second = second;
	}

	/**
	 * Sets the second value
	 * 
	 * @param element
	 *            second value
	 */
	public void setElement2(E2 element) {
		this.second = element;
	}

	@Override
	public String toString() {
		return "<" + first + ", " + second + ">";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Pair))
			return false;
		Pair other = (Pair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	/**
	 * Creates a new instance of Pair with the given values
	 * 
	 * @param element1
	 *            first value
	 * @param element2
	 *            second value
	 * @return instance of Pair with the given values
	 */
	public static <E1, E2> Pair<E1, E2> newInstance(E1 element1, E2 element2) {
		return new Pair<E1, E2>(element1, element2);
	}

	@SuppressWarnings("serial")
	public static <E1> UnaryFunction<? super Pair<E1, ?>, E1> getFirstElementProjection() {
		return new UnaryFunction<Pair<E1, ?>, E1>() {
			@Override
			public E1 invoke(Pair<E1, ?> pair) {
				return pair.getElement1();
			}
		};
	}

	@SuppressWarnings("serial")
	public static <E2> UnaryFunction<? super Pair<?, E2>, E2> getSecondElementProjection() {
		return new UnaryFunction<Pair<?, E2>, E2>() {
			@Override
			public E2 invoke(Pair<?, E2> pair) {
				return pair.getElement2();
			}
		};
	}
}
