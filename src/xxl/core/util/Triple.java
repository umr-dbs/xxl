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

import xxl.core.functions.Functional.UnaryFunction;

/**
 * A class to store three values of (usually) different types.
 * 
 * @param <E1>
 *            First value's type
 * @param <E2>
 *            Second value's type
 * @param <E3>
 *            Third value's type
 */
public class Triple<E1, E2, E3> extends Pair<E1, E2> {

	private static final long serialVersionUID = 2733175995620211912L;

	/** Third object */
	protected E3 third;

	/**
	 * Creates an empty triple
	 */
	public Triple() {
	}

	/**
	 * Creates a triple with the given values
	 * 
	 * @param first
	 *            first value
	 * @param second
	 *            second value
	 * @param third
	 *            third value
	 */
	public Triple(E1 first, E2 second, E3 third) {
		super(first, second);
		this.third = third;
	}

	/**
	 * Returns the third value
	 * 
	 * @return third value
	 */
	public E3 getThird() {
		return third;
	}

	/**
	 * Returns the third value
	 * 
	 * @return third value
	 */
	public E3 getElement3() {
		return third;
	}

	/**
	 * Sets the third value
	 * 
	 * @param third
	 *            third value
	 */
	public void setThird(E3 third) {
		this.third = third;
	}

	/**
	 * Sets the third value
	 * 
	 * @param element
	 *            third value
	 */
	public void setElement3(E3 element) {
		this.third = element;
	}

	@Override
	public String toString() {
		return "<" + getFirst() + ", " + getSecond() + ", " + getThird() + ">";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
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
		Triple other = (Triple) obj;
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
		if (third == null) {
			if (other.third != null)
				return false;
		} else if (!third.equals(other.third))
			return false;
		return true;
	}

	/**
	 * Returns a new instance of Triple with the given values
	 * 
	 * @param element1
	 *            first value
	 * @param element2
	 *            second value
	 * @param element3
	 *            third value
	 * @return instance of Triple with the given values
	 */
	public static <E1, E2, E3> Triple<E1, E2, E3> newInstance(E1 element1, E2 element2, E3 element3) {
		return new Triple<E1, E2, E3>(element1, element2, element3);
	}

	@SuppressWarnings("serial")
	public static <E3> UnaryFunction<? super Triple<?, ?, E3>, E3> getThirdElementProjection() {
		return new UnaryFunction<Triple<?, ?, E3>, E3>() {
			@Override
			public E3 invoke(Triple<?, ?, E3> triple) {
				return triple.getElement3();
			}
		};
	}
}
