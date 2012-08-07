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
 * A class to store four values of (usually) different types.
 * 
 * @param <E1>
 *            First value's type
 * @param <E2>
 *            Second value's type
 * @param <E3>
 *            Third value's type
 * @param <E4>
 *            Fourth value's type
 */
public class Quadruple<E1, E2, E3, E4> extends Triple<E1, E2, E3> {

	private static final long serialVersionUID = 6500696004688037927L;

	/** Fourth object */
	protected E4 fourth;

	/**
	 * Creates an empty quadruple
	 */
	public Quadruple() {
	}

	/**
	 * Creates a quadruple with the given values
	 * 
	 * @param first
	 *            first value
	 * @param second
	 *            second value
	 * @param third
	 *            third value
	 * @param fourth
	 *            fourth value
	 */
	public Quadruple(E1 first, E2 second, E3 third, E4 fourth) {
		super(first, second, third);
		this.fourth = fourth;
	}

	/**
	 * Returns the fourth value
	 * 
	 * @return fourth value
	 */
	public E4 getFourth() {
		return fourth;
	}

	/**
	 * Returns the fourth value
	 * 
	 * @return fourth value
	 */
	public E4 getElement4() {
		return fourth;
	}

	/**
	 * Sets the fourth value
	 * 
	 * @param fourth
	 *            fourth value
	 */
	public void setFourth(E4 fourth) {
		this.fourth = fourth;
	}

	/**
	 * Sets the fourth value
	 * 
	 * @param element
	 *            fourth value
	 */
	public void setElement4(E4 element) {
		this.fourth = element;
	}

	@Override
	public String toString() {
		return "<" + getFirst() + ", " + getSecond() + ", " + getThird() + ", " + getFourth() + ">";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
		result = prime * result + ((fourth == null) ? 0 : fourth.hashCode());
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
		Quadruple other = (Quadruple) obj;
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
		if (fourth == null) {
			if (other.fourth != null)
				return false;
		} else if (!fourth.equals(other.fourth))
			return false;
		return true;
	}

	/**
	 * Returns a new instance of Quadruple with the given values
	 * 
	 * @param element1
	 *            first value
	 * @param element2
	 *            second value
	 * @param element3
	 *            third value
	 * @param element4
	 *            fourth value
	 * @return instance of Quadruple with the given values
	 */
	public static <E1, E2, E3, E4> Quadruple<E1, E2, E3, E4> newInstance(E1 element1, E2 element2, E3 element3,
			E4 element4) {
		return new Quadruple<E1, E2, E3, E4>(element1, element2, element3, element4);
	}

	@SuppressWarnings("serial")
	public static <E4> UnaryFunction<? super Quadruple<?, ?, ?, E4>, E4> getFourthElementProjection() {
		return new UnaryFunction<Quadruple<?, ?, ?, E4>, E4>() {
			@Override
			public E4 invoke(Quadruple<?, ?, ?, E4> quadruple) {
				return quadruple.getElement4();
			}
		};
	}
}
