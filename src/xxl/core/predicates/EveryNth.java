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

package xxl.core.predicates;

import java.util.List;

/**
 * This class provides an unary predicate that returns a boolean
 * <code>value</code> for every <code>n</code>-th argument. Internally the
 * predicate counts how often the <code>invoke</code> method has been called in
 * order to return a user-defined boolean <code>value</code> for every
 * <code>n</code>-th call.
 *
 * @param <P> the type of the predicate's parameters.
 */
public class EveryNth<P> extends AbstractPredicate<P> {

	/**
	 * The number of times the <code>invoke</code> method has been called.
	 */
	protected long count = 0;

	/**
	 * The boolean value to be returned, if the <code>n</code>-th call to
	 * <code>invoke</code> is performed.
	 */
	protected boolean value;

	/**
	 * A positive non-zero number that specifies on which argument the
	 * predicate returns <code>value</code>. Every <code>n</code>-th call of
	 * the <code>invoke</code> method <code>true</code> is returned.
	 */
	protected long n;

	/**
	 * Creates a new predicate that returns <code>value</code> everytime the
	 *  <code>invoke</code> method is called the <code>n</code>-th time.
	 *
	 * @param n a positive non-zero number that specifies on which argument the
	 *        predicate returns <code>value</code>. Every <code>n</code>-th
	 *        call of the <code>invoke</code> method <code>value</code> is
	 *        returned.
	 * @param value boolean value to be returned.
	 * @throws IllegalArgumentException if <code>n</code> is less or equal zero
	 *         (<code>n&le;0</code>).
	 */
	public EveryNth(int n, boolean value) {
		if (n <= 0)
			throw new IllegalArgumentException("only positive values could be used");
		this.n = n;
		this.value = value;
	}

	/**
	 * Creates a new predicate that returns <code>true</code> everytime the
	 * <code>invoke</code> method is called the <code>n</code>-th time.
	 *
	 * @param n a positive non-zero number that specifies on which argument the
	 *        predicate returns <code>true</code>. Every <code>n</code>-th call
	 *        of the <code>invoke</code> method <code>true</code> is returned.
	 * @throws IllegalArgumentException if <code>n</code> is less or equal zero
	 *         (<code>n&le;0</code>).
	 */
	public EveryNth(int n) {
		this(n, true);
	}
	
	/**
	 * Returns <code>value</code> if this method is called the
	 * <code>n</code>-th time. Internally a counter is increased every time the
	 * method is called.
	 *
	 * @param arguments the arguments to the predicate.
	 * @return <code>value</code> if this method is called the
	 *         <code>n</code>-th time, otherwise <code>!value</code>.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
	    return (++count % n) == 0 ? value : !value;
	}
	
}
