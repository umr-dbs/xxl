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

/**
 * This class provides a binary predicate that determines whether two given
 * arguments are equal. The <code>invoke</code> method of the predicate is
 * based on the {@link Object#equals(Object) equals} method of the class
 * {@link Object}.
 *
 * @param <P> the type of the predicate's parameters.
 */
public class Equal<P> extends AbstractPredicate<P> {

	/**
	 * This instance can be used for getting a default instance of Equal. It is
	 * similar to the <i>Singleton Design Pattern</i> (for further details see
	 * Creational Patterns, Prototype in <i>Design Patterns: Elements of
	 * Reusable Object-Oriented Software</i> by Erich Gamma, Richard Helm,
	 * Ralph Johnson, and John Vlissides) except that there are no mechanisms
	 * to avoid the creation of other instances of Equal.
	 */
	public static final Equal<Object> DEFAULT_INSTANCE = new Equal<Object>();
	
	/**
	 * Returns whether the given arguments are equal or not. If a
	 * <code>null</code> value is allowed to be passed as argument, the user
	 * has to wrap this predicate by calling
	 * {@link Predicates#newNullSensitiveEqual(boolean)}. The exact
	 * implementation of this method is
	 * <code><pre>
	 * 	return argument0.equals(argument1);
	 * </pre></code>
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return <code>true</code> if the given arguments are equal, otherwise
	 *         <code>false</code>.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return argument0.equals(argument1);
	}
}
