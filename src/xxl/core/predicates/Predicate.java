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
 * This interface provides a predicate, i.e. a kind of function that implements
 * a logical statement and, when it is invoked, determines whether specified
 * objects fulfill the statement. Predicates are highly related to
 * {@link xxl.core.functions.Function functions}. Like functions, predicates
 * provide a set of <code>invoke</code> methods that can be used to evaluate
 * the predicate. For providing predicates with and without parameters, this
 * class contains invoke methods with zero, one and two arguments and with a
 * typed list of arguments.
 *
 * @param <P> the type of the predicate's parameters.
 */
public interface Predicate<P> {

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 *
	 * @param arguments the arguments to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public abstract boolean invoke(List<? extends P> arguments);

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 *
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public abstract boolean invoke();

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 *
	 * @param argument the argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public abstract boolean invoke(P argument);

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	public abstract boolean invoke(P argument0, P argument1);

}
