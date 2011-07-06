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

package xxl.core.functions;

import java.util.List;

import xxl.core.predicates.Predicate;

/**
 * Provides a wrapper for {@link xxl.core.predicates.Predicate Predicates} to
 * {@link xxl.core.functions.Function Functions}.
 * 
 * @param <P> the type of the function's parameters.
 * @see xxl.core.predicates.FunctionPredicate
 * @see xxl.core.functions.Function
 * @see xxl.core.predicates.Predicate
 */
@SuppressWarnings("serial")
public class PredicateFunction<P> extends AbstractFunction<P, Boolean> {

	/**
	 * {@link xxl.core.predicates.Predicate Predicate} to wrap.
	 */
	protected Predicate<? super P> predicate;

	/**
	 * Constructs a new wrapper for
	 * {@link xxl.core.predicates.Predicate Predicates} to
	 * {@link xxl.core.functions.Function Functions} wrapping the given
	 * predicate.
	 *
	 * @param predicate {@link xxl.core.predicates.Predicate Predicate} to
	 *        wrap.
	 */
	public PredicateFunction(Predicate<? super P> predicate) {
		this.predicate = predicate;
	}

	/**
	 * Invokes the function by calling the wrapped predicate and returning the
	 * corresponding value of the Boolean object.
	 * 
	 * @param arguments passed to the wrapped predicate.
	 * @return Boolean.TRUE if the wrapped predicate returns true,
	 *         otherwise Boolean.FALSE.
	 */
	@Override
	public Boolean invoke(List<? extends P> arguments) {
		return predicate.invoke(arguments);
	}
}
