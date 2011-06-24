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

import xxl.core.collections.MappedList;
import xxl.core.functions.Function;

/**
 * This class provides a decorator predicate that applies a mapping to the
 * arguments of the underlying predicate. Everytime arguments are passed to an
 * <code>invoke</code> method of this class, the arguments are first mapped
 * using a given mapping function and afterwards passed to the
 * <code>invoke</code> method of the underlying predicate.
 * 
 * @param <P> the type of the predicate's parameters.
 * @param <R> the type of the underlying predicate's parameters (the return
 *        type of the mapping function).
 */
public class FeaturePredicate<P, R> implements Predicate<P> {

	/**
	 * A reference to the predicate to be decorated. This reference is used to
	 * perform method calls on the underlying predicate.
	 */
	protected Predicate<? super R> predicate;
	
	/**
	 * 
	 * A reference to the mapping function that is applied to the arguments of
	 * this predicate's <code>invoke</code> methods before the
	 * <code>invoke</code> method of the underlying predicate is called.
	 */
	protected Function<? super P, ? extends R> mapping;

	/**
	 * Creates a new feature predicate that applies the specified mapping to
	 * the arguments of it's <code>invoke</code> methods before the
	 * <code>invoke</code> method of the given predicate is called.
	 *
	 * @param predicate the predicate which input arguments should be mapped.
	 * @param mapping the mapping function that is applied to the arguments of
	 *        the wrapped predicate.
	 */
	public FeaturePredicate(Predicate<? super R> predicate, Function<? super P, ? extends R> mapping) {
		this.predicate = predicate;
		this.mapping = mapping;
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value. The
	 * mapped arguments of this method are passed to the underlying predicate's
	 * <code>invoke</code> method.
	 *
	 * @param arguments the arguments to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		return predicate.invoke(new MappedList<P, R>(arguments, mapping));
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 *
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke() {
		return predicate.invoke();
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value. The
	 * mapped argument of this method is passed to the underlying predicate's
	 * <code>invoke</code> method.
	 *
	 * @param argument the argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument) {
		return predicate.invoke(mapping.invoke(argument));
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value. The
	 * mapped arguments of this method are passed to the underlying predicate's
	 * <code>invoke</code> method.
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return predicate.invoke(mapping.invoke(argument0), mapping.invoke(argument1));
	}
}
