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
 * This class provides a binary predicate which right (second) argument is
 * bound to a constant object.
 * 
 * <p>For example, consider the predicate {@link Less} that returns true if the
 * first argument is less than the second. By creating a <code>RightBind</code>
 * instance
 * <code><pre>
 *   Predicate&lt;Integer&gt; p = new RightBind&lt;Integer&gt;(new Less&lt;Integer&gt;(), 42);
 * </pre></code>
 * <code>p</code> can be evaluated by calling
 * <code><pre>
 *   p.invoke(2);                   //predicate: 2 < 42
 * </pre></code>
 * which corresponds to the call
 * <code><pre>
 *   p.invoke(2, 42);               //predicate: 2 < 42
 * </pre></code></p>
 *
 * @param <P> the type of the predicate's parameters.
 * @see LeftBind
 */
public class RightBind<P> extends PredicateMetaDataPredicate<P, P> {

	/**
	 * This object is used as constant right (second) object of this
	 * predicate's <code>invoke</code> methods.
	 */
	protected P constArgument;

	/**
	 * Creates a new predicate which binds the right (second) argument of the
	 * specified predicate to the given constant object.
	 *
	 * @param predicate the predicate which right (second) argument should be
	 *        bound.
	 * @param constArgument the constant argument to be used for the right
	 *        (second) argument of the predicate.
	 */
	public RightBind(Predicate<? super P> predicate, P constArgument) {
		super(predicate);
		this.constArgument = constArgument;
	}

	/**
	 * Creates a new predicate which binds the right (second) argument of the
	 * specified predicate to <code>null</code>.
	 *
	 * @param predicate the predicate which right (second) argument should be
	 *        bound to <code>null</code>.
	 */
	public RightBind(Predicate<? super P> predicate) {
		this(predicate, null);
	}

	/**
	 * Sets the constant value to which the right (second) argument of the
	 * wrapped predicate should be bound and returns the changed predicate.
	 *
	 * @param constArgument the constant value to which the right (second)
	 *        argument of the wrapped predicate should be bound.
	 * @return the predicate which right (second) argument is bound to the
	 *         specified object.
	 */
	public RightBind<P> setRight(P constArgument) {
		this.constArgument = constArgument;
		return this;
	}

	/**
	 * Returns the result of the underlying predicate's <code>invoke</code>
	 * method that is called with the bound right (second) argument. The
	 * implementation of this method is
	 * <code><pre>
	 *   return predicate.invoke(arguments.get(0), constArgument);
	 * </pre></code>
	 *
	 * @param arguments the arguments to the underlying predicate.
	 * @return the result of the underlying predicate's <code>invoke</code>
	 *         method that is called with the bound right (second) argument.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		return predicate.invoke(arguments.get(0), constArgument);
	}

	/**
	 * Returns the result of the underlying predicate's <code>invoke</code>
	 * method that is called with the bound right (second) argument. The
	 * implementation of this method is
	 * <code><pre>
	 *   return predicate.invoke(argument, constArgument);
	 * </pre></code>
	 *
	 * @param argument the left (first) argument to the underlying predicate.
	 * @return the result of the underlying predicate's <code>invoke</code>
	 *         method that is called with the bound right (second) argument.
	 */
	@Override
	public boolean invoke(P argument) {
		return predicate.invoke(argument, constArgument);
	}

	/**
	 * Returns the result of the underlying predicate's <code>invoke</code>
	 * method that is called with the bound right (second) argument. The
	 * implementation of this method is
	 * <code><pre>
	 *   return predicate.invoke(argument0, constArgument);
	 * </pre></code>
	 *
	 * @param argument0 the left (first) argument to the underlying predicate.
	 * @param argument1 the right argument to this predicate that is replaced
	 *        by the bound argument.
	 * @return the result of the underlying predicate's <code>invoke</code>
	 *         method that is called with the bound right (second) argument.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return predicate.invoke(argument0, constArgument);
	}

	/**
	 * Returns the meta data for this meta data predicate which is set to the
	 * constant right (second) argument of this predicate.
	 *
	 * @return the constant right (second) argument of this predicate.
	 */
	@Override
	public P getMetaData() {
		return constArgument;
	}
}
