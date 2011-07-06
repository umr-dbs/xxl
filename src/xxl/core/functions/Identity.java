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

/** 
 * The function provides a simple identity-function.
 *
 * @param <T> the type of the identity-functions parameters (and it's return
 *        type.
 * @deprecated replaced by {@link Functional.Identity}
 */
@SuppressWarnings("serial")
@Deprecated
public class Identity<T> extends AbstractFunction<T, T> {

	/**
	 * A prototype function simply returns its arguments (identity-function).
	 */
	public static final Function<Object, Object> DEFAULT_INSTANCE = new Identity<Object>();

	/**
	 * Simply returns the given argument.
	 * 
	 * @param argument the argument to be returned.
	 * @return the given argument.
	 */
	@Override
	public T invoke(T argument) {
		return argument;
	}
}
