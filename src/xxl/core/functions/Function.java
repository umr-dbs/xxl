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

/**
 * In Java, there is no direct support for the declaration of functional types
 * and functional variables. This deficiency is eliminated by the interface
 * Function. The invoke method defines the functionality of a function. This
 * method accepts both separate input parameters and a typed list holding them.
 * The output of invoke is an object of the given return type. Simplified
 * versions of invoke exists suitable for functions with none, one or two input
 * parameters. The invocation of a function is then triggered by simply calling
 * invoke.
 * 
 * @param <P> the type of the function's parameters.
 * @param <R> the return type of the function.
 * @deprecated replaced {@link Functional} 
 */
@Deprecated
public interface Function<P, R> {

	/**
	 * Returns the result of the function as an object of the return type.
	 * 
	 * @param arguments a list of the arguments to the function.
	 * @return the function value is returned.
	 */
	public abstract R invoke(List<? extends P> arguments);

	/**
	 * Returns the result of the function as an object of the result type.
	 * 
	 * @return the function value is returned.
	 */
	public abstract R invoke();

	/**
	 * Returns the result of the function as an object of the result type.
	 * 
	 * @param argument the argument to the function.
	 * @return the function value is returned.
	 */
	public abstract R invoke(P argument);

	/**
	 * Returns the result of the function as an object of the result type.
	 * 
	 * @param argument0 the first argument to the function
	 * @param argument1 the second argument to the function
	 * @return the function value is returned
	 */
	public abstract R invoke(P argument0, P argument1);

}
