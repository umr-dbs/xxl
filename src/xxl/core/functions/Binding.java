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
 * This class represent an object with free variables which can bind with
 * values (arguments). This object can be a function, a predicate or a whole
 * subquery.
 * 
 * @param <T> the type of the values to be bound.
 */
public interface Binding<T> {
	
	/**
	 * Set the constant values to which a part of the free arguments 
	 * should be bound.
	 *
	 * @param constArguments the constant values to which a part of the
	 *        free arguments should be bound.
	 */
	public void setBinds(List<? extends T> constArguments);
	
	/**
	 * Set the constant values to which a part of the free arguments
	 * should be bound.
	 *
	 * @param constIndices the indices of the arguments which
	 *        should be bound to given arguments. The important is
	 *		  that it should always be sorted.
	 * @param constArguments the constant values to which a part of the
	 *        free arguments should be bound.
	 */
	public void setBinds(List<Integer> constIndices, List<? extends T> constArguments);
	
	/**
	 * Set a constant value to which a free argument should be bound.
	 *
	 * @param constIndex the index of the arguments which should be bound to
	 *        the given argument.
	 * @param constArgument the constant value to which a free argument
	 *        should be bound.
	 */
	public void setBind(int constIndex, T constArgument);
	
	/**
	 * Set free all bound arguments.
	 */
	public void restoreBinds();
	
}
