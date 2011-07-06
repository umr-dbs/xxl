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
 * Maps the arguments to a tuple (=Object[]). 
 */
@SuppressWarnings("serial")
public class Tuplify extends AbstractFunction<Object, Object[]> {

	/**
	 * This instance can be used for getting a default instance of Tuplify. It
	 * is similar to the <i>Singleton Design Pattern</i> (for further details
	 * see Creational Patterns, Prototype in <i>Design Patterns: Elements of
	 * Reusable Object-Oriented Software</i> by Erich Gamma, Richard Helm,
	 * Ralph Johnson, and John Vlissides) except that there are no mechanisms
	 * to avoid the creation of other instances of Tuplify.
	 */
	public static final Tuplify DEFAULT_INSTANCE = new Tuplify();

	/**
	 * Tuplifies the given arguments 
	 * 	  
	 * @param arguments list with elements representing the objects to be
	 *        tuplified.
	 * @throws IllegalArgumentException if no argument is given or if too many
	 *         arguments are given. There is no sense in typlifying three or
	 *         more objects.
	 * @return the tuple containing the given arguments.
	 */
	@Override
	public Object[] invoke(List<? extends Object> arguments) throws IllegalArgumentException{
		if (arguments == null)
			throw new IllegalArgumentException("You can't tuplify null!"); 
		return arguments.toArray();
	}
}
