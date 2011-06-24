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

package xxl.core.cursors.sources;

/**
 * This class provides a cursor that returns a given object exactly one time.
 * It simply depends on a {@link Repeater repeater} that's number of repeating
 * times is set to 1.
 * 
 * @param <E> the type of the elements returned by this iteration.
 */
public class SingleObjectCursor<E> extends Repeater<E> {

	/**
	 * Creates a new single object-cursor that returns the given object exactly
	 * one time.
	 * 
	 * @param object the object that should be returned by the single
	 *        object-cursor exactly one time.
	 */
	public SingleObjectCursor(E object) {
		super(object, 1);
	}

}
