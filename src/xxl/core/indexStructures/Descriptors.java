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

package xxl.core.indexStructures;

import xxl.core.spatial.rectangles.Rectangle;

/**
 * This class contains <tt>static</tt> methods for descriptors.
 * 
 * @see Descriptor
 */
public abstract class Descriptors {

	/**
	 * Returns the union of the given descriptors (as a new Object).
	 * 
	 * @param d1
	 *            first descriptor
	 * @param d2
	 *            second descriptor
	 * @return the union of <tt>d1</tt> and <tt>d2</tt>
	 */
	public static Descriptor union(Descriptor d1, Descriptor d2) {
		Descriptor d = (Descriptor) d1.clone();
		d.union(d2);
		return d;
	}

	/**
	 * Returns the union of the given rectangles (as a new Object).
	 * 
	 * @param r1
	 *            first rectangle
	 * @param r2
	 *            second rectangle
	 * @return the union of <tt>r1</tt> and <tt>r2</tt>
	 * 
	 * @see xxl.core.spatial.rectangles.Rectangle
	 */
	public static Rectangle union(Rectangle r1, Rectangle r2) {
		Rectangle r = (Rectangle) ((Descriptor) r1).clone();
		r.union(r2);
		return r;
	}

}
