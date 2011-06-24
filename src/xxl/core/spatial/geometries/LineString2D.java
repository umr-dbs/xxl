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
package xxl.core.spatial.geometries;

/** A LineString is a Curve with linear interpolation between points. Each consecutive pair of points defines a
 *  line segment.
 */
public interface LineString2D extends Curve2D{
	
	/** Returns the number of points in this LineString.
	 * @return the number of points in this LineString.
	 */
	public int getNumberOfPoints();
	
	/** Returns the n-th point in this LineString.
	 * @param n the number of the Point2D to return
	 * @return the specified Point2D in this LineString
	 */
	public Point2D getPoint(int n); 
}
