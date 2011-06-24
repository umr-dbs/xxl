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

/**
 * A Curve is a one-dimensional geometric object usually stored as a sequence of points, with the subtype
 * of Curve specifying the form of the interpolation between points. The SFS defines only one subclass of 
 * Curve, LineString, which uses linear interpolation between points. 
 * <br><br>
 * Specifications of MultiPoint according to the SFS:
 *  <ul>
 *   <li> A Curve is simple if it does not pass through the same point twice
 *       </li>
 *   <li> A Curve is closed if its start point is equal to its end point
 *   	 </li> 
 *   <li> The boundary of a closed Curve is empty
 *	 	 </li>
 *	 <li> A Curve that is simple and closed is a Ring
 *		 </li>
 *   <li> The boundary of a non-closed Curve consists of its two end points
 *       </li>
 *   <li> A Curve is defined as topologically closed
 *       </li>
 *  </ul>
 *  <br><br>See <a href="./doc-files/ogc_sfs.pdf">Simple Feature Specification (pdf)</a>.
 */
public interface Polygon2D extends Surface2D {

	/**
	 * Returns the exterior ring of this Polygon.
	 * @return the exterior ring of this Polygon.
	 */
	public LinearRing2D getExteriorRing();
	
	/** Returns the number of interior rings in this Polygon.
	 * @return the number of interior rings in this Polygon.
	 */
	public int getNumInteriorRing();
	
	/** Returns the n-th interior ring for this Polygon as a LineString.
	 * @param n the number of the ring to return
	 * @return the n-th interior ring for this Polygon as a LineString.
	 */
	public LinearRing2D getInteriorRing(int n);
}
