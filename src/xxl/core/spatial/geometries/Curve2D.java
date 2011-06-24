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
public interface Curve2D extends Geometry2D{
	
	/**The length of this Curve in its associated spatial reference.
	 * @return the length of this Curve in its associated spatial reference.
	 */
	public double getLength();
	
	/**
	 * Returns the start point of this Curve.
	 * @return  the start point of this Curve
	 */
	public Point2D getStartPoint();
	
	/**
	 * Returns the end point of this Curve.
	 * @return  the end point of this Curve
	 */
	public Point2D getEndPoint();
	
	/** Returns <tt>true</tt> if this Curve is closed (<tt>getStartPoint()== getEndPoint()</tt>).
	 * @return <tt>true</tt> if this Curve is closed, otherwise <tt>false</tt>
	 */
	public boolean isClosed();
	
	/** Returns <tt>true</tt> if this Curve is closed and this Curve is simple.
	 * @return <tt>true</tt> if this Curve is closed and this Curve is simple.
	 */
	public boolean isRing();
}
