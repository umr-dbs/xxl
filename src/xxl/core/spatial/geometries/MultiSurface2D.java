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

/** A GeometryCollection which can only hold Surface2D- objects
 *
 *  <br><br>See <a href="./doc-files/ogc_sfs.pdf">Simple Feature Specification (pdf)</a>.
 * @param <T> the type of this collection's elements 
 */
public interface MultiSurface2D<T extends Surface2D> extends GeometryCollection2D<T>{
	
	/** Returns the area of this Surface, as measured 
	 *  in the spatial reference system of this Surface.
	 * @return the area of this Surface
	 */
	public double getArea();
	
	/** Returns the mathematical centroid for this Surface 
	 *  as a Point. The result is not guaranteed to be on 
	 *  this Surface.
	 * @return the mathematical centroid for this Surface 
	 *  	   as a Point
	 */
	public Point2D getCentroid();
	
	
	/** Returns a point guaranteed to be on this Surface.
	 * @return  a point guaranteed to be on this Surface
	 */
	public Point2D getInteriorPoint();
}
