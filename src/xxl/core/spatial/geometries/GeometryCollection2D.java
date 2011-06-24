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

/** A GeometryCollection2D is a geometry that is a collection of 1 or more 
 *  geometries. All the elements in a GeometryCollection must be in the 
 *  same Spatial Reference. This is also the Spatial Reference for the 
 *  GeometryCollection2D. <br>
 *  GeometryCollection places no other constraints on its elements. 
 *  Subclasses of GeometryCollection may restrict membership based on 
 *  dimension and may also place other constraints on the degree of spatial
 *  overlap between elements. 
 *  <br><br>See <a href="./doc-files/ogc_sfs.pdf">Simple Feature Specification (pdf)</a>.
 * @param <T> the supertype of the member geometries
 */
public interface GeometryCollection2D<T extends Geometry2D> extends Geometry2D{

	/** Returns the number of geometries in this GeometryCollection2D.
	 * @return the number of geometries in this GeometryCollection2D.
	 */
	public int getNumGeometries();

	/** Returns the n-th member geometry in this GeometryCollection2D.
	 * @param n the number of the element to return
	 * @return the n-th member geometry
	 */
	public T getGeometry(int n);
}
