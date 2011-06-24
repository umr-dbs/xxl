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

/** A <code>MultiPoint2D</code> is a 0 dimensional GeometryCollection 
 *  whose elements are Points. 
 *  <br><br>
 *  Specifications of MultiPoint according to the SFS:
 *  <ul>
 *   <li> The elements of a MultiPoint are not connected or ordered.
 *       </li>
 *   <li> A MultiPoint is simple if no two Points in the MultiPoint 
 *   	  are equal (have identical coordinate values).
 *   	 </li> 
 *   <li> The boundary of a MultiPoint is the empty set.
 *	 	 </li>
 *  </ul>
 *  <br><br>See <a href="./doc-files/ogc_sfs.pdf">Simple Feature Specification (pdf)</a>.
 * @param <T> the type of this collection's elements
 */
public interface MultiPoint2D<T extends Point2D> extends GeometryCollection2D<T> {
}
