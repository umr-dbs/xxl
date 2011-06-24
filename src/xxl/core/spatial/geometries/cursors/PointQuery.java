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
package xxl.core.spatial.geometries.cursors;

import xxl.core.functions.Function;
import xxl.core.indexStructures.RTree;
import xxl.core.spatial.KPE;
import xxl.core.spatial.geometries.Geometry2D;
import xxl.core.spatial.geometries.Point2D;
import xxl.core.spatial.geometries.predicates.Within;

/** A Point query returns all the indexed geometries which contain a specified point
 *  It can be realised as a special form of the window- query where the query- object
 *  is a point an the refinement predicate is 'within'.
 */
public class PointQuery extends RegionQuery{

	/** Creates a new PointQuery index. This constructor just calls the
		 *  super-class constructor with the given parameters and the 
		 *  {@link Within}. 
		 * @param index the {@link RTree} which indexes the spatial data
		 * @param point the query point
		 * @param getGeometry a Function which determines how to extract the geometries from the index- trees' leafs
		 * 		  <br>(The leaf- entries of the tree may contain the geometry or just a pointer to it) 
		 */
	public PointQuery(RTree index, Point2D point, Function<KPE, Geometry2D> getGeometry) {
		super(index, point, getGeometry, Within.DEFAULT_INSTANCE);			
	}	
}
