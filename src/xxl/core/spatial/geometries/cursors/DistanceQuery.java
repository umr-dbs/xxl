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

import xxl.core.cursors.filters.WhileTaker;
import xxl.core.predicates.AbstractPredicate;

/** A DistanceQuery delivers all neighbors of a query-object whose distance to the
 *  query- object is below epsilon.
 *  
 *  @see xxl.core.spatial.geometries.cursors.NearestNeighborQuery
 */
public class DistanceQuery extends WhileTaker<DistanceWeightedKPE>{

	/** Initializes the DistanceQuery.
	 * 
	 * @param nnQuery the query-operator which returns its results sorted by the minimum distance to 
	 * 				  the query object
	 * @param epsilon the maximum distance between the query object and qualifying result- objects 
	 */
	public DistanceQuery( NearestNeighborQuery nnQuery, final double epsilon){
		super( 	nnQuery, 
				new AbstractPredicate<DistanceWeightedKPE>(){
					public boolean invoke(DistanceWeightedKPE k){
						return k.getDistance()<= epsilon;
					}
				}
			);
	}
}
