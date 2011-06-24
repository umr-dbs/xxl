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

package xxl.core.spatial.predicates;

import xxl.core.predicates.DistanceWithin;
import xxl.core.spatial.points.Point;
import xxl.core.spatial.points.Points;
import xxl.core.util.Distance;

/**
 *	A distance predicate based on the
 *	euclidean distance-measure (This distance-measure corresponds to the
 *	L_infinity-metric).
 *
 *  We provide a separate class since it uses some optimizations.
 *
 * @param <P> the type of the predicate's parameters.
 *  @see xxl.core.predicates.DistanceWithin
 *  @see xxl.core.spatial.points.Points
 */
public class DistanceWithinMaximum<P extends Point> extends DistanceWithin<P> {

	/** Creates a new DistanceWithinMaximum instance.
     *
	 * @param epsilon the double value represents the maximum distance
	 *        between two objects such that the predicate returns
	 *        <tt>true</tt>.
	 */
	public DistanceWithinMaximum(double epsilon) {
		super(
			new Distance<P>() {
				public double distance(P object0, P object1) {
					return object0.distanceTo(object1);
				}
			},
			epsilon
		);
	}

	/** Checks the distance of two given objects (assumed to be of type Point).
	 *
	 * This implementation is based on <tt>Points.withinMaximumDistance(Point,Point)</tt>.
	 * 
	 * @param o1 first object (Point)
	 * @param o2 second object (Point)
	 * @return returns <tt>true</tt> if the coordinate difference between the points in
	 *  any dimension is not bigger than the given distance	 
	 */
	public boolean invoke(P o1, P o2){
		return Points.withinMaximumDistance(o1, o2, epsilon );
	}
}
