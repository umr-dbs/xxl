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

package xxl.core.predicates;

import xxl.core.util.Distance;

/**
 * This class provides a binary predicate that determines whether the distance
 * between two given objects is less or equal a given epsilon distance. For
 * determining the distance between two objects, a distance function must be
 * specified during the creation of the predicate.
 * 
 * <p>Assuming that the object <code>A</code> to be processed implements the
 * <code>DistanceTo&lt;A&gt;</code> interface, the required distance function
 * can be ascribed to this functionality shown in the following listing:
 * <code><pre>
 *   new DistanceWithin<A>(
 *       new Distance<A>() {
 *           public double distance(A a1, A a2) {
 *               return a1.distanceTo(a2);
 *           }
 *       },
 *       0.5
 *   );
 * </pre></code></p>
 *
 * @param <P> the type of the predicate's parameters.
 * @see xxl.core.util.Distance
 * @see xxl.core.util.DistanceTo
 */
public class DistanceWithin<P> extends AbstractPredicate<P> {

	/**
	 * The distance function that should be used for determining the distance
	 * between two objects.
	 */
	protected Distance<? super P> distance;

	/**
	 * The epsilon distance represents the maximum distance between two objects
	 * such that the predicate returns <code>true</code>. In other words, the
	 * predicate returns <code>true</code> if the result of the distance
	 * function is less or equal the given epsilon distance.
	 */
	protected double epsilon;

	/**
	 * Creates a new binary predicate that determines whether the distance
	 * between two given objects is within a specified epsilon distance.
	 *
	 * @param distance the distance function that should be used for
	 *        determining the distance between two objects.
	 * @param epsilon the double value represents the maximum distance between
	 *        two objects such that the predicate returns <code>true</code>.
	 */
	public DistanceWithin(Distance<? super P> distance, double epsilon) {
		this.epsilon = epsilon;
		this.distance = distance;
	}

	/**
	 * Returns whether the first specified object lies within an epsilon
	 * distance of the second specified object. In other words, determine the
	 * distance between the given objects and returns <code>true</code> if this
	 * distance is less or equal the given epsilon distance.
	 *
	 * @param argument0 the object which epsilon distance should be regarded.
	 * @param argument1 the object that should be tested whether it lies within
	 *        the epsilon distance of the first object.
	 * @return <code>true</code> if the second object lies within the epsilon
	 *         distance of the first object, otherwise <code>false</code>.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		return distance.distance(argument0, argument1) <= epsilon;
	}
}
