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

package xxl.core.spatial;

import xxl.core.spatial.points.Point;
import xxl.core.spatial.points.Points;
import xxl.core.util.Distance;

/**
 *	The L_p-Metric.
 *
 * @see xxl.core.spatial.points.Point
 *
 */
public class LpMetric implements Distance<Point> {

	/**
	 * The L_1 metric ("manhatten" metric).
	 */
	public static final LpMetric MANHATTEN = new LpMetric(1);

	/**
	 * The L_2 metric ("euclidean" metric).
	 */
	public static final LpMetric EUCLIDEAN = new LpMetric(2);
	
	/**
	 * The L_infinity metric ("maximum" metric).
	 */
	public static final LpMetric MAXIMUM = new LpMetric(0) {
		public double distance(Point object1, Point object2) {
			return Points.maxDistance(object1, object2);
		}
	};

	/**
	 * The value p of the L_p metric.
	 */
	protected int p;

	/**
	 * Creates a new instance of the LpMetric.
	 *
	 * @param p the value for L_p.
	 */
	public LpMetric(int p){
		this.p = p;	
	}

	/**
	 * Returns the L_p distance of the given objects.
	 * 
	 * <p>Implementation:
	 * <pre><code>
	 *   public double distance(Object o1, Object o2){
	 *     return Points.lpDistance( (Point)o1, (Point)o2, p );
	 *  }
	 *
	 *  </code></pre>
	 * @param o1 first object
	 * @param o2 second object  
	 * @return returns the L_p distance of the given objects
	 */
	public double distance(Point o1, Point o2){
		return Points.lpDistance(o1, o2, p);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + p;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof LpMetric))
			return false;
		LpMetric other = (LpMetric) obj;
		if (p != other.p)
			return false;
		return true;
	}
}
