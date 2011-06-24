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

package xxl.core.math.queries;

/**
 * This interface provides the support for the estimation of point queries respectively
 * the association of a numerical value with a <tt>point query<tt>.
 * A <tt>point query</tt> is performed on categorical data over a nominal scale,
 * i.e., additional information of the data is not required. As an estimation of
 * the <tt>point query<\tt>, a double value is returned.
 *
 * @see xxl.core.math.queries.WindowQuery
 * @see xxl.core.math.queries.RangeQuery
 */

public interface PointQuery {

	/** Performs an estimation of the given <tt>point query</tt>.
	 *
	 * @param query point query to process
	 * @return a numerical value delivering an estimation of the point query
	 */
	public abstract double pointQuery(Object query);
}
