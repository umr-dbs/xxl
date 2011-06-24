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

package xxl.core.math.statistics.parametric.aggregates;

/** This class provides an interface for online aggregation functions.
 * Online aggregation functions are dedicated to support an online control of the
 * processed aggregation. Thus, online aggregation functions must at least support
 * monitoring, i.e. the aggregation process can be 'watched' during runtime.
 * Aggregation functions implementing this interface could be watched by {@link #getState()}.
 * Furthermore many aggregation functions depend on a (current) state in their computations. The 
 * state has to changeable during runtime. This could be done by using the {@link #setState(Object)} method.
 * <b>Warning: Using the {@link #setState(Object)} method could harm the correctness of the computed
 * aggregation value.</b>
 *
 * @see xxl.core.cursors.mappers.Aggregator
 */

public interface OnlineAggregation {

	/** Returns the current state of the on-line aggregation function
	 * implementing this interface.
	 * 
	 * @return current state of this function
	 */
	public Object getState();

	/** Sets a new state of the on-line aggregation function
	 * implementing this interface (optional).
	 * 
	 * @param state status to set
	 * @throws UnsupportedOperationException if this method is not supported by this class
	 */
	public void setState(Object state) throws UnsupportedOperationException;
}
