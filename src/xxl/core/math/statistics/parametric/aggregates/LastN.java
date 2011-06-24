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

import xxl.core.math.functions.AggregationFunction;

/**
 * Stores the last 'seen' n objects.
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * <br>
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers only <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized, meaning to this class the first (n-1)-th delivered
 * objects are <tt>null</tt>!
 * 
 * Consider the following example:
 * <code><pre>
 * Aggregator agg = new Aggregator(
		it ,			// input-Cursor
		new LastN( 3)	// aggregate function
	);
 * <\code><\pre>
 * <br>
 *
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 */

public class LastN extends AggregationFunction<Number,Object[]> {

	/** number of objects to store */
	protected int n;
	
	/** mimimun number of objects in store before result is returned */
	protected int m;

	/** position of the number in the internally stored array to substitute next */
	protected int pos;

	/** internally used object storage */
	protected Object[] store;

	/** indicates whether the storage has been initialized or not */
	protected boolean init;

	/** Constructs a new object of this type.
	 * 
	 * @param n number of objects to store
	 * @param m minimum number of objects before result is returned
	 * @throws IllegalArgumentException if a number less or equal 0 is given
	 */
	public LastN(int n, int m) throws IllegalArgumentException {
		if (n < 1)
			throw new IllegalArgumentException("Can't store " + n + " numbers! There must be given a number n >= 1!");
		this.n = n;
		this.m = m;
		pos = 0;
		init = false;
		store = new Object[n];
	}

	public LastN(int n) throws IllegalArgumentException {
		this(n, n);
	}
	
	public LastN(Integer n, Integer m) throws IllegalArgumentException {
		this(n.intValue(), m.intValue());
	}
	
	public LastN(Integer n) throws IllegalArgumentException {
		this(n.intValue(), n.intValue());
	}
	

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param oldStore result of the aggregation function of the last n seen 
	 * numbers in the previous computation step
	 * @param next next object used for computation
	 * @return object array with the last n seen elements
	 */
	public Object[] invoke(Object[] oldStore, Number next) {
		// reinit if a previous aggregation value == null is given
		// and the aggregation function has already been initialized
		if ((oldStore == null) && (init)) {
			pos = 0;
			init = false;
			store = new Object[n];
		}
		store[pos] = next;
		pos++;
		if (pos == n) {
			init = true;
			pos = 0;
		}
		if (init)
			return store;
		if (pos<m)
			return null;
		Object [] result = new Object[pos];
		System.arraycopy(store, 0, result, 0, pos);
		return result;
	}
}
