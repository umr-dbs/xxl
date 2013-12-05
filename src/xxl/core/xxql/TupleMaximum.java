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

package xxl.core.xxql;

import java.util.Comparator;

import xxl.core.comparators.ComparableComparator;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.relational.tuples.Tuple;

@SuppressWarnings("unchecked")
public class TupleMaximum extends AggregationFunction<Tuple, Number> {

	private static final long serialVersionUID = -7973205770013142787L;
	
	/** 
	 * comparator imposing the total ordering used for determining the minimum of the given data 
	 */
	protected Comparator comparator;

	/** Constructs a new object of this class.
	 * 
	 * @param comparator that imposes the total ordering used for determining the maximum of the given data
	 */
	public TupleMaximum(Comparator comparator) {
		this.comparator = comparator;
	}

	/** Constructs a new object of this class using the 'natural ordering' of the treated objects.
	 */
	
	public TupleMaximum() {
		this(new ComparableComparator());
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers only <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param max result of the aggregation function in the previous computation step (maximum object so far)
	 * @param next next object used for computation
	 * @return aggregation value after n steps
	 */
	public Number invoke(Number max, Tuple next) {
		return next == null ? max : max == null ? ((Number) next.getObject(1)).doubleValue() : Math.max(max.doubleValue(),((Number) next.getObject(1)).doubleValue());
	}
}
