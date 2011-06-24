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

import java.util.Comparator;

import xxl.core.comparators.ComparableComparator;
import xxl.core.math.functions.AggregationFunction;

/**
 * Computes the number of runs in a given sequence with respect to an ordering 
 * imposed by a {@link java.util.Comparator comparator}. There, a run is an ordered
 * sequence.
 * For an overview of adaptive sorting strategies and sortness measures of datasets have a look at<BR>
 * Vladimir Estivill-Castro and Derick Wood:  A Survey of Adaptive Sorting Algorithms, ACM Computing Surveys,
 * volume 24, number 4, pages 441 - 476, 1992.<BR>
 *
 * <br>
 * <p><b>Objects of this type are recommended for the usage with aggregator cursors!</b></p>
 * <br>
 * Each aggregation function must support a function call of the following type:<br>
 * <tt>agg_n = f (agg_n-1, next)</tt>, <br>
 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps,
 * <tt>f</tt> the aggregation function,
 * <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
 * and <tt>next</tt> the next object to use for computation.
 * An aggregation function delivers just <tt>null</tt> as aggregation result as long as the aggregation
 * function has not yet fully initialized.
 * <br>
 * Objects of this class use internally stored informations about the sequence seen so far.
 * Thus, objects of this type have a status.
 * 
 * Consider the following example:
 * <code><pre>
 * Aggregator aggregator = new Aggregator( 
		new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(20), 40), 
		new NumberOfRuns()
	);
 * <\code><\pre>
 * <br>
 * 
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.functions.Function
 * @see java.util.Comparator
 */

public class NumberOfRuns extends AggregationFunction<Number,Number> {

	/** comparator imposing the total ordering used for determining the sequences of the given data */
	protected Comparator comparator;

	/** stores the last seen object for comparing */
	protected Object lastSeenObject = null;

	/** Constructs a new object of this class.
	 * 
	 * @param comparator imposes the total ordering used for determining the sequences of the given data
	 */
	public NumberOfRuns(Comparator comparator) {
		this.comparator = comparator;
	}

	/** Constructs a new object of this class using the 'natural ordering' of the treated objects.
	 * 
	 * @see xxl.core.comparators.ComparableComparator
	 */
	public NumberOfRuns() {
		this(new ComparableComparator());
	}

	/** Two-figured function call for supporting aggregation by this function.
	 * Each aggregation function must support a function call like <tt>agg_n = f (agg_n-1, next)</tt>,
	 * where <tt>agg_n</tt> denotes the computed aggregation value after <tt>n</tt> steps, <tt>f</tt>
	 * the aggregation function, <tt>agg_n-1</tt> the computed aggregation value after <tt>n-1</tt> steps
	 * and <tt>next</tt> the next object to use for computation.
	 * This method delivers just <tt>null</tt> as aggregation result as long as the aggregation
	 * has not yet initialized.
	 * 
	 * @param old result of the aggregation function in the previous computation step (number of runs counted so far)
	 * @param next next object used for computation
	 * @return aggregation value after n steps
	 */
	public Number invoke(Number old, Number next) {
//		if (old == null) { // initializing
//			lastSeenObject = next;
//			return new Integer(1);
//		} else {
//			if (lastSeenObject == null) { // reinitializing if null-Objects are given
//				lastSeenObject = next;
//				return new Integer(1);
//			} else {
//				int runs = ((Number) old).intValue();
//				if (comparator.compare(next, lastSeenObject) < 0)
//					runs++;
//				lastSeenObject = next;
//				return new Integer(runs);
//			}
//		}
		if (old == null || lastSeenObject == null) { // (re)initializing if null-Objects are given
			lastSeenObject = next;
			return new Integer(1);
		}
		else {
			lastSeenObject = next;
			return comparator.compare(next, lastSeenObject) < 0 ?
				new Integer(((Number)old).intValue() + 1) :
				old;
		}

	}
}
