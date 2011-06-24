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

package xxl.core.cursors.groupers;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.math.functions.AggregationFunction;
import xxl.core.math.statistics.parametric.aggregates.Count;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;

/**
 * An aggregate grouper provides a grouping and an aggregation in only one step.
 * This could be useful in the case of a lack of memory if all data objects must
 * have been seen to provide a grouping meaning the grouping is data-driven.
 * 
 * <p>An instance of this class could easily be used to construct an equi-width
 * histogram of numerical data. To do so one must provide a function to identify
 * the corresponding histogram bucket similar to a hashing function and a
 * function for aggregation normally an average function. The corresponding code
 * could be like this:
 * <pre>
 *     Cursor histogram = new AggregateGrouper(
 *         new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 500),
 *         new AbstractFunction() {
 *             int start = 0;
 *             int binWidth = 3;
 *             public Object invoke(Object integer) {
 *                 int v = ((Integer)integer).intValue();
 *                 int bin = v / binWidth;
 *                 return "[ " + Integer.toString(bin*binWidth) + ", " + Integer.toString((bin+1)*binWidth) + ")";
 *             }
 *         },
 *         new AbstractFunction() {
 *             public Object invoke() {
 *                 return new StatefulAverage();
 *             }
 *         }
 *     );
 * </pre></p>
 * 
 * <p><b>Further examples:</b><br />
 * cumulative frequency distribution:
 * <pre>
 *     Cursor cfd = new CFDCursor(
 *         new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 100)
 *     );
 * 
 *     cfd.open();
 * 
 *     System.out.println ("cumulative frequency distribution (unsorted):");
 *     while (cfd.hasNext()) {
 *         Object[] next = (Object[])cfd.next();
 *         System.out.println(next[0] + " has occured " + next[1] + " times");
 *     }
 * 
 *     cfd.close();
 * </pre>
 * remove duplicates:
 * <pre>
 *     Cursor noDubs = new Mapper(
 *         new DuplicatesRemover(
 *             new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 100)
 *         ),
 *         new AbstractFunction() {
 *             public Object invoke(Object o) {
 *                 return ((Object[])o)[0];
 *             }
 *         }
 *     );
 * 
 *     noDubs.open();
 * 
 *     System.out.println("remove any duplicates");
 *     while (noDubs.hasNext())
 *         System.out.println(noDubs.next());
 * 
 *     noDubs.close();
 * </pre>
 * cumulative frequency distribution (sorted by value):
 * <pre>
 *     Cursor cfdSort = new ReplacementSelection(
 *         new CFDCursor(
 *             new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 100)
 *         ),
 *         100,
 *         new Comparator() {
 *             public int compare(Object o1, Object o2) {
 *                 return ((Integer)((Object[])o1)[0]).intValue() - ((Integer)((Object[])o2)[0]).intValue();
 *             }
 *         }
 *     );
 * 
 *     cfdSort.open();
 * 
 *     System.out.println("cumulative frequency distribution (sorted by value):");
 *     while (cfdSort.hasNext()) {
 *         Object[] next = (Object[])cfdSort.next();
 *         System.out.println(next[0] + " has occured " + next[1] + " times");
 *     }
 * 
 *     cfdSort.close();
 * </pre>
 * cumulative frequency distribution (sorted by frequency)
 * <pre>
 *     Cursor cfdSort2 = new ReplacementSelection(
 *         new CFDCursor(
 *             new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 100)
 *         ),
 *         100,
 *         new Comparator() {
 *             public int compare(Object o1, Object o2) {
 *                 return ((Long)((Object[])o1)[1]).intValue() - ((Long)((Object[])o2)[1]).intValue();
 *             }
 *         }
 *     );
 * 
 *     cfdSort2.open();
 * 
 *     System.out.println("cumulative frequency distribution (sorted by frequency):");
 *     while (cfdSort2.hasNext()) {
 *         Object[] next = (Object[])cfdSort2.next();
 *         System.out.println(next[0] + " has occured " + next[1] + " times");
 *     }
 * 
 *     cfdSort2.close();
 * </pre></p>
 *
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.functions.Function
 */
public class AggregateGrouper extends AbstractCursor {
	
	/**
	 * The input iteration providing the data to be grouped and aggregated.
	 */
	protected Iterator iterator;
	
	/**
	 * A function providing representatives for objects (the identity as a rule).
	 */
	protected Function representatives;

	/**
	 * A factory for the aggregation functions used for aggregating the data
	 * (e.g., count).
	 */
	protected Function aggregationFunctionFactory;

	/**
	 * The number of already delivered data.
	 */
	protected int pos;

	/**
	 * A list storing the aggregated tuples internally.
	 */
	protected List cumulative;

	/**
	 * A predicate providing the equality of the representatives of the objects.
	 */
	protected Predicate equals;

	/*
	 * Cumulative Frequency Distribution (CFD)
	 */
	
	/**
	 * This class provides a cumulative frequency distribution (CFD) of given
	 * data. To do this all equal data will be stored and counted. Calling the
	 * <tt>next</tt> method delivers an <tt>Object[]</tt> containing an object
	 * and the number of occurences of this object or equal objects.
	 */
	public static class CFDCursor extends AggregateGrouper {

		/**
		 * Creates a new CFD cursor.
		 *
		 * @param iterator the input iteration prodiving the data.
		 */
		public CFDCursor(Iterator iterator) {
			super(
				iterator,
				new AbstractFunction() {
					public Object invoke() {
						return new Count();
					}
				}
			);
		}
	}

	/**
	 * This class provides a duplicate remover.
	 */
	public static class DuplicatesRemover extends AggregateGrouper {

		/**
		 * Creates a new duplicate remover.
		 *
		 * @param iterator the input iteration providing the data.
		 */
		public DuplicatesRemover(Iterator iterator) {
			super(
				iterator,
				new AbstractFunction() {
					public Object invoke() {
						return new AggregationFunction() {
							public Object invoke(Object old, Object next) {
								return null;
							}
						};
					}
				}
			);
		}
	}

	/**
	 * Constructs a new aggregate grouper.
	 *
	 * @param iterator the input iteration delivering the data to aggregate.
	 * @param representatives this function provides to any given object a
	 *        representative to identify the corresponding objects. Complies to a
	 *        hashing function but unlike determining the hash bucket this
	 *        function returns a substitute for each object. This substitute will
	 *        be returned combined with the computed aggregation value by calling
	 *        the <tt>next</tt> method.
	 * @param aggregationFunctionFactory a factory method providing new
	 *        aggregation functions.
	 * @param equals equality predicate to determine the equality (resp.
	 *        inequaltity) of two given representatives. Using this predicate one
	 *        is able to use different representatives for the same group.
	 */
	public AggregateGrouper(Iterator iterator, Function representatives, Function aggregationFunctionFactory, Predicate equals) {
		this.iterator = iterator;
		this.representatives = representatives;
		this.aggregationFunctionFactory = aggregationFunctionFactory;
		this.equals = equals;
		cumulative = new LinkedList();
	}

	/**
	 * Constructs a new aggregate grouper using an
	 * {@link xxl.core.predicates.Equal equality} predicate as default.
	 *
	 * @param iterator the input iteration delivering the data to aggregate.
	 * @param representatives this function provides to any given object a
	 *        representative to identify the corresponding objects. Complies to a
	 *        hashing function but unlike determining the hash bucket this
	 *        function returns a substitute for each object. This substitute will
	 *        be returned combined with the computed aggregation value by calling
	 *        the <tt>next</tt> method.
	 * @param aggregationFunctionFactory a factory method providing new
	 *        aggregation functions.
	 */
	public AggregateGrouper(Iterator iterator, Function representatives, Function aggregationFunctionFactory) {
		this(
			iterator,
			representatives,
			aggregationFunctionFactory,
			Equal.DEFAULT_INSTANCE
		);
	}

	/**
	 * Constructs a new aggregate grouper using the default an
	 * {@link xxl.core.predicates.Equal equality} predicate and an
	 * {@link xxl.core.functions.Identity#DEFAULT_INSTANCE identity} function for the
	 * representative mapping as default.
	 *
	 * @param iterator the input iteration delivering the data to aggregate.
	 * @param aggregationFunctionFactory a factory method providing new
	 *        aggregation functions.
	 */
	public AggregateGrouper(Iterator iterator, Function aggregationFunctionFactory) {
		this(
			iterator,
			Identity.DEFAULT_INSTANCE,
			aggregationFunctionFactory
		);
	}

	/**
	 * Consumes every given object. I.e., determines the representive of the
	 * given object and computes the aggregation.
	 *
	 * @param next the next object to consume.
	 */
	private void consume(Object next) {
		Object value = representatives.invoke(next);
		Iterator it = cumulative.iterator();
		boolean notFound = true;
		while (it.hasNext() && notFound) {
			Object[] rep = (Object[])it.next();
			if (equals.invoke(value, rep[0])) {
				notFound = false;
				rep[1] = ((AggregationFunction)rep[2]).invoke(rep[1], next);
			}
		}
		if (notFound) {
			Object[] c = new Object[3];
			c[2] = aggregationFunctionFactory.invoke();
			c[0] = value;
			c[1] = ((AggregationFunction)c[2]).invoke(null, next);
			cumulative.add(c);
		}
	}

	/**
	 * Opens the aggregate grouper, i.e., signals the cursor to reserve
	 * resources and consume the input iteration to compute its groups. Before a
	 * cursor has been opened calls to methods like <tt>next</tt> or
	 * <tt>peek</tt> are not guaranteed to yield proper results. Therefore
	 * <tt>open</tt> must be called before a cursor's data can be processed.
	 * Multiple calls to <tt>open</tt> do not have any effect, i.e., if
	 * <tt>open</tt> was called the cursor remains in the state <i>opened</i>
	 * until its <tt>close</tt> method is called.
	 * 
	 * <p>Note, that a call to the <tt>open</tt> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (!isOpened) {
			pos = 0;
			while (iterator.hasNext())
				consume(iterator.next());
		}
		super.open();
	}

	/**
	 * Returns <tt>true</tt> if the iteration has more elements. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
	 * return an element rather than throwing an exception.)
	 * 
	 * @return <tt>true</tt> if the cursor has more elements.
	 */
	protected boolean hasNextObject() {
		return pos < cumulative.size();
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected Object nextObject() {
		Object[] n = new Object[2];
		n[0] = ((Object[])cumulative.get(pos))[0];
		n[1] = ((Object[])cumulative.get(pos))[1];
		pos++;
		return n;
	}
}
