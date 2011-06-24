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

package xxl.core.cursors.intersections;

import java.util.Comparator;
import java.util.Iterator;

import xxl.core.collections.sweepAreas.SortMergeEquiJoinSA;
import xxl.core.collections.sweepAreas.SweepAreaImplementor;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;

/**
 * A sort-based implementation of the intersection operator. The sort-based
 * intersection operator is based on a step-by-step processing of the two input
 * iterations in consideration of their sort-order. The sweep-line status
 * structure, here called sweep-area, consists of a bag with an additional
 * method for reorganisation and is used to store the elements of the first
 * input iteration. When an element of the second input iteration is processed,
 * it is used to query the sweep-area for matching elements that can be
 * returned as result of the intersection operator. Therefor an user defined
 * predicate is used to decide whether two elements of the input iterations are
 * equal concerning their values.
 * 
 * <p><b>Precondition:</b> The input cursors have to be sorted!</p>
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link Iterator} it is wrapped to a cursor by a call to the static method
 * {@link Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   SortBasedIntersection&lt;Integer&gt; intersection = new SortBasedIntersection&lt;Integer&gt;(
 *       Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).iterator(),
 *       Arrays.asList(0, 2, 4, 6, 8, 10).iterator(),
 *       new ListSAImplementor&lt;Integer&gt;(),
 *       ComparableComparator.INTEGER_COMPARATOR
 *   );
 *   
 *   intersection.open();
 *   
 *   Cursors.println(intersection);
 *   
 *   intersection.close();
 * </pre></code>
 * The input iteration for the intersection operation are given by two
 * list-iterators. The the first one contains all integer elements of the
 * interval [0, 10]. The second one delivers only the even elements of the same
 * interval. So the intersection operator should return the same integer values
 * as contained in the second input cursor.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *   intersection = new SortBasedIntersection&lt;Integer&gt;(
 *       Arrays.asList(1, 2, 2).iterator(),
 *       Arrays.asList(2, 2, 2).iterator(),
 *       new ListSAImplementor&lt;Integer&gt;(),
 *       ComparableComparator.INTEGER_COMPARATOR
 *   );
 *   
 *   intersection.open();
 *   
 *   Cursors.println(intersection);
 *   
 *   intersection.close();
 * </pre></code>
 * The second example usage computes the intersection of the input iterations
 * {1,2,2} and {2,2,2}. The intersection is created in the same way as in the
 * previous example. Conforming with the correct duplicate handling the result
 * will be {2,2}.</p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.intersections.NestedLoopsIntersection
 * @see xxl.core.relational.cursors.SortBasedIntersection
 * @see xxl.core.collections.sweepAreas.SweepArea
 */
public class SortBasedIntersection<E> extends AbstractCursor<E> {

	/**
	 * The two sorted input iteration of the sort-based intersection operator.
	 */
	@SuppressWarnings("unchecked") // only object of type Cursor<? extends E> are stored inside the array
	protected Cursor<? extends E>[] inputs = new Cursor[2];
	
	/**
	 * The sweep-area that is used for storing the elements of the first input
	 * iteration (<code>inputs[0]</code>) and that is probed with elements of
	 * the second input iteration (<code>inputs[1]</code>).
	 */
	protected SortMergeEquiJoinSA<E> sweepArea;
	
	/**
	 * The comparator used to compare the elements of the two input iterations.
	 */
	protected Comparator<? super E> comparator;
	
	/**
	 * A binary predicate evaluated for each tuple of elements backed on one
	 * element of each input iteration in order to select them. Only these
	 * tuples where the predicate's evaluation result is <code>true</code> have
	 * been qualified to be a result of the intersection operation.
	 */
	protected Predicate<? super E> equals;
	
	/**
	 * Creates a new sort-based intersection operator backed on two sorted
	 * input iterations using the given sweep-area to store the elements of the
	 * first input iteration and probe with the elements of the second one for
	 * matchings.
	 * 
	 * <p><b>Precondition:</b> The input iterations have to be sorted!</p>
	 * 
	 * <p>The given binary predicate to decide whether two tuples match will be
	 * used along with the sweep-area implementor to create a new sweep-area.
	 * Every iterator given to this constructor is wrapped to a cursor.</p>
	 *
	 * @param sortedInput0 the first sorted input iteration to be intersected.
	 * @param sortedInput1 the second sorted input iteration to be intersected.
	 * @param impl the sweep-area implementor used for storing elements of the
	 *        first sorted input iteration (<code>sortedInput0</code>).
	 * @param comparator the comparator that is used for comparing elements of
	 *        the two sorted input iterations.
	 * @param equals the binary predicate evaluated for each tuple of elements
	 *        backed on one element of each input iteration in order to select
	 *        them. Only these tuples where the predicate's evaluation result
	 *        is <code>true</code> have been qualified to be a result of the
	 *        intersection operation.
	 */
	public SortBasedIntersection(Iterator<? extends E> sortedInput0, Iterator<? extends E> sortedInput1, SweepAreaImplementor<E> impl, Comparator<? super E> comparator, Predicate<? super E> equals) {
		this.inputs[0] = Cursors.wrap(sortedInput0);
		this.inputs[1] = Cursors.wrap(sortedInput1);
		this.sweepArea = new SortMergeEquiJoinSA<E>(impl, 0, 2, equals);
		this.comparator = comparator;
		this.equals = equals;
	}
	
	/**
	 * Creates a new sort-based intersection operator backed on two sorted
	 * input iterations using the given sweep-area to store the elements of the
	 * first input iteration and probe with the elements of the second one for
	 * matchings.
	 * 
	 * <p><b>Precondition:</b> The input iterations have to be sorted!</p>
	 * 
	 * <p>A default {@link xxl.core.predicates.Equal equality} predicate to
	 * decide whether two tuples match will be used along with the given
	 * sweep-area implementor to create a new sweep-area. Every iterator given
	 * to this constructor is wrapped to a cursor.</p>
	 *
	 * @param sortedInput0 the first sorted input iteration to be intersected.
	 * @param sortedInput1 the second sorted input iteration to be intersected.
	 * @param impl the sweep-area implementor used for storing elements of the
	 *        first sorted input iteration (<code>sortedInput0</code>).
	 * @param comparator the comparator that is used for comparing elements of
	 *        the two sorted input iterations.
	 */
	public SortBasedIntersection(Iterator<? extends E> sortedInput0, Iterator<? extends E> sortedInput1, SweepAreaImplementor<E> impl, Comparator<? super E> comparator) {
		this(sortedInput0, sortedInput1, impl, comparator, Equal.DEFAULT_INSTANCE);
	}
	
	/**
	 * Opens the cursor, i.e., signals the cursor to reserve resources, open
	 * files, etc. Before a cursor has been opened calls to methods like
	 * <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Therefore <code>open</code> must be called before a
	 * cursor's data can be processed. Multiple calls to <code>open</code> do
	 * not have any effect, i.e., if <code>open</code> was called the cursor
	 * remains in the state <i>opened</i> until its <code>close</code> method
	 * is called.
	 * 
	 * <p>Note, that a call to the <code>open</code> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		if (isOpened)
			return;
		super.open();
		inputs[0].open();
		inputs[1].open();
	}
	
	/**
	 * Closes the cursor, i.e., signals the cursor to clean up resources, close
	 * files, etc. When a cursor has been closed calls to methods like
	 * <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Multiple calls to <code>close</code> do not have any
	 * effect, i.e., if <code>close</code> was called the cursor remains in the
	 * state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		inputs[0].close();
		inputs[1].close();
		sweepArea.close();
	}
	
	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>The implementation of this method is as follows:
	 * <code><pre>
	 *   while (inputs[0].hasNext() || inputs[1].hasNext()) {
	 *       int j = !inputs[0].hasNext() ?
	 *           1 :
	 *           !inputs[1].hasNext() ?
	 *               0 :
	 *               comparator.compare(inputs[0].peek(), inputs[1].peek()) <= 0 ?
	 *                   0 :
	 *                   1;
	 *       E queryObject = inputs[j].next();
	 *       sweepArea.reorganize(queryObject, j);
	 *       if (j == 0)
	 *           sweepArea.insert(queryObject);
	 *       else {
	 *           Iterator&lt;? extends E&gt; results = sweepArea.query(queryObject, j);
	 *           if (results.hasNext()) {
	 *               next = results.next();
	 *               results.remove();
	 *               return true;
	 *           }
	 *       }
	 *   }
	 *   return false;
	 * </pre></code>
	 * The int value <code>j</code> holds the index of the input iteration that
	 * delivers the next object to be proceeded (according to the sort-order of
	 * the input iterations) and <code>queryObject</code> stores this object.
	 * Thereafter the sweep-area is reorganized in order to remove object that
	 * cannot find a match in the second input iteration any more. Finally
	 * <code>queryObject</code> will be inserted into the sweep-area, if it
	 * comes from the first input iteration, or it will be used to query the
	 * sweep-area for matches, if it comes from the second one.</p>
	 * 
	 * @return <code>true</code> if the intersection operator has more
	 *         elements.
	 */
	protected boolean hasNextObject() {
		while (inputs[0].hasNext() || inputs[1].hasNext()) {
			int j = !inputs[0].hasNext() ?
				1 :
				!inputs[1].hasNext() ?
					0 :
					comparator.compare(inputs[0].peek(), inputs[1].peek()) <= 0 ?
						0 :
						1;
			E queryObject = inputs[j].next();
			sweepArea.reorganize(queryObject, j);
			if (j == 0)
				sweepArea.insert(queryObject);
			else {	
				Iterator<? extends E> results = sweepArea.query(queryObject, j);
				if (results.hasNext()) {
					next = results.next();
					results.remove();
					return true;
				} 
			}
		}
		return false;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <code>update</code> or
	 * <code>remove</code>, until a call to <code>next</code> or
	 * <code>peek</code> occurs. This is calling <code>next</code> or
	 * <code>peek</code> proceeds the iteration and therefore its previous
	 * element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return next;
	}
	
	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and does not work for this
	 * cursor.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		inputs[0].reset();
		inputs[1].reset();
		sweepArea.clear();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * <p>The current implementation of this method is as follows:
	 * <code><pre>
	 *   public boolean supportsReset() {
	 *       return false;
	 *   }
	 * </pre></code></p>
	 * 
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return inputs[0].supportsReset() && inputs[1].supportsReset();
	}
}
