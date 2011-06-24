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

package xxl.core.cursors.differences;

import java.util.Iterator;

import xxl.core.collections.bags.Bag;
import xxl.core.collections.bags.ListBag;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.RightBind;
import xxl.core.util.BitSet;

/**
 * A nested-loops implementation of the difference operator
 * (<code>input1 - input2</code>). This operation can be performed in two
 * different ways, namely the first realization removes an element of
 * <code>input1</code> if the same element exists in <code>input2</code>. The
 * second way of processing removes all elements of <code>input1</code> that
 * match with an element of <code>input2</code>. This second approch implies
 * that no duplicates will be returned by the difference operator, whereas the
 * first solution may contain duplicates if the number of equal elements in
 * cursor <code>input1</code> is greater than that of <code>input2</code>.
 * 
 * <p>The difference operator implemented by this class supports realization
 * depending on a boolean flag <code>all</code> that signals if all elements of
 * cursor <code>input1</code> that fulfill the given predicate will be removed
 * or only one element will be removed. As mentioned above a predicate is used
 * to determine if an element of <code>input2</code> matches with an element of
 * <code>input1</code>. If no predicate has been specified, internally the
 * {@link Equal equality} predicate is used by default. The function
 * <code>newBag</code> generates an empty bag each time it is invoked. This bag
 * should reside in main memory and contains as much elements of
 * <code>input1</code> as possible. Each element of <code>input2</code> gets
 * checked for a match with an element contained in this bag. The size of the
 * bag depends on the specified arguments <code>memSize</code> and
 * <code>objectSize</code>. The maximum number of elements this bag is able to
 * contain is computed by the formula:<br />
 * <pre>
 *   maxTuples = memSize / objectSize - 1
 * </pre>
 * One element is subtracted due to the reason that a minimum of one element of
 * cursor <code>input2</code> has also to be located in main memory.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   NestedLoopsDifference&lt;Integer&gt; difference = new NestedLoopsDifference&lt;Integer&gt;(
 *       new Enumerator(21),
 *       new Filter&lt;Integer&gt;(
 *           new Enumerator(21),
 *           new Predicate&lt;Integer&gt;() {
 *               public boolean invoke(Integer next) {
 *                   return next % 2 == 0;
 *               }
 *           }
 *       ),
 *       32,
 *       8,
 *       Bag.FACTORY_METHOD,
 *       new Predicate&lt;Integer&gt;() {
 *           public boolean invoke(Integer previous, Integer next) {
 *               return previous == next;
 *           }
 *       },
 *       false
 *   );
 *   
 *   difference.open();
 *   
 *   while (difference.hasNext())
 *       System.out.println(difference.next());
 *       
 *   difference.close();
 * </pre></code>
 * This nested-loops difference substracts all even numbers contained in the
 * interval [0, 21) from all numbers of the same interval (input1). The
 * available mememory size is set to 32 bytes and an object has the size of 8
 * bytes. So a maximum of 3 elements can be stored in the temporal main memory
 * bag. The FACTORY_METHOD of the class {@link Bag} delivers a new empty bag,
 * therefore a {@link xxl.core.collections.bags.ListBag list-bag} will be used
 * to store the elements of cursor <code>input1</code>. The specified predicate
 * returns <code>true</code> if an element of <code>input1</code> and an
 * element of <code>input2</code> are equal in their integer values. In this
 * example the flag <code>all</code> can be specified arbitrary due to
 * <code>input1</code> contains no duplicates. But if the first input cursor
 * would contain all elements from 0 to 20 twice the result would be the
 * same.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *   difference = new NestedLoopsDifference&lt;Integer&gt;(
 *       Arrays.asList(1, 2, 3, 4).iterator(),
 *       Arrays.asList(1, 2, 3).iterator(),
 *       32,
 *       8,
 *       new Function&lt;Object, Iterator&lt;Integer&gt;&gt;() {
 *           public Iterator&lt;Integer&gt; invoke() {
 *               return Arrays.asList(1, 2, 3).iterator();
 *           }
 *       },
 *       false
 *   );
 *   
 *   difference.open();
 *   
 *   while (difference.hasNext())
 *       System.out.println(difference.next());
 *   
 *   difference.close();
 * </pre></code>
 * This example computes the difference between to iterators based on the lists
 * {1, 2, 2, 3} and {1, 2, 3}. The memory usage is equal to that in example 1,
 * but in this case the flag <code>all</code> leads to different results.
 * <ul>
 *     <li>
 *         If <code>all == true</code> this operator delivers no results,
 *         because each element of <code>input2</code> is equal to an element
 *         of <code>input1</code>.
 *     </li>
 *     <li>
 *         But if <code>all == false</code> the element '2' is returned because
 *         it is only substracted once from <code>input1</code>.
 *     </li>
 * </ul>
 * The function specified above resets and returns the input cursor
 * <code>input2</code>, because it supports no <code>reset</code> operation,
 * but has to be traversed several times (inner loop). The predicate comparing
 * two elements concerning equality is set to {@link Equal#DEFAULT_INSTANCE} by
 * default.</p>
 *
 * <p><b>Note:</b> If an input iteration is given by an object of the class
 * {@link Iterator}, i.e., it does not support the <code>peek</code> operation,
 * it is internally wrapped to a cursor.</p>
 * 
 * @param <E> the type of the elements returned by this difference.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.functions.Function
 * @see xxl.core.predicates.Predicate
 * @see xxl.core.predicates.Equal
 * @see xxl.core.collections.bags.Bag
 * @see xxl.core.cursors.differences.SortBasedDifference
 * @see xxl.core.relational.cursors.NestedLoopsDifference
 */
public class NestedLoopsDifference<E> extends AbstractCursor<E> {

	/**
	 * The first (or left) input cursor of the difference operator.
	 */
	protected Cursor<E> input1;
	
	/**
	 * The second (or right) input cursor of the difference operator.
	 */
	protected Cursor<? extends E> input2;
	
	/**
	 * The output cursor of the difference operator.
	 */
	protected Cursor<E> results = null;

	/**
	 * A parameterless function returning a new bag on demand.
	 */
 	protected Function<?, ? extends Bag<E>> newBag;

	/**
	 * The maximum number of elements that can be stored in the bag returned by
	 * the function <code>newBag</code>.
	 */
	protected int maxTuples;


	/**
	 * The predicate used to determine a match between an element of
	 * <code>input2</code> and an element of <code>input1</code>.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * A flag signaling if all matches or only one matche returned by the
	 * predicate should be removed from the internal bag.
	 */
	protected boolean all;

	/**
	 * A parameterless function used to reset and return the second input
	 * cursor.
	 */
	protected Function<?, ? extends Cursor<? extends E>> resetInput2;

	/**
	 * A bit set storing which elements of cursor <code>input2</code> have been
	 * removed from cursor <code>input1</code> already.
	 */
	protected BitSet removedElements;

	/**
	 * Creates a new instance of the nested-loops difference operator. Every
	 * input iterator is wrapped to a cursor. Determines the maximum number of
	 * elements that can be stored in the bag used for the temporal storage of
	 * the elements of <code>input1</code> in main memory:
	 * <pre>
	 *     maxTuples = memSize / objectSize - 1
	 * </pre>
	 * This constructor should only be used if cursor <code>input2</code> is
	 * not resetable.
	 *
	 * @param input1 the first input iterator where the elements have to be
	 *        subtracted from.
	 * @param input2 the second input iterator containing the elements that
	 *        have to be subtracted.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input cursor.
	 * @param newBag a parameterless function delivering an empty bag on
	 *        demand. This bag is used to store the elements of cursor
	 *        <code>input1</code>.
	 * @param resetInput2 a parameterless function that delivers the second
	 *        input cursor again. This constructor should only be used if the
	 *        second input cursor does not support the <code>reset</code>
	 *        functionality.
	 * @param predicate a binaray predicate that has to determine a match
	 *        between an element of <code>input1</code> and an element of
	 *        <code>input2</code>.
	 * @param all a boolean flag signaling if all elements contained in the bag
	 *        that have a positiv match concerning the predicate will be
	 *        removed or only one element will be removed.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(Iterator<E> input1, Iterator<? extends E> input2, int memSize, int objectSize, Function<?, ? extends Bag<E>> newBag, final Function<?, ? extends Iterator<? extends E>> resetInput2, Predicate<? super E> predicate, boolean all) {
		this.input1 = Cursors.wrap(input1);
		this.input2 = Cursors.wrap(input2);
		this.newBag = newBag;
		this.resetInput2 = new AbstractFunction<Object, Cursor<? extends E>>() {
			public Cursor<? extends E> invoke() {
	 			return Cursors.wrap(resetInput2.invoke());
	 		}
		};
		this.predicate = predicate;
		this.all = all;
		this.maxTuples = memSize / objectSize - 1;
		if (memSize < 2*objectSize)
			throw new IllegalArgumentException("Insufficient main memory available.");
		if (!all) {
			int counter = 0;
			for ( ; input2.hasNext(); counter++)
				input2.next();
			removedElements = new BitSet(counter);
			input2 = this.resetInput2.invoke();
		}
	}

	/**
	 * Creates a new instance of the nested-loops difference operator. Every
	 * input iterator is wrapped to a cursor. Determines the maximum number of
	 * elements that can be stored in the bag used for the temporal storage of
	 * the elements of <code>input1</code> in main memory:
	 * <pre>
	 *     maxTuples = memSize / objectSize - 1
	 * </pre>
	 * Uses the factory method for bags,
	 * {@link xxl.core.collections.bags.Bag#FACTORY_METHOD}. Determines the
	 * equality between an element of <code>input1</code> and
	 * <code>input2</code> with the help of the default instance of the
	 * {@link Equal equality} predicate. This constructor should only be used
	 * if cursor <code>input2</code> is not resetable.
	 *
	 * @param input1 the first input iterator where the elements have to be
	 *        subtracted from.
	 * @param input2 the second input iterator containing the elements that
	 *        have to be subtracted.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input cursor.
	 * @param resetInput2 a parameterless function that delivers the second
	 *        input cursor again. This constructor should only be used if the
	 *        second input cursor does not support the <code>reset</code>
	 *        functionality.
	 * @param all a boolean flag signaling if all elements contained in the bag
	 *        that have a positiv match concerning the predicate will be
	 *        removed or only one element will be removed.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(Iterator<E> input1, Iterator<? extends E> input2, int memSize, int objectSize, Function<?, ? extends Iterator<? extends E>> resetInput2, boolean all) {
		this(
			input1,
			input2,
			memSize,
			objectSize,
			new AbstractFunction<Object, ListBag<E>>() {
				public ListBag<E> invoke() {
					return new ListBag<E>();
				}
			},
			resetInput2,
			Equal.DEFAULT_INSTANCE,
			all
		);
	}

	/**
	 * Creates a new instance of the nested-loops difference operator. Every
	 * input iterator is wrapped to a cursor. Determines the maximum number of
	 * elements that can be stored in the bag used for the temporal storage of
	 * the elements of <code>input1</code> in main memory:
	 * <pre>
	 *     maxTuples = memSize / objectSize - 1
	 * </pre>
	 * <code>Input2</code> has to support the <code>reset</code> operation,
	 * otherwise an {@link java.lang.UnsupportedOperationException} will be
	 * thrown!
	 *
	 * @param input1 the first input iterator where the elements have to be
	 *        subtracted from.
	 * @param input2 the second input iterator containing the elements that
	 *        have to be subtracted.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input cursor.
	 * @param newBag a parameterless function delivering an empty bag on
	 *        demand. This bag is used to store the elements of cursor
	 *        <code>input1</code>.
	 * @param predicate a binaray predicate that has to determine a match
	 *        between an element of <code>input1</code> and an element of
	 *        <code>input2</code>.
	 * @param all a boolean flag signaling if all elements contained in the bag
	 *        that have a positiv match concerning the predicate will be
	 *        removed or only one element will be removed.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(Iterator<E> input1, final Cursor<? extends E> input2, int memSize, int objectSize, Function<?, ? extends Bag<E>> newBag, Predicate<? super E> predicate, boolean all) {
		this(
			input1,
			input2,
			memSize,
			objectSize,
			newBag,
			new AbstractFunction<Object, Cursor<? extends E>>() {
				public Cursor<? extends E> invoke() {
					input2.reset();
					return input2;
				}
			},
			predicate,
			all
		);
	}

	/**
	 * Creates a new instance of the nested-loops difference operator. Every
	 * input iterator is wrapped to a cursor. Determines the maximum number of
	 * elements that can be stored in the bag used for the temporal storage of
	 * the elements of <code>input1</code> in main memory:
	 * <pre>
	 *     maxTuples = memSize / objectSize - 1
	 * </pre>
	 * Uses the factory method for bags, {@link Bag#FACTORY_METHOD}. Determines
	 * the equality between an element of <code>input1</code> and
	 * <code>input2</code> with the help of the default instance of the
	 * {@link Equal equality} predicate. <code>Input2</code> has to support the
	 * <code>reset</code> operation, otherwise an
	 * {@link java.lang.UnsupportedOperationException} will be thrown!
	 *
	 * @param input1 the first input iterator where the elements have to be
	 *        subtracted from.
	 * @param input2 the second input iterator containing the elements that
	 *        have to be subtracted.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input cursor.
	 * @param all a boolean flag signaling if all elements contained in the bag
	 *        that have a positiv match concerning the predicate will be
	 *        removed or only one element will be removed.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(Iterator<E> input1, Cursor<? extends E> input2, int memSize, int objectSize, boolean all) {
		this(
			input1,
			input2,
			memSize,
			objectSize,
			new AbstractFunction<Object, ListBag<E>>() {
				public ListBag<E> invoke() {
					return new ListBag<E>();
				}
			},
			Equal.DEFAULT_INSTANCE,
			all
		);
	}

	/**
	 * Opens the nested-loops difference operator, i.e., signals the cursor to
	 * reserve resources, open input iterations, etc. Before a cursor has been
	 * opened calls to methods like <code>next</code> or <code>peek</code> are
	 * not guaranteed to yield proper results. Therefore <code>open</code> must
	 * be called before a cursor's data can be processed. Multiple calls to
	 * <code>open</code> do not have any effect, i.e., if <code>open</code> was
	 * called the cursor remains in the state <i>opened</i> until its
	 * <code>close</code> method is called.
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
		input1.open();
		input2.open();
	}
	
	/**
	 * Closes the nested-loops difference operator, i.e., signals the cursor to
	 * clean up resources and close its input and output cursors. When a cursor
	 * has been closed calls to methods like <code>next</code> or
	 * <code>peek</code> are not guaranteed to yield proper results. Multiple
	 * calls to <code>close</code> do not have any effect, i.e., if
	 * <code>close</code> was called the cursor remains in the state
	 * <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		input1.close();
		input2.close();
		results.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>Builds a temporal bag calling <code>newBag.invoke()</code> and stores
	 * as much elements of cursor <code>input1</code> in this bag as possible.
	 * After that each element of the second input cursor is taken and with the
	 * help of the bag's <code>query</code> method a cursor containing all
	 * elements that have to be removed from <code>input1</code> are
	 * determined. Depending on the flag <code>all</code> all elements
	 * contained in that cursor are removed from the bag or only one element is
	 * removed. At last the bag's <code>cursor</code> method is called and the
	 * result cursor's reference is set to this cursor. If the result cursor
	 * contains any elements, <code>true</code> is returned, otherwise
	 * <code>false</code>. If the cursor <code>input1</code> contains further
	 * elements the whole procedure is returned.
	 *
	 * @return <code>true</code> if the nested-loops difference operator has
	 *         more elements.
	 */
	protected boolean hasNextObject() {
		if (results == null || !results.hasNext()) {
			Bag<E> tmpBag = newBag.invoke();
			while (input1.hasNext()) {
				if (tmpBag.size() < maxTuples)
					tmpBag.insert(input1.next());
				Cursor<E> tmpCursor;
				int position = 0;
				while (input2.hasNext()) {
					tmpCursor = tmpBag.query(new RightBind<E>(predicate, input2.peek()));
					while (tmpCursor.hasNext()) {
						tmpCursor.next();
						if (!all) {
							if (!removedElements.get(position)) {
								tmpCursor.remove();
								removedElements.set(position);
							}
							break;
						}
						else
							tmpCursor.remove();
					}
					if (input2.hasNext()) {
						input2.next();
						position++;
					}
				}
				input2 = resetInput2.invoke();
				if (tmpBag.size() == maxTuples)
					break;
			}
			results = tmpBag.cursor();
			return results.hasNext();
		}
		return true;
	}

	/**
	 * Returns the next element in the iteration. This element will be removed
	 * from the iteration, if <code>next</code> is called. This method returns
	 * the next element of the result cursor and removes it from the underlying
	 * bag.
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		E result = results.next();
		results.remove();
		return result;
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the nested-loops difference operator (optional operation). This method
	 * can be called only once per call to <code>next</code> or
	 * <code>peek</code> and removes the element returned by this method. Note,
	 * that between a call to <code>next</code> and <code>remove</code> the
	 * invocation of <code>peek</code> or <code>hasNext</code> is forbidden.
	 * The behaviour of a cursor is unspecified if the underlying data
	 * structure is modified while the iteration is in progress in any way
	 * other than by calling this method. This method is only supported if the
	 * bag's size is limited to only one element, otherwise an
	 * {@link java.lang.UnsupportedOperationException} will be thrown.
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the nested-loops difference
	 *         operator, i.e., thebag's size is greater than 1.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		results.remove();
		input1.remove();
	}
	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the nested-loops difference operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return maxTuples == 1 && input1.supportsRemove();
	}

	/**
	 * Replaces the object that was returned by the last call to
	 * <code>next</code> or <code>peek</code> (optional operation). This
	 * operation must not be called after a call to <code>hasNext</code>. It
	 * should follow a call to <code>next</code> or <code>peek</code>. This
	 * method should be called only once per call to <code>next</code> or
	 * <code>peek</code>. The behaviour of a nested-loops difference operator
	 * is unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * This method is only supported if the bag's size is limited to only one
	 * element, otherwise an {@link java.lang.UnsupportedOperationException}
	 * will be thrown.
	 *
	 * @param object the object that replaces the object returned by the last
	 *        call to <code>next</code> or <code>peek</code>.
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>update</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>update</code>
	 *         operation is not supported by the nested-loops difference
	 *         operator, i.e., the bag's size is greater than 1.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		super.update(object);
		results.update(object);
		input1.update(object);
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the nested-loops difference operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>update</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return maxTuples == 1 && input1.supportsRemove();
	}

	/**
	 * Resets the nested-loops difference operator to its initial state
	 * (optional operation). So the caller is able to traverse the underlying
	 * data structure again. The modifications, removes and updates concerning
	 * the underlying data structure, are still persistent. This method resets
	 * the input cursors, closes the result cursor and sets it to
	 * <code>null</code>.
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the nested-loops difference
	 *         operator.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		input1.reset();
		input2.reset();
		results.close();
		results = null;
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the nested-loops difference operator. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		return input1.supportsReset() && input2.supportsReset();
	}
}
