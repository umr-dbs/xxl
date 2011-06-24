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

package xxl.core.cursors.unions;

import java.util.Iterator;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.ArrayCursor;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Constant;
import xxl.core.functions.Function;

/**
 * A sequentializer concatenates <code>n</code> input iterations to a single
 * one. To provide this functionality, it internally accesses an iteration of
 * input iterations specified by the user and stores a reference to the
 * currently processed inout iteration. If this input iteration has been
 * completely consumed, the next input iteration delivered by the internal
 * iteration of input iterations will be processed. Moreover it is possible to
 * define a function delivering the second input input iteration to be
 * processed.
 * 
 * <p><b>Implementation details:</b> The attribute <code>cursor</code>
 * represents the currently processed input iteration and is set by the method
 * <code>hasNextObject</code> to the next input iteration as follows:
 * <code><pre>
 *     while (!cursor.hasNext()) {
 *         cursor.close();
 *         if (iteratorsCursor.hasNext())
 *             cursor = Cursors.wrap(iteratorsCursor.next());
 *         else
 *             cursor = new EmptyCursor&lt;E&gt;();
 *     }
 * </pre><code>
 * The method <code>next</code> returns the next element of the currently used
 * input iteration, i.e., the <code>next</code> method of <code>cursor</code>
 * is called. So a sequentializer sets the attribute <code>cursor</code> to the
 * first input iteration and returns this iteration by lazy evaluation, after
 * that the attribute <code>cursor</code> is set to the next input iteration
 * and so on.</p>
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link java.util.Iterator} it is wrapped to a cursor by a call to the static
 * method {@link xxl.core.cursors.Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *     Sequentializer&lt;Integer&gt; sequentializer = new Sequentializer&lt;Integer&gt;(
 *         new Enumerator(11),
 *         new Enumerator(11, 21)
 *     );
 * 
 *     sequentializer.open();
 * 
 *     while (sequentializer.hasNext())
 *         System.out.print(sequentializer.next() + "; ");
 *     System.out.flush();
 *     System.out.println();
 * 
 *     sequentializer.close();
 * </pre></code>
 * This instance of a sequentializer concatenates the two given enumerators.
 * The first enumerator contains the elements 0,...,10. The second one contains
 * the elements 11,...,20. So the result of the completely consumed
 * sequentializer is an ascending sequence with range [0, 20].</p>
 *
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *     sequentializer = new Sequentializer&lt;Integer&gt;(
 *         new Enumerator(1, 4),
 *         new Function&lt;Object, Cursor&lt;Integer&gt;&gt;() {
 *             public Cursor&lt;Integer&gt; invoke() {
 *                 return new Enumerator(4, 7);
 *             }
 *         }
 *     );
 * 
 *     sequentializer.open();
 * 
 *     while (sequentializer.hasNext())
 *         System.out.print(sequentializer.next() + "; ");
 * 
 *     System.out.flush();
 *     System.out.println();
 * 
 *     sequentializer.close();
 * </pre></code>
 * This instance of a sequentializer concatenates the three elements of the
 * first enumerator (1, 2, 3) with the elements of the second enumerator
 * delivered by invoking the defined function. So the output printed to the
 * output stream is:
 * <pre>
 *     1; 2; 3; 4; 5; 6;
 * </pre></p>
 *
 * <p><b>Example usage (3):</b>
 * <code><pre>
 *     sequentializer = new Sequentializer&lt;Integer&gt;(
 *         new HashGrouper&lt;Integer&gt;(
 *             new Enumerator(21),
 *             new Function&lt;Integer, Integer&gt;() {
 *                 public Integer invoke(Integer next) {
 *                     return next % 5;
 *                 }
 *             }
 *         )
 *     );
 * 
 *     sequentializer.open();
 * 
 *     while (sequentializer.hasNext())
 *         System.out.print(sequentializer.next() + "; ");
 *     System.out.flush();
 *     System.out.println();
 * 
 *     sequentializer.close();
 * </pre></code>
 * This example demonstrates the sequentializer's concatentation using a
 * constructor that receives an iteration of input iterations as paramater. The
 * used {@link xxl.core.cursors.groupers.HashGrouper hash-grouper} is a cursor
 * and each element of this cursor points to a group of the used
 * {@link java.util.HashMap hash-map}, realized as a cursor. The elements of
 * the enumerator are inserted in the buckets of the hash-map by applying the
 * given function on them. So the buckets with keys 0,...,4 remain and are
 * filled up. For further details see
 * {@link xxl.core.cursors.groupers.HashGrouper}. Now the sequentializer takes
 * these buckets (cursors) and concatenates them. So the output is:
 * <pre>
 *     4; 9; 14; 19; 3; 8; 13; 18; 2; 7; 12; 17; 1; 6; 11; 16; 0; 5; 10; 15; 20;
 * </pre>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class Sequentializer<E> extends AbstractCursor<E> {

	/**
	 * The iteration containing the input iterations to be sequentialized.
	 */
	protected Iterator<? extends Iterator<E>> iteratorsCursor;

	/**
	 * The currently processed input iteration. The constructors set this
	 * cursor to a new
	 * {@link xxl.core.cursors.sources.EmptyCursor empty cursor}. This cursor
	 * is set in the method <code>hasNextObject</code> to the next input
	 * iteration as follows:
	 * <code><pre>
	 *     while (!cursor.hasNext()) {
	 *         cursor.close();
	 *         if (iteratorsCursor.hasNext())
	 *             cursor = Cursors.wrap(iteratorsCursor.next());
	 *         else
	 *             cursor = new EmptyCursor&lt;E&gt;();
	 *     }
	 * </pre></code>
	 * If the input iteration is given by an iterator it is wrapped to a
	 * cursor.
	 */
	protected Cursor<E> cursor = null;

	/**
	 * Creates a new sequentializer backed on an iteration of input iterations.
	 * Every iterator given to this constructor is wrapped to a cursor.
	 *
	 * @param iteratorsCursor iteration of input iterations to be
	 *        sequentialized.
	 */
	public Sequentializer(Iterator<? extends Iterator<E>> iteratorsCursor) {
		this.iteratorsCursor = iteratorsCursor;
		this.cursor = new EmptyCursor<E>();
	}

	/**
	 * Creates a new sequentializer backed on an array of input iterations.
	 * This array is converted to a cursor of input iterations using an
	 * {@link xxl.core.cursors.sources.ArrayCursor array-cursor}. Every
	 * iterator given to this constructor is wrapped to a cursor.
	 *
	 * @param iterators an array of input iterations to be sequentialized.
	 */
	public Sequentializer(Iterator<E>... iterators) {
		this(new ArrayCursor<Iterator<E>>(iterators));
	}

	/**
	 * Creates a new sequentializer backed on an input iteration and a
	 * parameterless function returning the second input iteration on demand.
	 * Every iterator given to this constructor is wrapped to a cursor.
	 *
	 * @param iterator the first input iteration to be sequentialized.
	 * @param function a parameterless function returning the second input
	 *        iteration to be sequentialized on demand. This function is
	 *        invoked after the first input iteration has been processed
	 *        completely.
	 */
	public Sequentializer(Iterator<E> iterator, Function<?, ? extends Iterator<E>> function) {
		this(
			new Mapper<Function<?, ? extends Iterator<E>>, Iterator<E>>(
				new AbstractFunction<Function<?, ? extends Iterator<E>>, Iterator<E>>() {
					public Iterator<E> invoke(Function<?, ? extends Iterator<E>> function) {
						return function.invoke();
					}
				},
				new ArrayCursor<Function<?, ? extends Iterator<E>>>(
					new Constant<Iterator<E>>(iterator),
					function
				)
			)
		);
	}

	/**
	 * Closes the sequentialier, i.e., signals it to clean up resources, close
	 * input iterations, etc. When a cursor has been closed calls to methods
	 * like <code>next</code> or <code>peek</code> are not guaranteed to yield
	 * proper results. Multiple calls to <code>close</code> do not have any
	 * effect, i.e., if <code>close</code> was called the cursor remains in the
	 * state <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		if (isClosed) return;
		super.close();
		cursor.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>The attribute <code>cursor</code> is set by this method to the next
	 * input iteration as follows:
	 * <code><pre>
	 *     while (!cursor.hasNext()) {
	 *         cursor.close();
	 *         if (iteratorsCursor.hasNext())
	 *             cursor = Cursors.wrap(iteratorsCursor.next());
	 *         else
	 *             cursor = new EmptyCursor&lt;E&gt;();
	 *     }
	 * </pre></code>
	 * If the next input iteration is given by an iterator it is wrapped to a
	 * cursor. The method returns whether the currently processed input
	 * iteration contains further elements, i.e., the result of the
	 * <code>hasNext</code> method of <code>cursor</code>.
	 *
	 * @return <code>true</code> if the sequentializer has more elements.
	 */
	protected boolean hasNextObject() {
		while (!cursor.hasNext()) {
			cursor.close();
			if (iteratorsCursor.hasNext())
				cursor = Cursors.wrap(iteratorsCursor.next());
			else {
				cursor = new EmptyCursor<E>();
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the sequentializer's methods, e.g.,
	 * <code>update</code> or <code>remove</code>, until a call to
	 * <code>next</code> or <code>peek</code> occurs. This is calling
	 * <code>next</code> or <code>peek</code> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 * 
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		return cursor.next();
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the sequentializer (optional operation). This method can be called only
	 * once per call to <code>next</code> or <code>peek</code> and removes the
	 * element returned by this method. Note, that between a call to
	 * <code>next</code> and <code>remove</code> the invocation of
	 * <code>peek</code> or <code>hasNext</code> is forbidden. The behaviour of
	 * a cursor is unspecified if the underlying data structure is modified
	 * while the iteration is in progress in any way other than by calling this
	 * method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the sequentializer.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		cursor.remove();
	}
	
	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the sequentializer. Otherwise it returns
	 * <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the sequentializer, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		return cursor.supportsRemove();
	}

	/**
	 * Replaces the last element returned by the sequentializer in the
	 * underlying data structure (optional operation). This method can be
	 * called only once per call to <code>next</code> or <code>peek</code> and
	 * updates the element returned by this method. Note, that between a call
	 * to <code>next</code> and <code>update</code> the invocation of
	 * <code>peek</code> or <code>hasNext</code> is forbidden. The behaviour of
	 * a cursor is unspecified if the underlying data structure is modified
	 * while the iteration is in progress in any way other than by calling this
	 * method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        sequentializer.
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>update</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>update</code>
	 *         operation is not supported by the sequentializer.
	 */
	public void update(E object) throws IllegalStateException, UnsupportedOperationException {
		super.update(object);
		cursor.update(object);
	}
	
	/**
	 * Returns <code>true</code> if the <code>update</code> operation is
	 * supported by the sequentializer. Otherwise it returns
	 * <code>false</code>.
	 *
	 * @return <tcodet>true</code> if the <code>update</code> operation is
	 *         supported by the sequentializer, otherwise <code>false</code>.
	 */
	public boolean supportsUpdate() {
		return cursor.supportsUpdate();
	}
}
