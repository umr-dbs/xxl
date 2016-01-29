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

package xxl.core.cursors.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.Function;

/**
 * A mapper invokes a given mapping function on an array of input iterations.
 * The mapping function is applied to <code>n</code> input iterations at the
 * same time, that means a <code>n</code>-dimensional function is called and
 * its arguments are the elements of each input iteration (an array storing one
 * element per input iteration). The result of this mapping is an array storing
 * the results of the different functions that is returned by the mapper. Also
 * a partial input is allowed with the intention to apply the mapping function
 * on less than <code>n</code> arguments.
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link java.util.Iterator} it is wrapped to a cursor by a call to the static
 * method {@link xxl.core.cursors.Cursors#wrap(Iterator) wrap}. Additionally
 * some mapping functions are supplied in the class
 * {@link xxl.core.functions.Functions}.</p>
 * 
 * <p><b>Example usage (1):</b>
 * <pre>
 *     Mapper&lt;Integer, Integer&gt; mapper = new Mapper&lt;Integer, Integer&gt;(
 *         new Function;Integer, Integer&gt;() {
 *             public Integer invoke(Integer[] arguments) {
 *                 return arguments[0] * 2;
 *             }
 *         },
 *         new Enumerator(21)
 *     );
 * 
 *     mapper.open();
 * 
 *     while (mapper.hasNext())
 *         System.out.print(mapper.next() + "; ");
 *     System.out.flush();
 * 
 *     mapper.close();
 * </pre>
 * This mapper maps the given numbers of the enumerator with range 0,...,20
 * concerning the above defined function
 * <pre>
 *     f : x &rarr; 2*x.
 * </pre>
 * The function is applied on each element of the given enumerator, therefore
 * the following output is printed to the output stream:
 * <pre>
 *     0; 2; 4; 6; 8; 10; ... ; 36; 38; 40;
 * </pre>
 * But pay attention that the function's <tt>invoke</tt> method gets an object
 * array as parameter!</p>
 *
 * <p><b>Example usage (2):</b>
 * <pre>
 *     HashGrouper&lt;Integer&gt; hashGrouper = new HashGrouper&lt;Integer&gt;(
 *         new Function&lt;Integer, Integer&gt;() {
 *             public Integer invoke(Integer next) {
 *                 return next % 5;
 *             }
 *         },
 *         new Enumerator(21)
 *     );
 * 
 *     hashGrouper.open();
 * 
 *     Cursor&lt;Integer&gt;[] cursors = (Cursor&lt;Integer&gt;[])new Object[5];
 *     for (int i = 0; hashGrouper.hasNext(); i++)
 *         cursors[i] = hashGrouper.next();
 *     
 *     mapper = new Mapper&lt;Integer, Integer&gt;(
 *         cursors,
 *         new Function&lt;Integer, Integer&gt;() {
 *             public Integer invoke(Integer[] arguments) {
 *                 return Cursors.minima(new ArrayCursor&lt;Integer&gt;(arguments)).getFirst();
 *             }
 *         }
 *     );
 * 
 *     mapper.open();
 * 
 *     while (mapper.hasNext())
 *         System.out.print(mapper.next() + "; ");
 *     System.out.flush();
 *     
 *     mapper.close();
 *     hashGrouper.close();
 * </pre>
 * This example uses the a hash-grouper to partition the delivered numbers of
 * the input enumerator. For further information see
 * {@link xxl.core.cursors.groupers.HashGrouper}. The buckets of the hash-map
 * used by the hash-grouper are stored in the cursor list <code>cursors</code>.
 * Then a new mapper is created that maps this cursor array to the first
 * element of its contained minima. Therefore the static method
 * <code>minima</code> is used returning a linked list, where the
 * <code>getFirst</code> method is applied. For the first call to
 * <code>next</code> the cursor array contains the integer objects with value
 * 0,...,4, namely the first element of each bucket. The returned minimum is 0.
 * The next call to this method returns the minimum of 5,...,10, namely the
 * second element in each bucket. So the output of this use case is:
 * <pre>
 *     0; 5; 10; 15;
 * </pre>
 *
 * @param <I> the type of the elements consumed by this iteration.
 * @param <E> the type of the elements returned by this mapped iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.AbstractCursor
 */
public class Mapper<I, E> extends AbstractCursor<E> {

	/**
	 * The list of input iterations holding the data to be mapped.
	 */
	protected List<Cursor<? extends I>> inputs;

	/**
	 * The function used to map the input-elements to an output-element.
	 */
	protected Function<? super I, ? extends E> function;

	/**
	 * The arguments the function is applied to.
	 */
	protected List<I> arguments = null;

	/**
	 * A flag to detect if the function can be applied to less than one element
	 * of each input iteration.
	 */
	protected boolean allowPartialInput = true;

	/**
	 * Creates a new mapper using an input iteration array and a user defined
	 * function to map the elements. The flag <code>allowPartialInput</code>
	 * defines whether the function can be used with a lower dimension than
	 * <code>iterators.length</code>. Every iterator to this constructor is
	 * wrapped to a cursor.
	 *
	 * @param iterators the input iterations.
	 * @param function the function used to map the input-elements to an
	 *        output-element.
	 * @param allowPartialInput <code>true</code> if the function can be
	 *        applied to less than one element of each input iterator.
	 */
	public Mapper(Function<? super I, ? extends E> function, boolean allowPartialInput, Iterator<? extends I>... iterators) {
		this.inputs = new ArrayList<Cursor<? extends I>>(iterators.length);
		for (Iterator<? extends I> iterator : iterators)
			inputs.add(Cursors.wrap(iterator));
		this.function = function;
		this.allowPartialInput = allowPartialInput;
	}

	/**
	 * Creates a new mapper using the given input iteration array and user
	 * defined function to map the elements. Every iterator given to this
	 * constructor is wrapped to a cursor and no partial input is allowed.
	 *
	 * @param iterators the input iterations.
	 * @param function the function used to map the input-elements to an
	 *        output-element.
	 */
	public Mapper(Function<? super I, ? extends E> function, Iterator<? extends I>... iterators) {
		this(function, false, iterators);
	}

	/**
	 * Opens the mapper, i.e., signals the cursor to reserve resources, open
	 * input iterations, etc. Before a cursor has been opened calls to methods
	 * like <code>next</code> or <code>peek</code> are not guaranteed to yield
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
		for (Cursor<?> input : inputs)
			input.open();
	}
	
	/**
	 * Closes the mapper. Signals the mapper to clean up resources, close input
	 * iterations, etc. After a call to <code>close</code> calls to methods
	 * like <code>next</code> or <code>peek</code> are not guarantied to yield
	 * proper results. Multiple calls to <code>close</code> do not have any
	 * effect, i.e., if <code>close</code> was called the mapper remains in the
	 * state "closed".
	 */
	public void close() {
		if (isClosed)
			return;
		super.close();
		for (Cursor<?> input : inputs)
			input.close();
	}

	/**
	 * Returns <code>true</code> if the iteration has more elements. (In other
	 * words, returns <code>true</code> if <code>next</code> or
	 * <code>peek</code> would return an element rather than throwing an
	 * exception.)
	 * 
	 * <p>This method returns <code>true</code> if all input iterations have
	 * more elements or if at least one input iteration has more elements and
	 * partial input is allowed.</p>
	 * 
	 * @return <code>true</code> if the mapper has more elements.
	 */
	protected boolean hasNextObject() {
		int count = 0;
		for (Cursor<?> input : inputs)
			if (input.hasNext())
				count++;
		return count == 0 ?
			false :
			count == inputs.size() ?
				true :
				allowPartialInput;
	}

	/**
	 * Computes the next element to be returned by a call to <code>next</code>
	 * or <code>peek</code>.
	 * 
	 * <p>A new object array <code>arguments</code> with length
	 * <code>size()</code> is assigned and filled with the next elements of the
	 * input iterations or <code>null</code>, if the next element of such an
	 * input iteration does not exist. Then the mapping function is applied and 
	 * the next element to be returned by the mapper is returned.</p>
	 *
	 * @return the next element in the iteration.
	 */
	protected E nextObject() {
		arguments = new ArrayList<I>(inputs.size());
		for (Cursor<? extends I> input : inputs)
			arguments.add(input.hasNext() ? input.next() : null);
		return function.invoke(arguments);
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the mapper (optional operation). This method can be called only once per
	 * call to <code>next</code> or <code>peek</code> and removes the element
	 * returned by this method. Note, that between a call to <code>next</code>
	 * and <code>remove</code> the invocation of <code>peek</code> or
	 * <code>hasNext</code> is forbidden. The behaviour of a cursor is
	 * unspecified if the underlying data structure is modified while the
	 * iteration is in progress in any way other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <code>next</code> or
	 *         <code>peek</code> method has not yet been called, or the
	 *         <code>remove</code> method has already been called after the
	 *         last call to the <code>next</code> or <code>peek</code> method.
	 * @throws UnsupportedOperationException if the <code>remove</code>
	 *         operation is not supported by the cursor.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		super.remove();
		for (Cursor<?> input : inputs)
			input.remove();
	}

	/**
	 * Returns <code>true</code> if the <code>remove</code> operation is
	 * supported by the mapper. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>remove</code> operation is
	 *         supported by the mapper, otherwise <code>false</code>.
	 */
	public boolean supportsRemove() {
		boolean supportsRemove = true;
		for (Cursor<?> input : inputs)
			supportsRemove &= input.supportsRemove();
		return supportsRemove;
	}
	
	/**
	 * Resets the mapper to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the mapper.
	 */
	public void reset() throws UnsupportedOperationException {
		super.reset();
		for (Cursor<?> input : inputs)
			input.reset();
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the mapper. Otherwise it returns <code>false</code>.
	 * 
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the mapper, otherwise <code>false</code>.
	 */
	public boolean supportsReset() {
		boolean supportsReset = true;
		for (Cursor<?> input : inputs)
			supportsReset &= input.supportsReset();
		return supportsReset;
	}
}
