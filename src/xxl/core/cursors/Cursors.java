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

package xxl.core.cursors;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.InverseComparator;
import xxl.core.cursors.groupers.Minimator;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.functions.PredicateFunction;
import xxl.core.functions.Print;
import xxl.core.io.converters.Converter;
import xxl.core.predicates.Predicate;
import xxl.core.util.ArrayResizer;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class contains various <tt>static</tt> methods for manipulating
 * iterators. For example :
 * <ul>
 *     <li>
 *         A user defined function can be applied to each element of an
 *         iterator, to a couple of elements of two iterators traversed
 *         synchronously or even to a set of elements of an array of iterators.
 *     </li>
 *     <li>
 *         A condition can be checked for a whole iterator by invoking a given
 *         boolean function or predicate on it.
 *     </li>
 *     <li>
 *         Useful methods are given to determine the first, the last or the
 *         <tt>n</tt>'th element of an iterator, its minima and maxima and to
 *         update or remove all elements of an iterator.
 *     </li>
 *     <li>
 *         Some converters are given, i.e., the caller is able to convert an
 *         iterator to a list, a map, a collection or an array.
 *      </li>
 * </ul>
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *     Cursor&lt;Integer&gt; cursor = new Enumerator(11);
 *     cursor.open();
 *     
 *     System.out.println("First element : " + Cursors.first(cursor));
 *     
 *     cursor.reset();
 *     System.out.println("Third element : " + Cursors.nth(cursor, 3));
 * 
 *     cursor.reset();
 *     System.out.println("Last element : " + Cursors.last(cursor));
 * 
 *     cursor.reset();
 *     System.out.println("Length : " + Cursors.count(cursor));
 * 
 *     cursor.close();
 * </pre></code>
 * These examples demonstrate some useful static methods for navigation in a
 * cursor.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *     Map.Entry&lt;?, LinkedList&lt;Integer&gt;&gt; entry = Cursors.maximize(
 *         cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 50),
 *         new Identity&lt;Integer&gt;()
 *     );
 *     System.out.println("Maximum value is : " + entry.getKey());
 * 
 *     System.out.print("Maxima : ");
 *     for (Integer next : entry.getValue())
 *         System.out.print(next + "; ");
 *     System.out.flush();
 *     
 *     cursor.close();
 * </pre></code>
 * This example computes the maximum value of 50 randomly distributed integers
 * of the interval [0, 100) using a mapping. Also an iterator containing all
 * maxima is printed to the output.</p>
 * 
 * <p><b>Example usage (3):</b>
 * <code><pre>
 *     Cursors.forEach(
 *         new Function&lt;Integer, Integer&gt;() {
 *             public Integer invoke(Integer object) {
 *                 Integer result = (int)Math.pow(object, 2);
 *                 System.out.println("Number : " + object + " ; Number^2 : " + result);
 *                 return result;
 *             }
 *         },
 *         cursor = new Enumerator(11)
 *     );
 *     cursor.close();
 * </pre></code>
 * This example computes the square value for each element of the given
 * enumerator with range 0,...,10.</p>
 * 
 * <p><b>Example usage (4):</b>
 * <code><pre>
 *     System.out.println(
 *         "Is the number '13' contained in the discrete random numbers' cursor : " +
 *         Cursors.any(
 *             new Predicate&lt;Integer&gt;() {
 *                 public boolean invoke(Integer object) {
 *                     return object == 13;
 *                 }
 *             },
 *             cursor = new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(1000), 200)
 *         )
 *     );
 *     cursor.close();
 * </pre></code>
 * This example checks if a cursor of 200 randomly distributed integers with
 * maximum value 999 contains the value 13 by evaluating a predictate on its
 * elements. It returns <code>true</code> if the predicate is <code>true</code>
 * for only one element of the input cursor.</p>
 * 
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.functions.Function
 * @see xxl.core.predicates.Predicate
 * @see java.util.Collection
 * @see java.util.List
 * @see java.util.Map
 * @see java.util.Map.Entry
 * @see java.util.Comparator
 */
public abstract class Cursors {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Cursors() {
		// private access in order to ensure non-instantiability
	}

	
	/**
	 * Invokes the higher-order function on each set of elements given by the
	 * input iterator array. The set contains one element per iterator and the
	 * function is only invoked while all iterators are not empty.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param <R> the type of the elements returned by the given function.
	 * @param function the function to be invoked on each set of elements.
	 * @param iterators the iterators used to get a set of elements by taking
	 *        one element per iterator.
	 */
	public static <E, R> void forEach(Function<? super E, R> function, Iterator<? extends E>... iterators) {
		Cursors.consume(new Mapper<E, R>(function, iterators));
	}

	/**
	 * Returns <code>true</code> if the boolean higher-order function returns
	 * {@link java.lang.Boolean#TRUE true} applied on each set of elements of
	 * the given input iterator array. The set contains one element per
	 * iterator and the function is only applied while all iterators are not
	 * empty.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param function the function to be invoked on each set of elements.
	 * @param iterators the iterators used to get a set of elements by taking
	 *        one element of each iterator.
	 * @return <code>true</code> if the function returns
	 *         {@link java.lang.Boolean#TRUE true} for each set of elements of
	 *         the given input iterator array, otherwise <code>false</code>.
	 */
	public static <E> boolean all(Function<? super E, Boolean> function, Iterator<? extends E>... iterators) {
		for (Iterator<Boolean> mapper = new Mapper<E, Boolean>(function, iterators); mapper.hasNext();)
			if (!mapper.next())
				return false;
		return true;
	}

	/**
	 * Returns <code>true</code> if the given predicate is <code>true</code>
	 * for each set of elements of the given input iterator array. The set
	 * contains one element per iterator and the predicate is only applied
	 * while all iterators are not empty.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param predicate the predicate to be invoked on each set of elements.
	 * @param iterators the iterators used to get a set of elements by taking one
	 *        element of each iterator.
	 * @return <code>true</code> if the predicate returns <code>true</code> for
	 *         each set of elements of the given input iterator array,
	 *         otherwise <code>false</code>.
	 */
	public static <E> boolean all(Predicate<? super E> predicate, Iterator<? extends E>... iterators) {
		for (Iterator<Boolean> mapper = new Mapper<E, Boolean>(new PredicateFunction<E>(predicate), iterators); mapper.hasNext();)
			if (!mapper.next())
				return false;
		return true;
	}

	/** 
	 * Returns <code>true</code> if the boolean higher-order function returns
	 * {@link java.lang.Boolean#TRUE true} applied on any set of elements of
	 * the given input iterator array. The set contains one element per
	 * iterator and the function is only applied while all iterators are not
	 * empty.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param function the function to be invoked on each set of elements.
	 * @param iterators the iterators used to get a set of elements by taking one
	 *        element of each iterator.
	 * @return <code>true</code> if the function returns
	 *         {@link java.lang.Boolean#TRUE true} for any set of elements of
	 *         the given input iterator array, otherwise <code>false</code>.
	 */
	public static <E> boolean any(Function<? super E, Boolean> function, Iterator<? extends E>... iterators) {
		for (Iterator<Boolean> mapper = new Mapper<E, Boolean>(function, iterators); mapper.hasNext();)
			if (mapper.next())
				return true;
		return false;
	}

	/**
	 * Returns <code>true</code> if the given predicate returns
	 * <code>true</code> for any set of elements of the given input iterator
	 * array. The set contains one element per iterator and the predicate is
	 * only applied while all iterators are not empty.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param predicate the predicate to be invoked on each set of elements.
	 * @param iterators the iterators used to get a set of elements by taking
	 *        one element of each iterator.
	 * @return <code>true</code> if the predicate returns <code>true</code> for
	 *         any set of elements of the given input iterator array, otherwise
	 *         <code>false</code>.
	 */
	public static <E> boolean any(Predicate<? super E> predicate, Iterator<? extends E>... iterators) {
		for (Iterator<Boolean> mapper = new Mapper<E, Boolean>(new PredicateFunction<E>(predicate), iterators); mapper.hasNext();)
			if (mapper.next())
				return true;
		return false;
	}

	/**
	 * Returns the last element of the given iterator.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @return the last element of the given iterator.
	 * @throws java.util.NoSuchElementException if the iterator does not
	 *         contain any elements.
	 */
	public static <E> E last(Iterator<E> iterator) throws NoSuchElementException {
		E result;
		do
			result = iterator.next();
		while (iterator.hasNext());
		return result;
	}

	/**
	 * Returns the <code>n</code>-th element of the given iterator.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param n the index of the element to return.
	 * @return the <code>n</code>-th element of the given iterator.
	 * @throws java.util.NoSuchElementException if the iterator does not
	 *         contain <code>n+1</code> elements.
	 */
	public static <E> E nth(Iterator<E> iterator, int n) throws NoSuchElementException {
		if (n < 0)
			throw new NoSuchElementException("the given index must be greater than or equal to 0");
		E result = null;
		for (int i = 0; i <= n; i++)
			result = iterator.next();
		return result;
	}

	/**
	 * Returns the first element of the given iterator.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @return the first element of the given iterator.
	 * @throws java.util.NoSuchElementException if the iterator does not
	 *         contain any elements.
	 */
	public static <E> E first(Iterator<E> iterator) throws NoSuchElementException {
		return nth(iterator, 0);
	}

	/**
	 * Counts the elements of a specified iterator.
	 *
	 * @param iterator the input iterator.
	 * @return the number of elements of the given iterator.
	 */
	public static int count(Iterator<?> iterator) {
		int size = 0;
		while (iterator.hasNext()) {
			iterator.next();
			size++;
		}
		return size;
	}

	/**
	 * Calls <code>next</code> on the iterator until the method
	 * <code>hasNext</code> returns <code>false</code>.
	 *
	 * @param iterator the input iterator.
	 */
	public static void consume(Iterator<?> iterator) {
		while (iterator.hasNext())
			iterator.next();
	}

	/**
	 * Calls <code>next</code> on the iterator until the method
	 * <code>hasNext</code> returns <code>false</code> and prints the elements
	 * of the iterator separated by a line feed to the standard output stream.
	 *
	 * @param iterator the input iterator.
	 */
	public static void println(Iterator<?> iterator) {
		forEach(Print.PRINTLN_INSTANCE, iterator);
	}

	/**
	 * Calls <code>next</code> on the iterator until the method
	 * <code>hasNext</code> returns <code>false</code> and prints the elements
	 * of the iterator to the standard output stream.
	 *
	 * @param iterator the input iterator.
	 */
	public static void print(Iterator<?> iterator) {
		forEach(Print.PRINT_INSTANCE, iterator);
	}

	/**
	 * Calls <code>next</code> on the iterator until the method
	 * <code>hasNext</code> returns <code>false</code> and prints the elements
	 * of the iterator separated by a line feed to the specified print stream.
	 *
	 * @param <E> the type of the elements returned by the iteration.
	 * @param iterator the input iterator.
	 * @param printStream the print stream the output is delegated to.
	 */
	public static <E> void println(Iterator<E> iterator, PrintStream printStream) {
		forEach(new Print<E>(printStream, true), iterator);
	}

	/**
	 * Calls <code>next</code> on the iterator until the method
	 * <code>hasNext</code> returns <code>false</code> and prints the elements
	 * of the iterator to the specified print stream.
	 *
	 * @param <E> the type of the elements returned by the iteration.
	 * @param iterator the input iterator.
	 * @param printStream the print stream the output is delegated to.
	 */
	public static <E> void print(Iterator<E> iterator, PrintStream printStream) {
		forEach(new Print<E>(printStream, false), iterator);
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the minimal
	 * value as its key and the list of objects with this minimal value in the
	 * same order as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be minimized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @param comparator the comparator that compares the values obtained by
	 *        <code>getFeature</code>.
	 * @return a map entry with the minimal value as its key and the list of
	 *         objects with this minimal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E, M> Entry<M, LinkedList<E>> minimize(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature, Comparator<? super M> comparator) {
		return last(new Minimator<E, M>(iterator, getFeature, comparator));
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the maximal
	 * value as its key and the list of objects with this maximal value in the
	 * same order as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be maximized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @param comparator the comparator that compares the values obtained by
	 *        <code>getFeature</code>.
	 * @return a map entry with the maximal value as its key and the list of
	 *         objects with this maximal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E, M> Entry<M, LinkedList<E>> maximize(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature, Comparator<? super M> comparator) {
		return last(new Minimator<E, M>(iterator, getFeature, new InverseComparator<M>(comparator)));
	}

	/**
	 * Returns the list of objects having the minimal value in the same order
	 * as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be minimized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @param comparator the comparator that compares the values obtained by
	 *        <code>getFeature</code>.
	 * @return a linked list of objects having the minimal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E, M> LinkedList<E> minima(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature, Comparator<? super M> comparator) {
		return (LinkedList<E>) minimize(iterator, getFeature, comparator).getValue();
	}

	/**
	 * Returns the list of objects having the maximal value in the same order
	 * as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be maximized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @param comparator the comparator that compares the values obtained by
	 *        <code>getFeature</code>.
	 * @return a linked list of objects having the maximal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E, M> LinkedList<E> maxima(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature, Comparator<? super M> comparator) {
		return (LinkedList<E>) maximize(iterator, getFeature, comparator).getValue();
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the minimal
	 * value as its key and the list of objects with this minimal value in the
	 * same order as provided by the iterator as its value. For being able to
	 * use a default
	 * {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be minimized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @return a map entry with the minimal value as its key and the list of
	 *         objects with this minimal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E, M extends Comparable<M>> Entry<M, LinkedList<E>> minimize(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature) {
		return last(new Minimator<E, M>(iterator, getFeature, new ComparableComparator<M>()));
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the maximal
	 * value as its key and the list of objects with this maximal value in the
	 * same order as provided by the iterator as its value. For being able to
	 * use a default
	 * {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be maximized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @return a map entry with the maximal value as its key and the list of
	 *         objects with this maximal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E, M extends Comparable<M>> Entry<M, LinkedList<E>> maximize(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature) {
		return last(new Minimator<E, M>(iterator, getFeature, new InverseComparator<M>(new ComparableComparator<M>())));
	}

	/**
	 * Returns the list of objects having the minimal value in the same order
	 * as provided by the iterator as its value. For being able to use a
	 * default {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be minimized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @return a linked list of objects having the minimal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E, M extends Comparable<M>> LinkedList<E> minima(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature) {
		return (LinkedList<E>) minimize(iterator, getFeature).getValue();
	}

	/**
	 * Returns the list of objects having the maximal value in the same order
	 * as provided by the iterator as its value. For being able to use a
	 * default {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param <M> the type of the mapped elements generated by the specified
	 *        mapping function.
	 * @param iterator the objects to be maximized.
	 * @param getFeature the function obtaining the value for each object used
	 *        to compare.
	 * @return a linked list of objects having the maximal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E, M extends Comparable<M>> LinkedList<E> maxima(Iterator<? extends E> iterator, Function<? super E, ? extends M> getFeature) {
		return (LinkedList<E>) maximize(iterator, getFeature).getValue();
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the minimal
	 * value as its key and the list of objects with this minimal value in the
	 * same order as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be minimized.
	 * @param comparator the comparator that compares the objects.
	 * @return a map entry with the minimal value as its key and the list of
	 *         objects with this minimal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E> Entry<E, LinkedList<E>> minimize(Iterator<? extends E> iterator, Comparator<? super E> comparator) {
		return last(new Minimator<E, E>(iterator, new Identity<E>(), comparator));
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the maximal
	 * value as its key and the list of objects with this maximal value in the
	 * same order as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be maximized.
	 * @param comparator the comparator that compares the objects.
	 * @return a map entry with the maximal value as its key and the list of
	 *         objects with this maximal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E> Entry<E, LinkedList<E>> maximize(Iterator<? extends E> iterator, Comparator<? super E> comparator) {
		return last(new Minimator<E, E>(iterator, new Identity<E>(), new InverseComparator<E>(comparator)));
	}

	/**
	 * Returns the list of objects having the minimal value in the same order
	 * as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be minimized.
	 * @param comparator the comparator that compares the objects.
	 * @return a linked list of objects having the minimal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E> LinkedList<E> minima(Iterator<? extends E> iterator, Comparator<? super E> comparator) {
		return (LinkedList<E>) minimize(iterator, comparator).getValue();
	}

	/**
	 * Returns the list of objects having the maximal value in the same order
	 * as provided by the iterator as its value.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be maximized.
	 * @param comparator the comparator that compares the objects.
	 * @return a linked list of objects having the maximal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E> LinkedList<E> maxima(Iterator<? extends E> iterator, Comparator<? super E> comparator) {
		return (LinkedList<E>) maximize(iterator, comparator).getValue();
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the minimal
	 * value as its key and the list of objects with this minimal value in the
	 * same order as provided by the iterator as its value. For being able to
	 * use a default
	 * {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be minimized.
	 * @return a map entry with the minimal value as its key and the list of
	 *         objects with this minimal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E extends Comparable<E>> Entry<E, LinkedList<E>> minimize(Iterator<? extends E> iterator) {
		return last(new Minimator<E, E>(iterator, new Identity<E>(), new ComparableComparator<E>()));
	}

	/**
	 * Returns a {@link java.util.Map.Entry Map.Entry} object with the maximal
	 * value as its key and the list of objects with this maximal value in the
	 * same order as provided by the iterator as its value. For being able to
	 * use a default
	 * {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be maximized.
	 * @return a map entry with the maximal value as its key and the list of
	 *         objects with this maximal value in the same order as provided by
	 *         the iterator as its value.
	 */
	public static <E extends Comparable<E>> Entry<E, LinkedList<E>> maximize(Iterator<? extends E> iterator) {
		return last(new Minimator<E, E>(iterator, new Identity<E>(), new InverseComparator<E>(new ComparableComparator<E>())));
	}

	/**
	 * Returns the list of objects having the minimal value in the same order
	 * as provided by the iterator as its value. For being able to use a
	 * default {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be minimized.
	 * @return a linked list of objects having the minimal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E extends Comparable<E>> LinkedList<E> minima(Iterator<? extends E> iterator) {
		return minimize(iterator).getValue();
	}

	/**
	 * Returns the list of objects having the maximal value in the same order
	 * as provided by the iterator as its value. For being able to use a
	 * default {@link xxl.core.comparators.ComparableComparator comparator} all
	 * elements of the iteration must implement the interface
	 * {@link java.lang.Comparable}.
	 *
	 * @param <E> the type of the elements returned by the iteration to be
	 *        aggregated.
	 * @param iterator the objects to be maximized.
	 * @return a linked list of objects having the maximal value in the same
	 *         order as provided by the iterator.
	 */
	public static <E extends Comparable<E>> LinkedList<E> maxima(Iterator<? extends E> iterator) {
		return maximize(iterator).getValue();
	}

	/**
	 * Removes all elements of the given iterator.
	 *
	 * @param iterator the input iterator.
	 */
	public static void removeAll(Iterator<?> iterator) {
		while (iterator.hasNext()) {
			iterator.next();
			iterator.remove();
		}
	}

	/**
	 * Removes the elements of the given iterator for that the specified
	 * predicate returns <code>true</code>.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param predicate a predicate that decides whether an element is removed
	 *        or not.
	 */
	public static <E> void removeAll(Iterator<E> iterator, Predicate<? super E> predicate) {
		while (iterator.hasNext())
			if (predicate.invoke(iterator.next()))
				iterator.remove();
	}

	/**
	 * Removes the first element of the given iterator for that the specified
	 * predicate returns <code>true</code>.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param predicate a predicate that decides whether an element is removed
	 *        or not.
	 * @return true if and only if an element was removed.
	 */
	public static <E> boolean removeFirst(Iterator<E> iterator, Predicate<? super E> predicate) {
		while (iterator.hasNext())
			if (predicate.invoke(iterator.next())) {
				iterator.remove();
				return true;
			}
		return false;
	}

	/**
	 * Updates all elements of a cursor.
	 *
	 * @param <E> the type of the elements returned by the given cursor.
	 * @param cursor the input cursor.
	 * @param objects an iterator with objects to be used for the
	 *        <code>update</code> operation.
	 */
	public static <E> void updateAll(Cursor<E> cursor, Iterator<? extends E> objects) {
		while (cursor.hasNext() && objects.hasNext()) {
			cursor.next();
			cursor.update(objects.next());
		}
	}

	/**
	 * Updates all elements of a cursor with the result of a given, unary
	 * function.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param cursor the input cursor.
	 * @param function the unary function used to compute the object for the
	 *        <code>update</code> operation.
	 */
	public static <E> void updateAll(Cursor<E> cursor, Function<E, ? extends E> function) {
		while (cursor.hasNext())
			cursor.update(function.invoke(cursor.next()));
	}

	/**
	 * Adds the elements of the given iteration to a specified
	 * {@link java.util.Collection collection}.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param collection the collection into which the elements of the given
	 *        iteration are to be stored.
	 * @return the specified collection containing the elements of the
	 *         iteration.
	 */
	public static <E> Collection<E> toCollection(Iterator<? extends E> iterator, Collection<E> collection) {
		while (iterator.hasNext())
			collection.add(iterator.next());
		return collection;
	}

	/**
	 * Adds the elements of the given iteration to a specified
	 * {@link java.util.List list}.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param list the list into which the elements of the given iteration are
	 *        to be stored.
	 * @return the specified list containing the elements of the iteration.
	 */
	public static <E> List<E> toList(Iterator<? extends E> iterator, List<E> list) {
		return (List<E>)toCollection(iterator, list);
	}

	/**
	 * Adds the elements of the given iteration to a 
	 * {@link java.util.LinkedList linked list} and returns it.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @return the linked list containing the elements of the iteration.
	 */
	public static <E> List<E> toList(Iterator<E> iterator) {
		return toList(iterator, new LinkedList<E>());
	}

	/**
	 * Converts an iterator to an object array whose length is equal to the
	 * number of the iterator's elements.
	 *
	 * @param iterator the input iterator.
	 * @return an object array containing the iterator's elements.
	 */
	public static Object[] toArray(Iterator<? extends Object> iterator) {
		return toFittingArray(iterator, new Object[0]);
	}

	/**
	 * Converts an iterator to an object array whose length is determined by
	 * the largest index. Stops as soon as one iterator is exhausted.
	 *
	 * @param iterator the input iterator.
	 * @param indices each index determines where to store the corresponding
	 *        iterator element.
	 * @return an object array of the iterator's elements following the order
	 *         defined by the iterator <tt>indices</tt>.
	 */
	public static Object[] toArray(Iterator<?> iterator, Iterator<Integer> indices) {
		return toFittingArray(iterator, new Object[0], indices);
	}

	/**
	 * Converts an iterator to an object array using the given one. The order
	 * is defined by the given indices iterator. Stops as soon as one iterator
	 * is exhausted. Throws an
	 * {@link java.lang.ArrayIndexOutOfBoundsException ArrayIndexOutOfBoundsException}
	 * if the given array is not long enough.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param array the array into which the elements of the given iteration
	 *        are to be stored.
	 * @param indices each index determines where to store the corresponding
	 *        iterator element.
	 * @return the object array containing the iterator's elements following
	 *         the order defined by the iterator <tt>indices</tt>.
	 * @throws java.lang.ArrayIndexOutOfBoundsException if the given array is
	 *         not long enough.
	 */
	public static <E> E[] toArray(Iterator<? extends E> iterator, E[] array, Iterator<Integer> indices) {
		while (iterator.hasNext() && indices.hasNext())
			array[indices.next()] = iterator.next();
		return array;
	}

	/**
	 * Converts an iterator to an object array using the given one. Throws an
	 * {@link java.lang.ArrayIndexOutOfBoundsException ArrayIndexOutOfBoundsException}
	 * if the number of the iterator's elements is larger than the length of
	 * the given array.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param array the array into which the elements of the given iteration
	 *        are to be stored.
	 * @return the object array containing the iterator's elements.
	 * @throws java.lang.ArrayIndexOutOfBoundsException if the given array is
	 *         not long enough.
	 */
	public static <E> E[] toArray(Iterator<? extends E> iterator, E[] array) {
		for (int index = 0; iterator.hasNext(); array[index++] = iterator.next());
		return array;
	}

	/**
	 * Converts an iterator to an object array whose length is equal to the
	 * number of the iterator's elements. When the iterator contains more
	 * elements than the array is able to store, a new array of the same
	 * component type is created.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param array the array into which the elements of the given iteration
	 *        are to be stored.
	 * @return an object array containing the iterator's elements.
	 */
	public static <E> E[] toFittingArray(Iterator<? extends E> iterator, E[] array) {
		ArrayResizer resizer = new ArrayResizer(0.5, 0);
		int size = array.length;
		for (int index = 0; iterator.hasNext(); array[index++] = iterator.next())
			if (index >= size)
				array = resizer.resize(array, size = index+1);
		return new ArrayResizer(1, 1).resize(array, size);
	}

	/**
	 * Converts an iterator to an object array whose length is determined by
	 * the largest index. Stops as soon as one iterator is exhausted. When the
	 * iterator contains more elements than the array is able to store, a new
	 * array of the same component type is created.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the input iterator.
	 * @param array the array into which the elements of the given iteration
	 *        are to be stored.
	 * @param indices each index determines where to store the corresponding
	 *        iterator element.
	 * @return an object array of the iterator's elements following the order
	 *         defined by the iterator <tt>indices</tt>.
	 */
	public static <E> E[] toFittingArray(Iterator<? extends E> iterator, E[] array, Iterator<Integer> indices) {
		ArrayResizer resizer = new ArrayResizer(0.5, 0);
		int size = array.length;
		for (int index; iterator.hasNext() && indices.hasNext(); array[index] = iterator.next())
			if ((index = indices.next()) >= size)
				array = resizer.resize(array, size = index+1);
		return new ArrayResizer(1, 1).resize(array, size);
	}

	/**
	 * Converts an iterator to a {@link java.util.Map map}. Partitions the
	 * iterator's elements according to the result of the unary function
	 * <code>getKey</code>. Each partition is the value of its corresponding
	 * map entry and is implemented as a collection.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param <K> the type of the keys used for mapping the elements of the
	 *        given iterator.
	 * @param iterator the input iterator.
	 * @param getKey unary function that determines the key for an iterator's
	 *        element.
	 * @param map the map used to store the partitions.
	 * @param newCollection creates a new collection to store the elements of a
	 *        new partition.
	 * @return the map implementing the partition of the iterator's elements.
	 */
	public static <E, K> Map<? super K, Collection<E>> toMap(Iterator<? extends E> iterator, Function<? super E, ? extends K> getKey, Map<? super K, Collection<E>> map, Function<?, ? extends Collection<E>> newCollection) {
		while (iterator.hasNext()) {
			E next = iterator.next();
			K key = getKey.invoke(next);
			Collection<E> collection = map.get(key);
			if (collection == null)
				map.put(key, collection = newCollection.invoke());
			collection.add(next);
		}
		return map;
	}

	/**
	 * Converts an iterator to a {@link java.util.Map map}. Partitions the
	 * iterator's elements according to the result of the unary function
	 * <code>getKey</code>. Each partition is the value of its corresponding
	 * map entry and is implemented as a
	 * {@link java.util.LinkedList linked list}.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param <K> the type of the keys used for mapping the elements of the
	 *        given iterator.
	 * @param iterator the input iterator.
	 * @param getKey unary function that determines the key for an iterator's
	 *        element.
	 * @param map the map used to store the partitions.
	 * @return the map implementing the partition of the iterator's elements.
	 */
	public static <E, K> Map<? super K, Collection<E>> toMap(Iterator<? extends E> iterator, Function<? super E, ? extends K> getKey, Map<? super K, Collection<E>> map) {
		return toMap(
			iterator,
			getKey,
			map,
			new AbstractFunction<Object, LinkedList<E>>() {
				@Override
				public LinkedList<E> invoke() {
					return new LinkedList<E>();
				}
			}
		);
	}

	/**
	 * Converts an iterator to a {@link java.util.HashMap hash map}. Partitions
	 * the iterator's elements according to the result of the unary function
	 * <code>getKey</code>. Each partition is the value part of its
	 * corresponding map entry and is implemented as a collection.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param <K> the type of the keys used for mapping the elements of the
	 *        given iterator.
	 * @param iterator the input iterator.
	 * @param getKey unary function that determines the key for an iterator's
	 *        element.
	 * @param newCollection creates a new collection to store the elements of a
	 *        new partition.
	 * @return the map implementing the partition of the iterator's elements.
	 */
	public static <E, K> Map<? super K, Collection<E>> toMap(Iterator<? extends E> iterator, Function<? super E, ? extends K> getKey, Function<?, ? extends Collection<E>> newCollection) {
		return toMap(iterator, getKey, new HashMap<K, Collection<E>>(), newCollection);
	}

	/**
	 * Converts an iterator to a {@link java.util.HashMap hash map}. Partitions
	 * the iterator's elements according to the result of the function
	 * <code>getKey</code>. Each partition is the value part of its
	 * corresponding map entry and is implemented as a
	 * {@link java.util.LinkedList linked list}.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param <K> the type of the keys used for mapping the elements of the
	 *        given iterator.
	 * @param iterator the input iterator.
	 * @param getKey unary function that determines the key for an iterator's
	 *        element.
	 * @return the map implementing the partition of the iterator's elements.
	 */
	public static <E, K> Map<? super K, Collection<E>> toMap(Iterator<? extends E> iterator, Function<? super E, ? extends K> getKey) {
		return toMap(iterator, getKey, new HashMap<K, Collection<E>>());
	}

	/**
	 * Wraps the given {@link java.util.Iterator iterator} to a
	 * {@link xxl.core.cursors.Cursor cursor}. If the given iterator is already
	 * a cursor, it is returned unchanged. The current implementation is as
	 * follows:
	 * <code><pre>
	 *   return iterator instanceof Cursor ?
	 *       (Cursor&lt;E&gt;)iterator :
	 *       new IteratorCursor&lt;E&gt;(iterator);
	 * </pre></code>
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param iterator the iterator to be wrapped.
	 * @return a cursor wrapping the given iterator.
	 * @see xxl.core.cursors.wrappers.IteratorCursor
	 */
	public static <E> Cursor<E> wrap(Iterator<E> iterator) {
		return iterator instanceof Cursor ?
			(Cursor<E>)iterator :
			new IteratorCursor<E>(iterator);
	}

	/**
	 * Wraps a given {@link java.util.Iterator iterator} (keep in mind that a
	 * {@link xxl.core.cursors.Cursor cursor} is also an iterator) to a
	 * {@link xxl.core.cursors.MetaDataCursor meta data cursor}. In that way an
	 * ordinary iterator can be enriched by meta data that is provided via the
	 * {@link xxl.core.cursors.MetaDataCursor#getMetaData() getMetaData}
	 * method.
	 *
	 * @param <E> the type of the elements returned by the given iterator.
	 * @param <M> the type of the given meta data object.
	 * @param iterator the iterator to be enriched by meta data information.
	 * @param metaData an object containing the meta data information.
	 * @return a meta data cursor enriching the given iterator by some meta
	 *         data information.
	 */
	public static <E, M> MetaDataCursor<E, M> wrapToMetaDataCursor(Iterator<E> iterator, final M metaData) {
		return new AbstractMetaDataCursor<E, M>(iterator) {
			@Override
			public M getMetaData() {
				return metaData;
			}
		};
	}
	
	/**
	 * Extends {@link FileInputCursor} with simple reset method; 
	 * the reset method closes internal data stream from the file, and reopens it.    
	 * @param converter
	 * @param file
	 * @return
	 */
	public static <E> Cursor<? extends E> resetableFileInputCursor(Converter<? extends E> converter, final  File file, final int bufferSize){
		return new FileInputCursor<E>(converter, file, bufferSize){
			@Override
			public boolean supportsReset() {
				return true;
			}
			@Override
			public void reset() throws UnsupportedOperationException {
				super.reset();
				try {
					try {
						input.close(); // close previous 
					}catch (IOException e) {}
						input = new DataInputStream(
							new BufferedInputStream(
								new FileInputStream(file),
								bufferSize
							)
					);
				}
				catch (IOException ie) {
					throw new WrappingRuntimeException(ie);
				}
				
			}
		};
	}
	
	/**
	 * Extends {@link FileInputCursor} with simple reset method; 
	 * the reset method closes internal data stream from the file, and reopens it.    
	 * @param converter
	 * @param file
	 * @return
	 */
	public static <E> Cursor<? extends E> resetableFileInputCursor(Converter<? extends E> converter, final  File file){
		return  resetableFileInputCursor(converter, file, 4096);
	}
}
