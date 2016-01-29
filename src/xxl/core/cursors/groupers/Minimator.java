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

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import xxl.core.collections.MapEntry;
import xxl.core.cursors.mappers.Aggregator;
import xxl.core.functions.Function;
import xxl.core.math.functions.AggregationFunction;

/**
 * The minimator is an {@link xxl.core.cursors.mappers.Aggregator aggregator}
 * that computes the minimal element for a given input iteration (lazy
 * evaluation). The minimator uses
 * {@link xxl.core.collections.MapEntry map entries} to arrange the elements
 * delivered by the input cursor. So the <code>next</code> and
 * <code>peek</code> method return instances of the class
 * <code>MapEntry</code>. The minimator computes the next minimal element
 * concerning the underlying iteration by lazy evaluation, i.e., the global
 * minimum is definitely detected if the whole input iteration has been
 * consumed. A map entry consists of a key and a value. The values are
 * implemented as {@link java.util.LinkedList linked lists} and the key is
 * determined by a user defined function, named <code>mapping</code>. For each
 * input element a key is computed applying the mapping function and the
 * element is inserted in the linked list refering to this key. Furthermore a
 * comparator can be specified, which is used to compare two keys of map
 * entries. So a minimator minimizes the given input iteration according to the
 * keys of map entries that were determined when a user defined, unary mapping
 * function is applied on an input element. A minimator may also detect the
 * maximum of the input iteration using an
 * {@link xxl.core.comparators.InverseComparator inverse} comparator instead
 * (see example (2)).
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *     Minimator&lt;Integer, Integer&gt; minimator = new Minimator&lt;Integer, Integer&gt;(
 *         new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(100), 10),
 *         new Identity&lt;Integer&gt;(),
 *         new ComparableComparator&lt;Integer&gt;()
 *     );
 * 
 *     minimator.open();
 * 
 *     while (minimator.hasNext())
 *         System.out.print(minimator.next().getKey() + "; ");
 *     System.out.flush();
 * 
 *     minimator.close();
 * </pre></code>
 * This minimator returns the minimal key of 10 randomly distributed integers
 * with range [0,...,100[. The <code>next</code> method returns a map entry,
 * namely the first aggregation value. The key's, the integer elements are
 * mapped to, are delivered by the identity function. And the keys are compared
 * by a default {@link xxl.core.comparators.ComparableComparator comparator}
 * which assumes that these keys implement the
 * {@link java.lang.Comparable comparable} interface. Since lazy evaluation is
 * performed the absolute minimum is definitively determined, when all elements
 * of the input iteration have been consumed.</p>
 * 
 * <p><b>Example usage (2):</b>
 * <code><pre>
 *     // Object[0] &rarr; name, Object[1] --> age
 * 
 *     Object[][] persons = new Object[][] {
 *         new Object[] {new String("Tobias"), new Integer(23)},
 *         new Object[] {new String("Juergen"), new Integer(23)},
 *         new Object[] {new String("Martin"), new Integer(26)},
 *         new Object[] {new String("Bjoern"), new Integer(28)},
 *         new Object[] {new String("Jens"), new Integer(27)},
 *         new Object[] {new String("Bernhard"), new Integer(35)},
 *         new Object[] {new String("Jochen"), new Integer(29)},
 *     };
 *     Minimator&lt;Object[], Integer&gt; minimator = new Minimator&lt;Object[], Integer&gt;(
 *         new ArrayCursor&lt;Object[]&gt;(persons),
 *         new Function&lt;Object[], Integer&gt;() {
 *             public Integer invoke(Object[] person) {
 *                 return (Integer)person[1];
 *             }
 *         },
 *         new InverseComparator&lt;Integer&gt;(new ComparableComparator&lt;Integer&gt;())
 *     );
 * 
 *     minimator.open();
 * 
 *     Iterator&lt;Object[]&gt; results;
 *     while (minimator.hasNext()) {
 *         results = minimator.next().getValue().listIterator(0);
 *         while (results.hasNext())
 *             System.out.print("Name: " + results.next()[0] + "; ");
 *         System.out.flush();
 *     }
 * 
 *     minimator.close();
 * </pre></code>
 * This minimator uses an array cursor which contains object arrays and each
 * object array defines a person in the following way:
 * <ul>
 *     <li>
 *         object[0] contains the name of a person
 *     </li>
 *     <li>
 *         object[1] contains the age of a person
 *     </li>
 * </ul>
 * The defined function maps an input object, a person, to a certain key,
 * namely the person's age. These keys are compared with an inverse comparator,
 * i.e., the person with the maximal age will be returned at last by the
 * minimator using lazy evalutation. Since a minimator returns map entries, the
 * next element returned by a minimator has to be casted to a map entry. The
 * key of the returned map entry is the person's age. With the intention to
 * receive the value of the returned map entry, namely a linked list of objects
 * sharing the same key, this value has to be casted to a linked list and for
 * further use this list is converted to an iterator by calling its method
 * <code>listIterator</code>. The elements of the result-iteration are object
 * arrays (persons), so the name of each person belonging to the key (age) is
 * printed to the output stream.</p>
 *
 * @param <E> the type of the elements returned by the iteration to be
 *        aggregated.
 * @param <M> the type of the mapped elements generated by the specified
 *        mapping function.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.mappers.Aggregator
 * @see xxl.core.collections.MapEntry
 */
public class Minimator<E, M> extends Aggregator<E, MapEntry<M, LinkedList<E>>> {

	/**
	 * The function used to map an input element to a certain key.
	 */
	protected Function<? super E, ? extends M> mapping;

	/**
	 * Creates a new minimator. If an iterator is given to this constructor it
	 * is wrapped to a cursor.
	 *
	 * @param iterator the input iteration providing the data to be minimized.
	 * @param mapping the function used to map an input element to a certain
	 *        key.
	 * @param comparator the comparator used to determine whether the current
	 *        object is smaller than the current minima.
	 */
	public Minimator(Iterator<? extends E> iterator, final Function<? super E, ? extends M> mapping, final Comparator<? super M> comparator) {
		// calling Aggregator(iterator, function)
		super(
			iterator,
			new AggregationFunction<E, MapEntry<M, LinkedList<E>>>() {
				public MapEntry<M, LinkedList<E>> invoke(MapEntry<M, LinkedList<E>> mapEntry, E next) {
					if (mapEntry == null) {
						// initializing aggregate by creating a new MapEntry
						LinkedList<E> objects = new LinkedList<E>();
						objects.add(next);
						return new MapEntry<M, LinkedList<E>>(mapping.invoke(next), objects);
					}
					else {
						// using mapping function to determine the value of the current object
						M value = mapping.invoke(next);
						// comparing current object with the MapEntry's key
						int comparison = comparator.compare(value, mapEntry.getKey());
						// MapEntry's value is a List
						LinkedList<E> objects = mapEntry.getValue();
	
						if (comparison <= 0) {
							if (comparison < 0) {
								objects.clear();
								mapEntry.setKey(value);
							}
							objects.add(next); // if comparison <= 0 object is added to the List
						}
						return mapEntry;
					}
				}
			}
		);
		this.mapping = mapping;
	}
}
