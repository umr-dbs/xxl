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

package xxl.core.cursors.sorters;

import java.util.Iterator;
import java.util.Random;

import xxl.core.collections.MapEntry;
import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.FeatureComparator;
import xxl.core.cursors.SecureDecoratorCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.sources.DiscreteRandomNumber;
import xxl.core.functions.AbstractFunction;
import xxl.core.util.random.JavaDiscreteRandomWrapper;

/**
 * A shuffle-cursor is a
 * {@link xxl.core.cursors.SecureDecoratorCursor decorator-cursor}, that permutes the
 * input iteration's elements randomly. This cursor is semilazy. Do not use it
 * with potentially infinite streams. This operator internally builds
 * {@link xxl.core.collections.MapEntry map entries} consisting of an element of
 * the input iteration (key) and a randomly distributed integer value (value)
 * delivered by the given iteration over
 * {@link xxl.core.cursors.sources.DiscreteRandomNumber discrete} random numbers.
 * These map entries are sorted according to their values and the result is a
 * shuffled output of the input iteration.
 * 
 * <p><b>Example usage :</b>
 * <pre>
 *     ShuffleCursor shuffler = new ShuffleCursor(
 *         new Enumerator(11),
 *         new DiscreteRandomNumber(
 *             new JavaDiscreteRandomWrapper(new Random())
 *         )
 *     );
 * 
 *     shuffler.open();
 * 
 *     while (shuffler.hasNext())
 *         System.out.println(shuffler.next());
 * 
 *     shuffler.close();
 * </pre>
 * This example demonstrates the shuffle-cursor functionality. The shuffle-cursor
 * returns the same set of elements of the input enumerator, but in a random
 * order. The random numbers are taken from an iteration over discrete random
 * number that has to be passed at construction time.
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class ShuffleCursor<E> extends SecureDecoratorCursor<E> {

	/**
	 * Creates a new shuffle-cursor.
	 *
	 * @param iterator the input iteration containing the elements to be
	 *        shuffled.
	 * @param randomCursor a cursor that delivers an infinite stream of randomly
	 *        distributed integer objects (discrete random numbers).
	 */
	public ShuffleCursor(Iterator<? extends E> iterator, final DiscreteRandomNumber randomCursor) {
		// Elemente verpacken
		super(
			new Mapper<MapEntry<E, Integer>, E>(
				new AbstractFunction<MapEntry<E, Integer>, E>() {
					@Override
					public E invoke(MapEntry<E, Integer> mapEntry) {
						return mapEntry.getKey();
					}
				},
				new MergeSorter<MapEntry<E, Integer>>(
					new Mapper<E, MapEntry<E, Integer>>(
						new AbstractFunction<E, MapEntry<E, Integer>>() {
							@Override
							public MapEntry<E, Integer> invoke(E element) {
								return new MapEntry<E, Integer>(element, randomCursor.next());
							}
						},
						iterator
					),
					new FeatureComparator<Integer, MapEntry<E, Integer>>(
						ComparableComparator.INTEGER_COMPARATOR,
						new AbstractFunction<MapEntry<E, Integer>, Integer>() {
							@Override
							public Integer invoke(MapEntry<E, Integer> mapEntry) {
								return mapEntry.getValue();
							}
						}
					),
					12,
					12*4096,
					4*4096
				)
			)
		);
	}

	/**
	 * Creates a new ShuffleCursor with the random number generator provided by
	 * Java.
	 *
	 * @param iterator the input iteration containing the elements to be
	 *        shuffled.
	 */
	public ShuffleCursor(Iterator<? extends E> iterator) {
		this(iterator, new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(new Random())));
	}
}
