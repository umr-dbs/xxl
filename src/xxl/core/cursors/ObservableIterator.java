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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Observable;

import xxl.core.functions.Function;
import xxl.core.functions.Identity;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.Predicates;

/**
 * This class provides a wrapper for {@link java.util.Iterator iterators} with
 * the intention to {@link java.util.Observer observe} them. The notification
 * can be controlled using a {@link xxl.core.predicates.Predicate predicate}
 * that decides whether the observers should be notified about the currently
 * delivered object by the wrapped iterator. Moreover a
 * {@link xxl.core.functions.Function function} could be used to map the
 * currently delivered object to informations the oberservers should get.
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   ObservableIterator&lt;Integer&gt; iterator = new ObservableIterator&lt;Integer&gt;(
 *       new new DiscreteRandomNumber(new JavaDiscreteRandomWrapper(10), 100),
 *       Functions.aggregateUnaryFunction(
 *           new CountAll()
 *       )
 *    );
 *    Observer observer = new Observer() {
 *        public void update(Observable observable, Object object) {
 *            System.out.println("getting " + object + " from " + observable);
 *        }
 *    };
 *    iterator.addObserver(observer);
 *    
 *    while (iterator.hasNext())
 *        System.out.println("next=" + iterator.next());
 * </pre></code>
 * This example illustrates how an iteration over random integers gets
 * observed, whereas the observer only sees the results of an unary aggregation
 * function, namely a count of all elements delivered by the wrapped cursor.
 * </p>
 *
 * @param <E> the type of the elements returned by this iteration.
 * @see java.util.Iterator
 * @see java.util.Observable
 * @see java.util.Observer
 */
public class ObservableIterator<E> extends Observable implements Iterator<E> {

	/**
	 * The iterator to observe.
	 */
	protected Iterator<? extends E> iterator;

	/**
	 * A predicate that determines whether the observers should be notified or
	 * not.
	 */
	protected Predicate<? super E> predicate;

	/**
	 * A function that maps the object received by the wrapped iterator to the
	 * object handed out to the observers.
	 */
	protected Function<? super E, ?> function;

	/**
	 * The last object that has been delivered by this iterator.
	 */
	protected E next;

	/**
	 * Constructs an observable iterator.
	 *
	 * @param iterator the iterator to observe.
	 * @param function function that maps the object received by the wrapped
	 *        iterator to the object handed out to the observers.
	 * @param predicate predicate that determines whether the observers should
	 *        be notified about the currently received object or not.
	 */
	public ObservableIterator(Iterator<? extends E> iterator, Function<? super E, ?> function, Predicate<? super E> predicate) {
		this.iterator = iterator;
		this.predicate = predicate;
		this.function = function;
	}

	/**
	 * Constructs an observable iterator. By using this constructor every seen
	 * object of this iterator will be passed to the registerd observers
	 * meaning the constant TRUE will be used as predicate for determining if
	 * the observers should be notified.
	 *
	 * @param iterator the iterator to observe.
	 * @param function function that maps the object received by the wrapped
	 *        iterator to the object handed out to the observers.
	 */
	public ObservableIterator(Iterator<? extends E> iterator, Function<? super E, ?> function) {
		this(iterator, function, Predicates.TRUE);
	}

	/**
	 * Constructs an observable iterator. By using this constructor every
	 * object itself will be passed to the registered observers meaning the
	 * identity function will be used to map the seen objects.
	 *
	 * @param iterator the iterator to observe.
	 * @param predicate predicate that determines whether the observers should
	 *        be notified about the currently received object or not.
	 */
	public ObservableIterator(Iterator<? extends E> iterator, Predicate<? super E> predicate) {
		this(iterator, Identity.DEFAULT_INSTANCE, predicate);
	}

	/**
	 * Constructs an observable iterator. Every seen object will be passed to
	 * the registered observers.
	 *
	 * @param iterator the iterator to observe.
	 */
	public ObservableIterator(Iterator<? extends E> iterator) {
		this(iterator, Identity.DEFAULT_INSTANCE, Predicates.TRUE);
	}

	/**
	 * Returns true if the wrapped iteration has more elements.
	 *
	 * @return true if the iterator has more elements
	 */
	public boolean hasNext() {
		return iterator.hasNext();
	}

	/**
	 * By calling the method <code>next</code> the next object of the wrapped
	 * iterator will be returned. All registerd observers will be notified if
	 * and only if the given predicate returns <code>true</code> for this
	 * object. Furthermore all observers receive only a transformed object
	 * given by the transformation function.
	 *
	 * @throws NoSuchElementException if the wrapped iterator has no further
	 *         objects.
	 * @return the next object of the wrapped iterator.
	 */
	public E next() throws NoSuchElementException {
		next = iterator.next();
		if (predicate.invoke(next)) {
			setChanged();
			notifyObservers(function.invoke(next));
		}
		return next;
	}

	/**
	 * By calling remove the last seen object of the wrapped iterator is
	 * removed (optional operation).
	 * 
	 * <p><b>Note</b> that the last delivered object will be removed,
	 * <b>not</b> the last object notified to the observers.</p>
	 *
	 * @throws UnsupportedOperationException if remove is not supported by the
	 *         wrapped iterator.
	 */
	public void remove() throws UnsupportedOperationException {
		iterator.remove();
		if (predicate.invoke(next)) {
			setChanged();
			notifyObservers();
		}
	}
}
