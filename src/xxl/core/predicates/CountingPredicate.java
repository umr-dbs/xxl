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

package xxl.core.predicates;

import java.util.List;

/**
 * A DecoratorPredicate that provides additional information about
 * calls, hits, and misses to its inner predicate. 
 * 
 * @param <P> the type of the predicate's parameters.
 */
public class CountingPredicate<P> extends DecoratorPredicate<P> {
	
	/**
	 * Counter for predicate calls.
	 */
	protected long calls;
	
	/**
	 * Counter for predicate hits.
	 */
	protected long hits;
	
	/**
	 * Decorates the given predicate by counters.
	 * 
	 * @param predicate the predicate whose calls should be counted.
	 */
	public CountingPredicate(Predicate<? super P> predicate) {
		super(predicate);
		resetCounters();
	}
	
	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>Increments the call and hit counters appropriately.</p>
	 *
	 * @param arguments the arguments to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(List<? extends P> arguments) {
		calls++;
		boolean ret = predicate.invoke(arguments);
		if (ret) hits++;					
		return ret;
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>Increments the call and hit counters appropriately.</p>
	 *
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke() {
		calls++;
		boolean ret = predicate.invoke();
		if (ret) hits++;					
		return ret;
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>Increments the call and hit counters appropriately.</p>
	 *
	 * @param argument the argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument) {
		calls++;
		boolean ret = predicate.invoke(argument);
		if (ret) hits++;					
		return ret;
	}

	/**
	 * Returns the result of the predicate as a primitive boolean value.
	 * 
	 * <p>Increments the call and hit counters appropriately.</p>
	 *
	 * @param argument0 the first argument to the predicate.
	 * @param argument1 the second argument to the predicate.
	 * @return the result of the predicate as a primitive boolean value.
	 */
	@Override
	public boolean invoke(P argument0, P argument1) {
		calls++;
		boolean ret = predicate.invoke(argument0, argument1);
		if (ret) hits++;					
		return ret;
	}

	/**
	 * Returns the number of predicate calls.
	 * 
	 * @return number of predicate calls.
	 */
	public long getNoOfCalls() {
		return calls;
	}
	
	/**
	 * Returns the number of hits, that means, the number of calls
	 * the predicate returned <tt>true</tt>.
	 * 
	 * @return number of hits.
	 */
	public long getNoOfHits() {
		return hits;
	}
	
	/**
	 * Returns the number of misses, that means, the number of calls
	 * the predicate returned <tt>false</tt>.
	 * 
	 * @return number of misses.
	 */
	public long getNoOfMisses() {
		return calls-hits;
	}
	
	/**
	 * Resets all counters.
	 */
	public void resetCounters() {
		this.calls = 0;
		this.hits = 0;
	}
	
}
