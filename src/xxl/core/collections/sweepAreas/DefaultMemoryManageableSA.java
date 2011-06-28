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

package xxl.core.collections.sweepAreas;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.filters.Remover;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;

/**
 * Default implementation for a memory manageable SweepArea.
 * If this SweepArea reaches its limit of main memory,
 * it randomly drops 20% of its content (load shedding).
 */
public class DefaultMemoryManageableSA<I,E> extends MemoryManageableSA<I,E> {

    public static <I,E> Function<SweepArea<I,E>,DefaultMemoryManageableSA<I,E>> getMemoryManageableSA(final SweepArea<I,E> sweepArea, final int objectSize, final int preferredMemSize) {
		return new AbstractFunction<SweepArea<I,E>,DefaultMemoryManageableSA<I,E>>() {		
			public DefaultMemoryManageableSA<I,E> invoke() {
				return new DefaultMemoryManageableSA<I,E>(sweepArea, objectSize, preferredMemSize);
			}
		};
	}
    
    public static <I,E> DefaultMemoryManageableSA wrapSweepArea(SweepArea<I,E> sweepArea, final int objectSize, final int preferredMemSize) {
    	return new DefaultMemoryManageableSA<I,E>(sweepArea, objectSize, preferredMemSize);
    }
		
	/**
	 * This inner class provides a predicate for the remove filter for the
	 * load shedder.
	 * The method <code>invoke</code> returns <code>true</code> for every
	 * x-th method call on the average, where x is the ratio of
	 * <code>p/q</code>.
	 */
	public static class EveryQth<E> extends AbstractPredicate<E> {
	// Fuer q=1 verhaelt sich EveryQth(p,q) identisch zu xxl.predicates.EveryNth(p).
	// Ansonsten ist EveryQth die Verallgemeinerung von EveryNth auf rationale Zahlen p/q.

		/**
		 * The counter how often the method <code>invoke</code> was called.
		 */
		protected long count = 0;

		/**
		 * The counter how often the method <code>invoke</code> returned the
		 * value <code>true</code>.
		 */
		protected long applied = 0;

		/**
		 * The numerator of the fraction <code>p/q</code>
		 */
		protected long p;

		/**
		 * The denominator of the fraction <code>p/q</code>
		 */
		protected long q;

		/**
		 * Constructs a new EveryQth object with <code>p/q</code> as its ratio.
		 * 
		 * @param p The numerator of the fraction <code>p/q</code>
		 * @param q The denominator of the fraction <code>p/q</code>
		 */
		public EveryQth (int p, int q) {
			if (q==0 || p/q<1)
				throw new IllegalArgumentException("the fraction p/q must be greater or equal to 1");
			this.p = p;
			this.q = q;
		}

		/**
		 * Returns <code>true</code> or <code>false</code> according to
		 * the number of previous method calls.
		 * On the average the value <code>true</code> will be returned
		 * <code>q/p</code> times of the method calls,
		 * and accordingly the value <code>false</code> will be returned
		 * <code>(1 - q/p)</code> times of the method calls.
		 * The returned <code>true</code>-values are uniformly distributed
		 * among the several method calls.
		 * 
		 * @param argument This argument is irrelevant, it may be <I>null</I>.
		 */
		public boolean invoke (E argument) {
			count++;
			if (p*applied<q*count) {
				applied++;
				return true;
			}
			return false;
		}

	} // END of inner class EveryQth


	/**
	 * Creates a memory manageable SweepArea by decorating the underlying
	 * SweepArea.
	 * 
	 * @param sweepArea The underlying SweepArea
	 * @param objectSize The size of the objects in this SweepArea (in bytes).
	 * @param memSize The preferred amount of memory (in bytes).
	 */
	public DefaultMemoryManageableSA(SweepArea<I,E> sweepArea, int objectSize, int memSize) {
		super(sweepArea, objectSize, memSize);
	}

	/**
	 * This method handles an overflow.
	 * If there is an overflow, this method causes a dropping of tuples,
	 * until this SweepArea uses only 80% of its assigned memory amount.
	 * The dropping of tuples occurs uniformly distributed among the tuples
	 * in the SweepArea according to the order the
	 * <code>iterator()</code>-method supplies the tuples.
	 */
	public void handleOverflow() {
		if (getCurrentMemUsage() <= assignedMemSize)
			return;
		int realNoOfObjects = size();
		int maxAllowedNoOfObjects = 4*assignedMemSize/(5*objectSize);
		int overage = realNoOfObjects - maxAllowedNoOfObjects;
		//System.out.println("handleOverflow("+realNoOfObjects+", "+overage+")");
		Cursors.consume(new Remover<E>(new Filter<E>(iterator(), new EveryQth<E>(realNoOfObjects, overage))));
	}
	
	public int getObjectSize() {
		return objectSize;
	}

}
