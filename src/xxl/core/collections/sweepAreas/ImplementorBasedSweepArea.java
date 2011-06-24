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

import static xxl.core.util.metaData.CostModelMetaDataIdentifiers.COST_MEASUREMENT;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.filters.Remover;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.CountingPredicate;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Not;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.Predicates;
import xxl.core.predicates.RightBind;
import xxl.core.util.memory.MemoryMonitorable;
import xxl.core.util.metaData.ExternalTriggeredPeriodicMetaData;

/**
 * A SweepArea is a highly dynamic datastructure with flexible
 * insertion, retrieval and reorganization capabilities. It is 
 * utilized to remain the state of an operation. <br>
 * This abstract implementation relies on the design pattern <i>Bridge</i>
 * with the intention to "decouple an abstraction from its 
 * implementation so that the two can vary independently". 
 * For further information see: "Gamma et al.: <i>DesignPatterns.
 * Elements of Reusable Object-Oriented Software.</i> Addision 
 * Wesley 1998."
 * 
 * @see SweepAreaImplementor
 * @see xxl.core.cursors.joins.SortMergeJoin
 * @see xxl.core.cursors.joins.MultiWaySortMergeJoin
 * @see xxl.core.predicates.Predicate
 * @see xxl.core.predicates.RightBind
 * @see java.util.Iterator
 */
public class ImplementorBasedSweepArea<E> extends AbstractSweepArea<E,E> {
	
	public class RemovingIterator implements Iterator<E> {

		protected Iterator<E> iterator;
		protected E lastNext;
		
		public RemovingIterator(Iterator<E> iterator) {
			this.iterator = iterator;
		}
		
		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public E next() {
			return (lastNext = iterator.next());
		}

		@Override
		public void remove() {
			outerRemove(lastNext);
			iterator.remove();
		}
		
	}
	
	
	/**
	 * The underlying implementor.
	 */
	protected final SweepAreaImplementor<E> impl;
	
	/**
	 * The ID of this SweepArea.
	 */
	protected final int ID;
	
	/**
	 * This flag determines if this SweepArea
	 * can reorganize itself. To trigger this event, the
	 * <tt>ID</tt> passed to the reorganize call
	 * has to be identical with the internal ID of this 
	 * SweepArea.
	 */
	protected final boolean selfReorganize;
	
	/**
	 * Binary predicates used to query this SweepArea and 
	 * its underlying SweepAreaImplementor, respectively.
	 * To offer a retrieval depending on the ID passed
	 * to the query calls, a SweepArea requires such an 
	 * array of predicates.
	 */
	protected final Predicate<? super E>[] queryPredicates; 
	
	/**
	 * Binary predicates determining if an element of a 
	 * SweepArea and its underlying SweepAreaImplementor, 
	 * respectively, can be removed. 
	 * To offer a removal depending on the ID passed
	 * to the reorganization or expiration calls, a SweepArea 
	 * requires such an array of predicates. <br>
	 */
	protected final Predicate<? super E>[] removePredicates;
	
	/**
	 * The default implementation removes all expired
	 * elements from a SweepArea by sequentially iterating
	 * over all elements. The iteration is performed by
	 * a {@link xxl.core.cursors.filters.Filter Filter} applying
	 * an unary selection predicate. For this purpose,
	 * the constructors of this class wrap the array
	 * of binary remove-predicates to an array of unary
	 * remove-predicates <code>removeRightBinds</code>.
	 * The element passed to the reorganization and
	 * expiration, respectively, is implicitly set as
	 * right argument. 
	 */
	protected final RightBind<E>[] removeRightBinds;
	
	
	public class ImplementorBasedSAMetaDataManagement extends AbstractSAMetaDataManagement implements ExternalTriggeredPeriodicMetaData {
					
		public ImplementorBasedSAMetaDataManagement() {
			super();
		}
		
		protected Set<Object> metaDataFromImplementor;
		
		protected boolean addMetaDataFromImplementor(final Object metaDataIdentifier) {
			if (impl.getMetaDataManagement().include(metaDataIdentifier)) {
				metaData.add(metaDataIdentifier, 
					new AbstractFunction<Object,Object>() {			
						@Override
						public Object invoke() {
							synchronized(metaDataManagement) {
								return ((Function<?,Object>)impl.getMetaData().get(metaDataIdentifier)).invoke();
							}
						}
					}
				);
				if (metaDataFromImplementor==null)
					metaDataFromImplementor = new HashSet<Object>();
				metaDataFromImplementor.add(metaDataIdentifier);
				return true;
			}
			return false;
		}
		
		protected boolean removeMetaDataFromImplementor(final Object metaDataIdentifier) {
			if (metaDataFromImplementor!=null && metaDataFromImplementor.remove(metaDataIdentifier)) {
				impl.getMetaDataManagement().exclude(metaDataIdentifier);
				if (metaDataFromImplementor.size()==0)
					metaDataFromImplementor = null;
				return true;
			}
			return false;
		}
		
		public static final String QUERY_PREDICATE_DECORATION = "QUERY_PREDICATE_DECORATION";
		protected volatile boolean decorateQueryPredicate = false;
		protected long[][] queryPredicateCounters;

		public static final String REMOVE_PREDICATE_DECORATION = "REMOVE_PREDICATE_DECORATION";
		protected volatile boolean decorateRemovePredicate = false;
		protected long[][] removePredicateCounters;
		
		public static final String SINGLE_QUERY_PREDICATE_COSTS = "SINGLE_QUERY_PREDICATE_COSTS";
		protected double singleQueryPredicateCosts[];
		public static final String QUERY_COSTS = "QUERY_COSTS";
		protected volatile boolean measureQueryCosts = false;
		protected double queryCosts = 0.0;
		
		public static final String SINGLE_REMOVE_PREDICATE_COSTS = "SINGLE_REMOVE_PREDICATE_COSTS";
		protected double singleRemovePredicateCosts[];
		public static final String REMOVE_COSTS = "REMOVE_COSTS";		
		protected volatile boolean measureRemoveCosts = false;
		protected double removeCosts = 0.0;
		
		protected volatile boolean measureCosts = false;
		protected double costs = 0.0;
		
		
		public boolean needsPeriodicUpdate(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(QUERY_PREDICATE_DECORATION) ||
				metaDataIdentifier.equals(QUERY_COSTS) ||
				metaDataIdentifier.equals(REMOVE_PREDICATE_DECORATION) ||
				metaDataIdentifier.equals(REMOVE_COSTS) ||
				metaDataIdentifier.equals(COST_MEASUREMENT)) {
					return true;
			}
			return false;
		}
		
		public void updatePeriodicMetaData(long period) {
			
			((ExternalTriggeredPeriodicMetaData)impl.getMetaDataManagement()).updatePeriodicMetaData(period);
			
			CountingPredicate<E> cp;
			if (decorateQueryPredicate) {
				queryPredicateCounters = new long [queryPredicates.length][2];
				for (int i = 0; i < queryPredicates.length; i++) {
					cp = (CountingPredicate<E>)queryPredicates[i];
					queryPredicateCounters[i][0] += cp.getNoOfHits();
					queryPredicateCounters[i][1] += cp.getNoOfCalls();
					cp.resetCounters();
				}
			}
			if (decorateRemovePredicate) {
				removePredicateCounters = new long [removePredicates.length][2];
				for (int i = 0; i < removePredicates.length; i++) {
					cp = (CountingPredicate<E>)removePredicates[i];
					removePredicateCounters[i][0] += cp.getNoOfHits();
					removePredicateCounters[i][1] += cp.getNoOfCalls();
					cp.resetCounters();
				}
			}
			if (measureQueryCosts) {				
				queryCosts = 0.0;
				if (singleQueryPredicateCosts!=null)
					for (int i=0; i<queryPredicates.length; i++)
						queryCosts += (queryPredicateCounters[i][1] * singleQueryPredicateCosts[i]) / period;
			}
			if (measureRemoveCosts) {
				removeCosts = 0.0;
				if (singleRemovePredicateCosts != null)
					for (int i=0; i<queryPredicates.length; i++)
						removeCosts += (removePredicateCounters[i][1] * singleRemovePredicateCosts[i]) / period;				
			}
			if (measureCosts) {
				costs = queryCosts + removeCosts;
				costs += ((Function<?,Double>)impl.getMetaDataManagement().getMetaData().get(COST_MEASUREMENT)).invoke();
			}
		}
		
		public void setSingleQueryPredicateCosts(double [] costs) {
			singleQueryPredicateCosts = costs;
		}
		
		public void setSingleRemovePredicateCosts(double [] costs) {
			singleRemovePredicateCosts = costs;
		}
		
		public void setCostFactors(double [] queryPredicateCosts, double[] removePredicateCosts) {
			setSingleQueryPredicateCosts(queryPredicateCosts);
			setSingleRemovePredicateCosts(removePredicateCosts);
		}
		
		@Override
		protected boolean addMetaData(Object metaDataIdentifier) {
			if (super.addMetaData(metaDataIdentifier)) 
				return true;
			
			if (metaDataIdentifier.equals(QUERY_PREDICATE_DECORATION)) {
				// decorate query predicates
				for (int i = 0; i < queryPredicates.length; i++)
					queryPredicates[i] = new CountingPredicate<E>(queryPredicates[i]);
				impl.initialize(ID, queryPredicates, equals);
				decorateQueryPredicate = true;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,long[][]>() {					
					@Override
					public long[][] invoke() {
						return queryPredicateCounters;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(SINGLE_QUERY_PREDICATE_COSTS)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,double[]>() {					
					@Override
					public double[] invoke() {
						return singleQueryPredicateCosts;
					}
				});				
			}
			if (metaDataIdentifier.equals(QUERY_COSTS)) {
				include(QUERY_PREDICATE_DECORATION);
				queryCosts = 0.0;
				measureQueryCosts = true;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {					
					@Override
					public Double invoke() {
						return queryCosts;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(REMOVE_PREDICATE_DECORATION)) {
				// decorate remove predicates
				for (int i = 0; i < removePredicates.length; i++) {
					removePredicates[i] = new CountingPredicate<E>(removePredicates[i]);
					removeRightBinds[i] = new RightBind<E>(removePredicates[i], null);
				}
				decorateRemovePredicate = true;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,long[][]>() {					
					@Override
					public long[][] invoke() {
						return removePredicateCounters;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(SINGLE_REMOVE_PREDICATE_COSTS)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,double[]>() {					
					@Override
					public double[] invoke() {
						return singleRemovePredicateCosts;
					}
				});				
			}
			if (metaDataIdentifier.equals(REMOVE_COSTS)) {
				include(REMOVE_PREDICATE_DECORATION);
				removeCosts = 0.0;
				measureRemoveCosts = true;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {					
					@Override
					public Double invoke() {
						return removeCosts;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(COST_MEASUREMENT)) {
				include(QUERY_COSTS);
				include(REMOVE_COSTS);
				boolean res = impl.getMetaDataManagement().include(COST_MEASUREMENT);
				costs = 0.0;
				measureCosts = true;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {					
					@Override
					public Double invoke() {
						return costs;
					}
				});
				return res;
			}
			return addMetaDataFromImplementor(metaDataIdentifier);
		}

		@Override
		protected boolean removeMetaData(Object metaDataIdentifier) {			
			if (super.removeMetaData(metaDataIdentifier)) 
				return true;
					
			if (metaDataIdentifier.equals(QUERY_PREDICATE_DECORATION)) {
				// undecorate query predicates
				for (int i = 0; i < queryPredicates.length; i++)
					queryPredicates[i] = ((CountingPredicate<E>)queryPredicates[i]).getDecoree();
				impl.initialize(ID, queryPredicates, equals);
				decorateQueryPredicate = false;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(QUERY_COSTS)) {
				exclude(QUERY_PREDICATE_DECORATION);
				measureQueryCosts = false;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(REMOVE_PREDICATE_DECORATION)) {
				// undecorate remove predicates
				for (int i = 0; i < removePredicates.length; i++) {
					removePredicates[i] = ((CountingPredicate<E>)removePredicates[i]).getDecoree();
					removeRightBinds[i] = new RightBind<E>(removePredicates[i], null);
				}
				decorateRemovePredicate = false;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(REMOVE_COSTS)) {
				exclude(REMOVE_PREDICATE_DECORATION);
				measureRemoveCosts = false;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(COST_MEASUREMENT)) {
				exclude(QUERY_COSTS);
				exclude(REMOVE_COSTS);
				impl.getMetaDataManagement().exclude(COST_MEASUREMENT);
				measureCosts = false;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(SINGLE_QUERY_PREDICATE_COSTS) ||
				metaDataIdentifier.equals(SINGLE_REMOVE_PREDICATE_COSTS)) {
					metaData.remove(metaDataIdentifier);
					return true;
			}
			return removeMetaDataFromImplementor(metaDataIdentifier);
		}
	}
	
	
	/**
	 * Constructs a new SweepArea.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicates An array of binary query-predicates used to probe this SweepArea. 
	 * 		  Depending on the ID passed to the query-call, different query-predicates can be chosen
	 * 		  for retrieval.
	 * @param removePredicates An array of binary remove-predicates utilized during the reorganization.
	 * 		  With the help of this predicates, the elements of a SweepArea are probed for expiration.
	 * 		  In analogy to the query-predicates, the predicate actually applied to this SweepArea 
	 * 		  depends on the ID passed to the expiration and reorganization call, respectively.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 * @param objectSize The object size of the elements in the sweeparea. 
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E>[] removePredicates, Predicate<? super E> equals, int objectSize) {
		super(equals, objectSize);
		this.impl = impl;
		this.ID = ID;
		this.selfReorganize = selfReorganize;
		this.queryPredicates = queryPredicates;
		this.removePredicates = removePredicates;
		this.removeRightBinds = new RightBind[removePredicates.length];
		for (int i = 0; i < removePredicates.length; i++)
			this.removeRightBinds[i] = new RightBind<E>(removePredicates[i], null);
		this.impl.initialize(ID, this.queryPredicates, this.equals);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E>[] removePredicates, int objectSize) {
		this(impl, ID, selfReorganize, queryPredicates, removePredicates, new Equal<E>(), objectSize);
	}
	
	/**
	 * Constructs a new SweepArea. Sets objectSize to unknown. 
	 * The object size will be determined with reflection during the first insert() call.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicates An array of binary query-predicates used to probe this SweepArea. 
	 * 		  Depending on the ID passed to the query-call, different query-predicates can be chosen
	 * 		  for retrieval.
	 * @param removePredicates An array of binary remove-predicates utilized during the reorganization.
	 * 		  With the help of this predicates, the elements of a SweepArea are probed for expiration.
	 * 		  In analogy to the query-predicates, the predicate actually applied to this SweepArea 
	 * 		  depends on the ID passed to the expiration and reorganization call, respectively.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E>[] removePredicates, Predicate<? super E> equals) {
		this(impl, ID, selfReorganize, queryPredicates, removePredicates, equals, MemoryMonitorable.SIZE_UNKNOWN);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E>[] removePredicates) {
		this(impl, ID, selfReorganize, queryPredicates, removePredicates, new Equal<E>(), MemoryMonitorable.SIZE_UNKNOWN);
	}

	/**
	 * Constructs a new SweepArea and uses the predicate <code>removePredicate</code> as
	 * default during reorganization.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicates An array of binary query-predicates used to probe this SweepArea. 
	 * 		  Depending on the ID passed to the query-call, different query-predicates can be chosen
	 * 		  for retrieval.
	 * @param removePredicate Default predicate for reorganization, which is applied independently 
	 * 		  from the ID passed to reorganization and expiration calls.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 * @param objectSize The object size of the elements in the sweeparea.
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E> removePredicate, Predicate<? super E> equals, int objectSize) {
		super(equals, objectSize);
		this.impl = impl;
		this.ID = ID;
		this.selfReorganize = selfReorganize;
		this.queryPredicates = queryPredicates;
		this.removePredicates = new Predicate[queryPredicates.length];
		this.removeRightBinds = new RightBind[queryPredicates.length];
		for (int i = 0; i < removePredicates.length; i++) {
			this.removePredicates[i] = removePredicate;
			this.removeRightBinds[i] = new RightBind<E>(removePredicate, null);
		}
		this.impl.initialize(ID, this.queryPredicates, this.equals);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E> removePredicate, int objectSize) {
		this(impl, ID, selfReorganize, queryPredicates, removePredicate, new Equal<E>(), objectSize);
	}
	
	/**
	 * Constructs a new SweepArea and uses the predicate <code>removePredicate</code> as
	 * default during reorganization.  Sets objectSize to unknown. 
	 * The object size will be determined with reflection during the first insert() call.
	 * 
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicates An array of binary query-predicates used to probe this SweepArea. 
	 * 		  Depending on the ID passed to the query-call, different query-predicates can be chosen
	 * 		  for retrieval.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 * @param removePredicate Default predicate for reorganization, which is applied independently 
	 * 		  from the ID passed to reorganization and expiration calls.
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E> removePredicate, Predicate<? super E> equals) {
		this(impl, ID, selfReorganize, queryPredicates, removePredicate, equals, MemoryMonitorable.SIZE_UNKNOWN);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates, Predicate<? super E> removePredicate) {
		this(impl, ID, selfReorganize, queryPredicates, removePredicate, new Equal<E>(), MemoryMonitorable.SIZE_UNKNOWN);
	}

	
	/**
	 * Constructs a new SweepArea, but performs no reorganization. 
	 * This is achieved by internally reorganizing with 
	 * <code>Predicates.FALSE</code>.  Sets objectSize to unknown. 
	 * The object size will be determined with reflection during the first insert() call.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicates An array of binary query-predicates used to probe this SweepArea. 
	 * 		  Depending on the ID passed to the query-call, different query-predicates can be chosen
	 * 		  for retrieval.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, Predicate<? super E> equals, boolean selfReorganize, Predicate<? super E>[] queryPredicates) {
		this(impl, ID, selfReorganize, queryPredicates, Predicates.FALSE, equals);
	}	

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E>[] queryPredicates) {
		this(impl, ID, selfReorganize, queryPredicates, Predicates.FALSE, new Equal<E>());
	}	

	/**
	 * Constructs a new SweepArea with a generally applicable query- and reorganization predicate.
	 * Sets objectSize to unknown. The object size will be determined with 
	 * reflection during the first insert() call. 
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicate A generally applicable binary query-predicate used to probe this SweepArea.
	 * 		  Hence, this predicate is applied independently from the ID passed to query calls.
	 * @param removePredicate A generally applicable binary remove-predicates utilized during the reorganization.
	 * 		  With the help of this predicates, the elements of a SweepArea are probed for expiration.
	 * 		  This predicate is applied independently from the ID passed to reorganization and expiration calls.
	 * @param dim The dimensionality of this SweepArea, i.e., the number of possible inputs or in other words
	 * 		  the number of different IDs that can be passed to method calls of this SweepArea.
	 * @param equals The predicate used to determine equality of objects within the SweepArea. 
	 * @param objectSize The object size of the elements in the sweeparea.
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E> queryPredicate, Predicate<? super E> removePredicate, Predicate<? super E> equals, int dim, int objectSize) {
		super(equals, objectSize);
		this.impl = impl;
		this.ID = ID;
		this.selfReorganize = selfReorganize;
		this.queryPredicates = new Predicate[dim];
		Arrays.fill(queryPredicates, queryPredicate);
		this.removePredicates = new Predicate[dim];
		this.removeRightBinds = new RightBind[dim];
		for (int i = 0; i < removePredicates.length; i++) {
			this.removePredicates[i] = removePredicate;
			this.removeRightBinds[i] = new RightBind<E>(removePredicate, null);
		}
		this.impl.initialize(ID, queryPredicates, equals);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E> queryPredicate, Predicate<? super E> removePredicate, int dim, int objectSize) {
		this(impl, ID, selfReorganize, queryPredicate, removePredicate, new Equal<E>(), dim, objectSize);
	}
	
	/**
	 * Constructs a new SweepArea with a generally applicable query- and reorganization predicate.
	 * Sets objectSize to unknown. The object size will be determined with 
	 * reflection during the first insert() call. 
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicate A generally applicable binary query-predicate used to probe this SweepArea.
	 * 		  Hence, this predicate is applied independently from the ID passed to query calls.
	 * @param removePredicate A generally applicable binary remove-predicates utilized during the reorganization.
	 * 		  With the help of this predicates, the elements of a SweepArea are probed for expiration.
	 * 		  This predicate is applied independently from the ID passed to reorganization and expiration calls.
	 * @param equals The predicate used to determine equality of objects within the SweepArea.
	 * @param dim The dimensionality of this SweepArea, i.e., the number of possible inputs or in other words
	 * 		  the number of different IDs that can be passed to method calls of this SweepArea. 
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E> queryPredicate, Predicate<? super E> removePredicate, Predicate<? super E> equals, int dim) {
		this(impl, ID, selfReorganize, queryPredicate, removePredicate, equals, dim, MemoryMonitorable.SIZE_UNKNOWN);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E> queryPredicate, Predicate<? super E> removePredicate, int dim) {
		this(impl, ID, selfReorganize, queryPredicate, removePredicate, new Equal<E>(), dim, MemoryMonitorable.SIZE_UNKNOWN);
	}

	
	/**
	 * Constructs a new SweepArea with a generally applicable query- and reorganization predicate.
	 * This SweepArea performs no reorganization. This is achieved by internally reorganizing with 
	 * <code>Predicates.FALSE</code>.  Sets objectSize to unknown. 
	 * The object size will be determined with reflection during the first insert() call.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param selfReorganize A flag to determine if this SweepArea can reorganize itself.
	 * @param queryPredicate A generally applicable binary query-predicate used to probe this SweepArea.
	 * 		  Hence, this predicate is applied independently from the ID passed to query calls.
	 * @param dim The dimensionality of this SweepArea, i.e., the number of possible inputs or in other words
	 * 		  the number of different IDs that can be passed to method calls of this SweepArea. 
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E> queryPredicate, int dim, Predicate<? super E> equals) {
		this(impl, ID, selfReorganize, queryPredicate, Predicates.FALSE, equals, dim);
	}	

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, boolean selfReorganize, Predicate<? super E> queryPredicate, int dim) {
		this(impl, ID, selfReorganize, queryPredicate, Predicates.FALSE, new Equal<E>(), dim);
	}	

	/**
	 * Constructs a new SweepArea with a generally applicable query- and reorganization predicate.
	 * This SweepArea uses <code>Equal.DEFAULT_INSTANCE</code> for querying and 
	 * <code>new Not(Equal.DEFAULT_INSTANCE)</code> for reorganization. A self-reorganization
	 * is permitted.  Sets objectSize to unknown. The object size will be determined 
	 * with reflection during the first insert() call.
	 * 
	 * @param impl The underlying implementor.
	 * @param ID The ID of this SweepArea.
	 * @param dim The dimensionality of this SweepArea, i.e., the number of possible inputs or in other words
	 * 		  the number of different IDs that can be passed to method calls of this SweepArea. 
	 */
	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, Predicate<? super E> equals, int ID, int dim) {
		this(impl, ID, true, equals, new Not<E>(equals), equals, dim);
	}

	public ImplementorBasedSweepArea(SweepAreaImplementor<E> impl, int ID, int dim) {
		this(impl, ID, true, new Equal<E>(), new Not<E>(new Equal<E>()), new Equal<E>(), dim);
	}

	
	/**
	 * Inserts the given element into this SweepArea. The default implementation
	 * simply forwards this call to the underlying implementor. Thus,
	 * it calls <code>impl.insert(o)</code>.
	 * 
	 * @param o The object to be inserted.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the insertion due to the passed argument.
	 */
	public void insert(E o) throws IllegalArgumentException {
		super.insert(o);
		impl.insert(o);
	}
	
	/**
	 * Clears this SweepArea by clearing the underlying
	 * implementor. Calls <code>impl.clear()</code>. <br>
	 * This method should remove all elements of a 
	 * SweepArea, but holds its allocated resources.
	 */
	public void clear() {
		impl.clear();
	}
	
	/**
	 * Closes this SweepArea by closing the underlying
	 * implementor. Calls <code>impl.close()</code>.<br>
	 * This method should release all allocated resources
	 * of a SweepArea.
	 */
	public void close() {
		impl.close();
	}
	
	/**
	 * Returns the size of this SweepArea by
	 * determining the size of the underlying implementor.
	 * Returns <code>impl.size()</code>.
	 * 
	 * @return The size of this SweepArea.
	 */
	public int size() {
		return impl.size();
	}
	
	/**
	 * Returns an iterator over this SweepArea by
	 * delivering an iterator over the underlying implementor.
	 * Returns <code>impl.iterator()</code>.
	 * 
	 * @return An iterator over this SweepArea.
	 * @throws UnsupportedOperationException if this SweepArea is not able
	 *         to provide an iteration over its elements.
	 */
	public Iterator<E> iterator() throws UnsupportedOperationException {
		return new RemovingIterator(impl.iterator());
	}
	
	/**
	 * Queries this SweepArea with the help of the
	 * specified query object <code>o</code>. Returns all 
	 * matching elements as an iterator. The default implementation 
	 * of this method directly passes the call to the underlying implementor,
	 * i.e., it returns <code>impl.query(o, ID)</code>. <br>
	 * <i>Note:</i>
	 * This iterator should not be used to remove any elements of a
	 * SweepArea!
	 * 
	 * @param o The query object. This object is typically probed against
	 * 		the elements contained in this SweepArea.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	@Override
	public Iterator<E> query(E o, int ID) throws IllegalArgumentException {
		return new RemovingIterator(impl.query(o, ID));
	}

	/**
	 * Queries this SweepArea with the help of the
	 * specified query objects <code>os</code>. Returns all matching elements
	 * as an iterator. This version of query additionally allows to use partially
	 * filled arrays and specifies how many entries of such a partially
	 * filled array are valid.<br> 
	 * The default implementation of this method
	 * directly passes the call to the underlying implementor and
	 * returns <code>impl.query(os, IDs, valid)</code>.
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this SweepArea.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @param valid Determines how many entries at the beginning of
	 *        <tt>os</tt> and <tt>IDs</tt> are valid and therefore taken into account.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	@Override
	public Iterator<E> query(E [] os, int [] IDs, int valid) throws IllegalArgumentException {
		return new RemovingIterator(impl.query(os, IDs, valid));
	}

	/**
	 * Queries this SweepArea with the help of the
	 * specified query objects <code>os</code>. Returns all matching elements
	 * as an iterator. <br>	The default implementation of this method
	 * directly passes the call to the underlying implementor.
	 * Returns <code>impl.query(os, IDs)</code>.
	 * 
	 * @param os The query objects. These objects are typically probed against
	 * 		the elements contained in this SweepArea.
	 * @param IDs IDs determining from which input the query objects come from.
	 * @return All matching elements of this SweepArea are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 */
	@Override
	public Iterator<E> query(E [] os, int [] IDs) throws IllegalArgumentException {
		return new RemovingIterator(impl.query(os, IDs));
	}

	/**
	 * Determines which elements in this SweepArea expire with respect to the object
	 * <tt>currentStatus</tt> and an <tt>ID</tt>. The latter is commonly used
	 * to differ by which input this reorganization step is initiated.<br>
	 * If no elements qualify for removal, an empty cursor is returned and all 
	 * elements are remained. <br>
	 * In order to remove the expired elements, either the returned iterator has to 
	 * support and execute the remove operation for each expired element during traversal
	 * or the {@link #reorganize(Object, int)} has to be overwritten to perform 
	 * the final removal. <p>
	 * The default implementation in this class performs a sequential scan:
	 * <code><pre>
	 * 	new Remover(new Filter(iterator(), removeRightBinds[ID].setRight(currentStatus)));
	 * </code></pre>
	 * Hence, specialized SweepAreas should overwrite this method to gain a more
	 * efficient reorganization.
	 * 
	 * @param currentStatus The object containing the necessary information
	 * 		to detect expired elements.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return an iteration over the elements which expire with respect to the
	 *         object <tt>currentStatus</tt> and an <tt>ID</tt>.
	 * @throws UnsupportedOperationException An UnsupportedOperationException is thrown, if
	 * 		this method is not supported by this SweepArea.
	 * @throws IllegalStateException Throws an IllegalStateException if
	 * 		   this method is called at an invalid state.
	 */
	@Override
	public Iterator<E> expire (E currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException {
		if (ID == this.ID && !selfReorganize)
			return new EmptyCursor<E>();
		return new Remover<E>(new Filter<E>(iterator(), removeRightBinds[ID].setRight(currentStatus)));
	}
	
	/**
	 * In contrast to the method {@link #expire(Object, int)}, this method removes
	 * all expired elements from a SweepArea without returning them. 
	 * The default implementation removes all elements returned by a call to 
	 * {@link #expire(Object, int)}.<BR>
	 * In order to perform a more efficient removal, this method should
	 * be overwritten, e.g., by implementing a bulk deletion. 
	 * 
	 * @param currentStatus The object containing the necessary information
	 * 		  to perform the reorganization step.
	 * @param ID An ID determining from which input this reorganization step
	 * 		   is triggered.
	 * @throws UnsupportedOperationException An UnsupportedOperationException is thrown, if
	 * 		   is method is not supported by this SweepArea.
	 * @throws IllegalStateException Throws an IllegalStateException if
	 * 		   this method is called at an invalid state.
	 */
	public void reorganize(E currentStatus, int ID) throws UnsupportedOperationException, IllegalStateException {
		Cursors.consume(expire(currentStatus, ID));	
	}
	
	protected void outerRemove(E object) {
	}
	
	protected void remove(E object) {
		impl.remove(object);		
	}
	
	protected boolean supportsRemove() {
		return true;
	}
	
	/**
	 * Returns a reference to the underlying implementor.
	 * 
	 * @return The underlying implementor.
	 * @throws UnsupportedOperationException If this operation is not supported.
	 */
	public SweepAreaImplementor<? super E> getImplementor() throws UnsupportedOperationException {
		return impl;
	}
	
	/**
	 * Returns the ID of this SweepArea.
	 * 
	 * @return The ID of this SweepArea.
	 */
	public int getID() {
		return ID;
	}
	
	/**
	 * Returns the array of binary query-predicates.
	 * 
	 * @return The Array of query-predicates.
	 * @throws UnsupportedOperationException If this operation is not supported.
	 */
	public Predicate<? super E>[] getQueryPredicates() throws UnsupportedOperationException {
		return this.queryPredicates;
	}
	
	/**
	 * Returns the array of binary remove-predicates.
	 * 
	 * @return The Array of remove-predicates
	 * @throws UnsupportedOperationException If this operation is not supported.
	 */
	public Predicate<? super E>[] getRemovePredicates() throws UnsupportedOperationException {
		return this.removePredicates;
	}
	
	/**
	 * Returns <tt>true</tt> if this SweepArea permits
	 * self-reorganization. In this case, the
	 * <tt>ID</tt> passed to the reorganize call
	 * is the same as the internal ID of this SweepArea.
	 * Otherwise <tt>false</tt> is returned.
	 * 
	 * @return <tt>True</tt> if this SweepArea permits
	 * 		self-reorganization, otherwise <tt>false</tt>.
	 */
	public boolean allowsSelfReorganize() {
		return selfReorganize;
	}

	public void createMetaDataManagement() {
		if (metaDataManagement != null)
			throw new IllegalStateException("An instance of MetaDataManagement already exists.");
		metaDataManagement = new ImplementorBasedSAMetaDataManagement();
	}
	
}
