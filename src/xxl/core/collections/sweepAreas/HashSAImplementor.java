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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.sources.EmptyCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.functions.Functions;

/**
 * A hash-based implementation of the interface
 * {@link SweepAreaImplementor}. The hash table
 * references {@link java.util.List lists} modelling
 * the buckets. 
 * 
 * @see SweepAreaImplementor
 * @see xxl.core.functions.Function
 * @see java.util.List
 */
public class HashSAImplementor<E> extends AbstractSAImplementor<E> {
	
	protected volatile boolean countHashFunctionCalls = false;
	protected volatile boolean countHashBucketOperations = false;

	public class HashSAImplementorMetaDataManagement extends AbstractSAImplementorMetaDataManagement {

		public final static String HASH_FUNCTION_CALLS = "HASH_FUNCTION_CALLS";
		public final static String HASH_BUCKET_OPERATIONS = "HASH_BUCKET_OPERATIONS";
		
		protected long[] hashFunctionCallCounter;
		protected long[] hashFunctionCalls;
		
		public final static String SINGLE_HASH_COSTS = "SINGLE_HASH_COSTS";
		protected double singleHashCosts[];
		public static final String HASH_COSTS = "HASH_COSTS";
		protected volatile boolean measureHashCosts = false;
		protected double hashCosts = 0.0;

		protected long hashBucketOperationsCounter;
		protected long hashBucketOperations;

		public final static String SINGLE_OPERATION_COSTS = "SINGLE_OPERATION_COSTS";
		protected double singleOperationCosts;
		public static final String OPERATIONS_COSTS = "OPERATIONS_COSTS";
		protected volatile boolean measureOperationsCosts = false;
		protected double operationsCosts = 0.0;

		protected volatile boolean measureCosts = false;
		protected double costs = 0.0;
		
		@Override
		public void updatePeriodicMetaData(long period) {
			if (countHashFunctionCalls) {
				hashFunctionCalls = hashFunctionCallCounter;
				hashFunctionCallCounter = new long[hashFunctions.length];
			}
			if (countHashBucketOperations) {
				hashBucketOperations = hashBucketOperationsCounter;
				hashBucketOperationsCounter = 0;
			}
			if (measureHashCosts) {
				hashCosts = 0.0;
				if (singleHashCosts!=null)
					for (int i=0; i<hashFunctions.length; i++)
						hashCosts += (hashFunctionCalls[i] * singleHashCosts[i]) / period; 
			}
			if (measureOperationsCosts) {
				operationsCosts = (hashBucketOperations * singleOperationCosts) / period;
			}
			if (measureCosts) {
				costs = hashCosts + operationsCosts;
			}
		}
		
		public boolean needsPeriodicUpdate(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(HASH_FUNCTION_CALLS) ||
				metaDataIdentifier.equals(HASH_BUCKET_OPERATIONS) ||
				metaDataIdentifier.equals(HASH_COSTS) ||
				metaDataIdentifier.equals(OPERATIONS_COSTS) ||
				metaDataIdentifier.equals(COST_MEASUREMENT)) {
					return true;
			}
			return false;
		}

		
		@Override
		protected boolean addMetaData(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(HASH_FUNCTION_CALLS)) {
				countHashFunctionCalls = true;
				hashFunctionCallCounter = new long[hashFunctions.length];
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,long[]>() {
					@Override
					public long[] invoke() {
						return hashFunctionCalls;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(HASH_BUCKET_OPERATIONS)) {
				countHashBucketOperations = true;
				hashBucketOperationsCounter = 0;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Long>() {
					@Override
					public Long invoke() {
						return hashBucketOperations;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(SINGLE_HASH_COSTS)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,double[]>() {
					@Override
					public double[] invoke() {
						return singleHashCosts;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(HASH_COSTS)) {
				include(HASH_FUNCTION_CALLS);
				measureHashCosts = true;
				hashCosts = 0.0;		
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {
					@Override
					public Double invoke() {
						return hashCosts;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(SINGLE_OPERATION_COSTS)) {
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {
					@Override
					public Double invoke() {
						return singleOperationCosts;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(OPERATIONS_COSTS)) {
				include(HASH_BUCKET_OPERATIONS);
				measureOperationsCosts = true;
				operationsCosts = 0.0;	
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {
					@Override
					public Double invoke() {
						return operationsCosts;
					}
				});
				return true;
			}
			if (metaDataIdentifier.equals(COST_MEASUREMENT)) {
				include(HASH_COSTS);
				include(OPERATIONS_COSTS);
				costs = 0.0;
				measureCosts = true;
				metaData.add(metaDataIdentifier, new AbstractFunction<Object,Double>() {
					@Override
					public Double invoke() {
						return costs;
					}
				});
				return true;
			}
			if (super.addMetaData(metaDataIdentifier))
				return true;
			return false;
		}

		public void setSingleHashCosts(double [] costs) {
			singleHashCosts = costs;
		}
		
		public void setSingleOperationCosts(double costs) {
			singleOperationCosts = costs;
		}
		
		public void setCostFactors(double [] hashCosts, double operationCosts) {
			setSingleHashCosts(hashCosts);
			setSingleOperationCosts(operationCosts);
		}
		
		@Override
		protected boolean removeMetaData(Object metaDataIdentifier) {
			if (metaDataIdentifier.equals(HASH_FUNCTION_CALLS)) {
				countHashFunctionCalls = false;
				hashFunctionCalls = null;
				hashFunctionCallCounter = null;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(HASH_BUCKET_OPERATIONS)) {
				countHashBucketOperations = false;
				hashBucketOperations = 0;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(HASH_COSTS)) {
				exclude(HASH_FUNCTION_CALLS);
				measureHashCosts = false;
				hashCosts = 0.0;	
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(OPERATIONS_COSTS)) {
				exclude(HASH_BUCKET_OPERATIONS);
				measureOperationsCosts = false;
				operationsCosts = 0.0;	
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(COST_MEASUREMENT)) {
				exclude(HASH_COSTS);
				exclude(OPERATIONS_COSTS);
				costs = 0.0;
				measureCosts = false;
				metaData.remove(metaDataIdentifier);
				return true;
			}
			if (metaDataIdentifier.equals(SINGLE_HASH_COSTS) ||
				metaDataIdentifier.equals(SINGLE_OPERATION_COSTS)) {
					metaData.remove(metaDataIdentifier);
					return true;
			}
			return false;
		}
	}
	
	/**
	 * The hash table.
	 */
	protected HashMap<Integer, List<E>> hashTable;

	/**
	 * An array of hash functions offering insertion, 
	 * retrieval and reorganization depending on the 
	 * ID passed to the method calls. Each hash function
	 * is a map from Object &rarr; Integer. 
	 */
	protected Function<? super E,Integer>[] hashFunctions;

	/**
	 * A parameterless function that delivers
	 * an empty {@link java.util.List List} representing 
	 * a new bucket.
	 */
	protected Function<?,List<E>> newList;

	protected int size;
		
	/**
	 * Constructs a new HashListSAImplementor.
	 * 
	 * @param hashFunctions The array of hash functions. Each is a 
	 * 		  map from Object &rarr; Integer. 
	 * @param newList A parameterless function that returns a new 
	 * 		  list at each invocation. These lists represent the
	 * 		  buckets of the hash table.
	 * 
	 */
	public HashSAImplementor(Function<? super E,Integer>[] hashFunctions, Function<?,List<E>> newList) {
		this.hashFunctions = hashFunctions;
		this.hashTable = new HashMap<Integer, List<E>>();
		this.newList = newList;
		this.size = 0;
	}
	
	/**
	 * Constructs a new HashListSAImplementor.
	 * 
	 * @param hashFunctions The array of hash functions. Each is a 
	 * 		  map from Object &rarr; Integer.
	 */
	public HashSAImplementor(Function<? super E,Integer>[] hashFunctions) {
		this.hashFunctions = hashFunctions;
		this.hashTable = new HashMap<Integer, List<E>>();
		this.newList = new AbstractFunction<Object,List<E>>() {
			public List<E> invoke() {
				return new LinkedList<E>();
			}
		};
		this.size = 0;
	}
		
	/**
	 * Constructs a new HashListSAImplementor which uses
	 * the specified hash function independently from the ID 
	 * passed to query, expire and reorganize calls.
	 * 
	 * @param hashFunction The hash function, which is a 
	 * 		  map from Object &rarr; Integer. 
	 * @param newList A parameterless function that returns a new 
	 * 		  list at each invocation. These lists represent the
	 * 		  buckets of the hash table.
	 * @param dim The number of possible inputs or in other words,
	 * 		  the number of different IDs that can be passed to 
	 * 		  method calls of this implementor. 
	 */
	public HashSAImplementor(Function<? super E,Integer> hashFunction, Function<?,List<E>> newList, int dim) {
		this.hashTable = new HashMap<Integer, List<E>>();
		this.newList = newList;
		this.hashFunctions = new Function[dim];
		Arrays.fill(this.hashFunctions, hashFunction);
		this.size = 0;
	}
		
	/**
	 * Constructs a new HashListSAImplementor which uses
	 * the specified hash function independently from the ID 
	 * passed to query, expire and reorganize calls.
	 * The function creating the buckets delivers
	 * instances of the class {@link java.util.LinkedList LinkedList}.
	 *  
	 * @param hashFunction The hash function, which is a 
	 * 		  map from Object &rarr; Integer. 
	 * @param dim The number of possible inputs or in other words,
	 * 		  the number of different IDs that can be passed to 
	 * 		  method calls of this implementor. 
	 */
	public HashSAImplementor(Function<? super E,Integer> hashFunction, int dim) {
		this(hashFunction, 
			new AbstractFunction<Object,List<E>>() {
				public List<E> invoke() {
					return new LinkedList<E>();
				}
			}, dim
		);
		this.size = 0;
	}

	/**
	 * Constructs a new HashListSAImplementor which uses
	 * the method {@link java.lang.Object#hashCode()} to determine
	 * the hash value of an object. The function creating the buckets delivers
	 * instances of the class {@link java.util.LinkedList LinkedList}.
	 *  
	 * @param dim The number of possible inputs or in other words,
	 * 		  the number of different IDs that can be passed to 
	 * 		  method calls of this implementor. 
	 */
	public HashSAImplementor(int dim) {
		this(Functions.hash(), dim);
	}

	/**
	 * Inserts the given element into the corresponding
	 * bucket of the hash table. The bucket number is
	 * determined by <code>((Integer)hashFunctions[ID].invoke(o)).intValue()</code>.
	 * If the hash table does not contains a bucket with this
	 * number, a new bucket is created by invoking
	 * the function <code>newList</code>.
	 * 
	 * @param o The object to be inserted.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the insertion due to the passed argument.
	 */
	@Override
	public void insert(E o) throws IllegalArgumentException {
		int bucketNo = hashFunctions[ID].invoke(o);
		if (countHashFunctionCalls) {
			((HashSAImplementorMetaDataManagement)metaDataManagement).hashFunctionCallCounter[ID]++;
		}
		List<E> list;
		if (hashTable.containsKey(bucketNo))
			list = hashTable.get(bucketNo);
		else {
			list = newList.invoke();
			hashTable.put(bucketNo, list);
			if (countHashBucketOperations) 
				((HashSAImplementorMetaDataManagement)metaDataManagement).hashBucketOperationsCounter++;
		}
		list.add(o);
		size++;
	}

	/**
	 * Removes the specified element from the hash table.
	 * Tries to access the corresponding bucket and to remove
	 * the element <code>o</code>.
	 * 
	 * @param o The object to be removed.
	 * @return <tt>True</tt> if the removal has been successful, otherwise <tt>false</tt>.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the removal due to the passed argument.
	 */
	@Override
	public boolean remove(E o) throws IllegalArgumentException {
		int bucketNo = hashFunctions[ID].invoke(o);
		if (countHashFunctionCalls) {
			((HashSAImplementorMetaDataManagement)metaDataManagement).hashFunctionCallCounter[ID]++;
		}
		if (hashTable.containsKey(bucketNo)) {
			Iterator<E> it = hashTable.get(bucketNo).iterator();
			while (it.hasNext())
				if (equals.invoke(o, it.next())) {
					it.remove();
					if (countHashBucketOperations) 
						((HashSAImplementorMetaDataManagement)metaDataManagement).hashBucketOperationsCounter++;
					size--;
					return true;
				}	
		}
		return false;
	}

	/**
	 * Checks if element <tt>o1</tt> is contained and 
	 * if <tt>true</tt> updates it with </tt>o2</tt>. 
	 * 
	 * @param o1 The object to be replaced.
	 * @param o2 The new object.
	 * @return The updated object is returned.
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong with the update operation due to the passed arguments.
	 * @throws UnsupportedOperationException Throws an UnsupportedOperationException
	 * 		if this method is not supported.
	 */
	@Override
	public E update(E o1, E o2) throws IllegalArgumentException {
		int hashValue1 = hashFunctions[ID].invoke(o1);
		if (hashValue1 != hashFunctions[ID].invoke(o2)) 
			throw new IllegalArgumentException("Incompatible hash values!");
		if (countHashFunctionCalls) {
			((HashSAImplementorMetaDataManagement)metaDataManagement).hashFunctionCallCounter[ID]+=2;
		}
		if (hashTable.containsKey(hashValue1)) {
			List<E> list = hashTable.get(hashValue1);
			for (int i = 0, j = list.size(); i < j; i++) {
				if (equals.invoke(o1, list.get(i))) {
					if (countHashBucketOperations)  
						((HashSAImplementorMetaDataManagement)metaDataManagement).hashBucketOperationsCounter++;
					return list.set(i, o2);
				}
			}
		}
		throw new IllegalArgumentException("Object o1 is not contained.");
	}

	/**
	 * Clears this implementor by clearing all buckets
	 * as well as the hash table.
	 */
	@Override
	public void clear() {
		for (List<E> bucket : hashTable.values())
			bucket.clear();
		hashTable.clear();
		size = 0;
	}

	/**
	 * Closes this implementor. In this case,
	 * only {@link #clear()} is executed.
	 */
	@Override
	public void close() {
		clear();
		size = 0;
	}

	/**
	 * Returns the size of this implementor which
	 * corresponds to the sum of the bucket sizes.
	 * 
	 * @return The size.
	 */
	@Override
	public int size() {
		return size;	
	}

	/**
	 * Returns an iterator over the elements of this
	 * implementor. Consequently, this iterator is 
	 * a concatenation of the bucket iterators.
	 * 
	 * @return An iterator over the elements of this HashListSAImplementor.
	 * @throws UnsupportedOperationException If this operation is not supported.
	 */
	@Override
	public Iterator<E> iterator() {
//		return new Sequentializer<E>(
//			new Mapper<List<E>, Iterator<E>>(
//				new AbstractFunction<List<E>, Iterator<E>>() {
//					public Iterator<E> invoke(List<E> bucket) {
//						return bucket.iterator();
//					}
//				},
//				hashTable.values().iterator()
//			)
//		);
		
		return new AbstractCursor<E>() {
			protected Iterator<E> it = null;
			protected Iterator<List<E>> buckets = hashTable.values().iterator();
			
			@Override
			public boolean hasNextObject() {
				if (it != null && it.hasNext())
					return true;
				if (it != null && !it.hasNext())
					it = null;
				while (it == null && buckets.hasNext()) {
					it = buckets.next().iterator();
					if (it.hasNext())
						return true;
					else it = null;
				}
				return false;
			}
	
			@Override
			public E nextObject() {
				return it.next();						
			}
			
		};
	}

	/**
	 * Queries this implementor with the help of the
	 * specified query object <code>o</code> and the query-predicates
	 * set during initialization, see method
	 * {@link #initialize(int, xxl.core.predicates.Predicate[])}. 
	 * At first, the corresponding bucket for retrieval
	 * is determined by applying the hash function on <code>o</code>.
	 * Then this bucket is filtered for matching elements which
	 * are returned as a cursor. <br>
	 * <i>Note:</i>
	 * This iterator should not be used to remove any elements from this
	 * implementor SweepArea!
	 * 
	 * @param o The query object. This object is typically probed against
	 * 		the elements contained in this implementor.
	 * @param ID An ID determining from which input this method
	 * 		is triggered.
	 * @return All matching elements of this implementor are returned as an iterator. 
	 * @throws IllegalArgumentException Throws an IllegalArgumentException
	 * 		if something goes wrong due to the passed arguments during retrieval.
	 * @see #filter(Iterator, Object, int)
	 */
	@Override
	public Iterator<E> query(E o, int ID) throws IllegalArgumentException {
		if (size == 0) return new EmptyCursor<E>();
		int bucketNo = hashFunctions[ID].invoke(o);
		if (countHashFunctionCalls) {
			((HashSAImplementorMetaDataManagement)metaDataManagement).hashFunctionCallCounter[ID]++;
		}
		return hashTable.containsKey(bucketNo) ?
			filter(hashTable.get(bucketNo).iterator(), o, ID) :
			new EmptyCursor<E>();
	}
	
	@Override
	public Iterator<E> query(E[] os, int [] IDs, int valid) throws IllegalArgumentException {
		if (size == 0) return new EmptyCursor<E>();
		int bucketNo = hashFunctions[ID].invoke(os[0]);
		for (int i=1; i<valid; i++)
			if (hashFunctions[ID].invoke(os[i])!=bucketNo)
				throw new IllegalArgumentException("Query elements are hashed to different buckets");					
		if (countHashFunctionCalls) {
			((HashSAImplementorMetaDataManagement)metaDataManagement).hashFunctionCallCounter[ID]+=valid;
		}
		return hashTable.containsKey(bucketNo) ?
			filter(hashTable.get(bucketNo).iterator(), os, IDs, valid) :
			new EmptyCursor<E>();
	}
		
	@Override
	public void createMetaDataManagement() {
		if (metaDataManagement != null)
			throw new IllegalStateException("An instance of MetaDataManagement already exists.");
		metaDataManagement = new HashSAImplementorMetaDataManagement();
	}

}
