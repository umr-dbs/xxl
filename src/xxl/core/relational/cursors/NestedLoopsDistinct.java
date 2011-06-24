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

package xxl.core.relational.cursors;

import java.sql.ResultSet;

import xxl.core.collections.bags.Bag;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * A nested-loops implementation of the operator "Distinct". This class uses
 * the algorithm of {@link xxl.core.cursors.distincts.NestedLoopsDistinct} and
 * additionally forwards the metadata.
 * 
 * <p>Depending on the specified memory size and object size as many elements
 * as possible will be inserted into a temporal bag (typically located in main
 * memory). To guarantee that no duplicates will be inserted the bag is
 * searched for duplicates with the help of a predicate specified in the
 * constructor. If not all elements can be inserted into the bag they will be
 * stored in a queue and will be inserted when the bag emptied due to calls to
 * <code>this.next()</code>.</p>
 * 
 * <p>Example:
 * <code><pre>
 *   new NestedLoopsDistinct&lt;Object&gt;(cursor, 100000, 200);
 *       // 100000: amount of memory used in Bytes
 *       // 200: size of each Tuple
 * </pre></code>
 * This distinct-operator handles up to (100000/200)-1 tuples in main memory at
 * one time. Additional tuples will be stored in a bag.</p>
 */
public class NestedLoopsDistinct extends xxl.core.cursors.distincts.NestedLoopsDistinct<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * An internal variable used for storing the metadata information of this
	 * distinct operator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Creates a new nested-loops distinct operator. Determines the maximum
	 * number of elements that can be stored in the bag used for the temporal
	 * storage of the elements of the input cursor:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.
	 *
	 * @param cursor the input metadata cursor delivering the elements.
	 * @param memSize the available memory size (bytes) for the bag.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @param predicate the predicate returning <code>true</code> if two
	 *        elements are equal.
	 * @param newBag a function without parameters returning an empty bag whose
	 *        <code>bag.cursor()</code> must support <code>remove()</code>.
	 * @param newQueue a function without parameters returning an empty queue.
	 * @throws IllegalArgumentException if not enough memory is available.
	 */
	public NestedLoopsDistinct(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, int memSize, int objectSize, Predicate<? super Tuple> predicate, Function<?, ? extends Bag<Tuple>> newBag, Function<?, ? extends Queue<Tuple>> newQueue) {
		super(cursor, memSize, objectSize, predicate, newBag, newQueue);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new nested-loops distinct operator. Determines the maximum
	 * number of elements that can be stored in the bag used for the temporal
	 * storage of the elements of the input cursor:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.
	 * Uses the Predicate <code>Equal.DEFAULT_INSTANCE</code> and default
	 * factory methods for the classes list-bags and array-queues.
	 *
	 * @param cursor the input metadata cursor delivering the elements.
	 * @param memSize the available memory size (bytes) for the bag.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @throws IllegalArgumentException if not enough memory is available.
	 */
	public NestedLoopsDistinct(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, int memSize, int objectSize) {
		super(cursor, memSize, objectSize);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new nested-loops distinct operator. Determines the maximum
	 * number of elements that can be stored in the bag used for the temporal
	 * storage of the elements of the input result set:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.
	 *
	 * @param resultSet the input rsult set delivering the elements. The result
	 *        set is wrapped to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param memSize the available memory size (bytes) for the bag.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @param predicate the predicate returning <code>true</code> if two
	 *        elements are equal.
	 * @param newBag a function without parameters returning an empty bag whose
	 *        <code>bag.cursor()</code> must support <code>remove()</code>.
	 * @param newQueue a function without parameters returning an empty queue.
	 * @throws IllegalArgumentException if not enough memory is available.
	 */
	public NestedLoopsDistinct(ResultSet resultSet, int memSize, int objectSize, Predicate<? super Tuple> predicate, Function<?, ? extends Bag<Tuple>> newBag, Function<?, ? extends Queue<Tuple>> newQueue) {
		this(new ResultSetMetaDataCursor(resultSet), memSize, objectSize, predicate, newBag, newQueue);
	}

	/**
	 * Creates a new nested-loops distinct operator. Determines the maximum
	 * number of elements that can be stored in the bag used for the temporal
	 * storage of the elements of the input result set:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.
	 * Uses the Predicate <code>Equal.DEFAULT_INSTANCE</code> and default
	 * factory methods for the classes list-bags and array-queues.
	 *
	 * @param resultSet the input rsult set delivering the elements. The result
	 *        set is wrapped to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param memSize the available memory size (bytes) for the bag.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @throws IllegalArgumentException if not enough memory is available.
	 */
	public NestedLoopsDistinct(ResultSet resultSet, int memSize, int objectSize) {
		this(new ResultSetMetaDataCursor(resultSet), memSize, objectSize);
	}

	/**
	 * Returns the metadata information for this metadata-cursor as a composite
	 * metadata ({@link CompositeMetaData}).
	 *
	 * @return the metadata information for this metadata-cursor as a composite
	 *         metadata ({@link CompositeMetaData}).
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetaData;
	}
}
