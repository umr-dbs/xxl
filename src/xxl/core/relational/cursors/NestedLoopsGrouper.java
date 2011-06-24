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
import java.util.Map;

import xxl.core.collections.bags.Bag;
import xxl.core.collections.queues.Queue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * A nested-loops grouper is an implementation of the group operator. It is
 * based on {@link xxl.core.cursors.groupers.NestedLoopsGrouper} and
 * additionally forwards the metadata.
 * 
 * <p>A call to the <code>next</code> method returns a cursor containing all
 * elements of a group.</p>
 * 
 * <p>Usually, an {@link xxl.core.relational.cursors.Aggregator} is applied on
 * the output of a grouper.</p>
 */
public class NestedLoopsGrouper extends xxl.core.cursors.groupers.NestedLoopsGrouper<Tuple> implements MetaDataCursor<Cursor<Tuple>, CompositeMetaData<Object, Object>> {

	/**
	 * An internal variable used for storing the metadata information of this
	 * group operator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Constructs an instance of the nested-loops grouper operator. Determines
	 * the maximum number of keys that can be stored in the main memory map
	 * <pre>
	 *   ((memSize - objectSize) / keySize) - 1
	 * </pre>
	 * This formula is based on the assumption that only the keys, i.e., the
	 * map, is stored in main memory whereas the bags storing the input
	 * cursor's elements are located in external memory.
	 *
	 * @param cursor the metadata cursor containing input elements.
	 * @param mapping an unary mapping function returning a key to a given
	 *        value.
	 * @param map the map which is used for storing the keys in main memory.
	 * @param memSize the maximum amount of available main memory (bytes) for
	 *        the map.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @param keySize the size (bytes) a key needs in main memory.
	 * @param newBag a parameterless function returning an empty bag.
	 * @param newQueue a parameterless function returning an empty queue.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsGrouper(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, Function<? super Tuple, ? extends Object> mapping, Map<Object, Bag<Tuple>> map, int memSize, int objectSize, int keySize, Function<?, ? extends Bag<Tuple>> newBag, Function<?, ? extends Queue<Tuple>> newQueue) {
		super(cursor, mapping, map, memSize, objectSize, keySize, newBag, newQueue);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Constructs an instance of the nested-loops grouper operator. Determines
	 * the maximum number of keys that can be stored in the main memory map
	 * <pre>
	 *   ((memSize - objectSize) / keySize) - 1
	 * </pre>
	 * This formula is based on the assumption that only the keys, i.e., the
	 * map, is stored in main memory whereas the bags storing the input
	 * cursor's elements are located in external memory. Uses default factory
	 * methods for list-bags and array-queues.
	 *
	 * @param cursor the metadata cursor containing input elements.
	 * @param mapping an unary mapping function returning a key to a given
	 *        value.
	 * @param map the map which is used for storing the keys in main memory.
	 * @param memSize the maximum amount of available main memory (bytes) for
	 *        the map.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @param keySize the size (bytes) a key needs in main memory.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsGrouper(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, Function<? super Tuple, ? extends Object> mapping, Map<Object, Bag<Tuple>> map, int memSize, int objectSize, int keySize) {
		super(cursor, mapping, map, memSize, objectSize, keySize);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Constructs an instance of the nested-loops grouper operator. Determines
	 * the maximum number of keys that can be stored in the main memory map
	 * <pre>
	 *   ((memSize - objectSize) / keySize) - 1
	 * </pre>
	 * This formula is based on the assumption that only the keys, i.e., the
	 * map, is stored in main memory whereas the bags storing the input
	 * result set's elements are located in external memory.
	 *
	 * @param resultSet the result set containing input elements.
	 * @param mapping an unary mapping function returning a key to a given
	 *        value.
	 * @param map the map which is used for storing the keys in main memory.
	 * @param memSize the maximum amount of available main memory (bytes) for
	 *        the map.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @param keySize the size (bytes) a key needs in main memory.
	 * @param newBag a parameterless function returning an empty bag.
	 * @param newQueue a parameterless function returning an empty queue.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsGrouper(ResultSet resultSet, Function<? super Tuple, ? extends Object> mapping, Map<Object, Bag<Tuple>> map, int memSize, int objectSize, int keySize, Function<?, ? extends Bag<Tuple>> newBag, Function<?, ? extends Queue<Tuple>> newQueue) {
		this(new ResultSetMetaDataCursor(resultSet), mapping, map, memSize, objectSize, keySize, newBag, newQueue);
	}

	/**
	 * Constructs an instance of the nested-loops grouper operator. Determines
	 * the maximum number of keys that can be stored in the main memory map
	 * <pre>
	 *   ((memSize - objectSize) / keySize) - 1
	 * </pre>
	 * This formula is based on the assumption that only the keys, i.e., the
	 * map, is stored in main memory whereas the bags storing the input
	 * result set's elements are located in external memory. Uses default
	 * factory methods for list-bags and array-queues.
	 *
	 * @param resultSet the result set containing input elements.
	 * @param mapping an unary mapping function returning a key to a given
	 *        value.
	 * @param map the map which is used for storing the keys in main memory.
	 * @param memSize the maximum amount of available main memory (bytes) for
	 *        the map.
	 * @param objectSize the size (bytes) needed to store one element.
	 * @param keySize the size (bytes) a key needs in main memory.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsGrouper(ResultSet resultSet, Function<? super Tuple, ? extends Object> mapping, Map<Object, Bag<Tuple>> map, int memSize, int objectSize, int keySize) {
		this(new ResultSetMetaDataCursor(resultSet), mapping, map, memSize, objectSize, keySize);
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
