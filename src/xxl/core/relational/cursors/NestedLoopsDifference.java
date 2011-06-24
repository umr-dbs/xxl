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
import java.sql.ResultSetMetaData;

import xxl.core.collections.bags.Bag;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.Equal;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * Operator computing the difference of two metadata cursors. This class uses
 * the algorithm of {@link xxl.core.cursors.differences.NestedLoopsDifference}
 * and additionally computes the metadata. The metadata are checked whether
 * they fit.
 *
 * <p>Caution: the runtime is quadratic!</p>
 */
public class NestedLoopsDifference extends xxl.core.cursors.differences.NestedLoopsDifference<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * An internal variable used for storing the metadata information of this
	 * difference operator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Constructs a nested-loops difference operator (metadata cursor) that
	 * computes the difference between two input metadata cursors (R-S). The
	 * parameters are the same compared to
	 * {@link xxl.core.cursors.differences.NestedLoopsDifference}.
	 *
	 * <p>Determines the maximum number of elements that can be stored in the
	 * bag used for the temporal storage of the elements of <code>input1</code>
	 * in main memory:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.</p>
	 *
	 * @param input1 the first input relation R (metadata cursor)
	 * @param input2 the second input relation S (metadata cursor) containing
	 *        the elements that become substracted. This cursor has to support
	 *        <code>reset</code>.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input cursor.
	 * @param newBag a parameterless function delivering an empty bag on
	 *        demand. This bag is used to store the elements of the metadata
	 *        cursor <code>input1</code>.
	 * @param predicate a binaray predicate that has to determine a match
	 *        between an element of <code>input1</code> and an element of
	 *        <code>input2</code>.
	 * @param all mode of the difference operation. If the mode is
	 *        <code>true</code>, all tuples of the first relation, which have a
	 *        counterpart in the second relation, are removed. If the mode is
	 *        <code>false</code>, for each tuple of the second relation, only
	 *        one tuple of the first relation is removed at most.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> input1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input2, int memSize, int objectSize, Function<?, ? extends Bag<Tuple>> newBag, Predicate<? super Tuple> predicate, boolean all) {
		super(input1, input2, memSize, objectSize, newBag, predicate, all);

		ResultSetMetaData metaData1 = ResultSetMetaDatas.getResultSetMetaData(input1);
		ResultSetMetaData metaData2 = ResultSetMetaDatas.getResultSetMetaData(input2);

		if (ResultSetMetaDatas.RESULTSET_METADATA_COMPARATOR.compare(metaData1, metaData2) != 0)
			throw new MetaDataException("the difference of the given cursors cannot be computed because they differ in their relational metadata");
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, metaData1);
	}

	/**
	 * Constructs a nested-loops difference operator (metadata cursor) that
	 * computes the difference between two input metadata cursors (R-S). The
	 * parameters are the same compared to
	 * {@link xxl.core.cursors.differences.NestedLoopsDifference}.
	 *
	 * <p>Determines the maximum number of elements that can be stored in the
	 * bag used for the temporal storage of the elements of <code>input1</code>
	 * in main memory:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.</p>
	 * 
	 * <p>This constructor uses the Equal.DEFAULT_INSTANCE predicate to
	 * determine if two objects are equal.</p>
	 *
	 * @param input1 the first input relation R (metadata cursor)
	 * @param input2 the second input relation S (metadata cursor) containing
	 *        the elements that become substracted. This cursor has to support
	 *        <code>reset</code>.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input cursor.
	 * @param newBag a parameterless function delivering an empty bag on
	 *        demand. This bag is used to store the elements of the metadata
	 *        cursor <code>input1</code>.
	 * @param all mode of the difference operation. If the mode is
	 *        <code>true</code>, all tuples of the first relation, which have a
	 *        counterpart in the second relation, are removed. If the mode is
	 *        <code>false</code>, for each tuple of the second relation, only
	 *        one tuple of the first relation is removed at most.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> input1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input2, int memSize, int objectSize, Function<?, ? extends Bag<Tuple>> newBag, boolean all) {
		this(input1, input2, memSize, objectSize, newBag, Equal.DEFAULT_INSTANCE, all);
	}

	/**
	 * Constructs a nested-loops difference operator (metadata cursor) that
	 * computes the difference between two input result sets (R-S). The
	 * parameters are the same compared to
	 * {@link xxl.core.cursors.differences.NestedLoopsDifference}.
	 *
	 * <p>Determines the maximum number of elements that can be stored in the
	 * bag used for the temporal storage of the elements of <code>input1</code>
	 * in main memory:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.</p>
	 *
	 * @param input1 the first input relation R (result set)
	 * @param input2 the second input relation S (result set) containing the
	 *        elements that become substracted. This cursor has to support
	 *        <code>reset</code>.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input result set.
	 * @param newBag a parameterless function delivering an empty bag on
	 *        demand. This bag is used to store the elements of the result set
	 *        <code>input1</code>.
	 * @param predicate a binaray predicate that has to determine a match
	 *        between an element of <code>input1</code> and an element of
	 *        <code>input2</code>.
	 * @param all mode of the difference operation. If the mode is
	 *        <code>true</code>, all tuples of the first relation, which have a
	 *        counterpart in the second relation, are removed. If the mode is
	 *        <code>false</code>, for each tuple of the second relation, only
	 *        one tuple of the first relation is removed at most.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(ResultSet input1, ResultSet input2, int memSize, int objectSize, Function<?, ? extends Bag<Tuple>> newBag, Predicate<? super Tuple> predicate, boolean all) {
		this(new ResultSetMetaDataCursor(input1), new ResultSetMetaDataCursor(input2), memSize, objectSize, newBag, predicate, all);
	}

	/**
	 * Constructs a nested-loops difference operator (metadata cursor) that
	 * computes the difference between  two input result sets (R-S). The
	 * parameters are the same compared to
	 * {@link xxl.core.cursors.differences.NestedLoopsDifference}.
	 *
	 * <p>Determines the maximum number of elements that can be stored in the
	 * bag used for the temporal storage of the elements of <code>input1</code>
	 * in main memory:
	 * <pre>
	 *   maxTuples = memSize / objectSize - 1
	 * </pre>.</p>
	 *
	 * <p>This constructor uses the Equal.DEFAULT_INSTANCE predicate to
	 * determine if two objects are equal.</p>
	 *
	 * @param input1 the first input relation R (result set)
	 * @param input2 the second input relation S (result set) containing the
	 *        elements that become substracted. This cursor has to support
	 *        <code>reset</code>.
	 * @param memSize the maximum amount of available main memory that can be
	 *        used for the bag.
	 * @param objectSize the size (bytes) needed to store one object of an
	 *        input result set.
	 * @param newBag a parameterless function delivering an empty bag on
	 *        demand. This bag is used to store the elements of the result set
	 *        <code>input1</code>.
	 * @param all mode of the difference operation. If the mode is
	 *        <code>true</code>, all tuples of the first relation, which have a
	 *        counterpart in the second relation, are removed. If the mode is
	 *        <code>false</code>, for each tuple of the second relation, only
	 *        one tuple of the first relation is removed at most.
	 * @throws IllegalArgumentException if not enough main memory is available.
	 */
	public NestedLoopsDifference(ResultSet input1, ResultSet input2, int memSize, int objectSize, Function<?, ? extends Bag<Tuple>> newBag, boolean all) {
		this(new ResultSetMetaDataCursor(input1), new ResultSetMetaDataCursor(input2), memSize, objectSize, newBag, Equal.DEFAULT_INSTANCE, all);
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
