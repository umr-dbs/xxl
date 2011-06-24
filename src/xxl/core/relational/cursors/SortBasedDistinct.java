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
import java.util.Comparator;

import xxl.core.collections.queues.Queue;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.Function;
import xxl.core.predicates.Predicate;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * A sort-based implementation of the operator "Distinct". This class uses the
 * algorithm of {@link xxl.core.cursors.distincts.SortBasedDistinct} and
 * additionally forwards the metadata.
 * 
 * <p>To get a correct result, the input relations have to be sorted when using
 * the first or the second constructor. To sort a metadata cursor use a
 * {@link MergeSorter merge-sorter}. The last two constructors perform an early
 * duplicate removal and do not need a sorted input.</p>
 */
public class SortBasedDistinct extends xxl.core.cursors.distincts.SortBasedDistinct<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the sort-based distinct.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;
	
	/**
	 * Creates a new instance of sort-based distinct.
	 *
	 * @param sortedCursor the input metadata cursor (must be sorted)
	 *        delivering the elements.
	 * @param predicate a binary predicate that has to determine a match
	 *        between two elements of the input iteration.
	 */
	public SortBasedDistinct(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> sortedCursor, Predicate<? super Tuple> predicate) {
		super(sortedCursor, predicate);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(sortedCursor));
	}

	/**
	 * Creates a new instance of the sort-based distinct operator and sorts the
	 * input at first using a {@link MergeSorter merge-sorter} with
	 * {@link xxl.core.collections.queues.DistinctQueue distinct-queues}. So,
	 * an object constructed with this constructor is not really a sort-based
	 * operator! <b>It performs an early duplicate elimination during the sort
	 * operation.</b>
	 *
	 * @param cursor the input metadata cursor delivering the elements.
	 * @param comparator the comparator used to compare the elements in the
	 *        heap (replacement selection).
	 * @param blockSize the size of a block (page).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge sorter during the
	 *        open-phase.
	 * @param firstOutputBufferRatio the ratio of memory available to the
	 *        output buffer during run-creation (0.0: use only one page for the
	 *        output buffer and what remains is used for the heap; 1.0: use as
	 *        much memory as possible for the output buffer).
	 * @param outputBufferRatio the amount of memory available to the output
	 *        buffer during intermediate merges (not the final merge) (0.0: use
	 *        only one page for the output buffer, what remains is used for the
	 *        merger and the input buffer, inputBufferRatio determines how the
	 *        remaining memory is distributed between them; 1.0: use as much
	 *        memory as possible for the output buffer).
	 * @param inputBufferRatio the amount of memory available to the input
	 *        buffer during intermediate merges (not the final merge) (0.0: use
	 *        only one page for the input buffer, what remains is used for the
	 *        merger (maximal FanIn); 1.0: use as much memory as possible for
	 *        the input buffer).
	 * @param finalMemSize the memory available to the merge sorter during the
	 *        next-phase.
	 * @param finalInputBufferRatio the amount of memory available to the input
	 *        buffer of the final (online) merge (0.0: use the maximum number
	 *        of inputs (maximal fanIn), i.e., perform the online merge as
	 *        early as possible; 1.0: write the entire data into a final queue,
	 *        the online "merger" just reads the data from this queue).
	 * @param newQueuesQueue if this function is invoked, the queue, that
	 *        should contain the queues to be merged, is returned. The function
	 *        takes an iterator and the comparator
	 *        <code>queuesQueueComparator</code> as parameters, e.g.,
	 *        <code><pre>
	 *          new Function&lt;Object, Queue&lt;E&gt;&gt;() {
	 *              public Queue&lt;E&gt; invoke(Object iterator, Object comparator) {
	 *                  return new DynamicHeap&lt;E&gt;((Iterator&lt;E&gt;)iterator, (Comparator&lt;E&gt;)comparator);
	 *              }
	 *          };
	 *        </pre></code>
	 *        The queues contained in the iterator are inserted in the dynamic
	 *        heap using the given comparator for comparison.
	 * @param queuesQueueComparator this comparator determines the next queue
	 *        used for merging.
	 *
	 * @see xxl.core.cursors.sorters.MergeSorter
	 */
	public SortBasedDistinct(
		MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor,
		Comparator<? super Tuple> comparator,
		int blockSize,
		int objectSize,
		int memSize,
		double firstOutputBufferRatio,
		double outputBufferRatio,
		double inputBufferRatio,
		int finalMemSize,
		double finalInputBufferRatio,
		Function<Object, ? extends Queue<Cursor<Tuple>>> newQueuesQueue,
		Comparator<? super Cursor<Tuple>> queuesQueueComparator
	) {
		super(
			cursor,
			comparator,
			blockSize,
			objectSize,
			memSize,
			firstOutputBufferRatio,
			outputBufferRatio,
			inputBufferRatio,
			finalMemSize,
			finalInputBufferRatio,
			newQueuesQueue,
			queuesQueueComparator
		);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new sort-based distinct operator and sorts the input at first
	 * using a {@link MergeSorter merge-sorter} with
	 * {@link xxl.core.collections.queues.DistinctQueue distinct-queues}.
	 * Performs an early duplicate elimination during the sort operation.
	 *
	 * @param cursor the input metadata cursor delivering the elements.
	 * @param comparator the comparator used to compare the elements in the
	 *        heap (replacement selection).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge sorter during the
	 *        next-phase.
	 *
	 * @see xxl.core.cursors.sorters.MergeSorter
	 */
	public SortBasedDistinct(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, Comparator<? super Tuple> comparator, int objectSize, int memSize, int finalMemSize) {
		super(
			cursor,
			comparator,
			objectSize,
			memSize,
			finalMemSize
		);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	 }

	/**
	 * Creates a new instance of sort-based distinct.
	 *
	 * @param sortedResultSet the input result set (must be sorted) delivering
	 *        the elements. The result set is wrapped internally to a metadata
	 *        cursor using {@link ResultSetMetaDataCursor}.
	 * @param predicate a binary predicate that has to determine a match
	 *        between two elements of the input iteration.
	 */
	public SortBasedDistinct(ResultSet sortedResultSet, Predicate<? super Tuple> predicate) {
		this(new ResultSetMetaDataCursor(sortedResultSet), predicate);
	}

	/**
	 * Creates a new instance of the sort-based distinct operator and sorts the
	 * input at first using a {@link MergeSorter merge-sorter} with
	 * {@link xxl.core.collections.queues.DistinctQueue distinct-queues}. So,
	 * an object constructed with this constructor is not really a sort-based
	 * operator! <b>It performs an early duplicate elimination during the sort
	 * operation.</b>
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param comparator the comparator used to compare the elements in the
	 *        heap (replacement selection).
	 * @param blockSize the size of a block (page).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge sorter during the
	 *        open-phase.
	 * @param firstOutputBufferRatio the ratio of memory available to the
	 *        output buffer during run-creation (0.0: use only one page for the
	 *        output buffer and what remains is used for the heap; 1.0: use as
	 *        much memory as possible for the output buffer).
	 * @param outputBufferRatio the amount of memory available to the output
	 *        buffer during intermediate merges (not the final merge) (0.0: use
	 *        only one page for the output buffer, what remains is used for the
	 *        merger and the input buffer, inputBufferRatio determines how the
	 *        remaining memory is distributed between them; 1.0: use as much
	 *        memory as possible for the output buffer).
	 * @param inputBufferRatio the amount of memory available to the input
	 *        buffer during intermediate merges (not the final merge) (0.0: use
	 *        only one page for the input buffer, what remains is used for the
	 *        merger (maximal FanIn); 1.0: use as much memory as possible for
	 *        the input buffer).
	 * @param finalMemSize the memory available to the merge sorter during the
	 *        next-phase.
	 * @param finalInputBufferRatio the amount of memory available to the input
	 *        buffer of the final (online) merge (0.0: use the maximum number
	 *        of inputs (maximal fanIn), i.e., perform the online merge as
	 *        early as possible; 1.0: write the entire data into a final queue,
	 *        the online "merger" just reads the data from this queue).
	 * @param newQueuesQueue if this function is invoked, the queue, that
	 *        should contain the queues to be merged, is returned. The function
	 *        takes an iterator and the comparator
	 *        <code>queuesQueueComparator</code> as parameters, e.g.,
	 *        <code><pre>
	 *          new Function&lt;Object, Queue&lt;E&gt;&gt;() {
	 *              public Queue&lt;E&gt; invoke(Object iterator, Object comparator) {
	 *                  return new DynamicHeap&lt;E&gt;((Iterator&lt;E&gt;)iterator, (Comparator&lt;E&gt;)comparator);
	 *              }
	 *          };
	 *        </pre></code>
	 *        The queues contained in the iterator are inserted in the dynamic
	 *        heap using the given comparator for comparison.
	 * @param queuesQueueComparator this comparator determines the next queue
	 *        used for merging.
	 *
	 * @see xxl.core.cursors.sorters.MergeSorter
	 */
	public SortBasedDistinct(
		ResultSet resultSet,
		Comparator<? super Tuple> comparator,
		int blockSize,
		int objectSize,
		int memSize,
		double firstOutputBufferRatio,
		double outputBufferRatio,
		double inputBufferRatio,
		int finalMemSize,
		double finalInputBufferRatio,
		Function<Object, ? extends Queue<Cursor<Tuple>>> newQueuesQueue,
		Comparator<? super Cursor<Tuple>> queuesQueueComparator
	) {
		this(
			new ResultSetMetaDataCursor(resultSet),
			comparator,
			blockSize,
			objectSize,
			memSize,
			firstOutputBufferRatio,
			outputBufferRatio,
			inputBufferRatio,
			finalMemSize,
			finalInputBufferRatio,
			newQueuesQueue,
			queuesQueueComparator
		);
	}

	/**
	 * Creates a new sort-based distinct operator and sorts the input at first
	 * using a {@link MergeSorter merge-sorter} with
	 * {@link xxl.core.collections.queues.DistinctQueue distinct-queues}.
	 * Performs an early duplicate elimination during the sort operation.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param comparator the comparator used to compare the elements in the
	 *        heap (replacement selection).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge sorter during the
	 *        next-phase.
	 *
	 * @see xxl.core.cursors.sorters.MergeSorter
	 */
	public SortBasedDistinct(ResultSet resultSet, Comparator<? super Tuple> comparator, int objectSize, int memSize, int finalMemSize) {
		this(new ResultSetMetaDataCursor(resultSet), comparator, objectSize, memSize, finalMemSize);
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
