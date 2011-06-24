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
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.relational.tuples.Tuples;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * A merge-sort implementation of a sort operator.
 * 
 * <p>This class uses the algorithm of
 * {@link xxl.core.cursors.sorters.MergeSorter} and additionally forwards the
 * metadata. A detailed description of the algorithm is contained in
 * {@link xxl.core.cursors.sorters.MergeSorter}.</p>
 * 
 * <p>In earlier versions of XXL it was possible to hand over a string array
 * instead of an array of indices. To get this functionality, use
 * {@link xxl.core.relational.resultSets.ResultSets#getColumnIndices(ResultSet, String[])}.</p>
 */
public class MergeSorter extends xxl.core.cursors.sorters.MergeSorter<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the merge-sorter.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Creates a new merge-sorter.
	 *
	 * @param cursor the input metadata cursor to be sorted.
	 * @param comparator the comparator used to compare the tuples in the heap
	 *        (replacement-selection).
	 * @param blockSize the size of a block (page).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param firstOutputBufferRatio the ratio of memory available to the
	 *        output-buffer during run-creation.
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                use only one page for the output buffer and what remains
	 *                is used for the heap
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                use as much memory as possible for the output buffer
	 *            </dd>
	 *        </dl>
	 * @param outputBufferRatio the amount of memory available to the
	 *        output-buffer during intermediate merges (not the final merge).
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                use only one page for the output buffer, what remains is
	 *                used for the merger and the input buffer,
	 *                <tt>inputBufferRatio</tt> determines how the remaining
	 *                memory is distributed between them
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                use as much memory as possible for the output buffer
	 *            </dd>
	 *        </dl>
	 * @param inputBufferRatio the amount of memory available to the
	 *        input-buffer during intermediate merges (not the final merge).
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                use only one page for the input buffer, what remains is
	 *                used for the merger (maximal fan-in)
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                use as much memory as possible for the input buffer
	 *            </dd>
	 *        </dl>
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 * @param finalInputBufferRatio the amount of memory available to the
	 *        input-buffer of the final (online) merge.
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                maximum number of inputs (maximal fan-in), i.e., perform
	 *                the online merge as early as possible
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                write the entire data into a final queue, the online
	 *                "merger" just reads the data from this queue
	 *            </dd>
	 *        </dl>
	 * @param newQueue the function <code>newQueue</code> should return a
	 *        queue, which is used by the algorithm to materialize the internal
	 *        runs, i.e., this function determines whether the sort operator
	 *        works on queues based on external storage or in main memory
	 *        (useful for testing and counting). The function takes two
	 *        parameterless functions <code>getInputBufferSize</code> and
	 *        <code>getOutputBufferSize</code> as parameters.
	 * @param newQueuesQueue if this function is invoked, the queue, that
	 *        should contain the queues to be merged, is returned. The function
	 *        takes an iterator and the comparator
	 *        <code>queuesQueueComparator</code> as parameters. E.g.,
	 *        <code><pre>
	 *          new Function&lt;Object, Queue&lt;Tuple&gt;&gt;() {
	 *              public Queue&lt;Tuple&gt; invoke(Object iterator, Object comparator) {
	 *                  return new DynamicHeap&lt;Tuple&gt;((Iterator&lt;Tuple&gt;)iterator, (Comparator&lt;Tuple&gt;)comparator);
	 *              }
	 *          };
	 *        </pre></code>
	 *        The queues contained in the iterator are inserted in the dynamic
	 *        heap using the given comparator for comparison.
	 * @param queuesQueueComparator this comparator determines the next queue
	 *        used for merging.
	 * @param verbose if the <code>verbose</code> flag set to <code>true</code>
	 *        the merge-sorter displays how the memory was distributed
	 *        internally. In addition, the number of merges is displayed.
	 */
	public MergeSorter(
		MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor,
		Comparator<? super Tuple> comparator,
		int blockSize,
		int objectSize,
		int memSize,
		double firstOutputBufferRatio,
		double outputBufferRatio,
		double inputBufferRatio,
		int finalMemSize,
		double finalInputBufferRatio,
		Function<Function<?, Integer>, ? extends Queue<Tuple>> newQueue,
		Function<Object, ? extends Queue<Cursor<Tuple>>> newQueuesQueue,
		Comparator<? super Cursor<Tuple>> queuesQueueComparator,
		boolean verbose
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
			newQueue,
			newQueuesQueue,
			queuesQueueComparator,
			verbose
		);
		
		this.globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap} and they
	 * are compared according their sizes.
	 *
	 * @param cursor the input metadata cursor to be sorted.
	 * @param comparator the comparator used to compare the tuples in the heap
	 *       (replacement-selection).
	 * @param objectSize the size of a tuple in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 */
	public MergeSorter(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, Comparator<? super Tuple> comparator, int objectSize, int memSize, int finalMemSize) {
		super(cursor, comparator, objectSize, memSize, finalMemSize);
		
		this.globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queues to be merged are
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap} and they
	 * are compared according their sizes.
	 *
	 * @param cursor the input metadata cursor to be sorted.
	 * @param comparator the comparator used to compare the tuples in the heap
	 *        (replacement-selection).
	 * @param objectSize the size of a tuple in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 * @param newQueue the function <code>newQueue</code> should return a
	 *        queue, which is used by the algorithm to materialize the internal
	 *        runs, i.e., this function determines whether the sort operator
	 *        works on queues based on external storage or in main memory
	 *        (useful for testing and counting). The function takes two
	 *        parameterless functions <code>getInputBufferSize</code> and
	 *        <code>getOutputBufferSize</code> as parameters.
	 * @param verbose if the <code>verbose</code> flag set to <code>true</code>
	 *        the merge-sorter displays how the memory was distributed
	 *        internally. In addition, the number of merges is displayed.
	 */
	public MergeSorter(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, Comparator<? super Tuple> comparator, int objectSize, int memSize, int finalMemSize, Function<Function<?, Integer>, ? extends Queue<Tuple>> newQueue, boolean verbose) {
		super(cursor, comparator, objectSize, memSize, finalMemSize, newQueue, verbose);
		
		this.globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap}. The tuples
	 * are compared using a comparator returned by the
	 * <code>Tuples.getTupleComparator</code> method. <b>This is the type of
	 * constructor that is used in most cases.</b>
	 *
	 * @param cursor the input metadata cursor to be sorted.
	 * @param onColumns an array of column indices identifying the column to be
	 *        compared: the first column is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 * @param objectSize the size of a tuple in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 */
	public MergeSorter(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, int[] onColumns, boolean[] ascending, int objectSize, int memSize, int finalMemSize) {
		this(cursor, Tuples.getTupleComparator(onColumns, ascending), objectSize, memSize, finalMemSize);
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap}. The object
	 * size is set to <code>128</code>, the memory size to <code>12*4096</code>
	 * and the final memory size to <code>4*4096</code>. The tuples are
	 * compared using a comparator returned by the
	 * <code>Tuples.getTupleComparator</code> method. <b>This is the type of
	 * constructor that is used in most cases.</b>
	 *
	 * @param cursor the input metadata cursor to be sorted.
	 * @param onColumns an array of column indices identifying the column to be
	 *        compared: the first column is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 */
	public MergeSorter(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, int[] onColumns, boolean[] ascending) {
		this(cursor, onColumns, ascending, 128, 12*4096, 4*4096);
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap} and they
	 * are compared according their sizes. The object size is set to
	 * <code>128</code>, the memory size to <code>12*4096</code> and the final
	 * memory size to <code>4*4096</code>.
	 *
	 * @param cursor the input metadata cursor to be sorted.
	 * @param comparator the comparator used to compare the tuples in the heap
	 *        (replacement-selection).
	 */
	public MergeSorter(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor, Comparator<? super Tuple> comparator) {
		this(cursor, comparator, 128, 12*4096, 4*4096);
	}

	/**
	 * Creates a new merge-sorter.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createTuple a function that maps a (row of the) result setet to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. It is forbidden to call the <code>next</code>,
	 *        <code>update</code> and similar methods of the result set from
	 *        inside the function!
	 * @param comparator the comparator used to compare the tuples in the heap
	 *        (replacement-selection).
	 * @param blockSize the size of a block (page).
	 * @param objectSize the size of an object in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param firstOutputBufferRatio the ratio of memory available to the
	 *        output-buffer during run-creation.
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                use only one page for the output buffer and what remains
	 *                is used for the heap
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                use as much memory as possible for the output buffer
	 *            </dd>
	 *        </dl>
	 * @param outputBufferRatio the amount of memory available to the
	 *        output-buffer during intermediate merges (not the final merge).
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                use only one page for the output buffer, what remains is
	 *                used for the merger and the input buffer,
	 *                <tt>inputBufferRatio</tt> determines how the remaining
	 *                memory is distributed between them
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                use as much memory as possible for the output buffer
	 *            </dd>
	 *        </dl>
	 * @param inputBufferRatio the amount of memory available to the
	 *        input-buffer during intermediate merges (not the final merge).
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                use only one page for the input buffer, what remains is
	 *                used for the merger (maximal fan-in)
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                use as much memory as possible for the input buffer
	 *            </dd>
	 *        </dl>
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 * @param finalInputBufferRatio the amount of memory available to the
	 *        input-buffer of the final (online) merge.
	 *        <dl>
	 *            <dt>
	 *                0.0
	 *            </dt>
	 *            <dd>
	 *                maximum number of inputs (maximal fan-in), i.e., perform
	 *                the online merge as early as possible
	 *            </dd>
	 *            <dt>
	 *                1.0
	 *            </dt>
	 *            <dd>
	 *                write the entire data into a final queue, the online
	 *                "merger" just reads the data from this queue
	 *            </dd>
	 *        </dl>
	 * @param newQueue the function <code>newQueue</code> should return a
	 *        queue, which is used by the algorithm to materialize the internal
	 *        runs, i.e., this function determines whether the sort operator
	 *        works on queues based on external storage or in main memory
	 *        (useful for testing and counting). The function takes two
	 *        parameterless functions <code>getInputBufferSize</code> and
	 *        <code>getOutputBufferSize</code> as parameters.
	 * @param newQueuesQueue if this function is invoked, the queue, that
	 *        should contain the queues to be merged, is returned. The function
	 *        takes an iterator and the comparator
	 *        <code>queuesQueueComparator</code> as parameters. E.g.,
	 *        <code><pre>
	 *          new Function&lt;Object, Queue&lt;Tuple&gt;&gt;() {
	 *              public Queue&lt;Tuple&gt; invoke(Object iterator, Object comparator) {
	 *                  return new DynamicHeap&lt;Tuple&gt;((Iterator&lt;Tuple&gt;)iterator, (Comparator&lt;Tuple&gt;)comparator);
	 *              }
	 *          };
	 *        </pre></code>
	 *        The queues contained in the iterator are inserted in the dynamic
	 *        heap using the given comparator for comparison.
	 * @param queuesQueueComparator this comparator determines the next queue
	 *        used for merging.
	 * @param verbose if the <code>verbose</code> flag set to <code>true</code>
	 *        the merge-sorter displays how the memory was distributed
	 *        internally. In addition, the number of merges is displayed.
	 */
	public MergeSorter(
		ResultSet resultSet,
		Function<? super ResultSet, ? extends Tuple> createTuple,
		Comparator<? super Tuple> comparator,
		int blockSize,
		int objectSize,
		int memSize,
		double firstOutputBufferRatio,
		double outputBufferRatio,
		double inputBufferRatio,
		int finalMemSize,
		double finalInputBufferRatio,
		Function<Function<?, Integer>, ? extends Queue<Tuple>> newQueue,
		Function<Object, ? extends Queue<Cursor<Tuple>>> newQueuesQueue,
		Comparator<? super Cursor<Tuple>> queuesQueueComparator,
		final boolean verbose
	) {
		this(
			new ResultSetMetaDataCursor(resultSet, createTuple),
			comparator,
			blockSize,
			objectSize,
			memSize,
			firstOutputBufferRatio,
			outputBufferRatio,
			inputBufferRatio,
			finalMemSize,
			finalInputBufferRatio,
			newQueue,
			newQueuesQueue,
			queuesQueueComparator,
			verbose
		);
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap} and they
	 * are compared according their sizes.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createTuple a function that maps a (row of the) result setet to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. It is forbidden to call the <code>next</code>,
	 *        <code>update</code> and similar methods of the result set from
	 *        inside the function!
	 * @param comparator the comparator used to compare the tuples in the heap
	 *       (replacement-selection).
	 * @param objectSize the size of a tuple in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 */
	public MergeSorter(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createTuple, Comparator<? super Tuple> comparator, int objectSize, int memSize, int finalMemSize) {
		this(new ResultSetMetaDataCursor(resultSet, createTuple), comparator, objectSize, memSize, finalMemSize);
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap}. The tuples
	 * are compared using a comparator returned by the
	 * <code>Tuples.getTupleComparator</code> method. <b>This is the type of
	 * constructor that is used in most cases.</b>
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createTuple a function that maps a (row of the) result setet to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. It is forbidden to call the <code>next</code>,
	 *        <code>update</code> and similar methods of the result set from
	 *        inside the function!
	 * @param onColumns an array of column indices identifying the column to be
	 *        compared: the first column is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 * @param objectSize the size of a tuple in main memory.
	 * @param memSize the memory available to the merge-sorter during the
	 *        open-phase.
	 * @param finalMemSize the memory available to the merge-sorter during the
	 *        next-phase.
	 */
	public MergeSorter(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createTuple, int[] onColumns, boolean[] ascending, int objectSize, int memSize, int finalMemSize) {
		this(resultSet, createTuple, Tuples.getTupleComparator(onColumns, ascending), objectSize, memSize, finalMemSize);
	}

	/**
	 * Creates a new merge-sorter. The parameters
	 * <code>inputBufferRatio</code>, <code>finalInputBufferRatio</code>,
	 * <code>firstOutputBufferRatio</code> and <code>outputBufferRatio</code>
	 * are set to <code>0.0</code>. That means only one page is reserved for
	 * input- and output-buffer and the maximal fan-in is used during the
	 * intermediate merges and for the final merge. The queue are given by
	 * {@link xxl.core.collections.queues.ListQueue list-queues}, i.e., the
	 * intermediate runs are materialized in main memory and they will be
	 * inserted in a
	 * {@link xxl.core.collections.queues.DynamicHeap dynamic heap}. The object
	 * size is set to <code>128</code>, the memory size to <code>12*4096</code>
	 * and the final memory size to <code>4*4096</code>. The tuples are
	 * compared using a comparator returned by the
	 * <code>Tuples.getTupleComparator</code> method. <b>This is the type of
	 * constructor that is used in most cases.</b>
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createTuple a function that maps a (row of the) result setet to a
	 *        tuple. The function gets a result set and maps the current row to
	 *        a tuple. It is forbidden to call the <code>next</code>,
	 *        <code>update</code> and similar methods of the result set from
	 *        inside the function!
	 * @param onColumns an array of column indices identifying the column to be
	 *        compared: the first column is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 */
	public MergeSorter(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createTuple, int[] onColumns, boolean[] ascending) {
		this(resultSet, createTuple, onColumns, ascending, 128, 12*4096, 4*4096);
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
