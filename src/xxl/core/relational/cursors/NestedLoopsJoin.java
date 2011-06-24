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

import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.MetaDataPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.Predicates;
import xxl.core.relational.JoinUtils;
import xxl.core.relational.metaData.MergedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * A nested-loops implementation of a flexible join operator.
 * 
 * <p>The following types of joins are available. For a complete discussion of
 * join operations read the adequate chapter of a good book on database
 * systems, e.g. "Datenbanksysteme" from Kemper, Eickler.
 * <ul>
 *   <li><tt>THETA_JOIN</tt></li>
 *   <li><tt>LEFT_OUTER_JOIN</tt></li>
 *   <li><tt>RIGHT_OUTER_JOIN</tt></li>
 *   <li><tt>OUTER_JOIN</tt></li>
 *   <li><tt>NATURAL_JOIN</tt></li>
 *   <li><tt>SEMI_JOIN</tt></li>
 *   <li><tt>CARTESIAN_PRODUCT</tt></li>
 * </ul>
 * Updates and removes are not supported.</p>
 */
public class NestedLoopsJoin extends xxl.core.cursors.joins.NestedLoopsJoin<Tuple, Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * An enumeration of constants specifying the join types supported by this
	 * class.
	 */
	public static enum Type {
			
		/**
		 * A constant specifying a theta join. Only the tuples for which the
		 * specified predicate is <code>true</code> will be returned.
		 */
		THETA_JOIN,
		
		/**
		 * A constant specifying a left outer join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * the <code>cursor1</code> not qualifying concerning the predicate
		 * will be returned. The function <code>newResult</code> is called with
		 * arguments <code>cursor1.peek()</code> and <code>null</code>.
		 */
		LEFT_OUTER_JOIN,
		
		/**
		 * A constant specifying a right outer join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all elements of
		 * the <code>cursor2</code> not qualifying concerning the predicate
		 * will be returned. The function <code>newResult</code> is called with
		 * arguments <code>null</code> and <code>cursor2.peek()</code>.
		 */
		RIGHT_OUTER_JOIN,
		
		/**
		 * A constant specifying a full outer join. The tuples for which the
		 * specified predicate is <code>true</code> as well as all tuples of
		 * the LEFT and RIGHT OUTER_JOIN will be returned.
		 */
		OUTER_JOIN,
		
		/**
		 * A constant specifying a natural join. The tuples are compared using
		 * their common columns.
		 */
		NATURAL_JOIN,
		
		/**
		 * A constant specifying a semi join. The tuples are compared using
		 * their common columns. The results become projected to the columns of
		 * the first input relation.
		 */
		SEMI_JOIN,
		
		/**
		 * A constant specifying a Cartesian product. Every combination of
		 * tuples of the two relations is returned.
		 */
		CARTESIAN_PRODUCT
	}

	/**
	 * An internal variable used for storing the metadata information of this
	 * join operator.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Advanced constructor creating a new nested-loops join with lazy
	 * evaluation backed on two cursors using a metadata predicate to select
	 * the resulting tuples. This constructor also supports the handling of a
	 * non-resetable input metadata cursor, <code>cursor2</code>, because a
	 * parameterless function can be defined that returns this input metadata
	 * cursor. Furthermore a function can be specified that is invoked on each
	 * qualifying tuple before it is returned to the caller concerning a call
	 * to <code>next</code>. This function is a kind of factory-method to model
	 * the resulting object.
	 * 
	 * <p>Handle with care! Do not use this constructor if you do not know
	 * exactly what you are doing.</p>
	 *
	 * @param cursor1 the input metadata cursor that is traversed in the
	 *        "outer" loop.
	 * @param cursor2 the input metadata cursor that is traversed in the
	 *        "inner" loop.
	 * @param newCursor a parameterless function that delivers a new metadata
	 *        cursor, when the cursor <code>cursor2</code> cannot be reset,
	 *        i.e., <code>cursor2.reset()</code> will cause a
	 *        {@link java.lang.UnsupportedOperationException}.
	 * @param predicate the predicate evaluated for each tuple of elements
	 *        backed on one element of each input-cursor in order to select
	 *        them. Only these tuples where the predicate's evaluation result
	 *        is <code>true</code> have been qualified to be a result of the
	 *        join-operation.
	 * @param createTuple a factory method (function) that takes two parameters
	 *        as arguments and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e. on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        <code>next()</code>.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 * @throws UnsupportedOperationException if the input metadata cursor
	 *         <code>cursor1</code> or <code>cursor2</code> cannot be reseted.
	 */
	public NestedLoopsJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor2, Function<?, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newCursor, MetaDataPredicate<? super Tuple, CompositeMetaData<Object, Object>> predicate, Function<Object, ? extends Tuple> createTuple, Type type) {
		super(
			cursor1,
			cursor2,
			newCursor,
			predicate,
			JoinUtils.genericJoinTupleFactory(
				createTuple,
				(MergedResultSetMetaData)ResultSetMetaDatas.getResultSetMetaData(predicate)
			),
			type == Type.LEFT_OUTER_JOIN ?
				xxl.core.cursors.joins.NestedLoopsJoin.Type.LEFT_OUTER_JOIN :
				type == Type.RIGHT_OUTER_JOIN ?
					xxl.core.cursors.joins.NestedLoopsJoin.Type.RIGHT_OUTER_JOIN :
					type == Type.OUTER_JOIN ?
						xxl.core.cursors.joins.NestedLoopsJoin.Type.OUTER_JOIN :
						xxl.core.cursors.joins.NestedLoopsJoin.Type.THETA_JOIN
		);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(predicate));
	}

	/**
	 * Creates a new nested-loops join which performs a theta join. Lazy
	 * evaluation is used backed on two cursors using a predicate to select the
	 * resulting tuples. This constructor also supports the handling of a
	 * non-resetable input metadata cursor, <code>cursor2</code>, because a
	 * parameterless function can be defined that returns this input metadata
	 * cursor. Furthermore a function can be specified that is invoked on each
	 * qualifying tuple before it is returned to the caller concerning a call
	 * to <code>next</code>. This function is a kind of factory-method to model
	 * the resulting object.
	 *
	 * @param cursor1 the input metadata cursor that is traversed in the
	 *        "outer" loop.
	 * @param cursor2 the input metadata cursor that is traversed in the
	 *        "inner" loop.
	 * @param newCursor a parameterless function that delivers a new metadata
	 *        cursor, when the cursor <code>cursor2</code> cannot be reset,
	 *        i.e., <code>cursor2.reset()</code> will cause a
	 *        {@link java.lang.UnsupportedOperationException}.
	 * @param theta the predicate evaluated for each tuple of elements backed
	 *        on one element of each input-cursor in order to select them. Only
	 *        these tuples where the predicate's evaluation result is
	 *        <code>true</code> have been qualified to be a result of the
	 *        join-operation.
	 * @param createTuple a factory method (function) that takes two parameters
	 *        as arguments and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e. on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        <code>next</code>.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 * @throws UnsupportedOperationException if the input metadata cursor
	 *         <code>cursor1</code> or <code>cursor2</code> cannot be reseted.
	 */
	public NestedLoopsJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor2, Function<?, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newCursor, Predicate<? super Tuple> theta, Function<Object, ? extends Tuple> createTuple) {
		this(
			cursor1,
			cursor2,
			newCursor,
			JoinUtils.thetaJoinMetaDataPredicate(
				theta,
				ResultSetMetaDatas.getResultSetMetaData(cursor1),
				ResultSetMetaDatas.getResultSetMetaData(cursor2)
			),
			createTuple,
			Type.THETA_JOIN
		);
	}

	/**
	 * Creates a new nested-loops join with lazy evaluation backed on two
	 * cursors using a predicate to select the resulting tuples. This
	 * constructor also supports the handling of a non-resetable input metadata
	 * cursor, <code>cursor2</code>, because a parameterless function can be
	 * defined that returns this input metadata cursor. Furthermore a function
	 * can be specified that is invoked on each qualifying tuple before it is
	 * returned to the caller concerning a call to <code>next</code>. This
	 * function is a kind of factory-method to model the resulting object.
	 *
	 * @param cursor1 the input metadata cursor that is traversed in the
	 *        "outer" loop.
	 * @param cursor2 the input metadata cursor that is traversed in the
	 *        "inner" loop.
	 * @param newCursor a parameterless function that delivers a new metadata
	 *        cursor, when the cursor <code>cursor2</code> cannot be reset,
	 *        i.e., <code>cursor2.reset()</code> will cause a
	 *        {@link java.lang.UnsupportedOperationException}.
	 * @param createTuple a factory method (function) that takes two parameters
	 *        as arguments and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e. on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        <code>next()</code>.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 * @throws UnsupportedOperationException if input metadata cursor
	 *         <code>cursor1</code> or <code>cursor2</code> cannot be reseted.
	 */
	public NestedLoopsJoin(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor1, MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor2, Function<?, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newCursor, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			cursor1,
			cursor2,
			newCursor,
			type == Type.THETA_JOIN || type == Type.CARTESIAN_PRODUCT ?
				JoinUtils.thetaJoinMetaDataPredicate(
					Predicates.TRUE,
					ResultSetMetaDatas.getResultSetMetaData(cursor1),
					ResultSetMetaDatas.getResultSetMetaData(cursor2)
				) :
				type == Type.SEMI_JOIN ?
					JoinUtils.semiJoinMetaDataPredicate(
						JoinUtils.naturalJoinMetaDataPredicate(
							ResultSetMetaDatas.getResultSetMetaData(cursor1),
							ResultSetMetaDatas.getResultSetMetaData(cursor2)
						),
						new ResultSetMetaData[] {
							ResultSetMetaDatas.getResultSetMetaData(cursor1),
							ResultSetMetaDatas.getResultSetMetaData(cursor2)
						},
						0
					) :
					JoinUtils.naturalJoinMetaDataPredicate(
						ResultSetMetaDatas.getResultSetMetaData(cursor1),
						ResultSetMetaDatas.getResultSetMetaData(cursor2)
					),
			createTuple,
			type
		);
	}

	/**
	 * Creates a new nested-loops join which performs a theta join. Lazy
	 * evaluation is used backed on two cursors using a predicate to select the
	 * resulting tuples. This constructor also supports the handling of a
	 * non-resetable input metadata cursor, <code>cursor2</code>, because a
	 * parameterless function can be defined that returns this input metadata
	 * cursor. Furthermore a function can be specified that is invoked on each
	 * qualifying tuple before it is returned to the caller concerning a call
	 * to <code>next</code>. This Function is a kind of factory-method to model
	 * the resulting object.
	 *
	 * @param resultSet1 the input result set that is traversed in the "outer"
	 *        loop.
	 * @param resultSet2 the input result set that is traversed in the "inner"
	 *        loop.
	 * @param newResultSet a function without parameters that delivers a new
	 *        result set, when the iterating over the "inner" result set
	 *        <code>resultSet2</code> cannot be reset, i.e.,
	 *        <code>cursor2.reset()</code> will cause an
	 *        {@link java.lang.UnsupportedOperationException}.
	 * @param predicate the predicate evaluated for each tuple of elements
	 *        backed on one element of each input-cursor in order to select
	 *        them. Only these tuples where the predicate's evaluation result
	 *        is <code>true</code> have been qualified to be a result of the
	 *        join-operation.
	 * @param createTuple a factory method (function) that takes two parameters
	 *        as arguments and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e. on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        <code>next</code>.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 * @throws UnsupportedOperationException if the input metadata cursor
	 *         <code>cursor1</code> or <code>cursor2</code> cannot be reseted.
	 */
	public NestedLoopsJoin(ResultSet resultSet1, ResultSet resultSet2, final Function<?, ? extends ResultSet> newResultSet, Predicate<? super Tuple> predicate, Function<Object, ? extends Tuple> createTuple) {
		this(
			new ResultSetMetaDataCursor(resultSet1),
			new ResultSetMetaDataCursor(resultSet2),
			new AbstractFunction<Object, MetaDataCursor<Tuple, CompositeMetaData<Object, Object>>>() {
				@Override
				public MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> invoke() {
					return new ResultSetMetaDataCursor(newResultSet.invoke());
				}
			},
			predicate,
			createTuple
		);
	}

	/**
	 * Creates a new nested-loops join with lazy evaluation backed on two
	 * cursors using a predicate to select the resulting tuples. This
	 * constructor also supports the handling of a non-resetable input metadata
	 * cursor, <code>cursor2</code>, because a parameterless function can be
	 * defined that returns this input metadata cursor. Furthermore a function
	 * can be specified that is invoked on each qualifying tuple before it is
	 * returned to the caller concerning a call to <code>next</code>. This
	 * function is a kind of factory-method to model the resulting object.
	 *
	 * @param resultSet1 the input result set that is traversed in the "outer"
	 *        loop.
	 * @param resultSet2 the input result set that is traversed in the "inner"
	 *        loop.
	 * @param newResultSet a function without parameters that delivers a new
	 *        result set, when the iterating over the "inner" result set
	 *        <code>resultSet2</code> cannot be reset, i.e.,
	 *        <code>cursor2.reset()</code> will cause an
	 *        {@link java.lang.UnsupportedOperationException}.
	 * @param createTuple a factory method (function) that takes two parameters
	 *        as arguments and is invoked on each tuple where the predicate's
	 *        evaluation result is <code>true</code>, i.e. on each qualifying
	 *        tuple before it is returned to the caller concerning a call to
	 *        <code>next</code>.
	 * @param type the type of the join operation. Possible values are the
	 *        elements of the enumeration defined above in this class.
	 * @throws IllegalArgumentException if the specified type is not valid.
	 * @throws UnsupportedOperationException if the input metadata cursor
	 *         <code>cursor1</code> or <code>cursor2</code> cannot be reseted.
	 */
	public NestedLoopsJoin(ResultSet resultSet1, ResultSet resultSet2, final Function<?, ? extends ResultSet> newResultSet, Function<Object, ? extends Tuple> createTuple, Type type) {
		this(
			new ResultSetMetaDataCursor(resultSet1),
			new ResultSetMetaDataCursor(resultSet2),
			new AbstractFunction<Object, MetaDataCursor<Tuple, CompositeMetaData<Object, Object>>>() {
				@Override
				public MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> invoke() {
					return new ResultSetMetaDataCursor(newResultSet.invoke());
				}
			},
			createTuple,
			type
		);
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
