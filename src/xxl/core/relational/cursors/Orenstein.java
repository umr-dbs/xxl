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
import java.util.ArrayList;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.AbstractMetaDataFunction;
import xxl.core.functions.Function;
import xxl.core.functions.MetaDataFunction;
import xxl.core.predicates.MetaDataPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.relational.JoinUtils;
import xxl.core.relational.Types;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.relational.tuples.Tuples;
import xxl.core.spatial.KPEzCode;
import xxl.core.spatial.cursors.Mappers;
import xxl.core.spatial.cursors.Orenstein.OrensteinSA;
import xxl.core.spatial.points.FloatPoint;
import xxl.core.spatial.points.Point;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * Operator computing a spatial join with the method proposed by Jack
 * Orenstein.
 * 
 * <p>The spatial join algorithm based on space-filling curves proposed by Jack
 * Orenstein. See: [Ore 91] Jack A. Orenstein: An Algorithm for Computing the
 * Overlay of k-Dimensional Spaces. SSD 1991: 381-400 for a detailed
 * explanation. See: [DS 01]: Jens-Peter Dittrich, Bernhard Seeger: GESS: a
 * Scalable Similarity-Join Algorithm for Mining Large Data Sets in High
 * Dimensional Spaces. ACM SIGKDD-2001. for a review on Orensteins
 * algorithm.</p>
 * 
 * <p>Orensteins algorithm is based on a binary recursive partitioning, where
 * the binary code represents the so-called Z-ordering (z-codes).</p>
 * 
 * <p>Orensteins algorithm (ORE) assigns each hypercube of the input relations
 * to disjoint subspaces of the recursive partitioning whose union entirely
 * covers the hypercube. ORE sorts the two sets of hypercubes derived from the
 * input relations (including the possible replicates) w.r.t. the
 * lexicographical ordering of its binary code. After that, the relations are
 * merged using two main-memory stacks Stack_R and Stack_S. It is guaranteed
 * that for two adjacent hypercubes in the stack, the prefix property is
 * satisfied for their associated codes. Only those hypercubes are joined that
 * have the same prefix code.</p>
 * 
 * <p>A deficiency of ORE is that the different assignment strategies examined
 * in [Ore91] cause substantial replication rates. This results in an increase
 * of the problem space and hence, sorting will be very expensive. Furthermore,
 * ORE has not addressed the problem of eliminating duplicates in the result
 * set.
 * 
 * <p>Note that the method <code>reorganize(final Object currentStatus)</code>
 * could actually be implemented with only 1 LOC. For efficiency reasons we use
 * a somewhat longer version of the method here.</p>
 * 
 * @see xxl.core.relational.cursors.SortMergeJoin
 * @see xxl.core.spatial.cursors.Mappers
 * @see xxl.core.spatial.cursors.GESS
 * @see xxl.core.spatial.cursors.Replicator
 * @see xxl.core.spatial.cursors.Orenstein
 */
public class Orenstein extends SortMergeJoin {

	/**
	 * Static function returning a mapper that gets a tuple as its only input
	 * parameter and returns a tuple containing a {@link FloatPoint}.
	 */
	public static MetaDataFunction<Tuple, ArrayTuple, CompositeMetaData<Object, Object>> FLOAT_VALUE_TUPLE_TO_FLOAT_POINT_TUPLE = new AbstractMetaDataFunction<Tuple, ArrayTuple, CompositeMetaData<Object, Object>>() {
		protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		{
			globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, new ColumnMetaDataResultSetMetaData(new StoredColumnMetaData(false, false, false, false, ResultSetMetaData.columnNoNulls, false, 20, "FLOAT_POINT", "FLOAT_POINT", "", 20, 0, "", "", java.sql.Types.JAVA_OBJECT, Types.getSqlTypeName(java.sql.Types.JAVA_OBJECT), true, false, false, "xxl.core.spatial.points.FloatPoint")));
		}
		
		@Override
		public ArrayTuple invoke(Tuple tuple) {
			float[] values = new float[tuple.getColumnCount()];
			for (int i = 1; i <= values.length; i++)
				values[i-1] = tuple.getFloat(i);
			return ArrayTuple.FACTORY_METHOD.invoke(new FloatPoint(values));
		}
		
		@Override
		public CompositeMetaData<Object, Object> getMetaData() {
			return globalMetaData;
		}
	};

	/**
	 * Static inner class mapping a tuple containg a {@link FloatPoint} to a
	 * tuple containing a {@link KPEzCode} using
	 * {@link Mappers#pointToKPEzCodeMappingFunction(float, int)}.
	 */
	public static class FloatPointKPEzCodeMapper extends Mapper {

		/**
		 * Constructs a new FloatPointKPEzCodeMapper.
		 *
		 * @param input the input metadata of the mapper
		 * @param epsilon epsilon-distance
		 * @param maxLevel maximum level of the partitioning
		 */
		public FloatPointKPEzCodeMapper(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> input, final float epsilon, final int maxLevel) {
			super(
				new AbstractMetaDataFunction<Tuple, Tuple, CompositeMetaData<Object, Object>>() {
					protected Function<Point, KPEzCode> pointToKPEzCodeMappingFunction = Mappers.pointToKPEzCodeMappingFunction(epsilon, maxLevel);
					protected CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
					{
						globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, new ColumnMetaDataResultSetMetaData(new StoredColumnMetaData(false, false, false, false, ResultSetMetaData.columnNoNulls, false, 20, "KPE_Z_CODE", "KPE_Z_CODE", "", 20, 0, "", "", java.sql.Types.JAVA_OBJECT, Types.getSqlTypeName(java.sql.Types.JAVA_OBJECT), true, false, false, "xxl.core.spatial.KPEzCode")));
					}
					
					
					@Override
					public Tuple invoke(Tuple tuple) {
						return new ArrayTuple(pointToKPEzCodeMappingFunction.invoke((Point)tuple.getObject(1)));
					}
					
					@Override
					public CompositeMetaData<Object, Object> getMetaData() {
						return globalMetaData;
					}
				},
				input
			);
		}
	}

	/**
	 * Constructs a new Orenstein join algorithm.
	 *
	 * @param input1 the first input metadata cursor.
	 * @param input2 the second input metadata cursor.
	 * @param joinPredicate the join predicate to use (metadata predicate!).
	 * @param newSorter a function for sorting input cursors.
	 * @param createTuple a factory method generating the desired tuples
	 *        contained in the cursors.
	 * @param initialCapacity the initial capacity of the sweep areas.
	 * @param p fraction of elements to be used from the input.
	 * @param seed the seed to be used for the sampler.
	 * @param epsilon epsilon-distance.
	 * @param maxLevel maximum level of the partitioning.
	 * @param type the join type (see {@link SortMergeJoin.Type Type}).
	 */
	private Orenstein(
		MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input1,
		MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input2,
		MetaDataPredicate<? super Tuple, CompositeMetaData<Object, Object>> joinPredicate,
		Function<? super MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newSorter,
		final Function<Object, ? extends Tuple> createTuple,
		int initialCapacity,
		double p,
		long seed,
		float epsilon,
		int maxLevel,
		Type type
	){
		super(
			newSorter.invoke(
				new FloatPointKPEzCodeMapper(
					new Sampler(
						new Mapper(
							FLOAT_VALUE_TUPLE_TO_FLOAT_POINT_TUPLE,
							input1
						),
						p,
						seed
					),
					epsilon,
					maxLevel
				)
			),
			newSorter.invoke(
				new FloatPointKPEzCodeMapper(
					new Sampler(
						new Mapper(
							FLOAT_VALUE_TUPLE_TO_FLOAT_POINT_TUPLE,
							input2
						),
						p,
						seed
					),
					epsilon,
					maxLevel
				)
			),
			joinPredicate,
			new OrensteinSA<Tuple>(0, initialCapacity, joinPredicate),
			new OrensteinSA<Tuple>(1, initialCapacity, joinPredicate),
			Tuples.getTupleComparator(new int[] {1}),
			new AbstractFunction<Object, Tuple> () {
				@Override
				public Tuple invoke(Object o1, Object o2) {
					Point p1 = (Point)((KPEzCode)o1).getData();
					Point p2 = (Point)((KPEzCode)o2).getData();
					ArrayList<Object> values = new ArrayList<Object>(p1.dimensions() + p2.dimensions());
					for (int i = 0; i < p1.dimensions(); i++)
						values.add(p1.getValue(i));
					for (int i = 0; i < p2.dimensions(); i++)
						values.add(p2.getValue(i));
					return createTuple.invoke(values);
				}
			},
			type
		);
	}
	
	/**
	 * Constructs a new Orenstein join algorithm.
	 *
	 * @param input1 the first input metadata cursor.
	 * @param input2 the second input metadata cursor.
	 * @param joinPredicate the join predicate to use.
	 * @param newSorter a function for sorting input cursors.
	 * @param createTuple a factory method generating the desired tuples
	 *        contained in the cursors.
	 * @param initialCapacity the initial capacity of the sweep areas.
	 * @param p fraction of elements to be used from the input.
	 * @param seed the seed to be used for the sampler.
	 * @param epsilon epsilon-distance.
	 * @param maxLevel maximum level of the partitioning.
	 * @param type the join type (see {@link SortMergeJoin.Type Type}). Here, only the types
	 *        THETA_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN and OUTER_JOIN are
	 *        allowed.
	 */
	public Orenstein(
		MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input1,
		MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> input2,
		Predicate<? super Tuple> joinPredicate,
		Function<? super MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newSorter,
		Function<Object, ? extends Tuple> createTuple,
		int initialCapacity,
		double p,
		long seed,
		float epsilon,
		int maxLevel,
		Type type
	) {
		this(
			input1,
			input2,
			JoinUtils.thetaJoinMetaDataPredicate(
				joinPredicate,
				ResultSetMetaDatas.getResultSetMetaData(input1),
				ResultSetMetaDatas.getResultSetMetaData(input2)
			),
			newSorter,
			createTuple,
			initialCapacity,
			p,
			seed,
			epsilon,
			maxLevel,
			type
		);
		if (type != Type.THETA_JOIN && type != Type.LEFT_OUTER_JOIN && type != Type.RIGHT_OUTER_JOIN && type != Type.OUTER_JOIN)
			throw new IllegalArgumentException ("Undefined type specified in used constructor. Only types THETA_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN and OUTER_JOIN are allowed.");
	}

	/**
	 * Constructs a new Orenstein join algorithm.
	 *
	 * @param input1 the first input result set.
	 * @param input2 the second input result set.
	 * @param joinPredicate the join predicate to use.
	 * @param newSorter a function for sorting input cursors.
	 * @param createTuple a factory method generating the desired tuples
	 *        contained in the cursors.
	 * @param initialCapacity the initial capacity of the sweep areas.
	 * @param p fraction of elements to be used from the input.
	 * @param seed the seed to be used for the sampler.
	 * @param epsilon epsilon-distance.
	 * @param maxLevel maximum level of the partitioning.
	 * @param type the join type (see {@link SortMergeJoin.Type Type}). Here, only the types
	 *        THETA_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN and OUTER_JOIN are
	 *        allowed.
	 */
	public Orenstein(
		ResultSet input1,
		ResultSet input2,
		Predicate<? super Tuple> joinPredicate,
		Function<? super MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>, ? extends MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>>> newSorter,
		Function<Object, ? extends Tuple> createTuple,
		int initialCapacity,
		double p,
		long seed,
		float epsilon,
		int maxLevel,
		Type type
	) {
		this(
			new ResultSetMetaDataCursor(input1),
			new ResultSetMetaDataCursor(input2),
			joinPredicate,
			newSorter,
			createTuple,
			initialCapacity,
			p,
			seed,
			epsilon,
			maxLevel,
			type
		);
	}
}
