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

package xxl.core.relational;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.MetaDataPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.predicates.PredicateMetaDataPredicate;
import xxl.core.predicates.Predicates;
import xxl.core.relational.metaData.AppendedResultSetMetaData;
import xxl.core.relational.metaData.MergedResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.metaData.UnifiedResultSetMetaData;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.ListTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * This class contains various <tt>static</tt> methods for managing result set
 * metadata, creating result tuples and composing predicates during join
 * operations.
 *
 * <p>Most of these methods are used internally by the join operation of this
 * package.</p>
 *
 * <p>This class cannot become instantiated.</p>
 *
 * @see xxl.core.relational.cursors.NestedLoopsJoin
 */
public class JoinUtils {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private JoinUtils() {
		// private access in order to ensure non-instantiability
	}

	/**
	 * Returns a function that builds up a result tuple after performing a
	 * join. To compute the resulting relation, this function has to be called
	 * for every list of tuples that qualify for the result.
	 *
	 * @param tupleFactory a function to create a tuple. The FACTORY_METHOD of
	 *        {@link ArrayTuple ArrayTuple} or {@link ListTuple ListTuple} can
	 *        be used.
	 * @param metadata the metadata information of the resulting relation.
	 * @return a function that builds up a result tuple after performing a
	 *         join.
	 */
	public static Function<Tuple, Tuple> genericJoinTupleFactory(final Function<Object, ? extends Tuple> tupleFactory, final MergedResultSetMetaData metadata) {
		return new AbstractFunction<Tuple, Tuple>() {
			@Override
			public Tuple invoke(List<? extends Tuple> tuples) {
				try {
					int columnCount = metadata.getColumnCount();
					List<Object> tuple = new ArrayList<Object>(columnCount);
					Iterator<int[]> originalColumnIndices;
					int[] originalColumnIndex;
					Tuple temp;
					
					column: for (int i = 1; i <= columnCount; i++) {
						originalColumnIndices = metadata.originalColumnIndices(i);
						while (originalColumnIndices.hasNext())
							if ((temp = tuples.get((originalColumnIndex = originalColumnIndices.next())[0])) != null) {
								tuple.add(temp.getObject(originalColumnIndex[1]));
								continue column;
							}
						tuple.add(null);
					}
					return tupleFactory.invoke(tuple);
				}
				catch (SQLException sqle) {
					throw new MetaDataException("meta data cannot be accessed due to the following sql exception: " + sqle.getMessage());
				}
			}
		};
	}

	/**
	 * Returns a predicate that compares tuples of the joined relations. This
	 * predicate can be passed to the join routines of the cursor package. So
	 * far, this method is used only by methods declared in this class.
	 *
	 * @param metadata the metadata information of the resulting relation.
	 * @return a predicate that compares tuples of the joined relations.
	 */
	public static Predicate<Tuple> naturalJoinPredicate(final MergedResultSetMetaData metadata) {
		return new AbstractPredicate<Tuple>() {
			@Override
			public boolean invoke(List<? extends Tuple> tuples) {
				try {
					Predicate<Object> equals = Predicates.newNullSensitiveEqual(true);
					int columnCount = metadata.getColumnCount();
					Iterator<int[]> originalColumnIndices;
					int[] originalColumnIndex;
					Object object;
					
					for (int i = 1; i <= columnCount; i++) {
						object = tuples.get((originalColumnIndex = (originalColumnIndices = metadata.originalColumnIndices(i)).next())[0]).getObject(originalColumnIndex[1]);
						while (originalColumnIndices.hasNext())
							if (!equals.invoke(object, tuples.get((originalColumnIndex = originalColumnIndices.next())[0]).getObject(originalColumnIndex[1])))
								return false;
					}
					return true;
				}
				catch (SQLException sqle) {
					throw new MetaDataException("meta data cannot be accessed due to the following sql exception: " + sqle.getMessage());
				}
			}
		};
	}

	/**
	 * Returns a metadata predicate for a natural join.
	 *
	 * <p>Consequently, a predicate and the metadata is computed.</p>
	 *
	 * <p>This factory method is used by the
	 * {@link xxl.core.relational.cursors.NestedLoopsJoin nested-loops join}.
	 * There it is used to generate a new metadata predicate that provides the
	 * metadata for the join and decides whether a list of tuple join or
	 * not.</p>
	 *
	 * @param metadata the metadata information of the resulting relation.
	 * @return a metadata predicate for a natural join.
	 */
	public static MetaDataPredicate<Tuple, CompositeMetaData<Object, Object>> naturalJoinMetaDataPredicate(ResultSetMetaData... metadata) {
		UnifiedResultSetMetaData localMetaData = new UnifiedResultSetMetaData(metadata);
		final CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, localMetaData);
		return new PredicateMetaDataPredicate<Tuple, CompositeMetaData<Object, Object>>(naturalJoinPredicate(localMetaData)) {
			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}
		};
	}

	/**
	 * Returns a metadata for a theta join.
	 *
	 * <p>Consequently, the metadata for the join (and the given predicate) is
	 * computed.</p>
	 *
	 * <p>This factory method is used by the
	 * {@link xxl.core.relational.cursors.NestedLoopsJoin nested-loops join}.
	 * There it is used to generate a new metadata predicate that provides the
	 * metadata for the join and decides whether a list of tuple join or
	 * not.</p>
	 *
	 * @param theta the predicate that is used to decide whether a list of
	 *        tuples join or not.
	 * @param metadata the metadata information of the resulting relation.
	 * @return a metadata predicate for a theta join.
	 */
	public static MetaDataPredicate<Tuple, CompositeMetaData<Object, Object>> thetaJoinMetaDataPredicate(Predicate<? super Tuple> theta, ResultSetMetaData... metadata) {
		AppendedResultSetMetaData localMetaData = new AppendedResultSetMetaData(metadata);
		final CompositeMetaData<Object, Object> globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, localMetaData);
		return new PredicateMetaDataPredicate<Tuple, CompositeMetaData<Object, Object>>(theta) {
			@Override
			public CompositeMetaData<Object, Object> getMetaData() {
				return globalMetaData;
			}
		};
	}
	
	/**
	 * Returns a metadata for a semi join.
	 *
	 * <p>Consequently, a predicate and the metadata is computed.</p>
	 *
	 * <p>This factory method is used by the
	 * {@link xxl.core.relational.cursors.NestedLoopsJoin nested-loops join}.
	 * There it is used to generate a new metadata predicate that provides the
	 * metadata for the join and decides whether a list of tuple join or
	 * not.</p>
	 *
	 * @param joinMetaDataPredicate the metadata predicate that is used to
	 *        decide whether a list of tuples join or not and that provides the
	 *        metadata for the semi join.
	 * @param metadata the metadata information of the resulting relation.
	 * @param metadataIndices the indices of the relations that are required
	 *        for building the resulting relation of the semi join.
	 * @return a metadata predicate for a semi join.
	 */
	public static MetaDataPredicate<Tuple, CompositeMetaData<Object, Object>> semiJoinMetaDataPredicate(MetaDataPredicate<Tuple, CompositeMetaData<Object, Object>> joinMetaDataPredicate, ResultSetMetaData[] metadata, int... metadataIndices) {
		ResultSetMetaData[] joinedMetadata = new ResultSetMetaData[metadataIndices.length];
		for (int i = 0; i < metadataIndices.length; i++)
			joinedMetadata[i] = metadata[metadataIndices[i]];
		UnifiedResultSetMetaData localMetaData = new UnifiedResultSetMetaData(joinedMetadata);
		joinMetaDataPredicate.getMetaData().replace(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, localMetaData);
		return joinMetaDataPredicate;
	}


}
