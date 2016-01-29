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

package xxl.core.relational.tuples;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import xxl.core.comparators.ComparableComparator;
import xxl.core.comparators.Comparators;
import xxl.core.comparators.FeatureComparator;
import xxl.core.comparators.InverseComparator;
import xxl.core.comparators.LexicographicalComparator;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.AbstractMetaDataFunction;
import xxl.core.functions.Function;
import xxl.core.functions.MetaDataFunction;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;

/**
 * This class contains various useful <tt>static</tt> methods for Tuples.
 * 
 * <p<This includes the methods for comparing tuples, accessing columns,
 * unpacking into an object array and calculating the size.</p>
 *
 * <p>This class cannot be instantiated.</p>
 */
public class Tuples {
	
	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Tuples() {
		// private access in order to ensure non-instantiability
	}
	
	/**
	 * Returns a metadata function with the given metadata that takes lists of
	 * objects and returns tuples.
	 * 
	 * @param metadata the relational metadata of the constructed tuples.
	 * @return a metadata function with the given metadata that takes lists of
	 *         objects and returns tuples.
	 */
	public MetaDataFunction<Object, Tuple, ResultSetMetaData> getTupleFactory(final ColumnMetaData... metadata) {
		return new AbstractMetaDataFunction<Object, Tuple, ResultSetMetaData>() {
			protected ResultSetMetaData resultSetMetaData = new ColumnMetaDataResultSetMetaData(metadata);

			@Override
			public Tuple invoke(List<? extends Object> args) {
				if (args.size() != metadata.length)
					throw new IllegalArgumentException("incorrect number of objects");
				return new ArrayTuple(args.toArray());
			}
			
			@Override
			public ResultSetMetaData getMetaData() {
				return resultSetMetaData;
			}
		};
	}

	/**
	 * Returns a comparator that compares two tuples of different type
	 * according to a subset of column indices.
	 * 
	 * <p>The comparison uses a lexicographical ordering of the column objects.
	 * The objects themselves have to be comparable, otherwise a
	 * <code>ClassCastException</code> will be thrown. The comparison can be
	 * processed in ascending or descending order.</p>
	 *
	 * @param columns1 an array of column indices of the first type of tuple:
	 *        the first column is 1, the second is 2, ...
	 * @param columns2 an array of column indices of the second type of tuple:
	 *        the first column is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 * @param caseInsensiveStringorderingColumns determines string columns that
	 *        are compared using a case insensitive ordering. These indices
	 *        have to be chosen according to the first type of tuple. If a
	 *        string column is not listed here, the comparison is case
	 *        sensitive. It is convenient to use the method
	 *        {@link xxl.core.relational.metaData.ResultSetMetaDatas#getStringColumns(java.sql.ResultSetMetaData)}.
	 * @return a comparator that compares two tuples of different type
	 *         according to a subset of column indices.
	 */
	public static Comparator<Tuple> getTupleComparator(int[] columns1, int[] columns2, boolean[] ascending, int[] caseInsensiveStringorderingColumns) {
		if (columns1.length == 0)
			throw new IllegalArgumentException("no columns specified");
		if (columns1.length != columns2.length || columns1.length != ascending.length)
			throw new IllegalArgumentException("length of specified parameter arrays does not match");
			
		java.util.Arrays.sort(caseInsensiveStringorderingColumns);
		
		List<Comparator<Tuple>> tupleComparators = new ArrayList<Comparator<Tuple>>(columns1.length);
		Comparator<Tuple> tupleComparator;
		
		for (int i = 0; i < columns1.length; i++) {
			tupleComparator = new FeatureComparator<Object, Tuple>(
				Comparators.newNullSensitiveComparator(
					java.util.Arrays.binarySearch(caseInsensiveStringorderingColumns, columns1[i]) < 0 ?
						Comparators.getObjectComparator(new ComparableComparator()) :
						Comparators.getObjectComparator(String.CASE_INSENSITIVE_ORDER)
				),
				getObjectFunction(columns1[i]),
				getObjectFunction(columns2[i])
			);
			tupleComparators.add(ascending[i] ? tupleComparator : new InverseComparator<Tuple>(tupleComparator));
		}
		return new LexicographicalComparator<Tuple>(tupleComparators.toArray(new Comparator[tupleComparators.size()]));
	}

	/**
	 * Returns a comparator that compares two tuples of different type
	 * according to a subset of column indices.
	 * 
	 * <p>The comparison uses a lexicographical ordering of the column objects.
	 * The objects themselves have to be comparable, otherwise a
	 * <code>ClassCastException</code> will be thrown. The comparison can be
	 * processed in ascending or descending order.</p>
	 *
	 * @param columns1 an array of column indices of the first type of tuple:
	 *        the first column is 1, the second is 2, ...
	 * @param columns2 an array of column indices of the second type of tuple:
	 *        the first column is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 * @return a comparator that compares two tuples of different type
	 *         according to a subset of column indices.
	 */
	public static Comparator<Tuple> getTupleComparator(int[] columns1, int[] columns2, boolean[] ascending) {
		return getTupleComparator(
			columns1,
			columns2,
			ascending,
			new int[0]
		);
	}

	/**
	 * Returns a comparator that compares two tuples of different type
	 * according to a subset of column indices.
	 * 
	 * <p>The comparison uses a lexicographical ordering of the column objects.
	 * The objects themselves have to be comparable, otherwise a
	 * <code>ClassCastException</code> will be thrown.</p>
	 *
	 * @param columns1 an array of column indices of the first type of tuple:
	 *        the first column is 1, the second is 2, ...
	 * @param columns2 an array of column indices of the second type of tuple:
	 *        the first column is 1, the second is 2, ...
	 * @return a comparator that compares two tuples of different type
	 *         according to a subset of column indices.
	 */
	public static Comparator<Tuple> getTupleComparator(int[] columns1, int[] columns2) {
		return getTupleComparator(
			columns1,
			columns2,
			xxl.core.util.Arrays.newBooleanArray(columns1.length, true)
		);
	}

	/**
	 * Returns a comparator that compares two tuples of the same type according
	 * to a subset of column indices.
	 * 
	 * <p>The comparison uses a lexicographical ordering of the column objects.
	 * The objects themselves have to be comparable, otherwise a
	 * <code>ClassCastException</code> will be thrown. The comparison can be
	 * processed in ascending or descending order.</p>
	 *
	 * @param columns an array of column indices of the tuple: the first column
	 *        is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 * @param caseInsensiveStringorderingColumns determines string columns that
	 *        are compared using a case insensitive ordering. These indices
	 *        have to be chosen according to the first type of tuple. If a
	 *        string column is not listed here, the comparison is case
	 *        sensitive. It is convenient to use the method
	 *        {@link xxl.core.relational.metaData.ResultSetMetaDatas#getStringColumns(java.sql.ResultSetMetaData)}.
	 * @return a comparator that compares two tuples of different type
	 *         according to a subset of column indices.
	 */
	public static Comparator<Tuple> getTupleComparator(int[] columns, boolean[] ascending, int[] caseInsensiveStringorderingColumns) {
		return getTupleComparator(columns, columns, ascending, caseInsensiveStringorderingColumns);
	}

	/**
	 * Returns a comparator that compares two tuples of the same type according
	 * to a subset of column indices.
	 * 
	 * <p>The comparison uses a lexicographical ordering of the column objects.
	 * The objects themselves have to be comparable, otherwise a
	 * <code>ClassCastException</code> will be thrown. The comparison can be
	 * processed in ascending or descending order.</p>
	 *
	 * @param columns an array of column indices of the tuple: the first column
	 *        is 1, the second is 2, ...
	 * @param ascending an array of <code>boolean</code> values that determines
	 *        the order (<tt>ascending=true</tt>/<tt>descending=false</tt>) for
	 *        each dimension.
	 * @return a comparator that compares two tuples of different type
	 *         according to a subset of column indices.
	 */
	public static Comparator<Tuple> getTupleComparator(int[] columns, boolean[] ascending) {
		return getTupleComparator(
			columns,
			ascending,
			new int[0]
		);
	}

	/**
	 * Returns a comparator that compares two tuples of the same type according
	 * to a subset of column indices.
	 * 
	 * <p>The comparison uses a lexicographical ordering of the column objects.
	 * The objects themselves have to be comparable, otherwise a
	 * <code>ClassCastException</code> will be thrown.</p>
	 *
	 * @param columns an array of column indices of the tuple: the first column
	 *        is 1, the second is 2, ...
	 * @return a comparator that compares two tuples of different type
	 *         according to a subset of column indices.
	 */
	public static Comparator<Tuple> getTupleComparator(int... columns) {
		return getTupleComparator(
			columns,
			xxl.core.util.Arrays.newBooleanArray(columns.length, true)
		);
	}

	/**
	 * Returns a function that accesses a column of a tuple parameter.
	 *
	 * <p>The function domains: <pre>Tuple &rarr; Object (column)</pre></p>
	 *
	 * @param columnIndex index of the column: the first column is 1, the
	 *        second is 2, ...
	 * @return the function that accesses the columns.
	 */
	public static Function<Tuple, Object> getObjectFunction(final int columnIndex) {
		return new AbstractFunction<Tuple, Object>() {
			@Override
			public Object invoke(Tuple tuple) {
				try {
					return tuple.getObject(columnIndex);
				}
				catch (IndexOutOfBoundsException e) {
					throw new IllegalStateException("required column index is invalid: " + e.getMessage());
				}
			}
		};
	}
	
	/**
	 * Maps the elements of the input cursor to array-tuples.
	 *
	 * @param cursor the input cursor delivering the objects.
	 * @return a cursor containing the tuples.
	 */
	public static Cursor<Tuple> mapObjectsToTuples(Cursor<? extends Object> cursor) {
		return new Mapper<Object, Tuple>(
			new AbstractFunction<Object, ArrayTuple>() {
				@Override
				public ArrayTuple invoke(List<? extends Object> arguments) {
					return ArrayTuple.FACTORY_METHOD.invoke(arguments);
				}
			},
			cursor
		);	
	}
	
}
