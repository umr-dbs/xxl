/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
 * of Mathematics and Computer Science University of Marburg Germany
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * If not, see <http://www.gnu.org/licenses/>.
 * 
 * http://code.google.com/p/xxl/
 */

package xxl.core.relational.tuples;

import java.io.PrintStream;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;

import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.relational.metaData.ExtendedColumnMetaData;
import xxl.core.relational.metaData.TupleMetaData;
import xxl.core.util.Arrays;

/**
 * The <code>ComparableTuples</code> class provides various algebraic or useful methods for dealing with
 * {@link Tuple tuple objects} .
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class ComparableTuples {

  /*
   * Check if an IndexOutOfBoundException would be occur and throw an exception if it is so
   */
  private static void checkIndexIsInBounds(int[] destProjectionIndizies,
      int srcColumnCount) throws IllegalArgumentException {

    for (int column : destProjectionIndizies) {
      if (column < 1 || column > srcColumnCount)
        throw new IllegalArgumentException(
            "Projection contains invalid column index. Column index is smaller than 1 or out of bounds (value is \""
                + column + "\")");
    }
  }

  /*
   * Check if the array of column indices is not empty or throw an exception
   */
  private static void checkProjectionIsNotEmpty(int[] destProjectionIndizies)
      throws IllegalArgumentException {

    if (destProjectionIndizies.length == 0)
      throw new IllegalArgumentException(
          "Projection indizies array have to contain at least one column index.");
  }

  /*
   * Check if tuple dimension of source is smaller than of the destination or throw an exception
   */
  private static void checkProjectionLength(int src, int dest)
      throws IndexOutOfBoundsException {

    if (src < dest)
      throw new IndexOutOfBoundsException(
          "Projection for tuple contains more columns than the source (#col src: "
              + src + ", #col dest: " + dest + ").");
  }

  /*
   * Prevent users to double columns by projection twice to the same column. If the column indices
   * are not unique throw an exception
   */
  private static ArrayList<ExtendedColumnMetaData> checkUniqueIndexAndProject(
      int[] destProjectionIndizies, ExtendedResultSetMetaData srcMetaData) {

    ArrayList<ExtendedColumnMetaData> subetColumnMetaData = new ArrayList<>();
    ArrayList<Integer> accPreviousProjectionIndizies = new ArrayList<>();

    for (int column : destProjectionIndizies) {
      if (!accPreviousProjectionIndizies.contains(column)) {
        subetColumnMetaData.add(srcMetaData.getColumnMetaData(column));
        accPreviousProjectionIndizies.add(column);
      } else
        throw new IllegalArgumentException(
            "Projection for meta data contains at least one column index twice (requested column is "
                + column
                + "). A column index have to be unique for projection operation.");
    }

    return subetColumnMetaData;
  }

  /*
   * Prevent users to double columns by projection twice to the same column. If the column indices
   * are not unique throw an exception
   */
  private static ArrayList<Object> checkUniqueIndexAndProject(
      int[] destProjectionIndizies, Tuple tuple) {

    ArrayList<Object> content = new ArrayList<Object>();
    ArrayList<Integer> accPreviousProjectionIndizies = new ArrayList<>();

    for (int column : destProjectionIndizies) {
      if (!accPreviousProjectionIndizies.contains(column)) {
        content.add(tuple.getObject(column));
        accPreviousProjectionIndizies.add(column);
      } else
        throw new IllegalArgumentException(
            "Projection for tuple contains at least one column index twice (requested column is "
                + column
                + "). A column index have to be unique for projection operation.");
    }

    return content;
  }

  /**
   * Creates a deep and hard copy of a tuple object <i>t</i>. Neither the pointer to the copy of
   * <i>t</i> nor the pointer of <i>t</i>'s components are identical to the original object
   * <i>t</i>. This ensures modifications of <i>t</i> will <b>not</b> effect it's copies.
   * 
   * @param t The tuple to copy
   * @return A tuple identical to <i>t</i> which is independent of <i>t</i> with respect to
   *         <i>t</i>'s references.
   */
  public static ColumnComparableTuple clone(ColumnComparableTuple t) {
    Object[] content = t.toArray();
    Object[] clone = new Object[content.length];

    for (int i = 0; i < clone.length; i++) {
      Object element = content[i];

      if (element instanceof String)
        clone[i] = new String(String.valueOf(element));
      else if (element instanceof BigInteger)
        clone[i] = new BigInteger(((BigInteger) element).toByteArray());
      else if (element instanceof Boolean)
        clone[i] =
            new Boolean(Boolean.valueOf(((Boolean) element).booleanValue()));
      else if (element instanceof Byte)
        clone[i] = new Byte(Byte.valueOf(((Byte) element).byteValue()));
      else if (element instanceof Date)
        clone[i] = new Date(((Date) element).getTime());
      else if (element instanceof Double)
        clone[i] = new Double(Double.valueOf(((Double) element).doubleValue()));
      else if (element instanceof Float)
        clone[i] = new Float(Float.valueOf(((Float) element).floatValue()));
      else if (element instanceof Integer)
        clone[i] = new Integer(Integer.valueOf(((Integer) element).intValue()));
      else if (element instanceof Long)
        clone[i] = new Long(Long.valueOf(((Long) element).longValue()));
      else if (element instanceof Short)
        clone[i] = new Short(Short.valueOf(((Short) element).shortValue()));
      else if (element instanceof Time)
        clone[i] = new Time(((Time) element).getTime());
      else if (element instanceof Timestamp)
        clone[i] = new Timestamp(((Timestamp) element).getTime());
      else
        throw new UnsupportedOperationException(
            "Not implemented yet for given class. ("
                + element.getClass().getName() + ")");
    }

    return new ColumnComparableArrayTuple(clone);
  }

  /**
   * Creates a deep and hard copy of a tuple object <i>t</i>. Neither the pointer to the copy of
   * <i>t</i> nor the pointer of <i>t</i>'s components are identical to the original object
   * <i>t</i>. This ensures modifications of <i>t</i> will <b>not</b> effect it's copies.
   * 
   * @param t The tuple to copy
   * @return A tuple identical to <i>t</i> which is independent of <i>t</i> with respect to
   *         <i>t</i>'s references.
   */
  public static Tuple clone(Tuple t) {
    Object[] content = t.toArray();
    Object[] clone = new Object[content.length];

    for (int i = 0; i < clone.length; i++) {
      Object element = content[i];

      if (element instanceof String)
        clone[i] = new String(String.valueOf(element));
      else if (element instanceof BigInteger)
        clone[i] = new BigInteger(((BigInteger) element).toByteArray());
      else if (element instanceof Boolean)
        clone[i] =
            new Boolean(Boolean.valueOf(((Boolean) element).booleanValue()));
      else if (element instanceof Byte)
        clone[i] = new Byte(Byte.valueOf(((Byte) element).byteValue()));
      else if (element instanceof Date)
        clone[i] = new Date(((Date) element).getTime());
      else if (element instanceof Double)
        clone[i] = new Double(Double.valueOf(((Double) element).doubleValue()));
      else if (element instanceof Float)
        clone[i] = new Float(Float.valueOf(((Float) element).floatValue()));
      else if (element instanceof Integer)
        clone[i] = new Integer(Integer.valueOf(((Integer) element).intValue()));
      else if (element instanceof Long)
        clone[i] = new Long(Long.valueOf(((Long) element).longValue()));
      else if (element instanceof Short)
        clone[i] = new Short(Short.valueOf(((Short) element).shortValue()));
      else if (element instanceof Time)
        clone[i] = new Time(((Time) element).getTime());
      else if (element instanceof Timestamp)
        clone[i] = new Timestamp(((Timestamp) element).getTime());
      else
        throw new UnsupportedOperationException(
            "Not implemented yet for given class. ("
                + element.getClass().getName() + ")");
    }

    return new ArrayTuple(clone);
  }

  /**
   * Prints a given tuple to the <code>PrintStream</code> and adds a new line.
   * 
   * @param s PrintStream
   * @param tuple Tuple to be printed
   */
  public static void println(PrintStream s, Tuple tuple) {
    Object[] content = tuple.toArray();
    Arrays.println(content, s);

  }

  /**
   * An unary operation where <b>columns</b> is an array of column indices. The result of this
   * operation is a subset of <b>metaData</b> in which other attributes (column indices) than
   * <b>columns</b> are discarded (or excluded). <br/>
   * <br/>
   * 
   * <b>Note:</b> The first column index is <b>1</b> not <i>0</i>.
   * 
   * @param metaData The source meta data
   * @param columns Array of indices which represents attribute columns and which should be keep in
   *        the result.
   * @return A subset of <b>metaData</b> which is clipped to the given columns.
   * 
   *         <b>Note: </b> This operation does not change the original object. Instead it returns a
   *         new object which is a subset of the original object.
   * 
   * @throws IllegalArgumentException If <b>columns</b> is empty or at least one components is
   *         lesser than 1. This exception will be also thrown if a projection column is set twice
   *         or more in <b>columns</b>.
   * 
   * @throws IndexOutOfBoundsException If <b>columns</b> contains more (unique) indices than the
   *         dimension of this tuple is or if at least one component of <b>columns</b> is out of
   *         bounds.
   * 
   * @throws SQLException If it fails to work with the underlying {@link java.sql.ResultSetMetaData}
   */
  public static ExtendedResultSetMetaData project(
      final ExtendedResultSetMetaData metaData, final int[] columns)
      throws SQLException, IllegalArgumentException, IndexOutOfBoundsException {

    int srcTupleDimension = metaData.getColumnCount();
    int desTupleDimension = columns.length;

    checkProjectionIsNotEmpty(columns);
    checkProjectionLength(srcTupleDimension, desTupleDimension);
    checkIndexIsInBounds(columns, srcTupleDimension);

    ArrayList<ExtendedColumnMetaData> columnSubsetMetaData =
        checkUniqueIndexAndProject(columns, metaData);
    return new TupleMetaData(metaData.getTableName(0),
        columnSubsetMetaData
            .toArray(new ExtendedColumnMetaData[columnSubsetMetaData.size()]));
  }

  /**
   * An unary operation where <b>columns</b> is an array of column indices. The result of this
   * operation is a subset of <b>tuple</b> in which other attributes (column indices) than
   * <b>columns</b> are discarded (or excluded). <br/>
   * <br/>
   * 
   * <b>Note:</b> The first column index is <b>1</b> not <i>0</i>.
   * 
   * @param tuple The source tuple
   * @param columns Array of indices which represents attribute columns and which should be keep in
   *        the result.
   * @return A subset of <b>tuple</b> which is clipped to the given columns.
   * 
   *         <b>Note: </b> This operation does not change the original object. Instead it returns a
   *         new object which is a subset of the original object.
   * 
   * @throws IllegalArgumentException If <b>columns</b> is empty or at least one components is
   *         lesser than 1. This exception will be also thrown if a projection column is set twice
   *         or more in <b>columns</b>.
   * 
   * @throws IndexOutOfBoundsException If <b>columns</b> contains more (unique) indices than the
   *         dimension of this tuple is or if at least one component of <b>columns</b> is out of
   *         bounds.
   */
  public static Tuple project(final Tuple tuple, final int[] columns)
      throws IllegalArgumentException, IndexOutOfBoundsException {
    int srcTupleDimension = tuple.getColumnCount();
    int desTupleDimension = columns.length;

    checkProjectionIsNotEmpty(columns);
    checkProjectionLength(srcTupleDimension, columns.length);
    checkIndexIsInBounds(columns, srcTupleDimension);

    ArrayList<Object> content = checkUniqueIndexAndProject(columns, tuple);

    return new ArrayTuple(content.toArray(new Object[desTupleDimension]));
  }
}
