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

package xxl.core.relational.metaData;

import java.sql.SQLException;
import java.util.Arrays;

import xxl.core.io.converters.meta.ExtendedResultSetMetaData;

/**
 * An object that can be used to get and set information about the types and properties of the
 * columns inside a tuple. It is a ready-to-use implementation of {@link java.sql.ResultSetMetaData}
 * . <br/>
 * <br/>
 * Please note that <tt>TupleMetaData</tt> contains meta data for each component.
 * 
 * @see xxl.core.relational.metaData.ColumnMetaData
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class TupleMetaData implements ExtendedResultSetMetaData {

  /*
   * The column meta data for each column/component of a tuple
   */
  private ExtendedColumnMetaData[] mColumnMetaDatas = null;
  /*
   * The table name for this
   */
  private String mTableName;

  /**
   * Setup up the columns meta data
   * 
   * @param tableName The table name
   * @param columnMetaDatas An array of meta data. The <i>n</i>-th entry in the array refers to the
   *        <i>n</i>-th component of the tuple
   */
  public TupleMetaData(String tableName,
      ExtendedColumnMetaData[] columnMetaDatas) {
    if (columnMetaDatas == null) throw new NullPointerException();

    mColumnMetaDatas = columnMetaDatas;
    mTableName = tableName;
  }

  /**
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getCatalogName();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getColumnClassName();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnCount()
   */
  @Override
  public int getColumnCount() throws SQLException {
    return mColumnMetaDatas.length;
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getColumnDisplaySize();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getColumnLabel();
  }

  @Override
  public ExtendedColumnMetaData getColumnMetaData(int column) {
    return mColumnMetaDatas[column - 1];
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getColumnName();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   * @see xxl.core.util.ConvertUtils#toRelationalType(int)
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getColumnType();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getColumnTypeName();
  }

  @Override
  public int getContentLength(int column) {
    return mColumnMetaDatas[column - 1].getMaxContainingStringLength();
  }

  /**
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getPrecision();
  }

  /**
   * @see java.sql.ResultSetMetaData#getScale(int)
   */
  @Override
  public int getScale(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getScale();
  }

  /**
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].getSchemaName();
  }

  /**
   * Returns the table name given in {@link #TupleMetaData(String, ExtendedColumnMetaData[])}
   * constructor.
   * 
   * @return the table name
   */
  public String getTableName() {
    return mTableName;
  }

  /**
   * Always returns the table name given in {@link #TupleMetaData(String, ExtendedColumnMetaData[])}
   * constructor
   * 
   * @see java.sql.ResultSetMetaData#getTableName(int)
   * @see #getTableName()
   */
  @Override
  public String getTableName(int column) throws SQLException {
    return mTableName;
  }

  /**
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isAutoIncrement();
  }

  /**
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isCaseSensitive();
  }

  /**
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isCurrency();
  }

  /**
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isDefinitelyWritable();
  }

  /**
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */
  @Override
  public int isNullable(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isNullable();
  }

  /**
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isReadOnly();
  }

  /**
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isSearchable();
  }

  /**
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isSigned();
  }

  /**
   * <b>This method is not supported yet</b>
   * 
   * @see java.sql.ResultSetMetaData#isWrapperFor(Class)
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    return mColumnMetaDatas[column - 1].isWritable();
  }

  @Override
  public String toString() {
    return Arrays.toString(mColumnMetaDatas);
  }

  /**
   * <b>This method is not supported yet</b>
   * 
   * @see java.sql.ResultSetMetaData#unwrap(Class)
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
