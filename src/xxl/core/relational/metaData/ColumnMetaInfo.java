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

import xxl.core.relational.JavaType;
import xxl.core.relational.RelationalType;
import xxl.core.util.ConvertUtils;

/**
 * This class provides relational meta data for a column in a result set. It is an implementation of
 * {@link xxl.core.relational.metaData.ColumnMetaData} for ready-to-use actions.<br/>
 * <br/>
 * <i>ColumnMetaInfo</i> or <i>ColumnMetaData</i> is used e.g. for defining the schema of a
 * <i>single</i> column for a {@link xxl.core.relational.tuples.Tuple Tuple}. <br/>
 * <br/>
 * <br/>
 * By default <code>ColumnMetaInfo</code> requires information about the columns <b>table</b> and
 * <b>column name</b> and also a {@link xxl.core.relational.RelationalType type} when instancing. <br/>
 * Because <code>ColumnMetaInfo</code> implements <code>ColumnMetaData</code> and is ready-to-use
 * all settings (without table name, column name and type) is set to a default value. You can modify
 * these with getter and setter. <br/>
 * <br/>
 * <b>Default settings</b><br/>
 * A column with <i>default</i> settings is
 * <ul>
 * <li>read only</li>
 * <li><b>not</b> auto incrementable</li>
 * <li><b>not</b> case sensitive</li>
 * <li><b>not</b> searchable</li>
 * <li><b>not</b> a currency type</li>
 * <li><b>not</b> nullable</li>
 * <li><b>not</b> singed</li>
 * <li><b>not</b> writable</li>
 * <li><b>not</b> definitely writable</li>
 * <li>has a column display size of <b>0</b></li>
 * <li>has the <b>same column label</b> as column name</li>
 * <li>has an <b>empty</b> schema name</li>
 * <li>has a precision of <b>0</b></li>
 * <li>has a scale value of <b>0</b></li>
 * <li>has an <b>empty</b> catalog name</li>
 * <li>has a column type name which is the <b>string representation</b> of its
 * <code>RelationalType columnType</code></li>
 * <li>has an <b>empty</b> column class name</li>
 * </ul>
 * 
 * @see xxl.core.relational.metaData.ColumnMetaData
 * @see xxl.core.io.converters.MeasuredTupleConverter
 * @see xxl.core.relational.RelationalType
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class ColumnMetaInfo implements ExtendedColumnMetaData {

  /*
   * Required field to implement getter/setter of ColumnMetaData interface
   */
  private boolean autoIncrementEnabled = false;
  private boolean caseSensitive = false;
  private String catalogName;
  private int catalogType = 0;
  private String columnClassName;
  private int columnDisplaySize = 0;
  private String columnLabel;
  private String columnName;
  private RelationalType columnType;
  private String columnTypeName;
  private boolean currency = false;
  private boolean definitelyWritable = false;
  private int nullable = 0;
  private int precision = 0;
  private boolean readOnly = true;
  private int scale = 0;
  private String schemaName;
  private boolean searchable = false;
  private boolean singed = false;
  private int stringLength = -1;
  private boolean writable = false;

  /**
   * Constructs the column meta info and sets a column type which is a relational type, a column
   * name and a table name which contains this column.<br/>
   * <br/>
   * 
   * <b>Note:</b> If the column content should <b>not</b> be a <code>String</code> use
   * {@link ColumnMetaInfo#ColumnMetaInfo(RelationalType, int, String)}
   * 
   * @param columnType A relational (SQL) data type. Please note, that you can use every
   *        {@link java.sql.Types SQL/JDBC type} but e.g.
   *        {@link xxl.core.io.converters.MeasuredTupleConverter TupleConverter} does not support
   *        all.
   * @param columnName A name for this column.
   * 
   * @see xxl.core.util.ConvertUtils#toRelationalType(int) if you want to use
   *      {@link java.sql.Types} instead of <code>RelationalType</code>
   * @see xxl.core.io.converters.MeasuredTupleConverter
   */
  public ColumnMetaInfo(RelationalType columnType, int stringLength,
      String columnName) {
    super();
    if (ConvertUtils.toJavaType(columnType).equals(JavaType.STRING)) {
      this.columnType = columnType;
      this.columnName = columnName;
      this.columnLabel = columnName;
      this.columnTypeName = columnType.toString();
      this.stringLength = stringLength;
    } else {
      throw new RuntimeException(
          "String length ony available for java string type");
    }

  }

  /**
   * Constructs the column meta info and sets a column type which is a relational type, a column
   * name and a table name which contains this column.<br/>
   * <br/>
   * 
   * <b>Note:</b> If the column content should be a <code>String</code> use
   * {@link ColumnMetaInfo#ColumnMetaInfo(RelationalType, int, String)}
   * 
   * @param columnType A relational (SQL) data type. Please note, that you can use every
   *        {@link java.sql.Types SQL/JDBC type} but e.g.
   *        {@link xxl.core.io.converters.MeasuredTupleConverter TupleConverter} does not support
   *        all.
   * @param columnName A name for this column.
   * 
   * @see xxl.core.util.ConvertUtils#toRelationalType(int) if you want to use
   *      {@link java.sql.Types} instead of <code>RelationalType</code>
   * @see xxl.core.io.converters.MeasuredTupleConverter
   */
  public ColumnMetaInfo(RelationalType columnType, String columnName) {
    super();

    if (!ConvertUtils.toJavaType(columnType).equals(JavaType.STRING)) {
      this.columnType = columnType;
      this.columnName = columnName;
      this.columnLabel = columnName;
      this.columnTypeName = columnType.toString();
    } else {
      throw new RuntimeException(
          "For string columns you have to specify the length");
    }
  }

  /*
   * Check if a column is declared as string column. This is needed for calculating the size for I/O
   * operations through Converts
   */
  private void checkColumnIsStringType(RelationalType type, int stringLength) {
    if (stringLength < 1
        || !ConvertUtils.toJavaType(type).equals(JavaType.STRING))
      throw new IllegalArgumentException(
          "Column type not a string type or string length is less than 1 character.");
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getCatalogName()
   */
  @Override
  public String getCatalogName() throws SQLException {
    return catalogName;
  }

  /**
   * Gets the catalog type
   * 
   * @return catalogType
   */
  public int getCatalogType() {
    return catalogType;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getColumnClassName()
   */
  @Override
  public String getColumnClassName() throws SQLException {
    return columnClassName;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getColumnDisplaySize()
   */
  @Override
  public int getColumnDisplaySize() throws SQLException {
    return columnDisplaySize;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getColumnLabel()
   */
  @Override
  public String getColumnLabel() throws SQLException {
    return columnLabel;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getColumnName()
   */
  @Override
  public String getColumnName() throws SQLException {
    return columnName;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getColumnType()
   */
  @Override
  public int getColumnType() throws SQLException {
    return ConvertUtils.toInt(columnType);
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getColumnTypeName()
   */
  @Override
  public String getColumnTypeName() throws SQLException {
    return columnTypeName;
  }

  /**
   * Retrieves this column's SQL type as {@link xxl.core.relational.RelationalType RelationalType}
   * 
   * @return SQL type from {@link xxl.core.relational.RelationalType RelationalType}
   * @throws SQLException if a database access error occurs.
   * 
   * @see xxl.core.relational.RelationalType
   * @see xxl.core.util.ConvertUtils#toRelationalType(int)
   * @see xxl.core.util.ConvertUtils#toInt(RelationalType)
   */
  public RelationalType getColumnTypeRelational() {
    return columnType;
  }

  /**
   * @see ExtendedColumnMetaData#getMaxContainingStringLength()
   */
  @Override
  public int getMaxContainingStringLength() {
    return stringLength;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getPrecision()
   */
  @Override
  public int getPrecision() throws SQLException {
    return precision;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getScale()
   */
  @Override
  public int getScale() throws SQLException {
    return scale;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getSchemaName()
   */
  @Override
  public String getSchemaName() throws SQLException {
    return schemaName;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#getTableName()
   */
  @Override
  public String getTableName() throws SQLException {
    throw new UnsupportedOperationException("Table in accessable here.");
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isAutoIncrement()
   */
  @Override
  public boolean isAutoIncrement() throws SQLException {
    return autoIncrementEnabled;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isCaseSensitive()
   */
  @Override
  public boolean isCaseSensitive() throws SQLException {
    return caseSensitive;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isCurrency()
   */
  @Override
  public boolean isCurrency() throws SQLException {
    return currency;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isDefinitelyWritable()
   */
  @Override
  public boolean isDefinitelyWritable() throws SQLException {
    return definitelyWritable;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isNullable()
   */
  @Override
  public int isNullable() throws SQLException {
    return nullable;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isReadOnly()
   */
  @Override
  public boolean isReadOnly() throws SQLException {
    return readOnly;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isSearchable()
   */
  @Override
  public boolean isSearchable() throws SQLException {
    return searchable;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isSigned()
   */
  @Override
  public boolean isSigned() throws SQLException {
    return singed;
  }

  /**
   * @see xxl.core.relational.metaData.ColumnMetaData#isWritable()
   */
  @Override
  public boolean isWritable() throws SQLException {
    return writable;
  }

  /**
   * Set or unset this column as auto incrementable
   */
  public void setAutoIncrementEnabled(boolean autoIncrementEnabled) {
    this.autoIncrementEnabled = autoIncrementEnabled;
  }

  /**
   * Set or unset this column as case sensitive
   */
  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  /**
   * Sets the columns catalog name
   */
  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  /**
   * Sets the catalog type
   */
  public void setCatalogType(int catalogType) {
    this.catalogType = catalogType;
  }

  /**
   * Sets the column class name
   */
  public void setColumnClassName(String columnClassName) {
    this.columnClassName = columnClassName;
  }

  /**
   * Sets the column display size
   */
  public void setColumnDisplaySize(int columnDisplaySize) {
    this.columnDisplaySize = columnDisplaySize;
  }

  /**
   * Sets the column label
   */
  public void setColumnLabel(String columnLabel) {
    this.columnLabel = columnLabel;
  }

  /**
   * Sets the column name
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  /**
   * Sets the column type with a relational (jdbc) type
   * 
   * @see xxl.core.relational.RelationalType
   * @see xxl.core.util.ConvertUtils#toInt(RelationalType)
   * @see xxl.core.util.ConvertUtils#toRelationalType(int)
   */
  public void setColumnType(RelationalType columnType) {
    this.columnType = columnType;
  }

  /**
   * Sets the columns type name. By default it is the string representation of the
   * {@link xxl.core.relational.RelationalType RelationalType} you set for this column within the
   * constructor.
   */
  public void setColumnTypeName(String columnTypeName) {
    this.columnTypeName = columnTypeName;
  }

  /**
   * Set or unset this column as a currency column
   */
  public void setCurrency(boolean currency) {
    this.currency = currency;
  }

  /**
   * Enables or disables the column definitely writable mode
   */
  public void setDefinitelyWritable(boolean definitelyWritable) {
    this.definitelyWritable = definitelyWritable;
  }

  /**
   * @see ExtendedColumnMetaData#setMaxContainingStringLength(int)
   */
  @Override
  public void setMaxContainingStringLength(int stringLength) {
    checkColumnIsStringType(this.getColumnTypeRelational(), stringLength);
    this.stringLength = stringLength;
  }

  /**
   * Mark or demarcate this column as nullable
   * 
   * @param nullable
   */
  public void setNullable(int nullable) {
    this.nullable = nullable;
  }

  /**
   * Sets the column precision
   */
  public void setPrecision(int precision) {
    this.precision = precision;
  }

  /**
   * Enables or disables the column read only mode
   */
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * Sets the column scale
   */
  public void setScale(int scale) {
    this.scale = scale;
  }

  /**
   * Sets the column schema name
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Set or unset this column as searchable
   */
  public void setSearchable(boolean searchable) {
    this.searchable = searchable;
  }

  /**
   * Set or unset this column as signed
   */
  public void setSinged(boolean singed) {
    this.singed = singed;
  }

  /**
   * Enables or disables the column writable mode
   */
  public void setWritable(boolean writable) {
    this.writable = writable;
  }

  /**
   * Writes the column name followed by column type into string
   */
  @Override
  public String toString() {
    return columnName + ": " + columnType;
  }

}
