/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2014 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
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

package xxl.core.relational.schema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import xxl.core.relational.RelationalType;
import xxl.core.relational.metaData.ColumnMetaInfo;

/**
 * TODO: Comment
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class Schema {

  private List<ColumnMetaInfo> mColumns = new ArrayList<>();
  /**
   * TODO: Comment
   */
  private String mSchemaName;

  // TODO: Comment
  public Schema(String schemaName) {
    mSchemaName = schemaName;
  }

  // TODO: Comment
  public Schema addArray(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.ARRAY, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addBigInt(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.BIGINT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addBinary(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.BINARY, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addBit(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.BIT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addBlob(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.BLOB, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addBoolean(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.BOOLEAN, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addChar(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.CHAR, 1, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addClob(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.CLOB, columnName));
    return this;
  }

  // TODO: Comment
  private void addColumn(ColumnMetaInfo column) throws SQLException {
    for (ColumnMetaInfo columnCursor : mColumns)
      if (columnCursor.getColumnName().toLowerCase().trim()
          .equals(column.getColumnName().toLowerCase().trim()))
        throw new IllegalArgumentException("Column \"" + column.getColumnName()
            + "\" already exists.");

    mColumns.add(column);
  }

  // TODO: Comment
  public Schema addDataLink(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.DATALINK, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addDate(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.DATE, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addDecimal(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.DECIMAL, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addDistinct(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.DISTINCT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addDouble(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.DOUBLE, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addFloat(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.FLOAT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addInteger(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.INTEGER, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addJavaObject(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.JAVA_OBJECT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addLongNVarChar(String columnName, int length)
      throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.LONGNVARCHAR, length,
        columnName));
    return this;
  }

  // TODO: Comment
  public Schema addLongVarBinary(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.LONGVARBINARY, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addLongVarChar(String columnName, int length)
      throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.LONGVARCHAR, length, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addNChar(String columnName, int n) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.NCHAR, n, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addNClob(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.NCLOB, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addNull(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.NULL, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addNumeric(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.NUMERIC, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addNVarChar(String columnName, int length) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.NVARCHAR, length, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addOther(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.OTHER, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addReal(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.REAL, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addRef(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.REF, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addRowId(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.ROWID, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addSmallInt(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.SMALLINT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addSqlXml(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.SQLXML, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addStruct(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.STRUCT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addTime(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.TIME, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addTimestamp(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.TIMESTAMP, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addTinyInt(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.TINYINT, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addVarBinary(String columnName) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.VARBINARY, columnName));
    return this;
  }

  // TODO: Comment
  public Schema addVarChar(String columnName, int length) throws SQLException {
    addColumn(new ColumnMetaInfo(RelationalType.VARCHAR, length, columnName));
    return this;
  }

  // TODO: Comment
  public ColumnMetaInfo[] getColumns() {
    return mColumns.toArray(new ColumnMetaInfo[mColumns.size()]);
  }

  // TODO: Comment
  public String getName() {
    return mSchemaName;
  }

}
