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

package xxl.core.indexStructures.builder.BPlusTree;

import java.sql.SQLException;

import xxl.core.indexStructures.BPlusTree;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.io.converters.meta.KeyFunctionFactory;
import xxl.core.io.converters.MeasuredTupleConverter;
import xxl.core.io.converters.meta.TupleKeyFunctionFactory;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.relational.JavaType;
import xxl.core.relational.tuples.Tuple;

/**
 * Use this class if the index structure should use {@link Tuple} objects. <br/><br/>
 * <b>Examples</b>
 * By the following a {@link BPlusTree} for using float objects is created. After data importing, 
 * a range query is executed.
 *
 * <code><pre>
 * TupleMetaData myTupleMetaData = new TupleMetaData("TupleTable", new ColumnMetaInfo[] {
 * new ColumnMetaInfo(RelationalType.FLOAT, "FloatColumn")});
 * 	
 * BPlusTreeConfiguration requirements = new BPlusTreeConfiguration(new TupleType(myTupleMetaData));
 * myTree = requirements.getBuilder().create();
 *
 * for(float i = 0; i < 100_000; i++) 
 *   myTree.insert(new ArrayTuple(i));
 *   
 * Cursor c = myTree.rangeQuery(new ColumnComparableArrayTuple(20_000), new ColumnComparableArrayTuple(20_020));
 * Cursors.println(c);
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see PrimitiveType Use primitive types like Integer or Float for an index structure
 * @see BPlusTreeBuilder How to setup a BPlus tree easy and fast
 * 
 */
public class TupleType extends ManagedType {

  /**
   * The entire {@link ManagedType#getContentType()} identification string which is "complex/tuple"
   * for this implementation
   */
  public static final String TYPE_NAME = "complex/tuple";

  /*
   * An array of integers which represents the indices of the compounded key. By default it's the
   * first column. Please note that the first item index is one not zero.
   */
  private int[] mCompoundKeyIndices = new int[] {1};

  /*
   * The table meta data
   */
  private ExtendedResultSetMetaData mMetaData;

  /**
   * Specifies the tuple meta data managed by the index structure. <b>Please note:</b> Check that
   * tuple meta data which should be inserted into the index structure and table meta data match.<br/>
   * <br/>
   * <b>Example</b> By the following a table called <i>TupleTable</i> is described. For further
   * processing with e.g. BPlus tree a <code>TupleType</code> object is created. It contains methods
   * which depend on the meta data and which are needed by the BPlus tree.
   * 
   * <code><pre>
   * TupleMetaData myTupleMetaData = new TupleMetaData("TupleTable", new ColumnMetaInfo[] {
   * 	new ColumnMetaInfo(RelationalType.INTEGER, "KEY_INTEGER"),
   * 	new ColumnMetaInfo(RelationalType.INTEGER, "KEY_INTEGER"),
   * 	new ColumnMetaInfo(RelationalType.VARCHAR, 50, "VARCHAR") });
   * TupleType t = new TupleType(myTupleMetaData));
   * </pre></code>
   * 
   * @param metaData The table meta data
   * @throws SQLException
   */
  public TupleType(ExtendedResultSetMetaData metaData) throws SQLException {
    if (metaData == null || metaData.getColumnCount() < 1)
      throw new IllegalArgumentException(
          "Tuple table meta data invalid arguments. Null or no columns defined");

    mMetaData = metaData;
  }

  /**
   * @see ManagedType#getCompoundKeyIndices()
   */
  @Override
  public int[] getCompoundKeyIndices() {
    return mCompoundKeyIndices;
  }

  @Override
  public ContentClass getContentClass() {
    return ContentClass.CONTENT_CLASS_COMPLEX;
  }

  @Override
  public JavaType getContentClassSubType() {
    return JavaType.ARRAY;
  }

  /**
   * {@link TupleType#TYPE_NAME} is returned.
   * 
   * @see ManagedType#getContentType()
   */
  @Override
  public String getContentType() {
    return TYPE_NAME;
  }

  /**
   * @see ManagedType#getDataConverter()
   */
  @Override
  MeasuredConverter getDataConverter() {
    return new MeasuredTupleConverter(mMetaData);
  }

  /**
   * @see ManagedType#getKeyFunctionFactory()
   */
  @Override
  KeyFunctionFactory getKeyFunctionFactory() {
    return new TupleKeyFunctionFactory(mMetaData, mCompoundKeyIndices);
  }

  /**
   * @see ManagedType#getMetaData()
   */
  @Override
  public ExtendedResultSetMetaData getMetaData() {
    return mMetaData;
  }

  /**
   * <b>Note</b>: If it fails the get the table name, <code>Unknown</code> is set as default table
   * name.
   * 
   * @see ManagedType#getTableName()
   * 
   */
  @Override
  public String getTableName() {
    try {
      return mMetaData.getTableName(0);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Unknown";
  }

  /**
   * @see ManagedType#setCompoundKey(int[])
   */
  @Override
  public void setCompoundKey(int[] compoundKeyIndices) {
    mCompoundKeyIndices = compoundKeyIndices;
  }

}
