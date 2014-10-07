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

import xxl.core.indexStructures.BPlusTree;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.io.converters.meta.KeyFunctionFactory;
import xxl.core.io.converters.MeasuredPrimitiveConverter;
import xxl.core.io.converters.meta.PrimitivesKeyFunctionFactory;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.relational.JavaType;

/**
 * Use this class if the index structure should use standard java objects. <br/><br/>
 * <b>Examples</b>
 * By the following a {@link BPlusTree} for using float objects is created. After data importing, 
 * a range query is executed.
 *
 * <code><pre>
 * BPlusTreeConfiguration requirements = new BPlusTreeConfiguration(new PrimitiveType(JavaType.FLOAT, "FloatTable"));
 * myTree = requirements.getBuilder().create();
 * for(float i = 0; i < 100_000; i++)
 *    myTree.insert(i);
 *    
 * Cursor c = myTree.rangeQuery(new Float(20_000), new Float(20_020));
 * Cursors.println(c);
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see TupleType Use tuples for an index structure
 * @see BPlusTreeBuilder How to setup a BPlus tree easy and fast
 * 
 */
public class PrimitiveType extends ManagedType {

  /**
   * The {@link ManagedType#getContentType()} identifier which is "primitive/" followed by the
   * concrete data type name
   */
  public static final String TYPE_PREFIX = "primitive/";


  /*
   * The managed java data type
   */
  private JavaType mJavaType;

  /*
   * The table name
   */
  private String mTableName;

/**
	 * Constructs a new instance of <code>PrimitiveType</code> by the given type and table name.<br/><br/>
	 * <b>Examples</b>
	 * By the following a {@link BPlusTree} for using <code>Float</code> objects of a table called "FloatTable" 
	 * is created. After data importing, 
	 * a range query is executed.
	 *
	 * <code><pre>
	 * BPlusTreeConfiguration requirements = new BPlusTreeConfiguration(new PrimitiveType(JavaType.FLOAT, "FloatTable"));
	 * myTree = requirements.getBuilder().create();
	 * for(float i = 0; i < 100_000; i++)
	 *    myTree.insert(i);
	 *    
	 * Cursor c = myTree.rangeQuery(new Float(20_000), new Float(20_020));
	 * Cursors.println(c);
	 * </pre></code>
	 * 	
	 * @param type
	 * @param tableName
	 */
  public PrimitiveType(JavaType type, String tableName) {
    if (type == null || tableName == null || tableName.trim().isEmpty())
      throw new IllegalArgumentException("Type or table name empty or null");

    mJavaType = type;
    mTableName = tableName;
  }

  /**
   * <b>Warning:</b>: A call to this method will always throw an
   * <code>IllegalArgumentException</code> because a primitive type has no compounded key.
   */
  @Override
  public int[] getCompoundKeyIndices() {
    throw new IllegalArgumentException(
        "For primitive types the call of \"getCompoundKeyIndices()\" is not allowed. There is no compounded key.");
  }

  @Override
  public ContentClass getContentClass() {
    return ContentClass.CONTENT_CLASS_PRIMITIVE;
  }

  @Override
  public JavaType getContentClassSubType() {
    return mJavaType;
  }

  @Override
  public String getContentType() {
    return TYPE_PREFIX + mJavaType.toString().toLowerCase();
  }

  @Override
  MeasuredConverter getDataConverter() {
    return new MeasuredPrimitiveConverter(mJavaType);
  }

  @Override
  KeyFunctionFactory getKeyFunctionFactory() {
    return new PrimitivesKeyFunctionFactory(mJavaType);
  }

  /**
   * <b>Warning:</b>: A call to this method will always throw an
   * <code>IllegalArgumentException</code> because a primitive type has no further meta data.
   */
  @Override
  public ExtendedResultSetMetaData getMetaData() {
    throw new IllegalArgumentException(
        "For primitive types the call of \"getMetaData()\" is not allowed. Because there is no meta data for primitive types.");
  }

  @Override
  public String getTableName() {
    return mTableName;
  }

  /**
   * <b>Warning:</b>: A call to this method will always throw an
   * <code>IllegalArgumentException</code> because a primitive type has no compounded key.
   */
  @Override
  public void setCompoundKey(int[] compoundKeyIndices) {
    throw new IllegalArgumentException(
        "For primitive types the call of \"setCompoundKey(int[])\" is not allowed.");
  }

}
