/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2013 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
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

package xxl.core.io.converters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import xxl.core.indexStructures.builder.BPlusTree.ManagedType;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ByteConverter;
import xxl.core.io.converters.DateConverter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FloatConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.io.converters.TimeConverter;
import xxl.core.io.converters.TimestampConverter;
import xxl.core.relational.JavaType;

/**
 * This class provides a converter which is able to read and write objects of <i>primitive</i> java
 * type, see {@link JavaType} with the built-in measured converters.
 * 
 * <b>Information:</b> The MeasuredTupleConverter auto matches Java types and relational types, e.g.
 * VARCHAR and LONGNVARCHAR to java.lang.String using {@link xxl.core.util.ConvertUtils#toJavaType
 * Convert.toJavaTyp()} and vice versa using {@link xxl.core.util.ConvertUtils#toRelationalType
 * Convert.toRelationalType()}. Please note that every type is matched currently but not every
 * relational type can be written or read at this point. If you use e.g. CLOB (JDBC) type the
 * MeasuredTupleConverter will throw an <code>UnsupportedOperationException</code>. <br/>
 * <br/>
 * <b>Note:</b> If you want to use <i>String</i> objects, please consider to use Tuples (
 * {@link MeasuredTupleConverter}) instead of primitive types. That's because String does not have a
 * fixed size which is required.
 * 
 * @see xxl.core.relational.JavaType Java Types
 * @see ManagedType Use different data types for an index structure
 * @see MeasuredTupleConverter Converter for tuples
 * 
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 */
public final class MeasuredPrimitiveConverter extends MeasuredConverter<Object> {

  /**
	 * 
	 */
  private static final long serialVersionUID = -462920155889816381L;

  private JavaType mJavaType;

  /**
   * Creates a new converter for objects of type Tuple. A tuple includes a lot of different typed
   * objects. Since a tuple does not know his own object types a description of the data type of the
   * contained objects will be needed here. <br/>
   * <b>Note:</b> Please check that meta data and tuples match in their object size (dimension) <br/>
   * 
   * @param type A type description of the contained objects within your tuples. You can use objects
   *        which implements {@link java.sql.ResultSetMetaData ResultSetMetaData}
   * 
   * @throws NullPointerException if tupleMetaData is null
   */
  public MeasuredPrimitiveConverter(JavaType type) {
    if (type == null)
      throw new NullPointerException(
          "MeasuredTupleConverter JavaType (type) is null");

    mJavaType = type;
  }

  @Override
  public int getMaxObjectSize() {
    int typeSize = 0;
    switch (mJavaType) {
      case BOOLEAN:
        typeSize = BooleanConverter.SIZE;
        break;
      case BYTE:
        typeSize = ByteConverter.SIZE;
        break;
      case DATE:
        typeSize = DateConverter.SIZE;
        break;
      case DOUBLE:
        typeSize = DoubleConverter.SIZE;
        break;
      case FLOAT:
        typeSize = FloatConverter.SIZE;
        break;
      case INT:
        typeSize = IntegerConverter.SIZE;
        break;
      case LONG:
        typeSize = LongConverter.SIZE;
        break;
      case SHORT:
        typeSize = ShortConverter.SIZE;
        break;
      // case STRING:
      // typeSize = FixedSizeStringConverter.calculateSize(mTupleMetaData
      // .getContentLength(columnIndex + 1));
      // break;
      case TIME:
        typeSize = TimeConverter.SIZE;
        break;
      case TIMESTAMP:
        typeSize = TimestampConverter.SIZE;
        break;
      default:
        throw new UnsupportedOperationException("Not implemented yet for \""
            + mJavaType + "\"");
    }
    return typeSize;
  }

  /**
   * Restores a tuple from an input. This read based on the meta data information given in
   * constructor. <br/>
   * <b>Note:</b> Please check that meta data and tuples match in their object size (dimension) <br/>
   * 
   * @param dataInput the input from which the tuple should be loaded<br/>
   * <br/>
   * @param _ ignore this argument, it's unused
   * @return The restored tuple with the schema you defined in the constructor
   * @throws IOException if it fails to read from <code>dataInput</code> RuntimeException if tuple
   *         dimension and meta data dimension does not match SQLException if a SQLException occurs
   *         within your <code>ResultSetMetaData</code>
   */
  @Override
  public Object read(DataInput dataInput, Object o) throws IOException {
    Object retval = null;

    try {
      switch (mJavaType) {
        case BOOLEAN:
          retval = BooleanConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case BYTE:
          retval = ByteConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case DATE:
          retval = DateConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case DOUBLE:
          retval = DoubleConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case FLOAT:
          retval = FloatConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case INT:
          retval = IntegerConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case LONG:
          retval = LongConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case SHORT:
          retval = ShortConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        // case STRING:
        // retval = StringConverter.DEFAULT_INSTANCE.read(dataInput);
        // checkStringJustFitsMaxCharacterLength((String) retval,
        // columnIndex + 1);
        // break;
        case TIME:
          retval = TimeConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case TIMESTAMP:
          retval = TimestampConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        default:
          throw new UnsupportedOperationException("Not implemented yet for \""
              + mJavaType.toString() + "\"");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return retval;
  }

  /**
   * Stores a tuple sequentially into <code>dataOutput</code>. This writing operations needs info
   * about each column type which you set in constructor of MeasuredTupleConverter.<br/>
   * <b>Note:</b> Please check that meta data and tuples match in their object size (dimension) <br/>
   * 
   * @throws NullPointerException if singleTuple is null <b>IOException</b> if it fails to write
   *         into <code>dataOutput</code> <b>RuntimeException</b> if tuple dimension and meta data
   *         dimension does not match <b>SQLException</b> if a SQLException occurs within your
   *         <code>ResultSetMetaData</code>
   */
  @Override
  public void write(DataOutput dataOutput, Object object) throws IOException {
    if (object == null)
      throw new NullPointerException(
          "MeasuredTupleConverter::write object (Object) is null");

    try {

      switch (mJavaType) {
        case BOOLEAN:
          BooleanConverter.DEFAULT_INSTANCE.write(dataOutput, (Boolean) object);
          break;
        case BYTE:
          ByteConverter.DEFAULT_INSTANCE.write(dataOutput, (Byte) object);
          break;
        case DATE:
          DateConverter.DEFAULT_INSTANCE.write(dataOutput,
              (object instanceof Long)
                  ? new Date((Long) object)
                  : (Date) object);
          break;
        case DOUBLE:
          DoubleConverter.DEFAULT_INSTANCE.write(dataOutput, (Double) object);
          break;
        case FLOAT:
          FloatConverter.DEFAULT_INSTANCE.write(dataOutput, (Float) object);
          break;
        case INT:
          IntegerConverter.DEFAULT_INSTANCE.write(dataOutput, (Integer) object);
          break;
        case LONG:
          LongConverter.DEFAULT_INSTANCE.write(dataOutput, (Long) object);
          break;
        case SHORT:
          ShortConverter.DEFAULT_INSTANCE.write(dataOutput, (Short) object);
          break;
        // Support String
        // case STRING:
        // String content = tuple.getString(columnIndex + 1);
        // checkStringJustFitsMaxCharacterLength(content, columnIndex + 1);
        // StringConverter.DEFAULT_INSTANCE.write(dataOutput, content);
        // break;
        case TIME:
          TimeConverter.DEFAULT_INSTANCE.write(dataOutput,
              (object instanceof Long)
                  ? new Time((Long) object)
                  : (Time) object);
          break;
        case TIMESTAMP:
          TimestampConverter.DEFAULT_INSTANCE.write(dataOutput,
              (object instanceof Long)
                  ? new Timestamp((Long) object)
                  : (Timestamp) object);
          break;
        default:
          throw new UnsupportedOperationException("Not implemented yet for \""
              + mJavaType + "\"");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
