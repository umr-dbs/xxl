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
import java.sql.SQLException;
import java.util.ArrayList;

import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ByteConverter;
import xxl.core.io.converters.DateConverter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FloatConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.io.converters.StringConverter;
import xxl.core.io.converters.TimeConverter;
import xxl.core.io.converters.TimestampConverter;
import xxl.core.io.converters.meta.ExtendedResultSetMetaData;
import xxl.core.relational.JavaType;
import xxl.core.relational.RelationalType;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.ConvertUtils;

/**
 * This class provides a converter which is able to read and write objects of type <b>Tuple</b>
 * which contains a set of individual typed objects. Therefore the MeasuredTupleConverter calls the
 * specific built-in XXL converter to read an write the single objects (components) of the tuple.
 * First the converter reads or writes the length of the array. Thereafter the objects are read or
 * written.<br/>
 * <br/>
 * 
 * <b>Note:</b> For various reasons, a tuple contains different typed objects but does not know its
 * own column types. Therefore, a MeasuredTupleConverter expects an object that implements
 * <b>ResultSetMetaData</b>. This provides the information about which types the tuple uses in its
 * columns.<br/>
 * <br/>
 * <b>Caution:</b> The MeasuredTupleConverter auto matches Java types and relational types, e.g.
 * VARCHAR and LONGNVARCHAR to java.lang.String using {@link xxl.core.util.ConvertUtils#toJavaType
 * Convert.toJavaTyp()} and vice versa using {@link xxl.core.util.ConvertUtils#toRelationalType
 * Convert.toRelationalType()}. Please note that every type is matched currently but not every
 * relational type can be written or read at this point. If you use e.g. CLOB (JDBC) type the
 * MeasuredTupleConverter will throw an <code>UnsupportedOperationException</code>. <br/>
 * <br/>
 * Actually you can write and read the following types
 * <ul>
 * <li>java BOOLEAN and jdbc BOOLEAN</li>
 * <li>java BYTE and jdbc TINYINT, BOOLEAN</li>
 * <li>java DATE and jdbc DATE</li>
 * <li>java DOUBLE and jdbc DOUBLE</li>
 * <li>java FLOAT and jdbc REAL</li>
 * <li>java INT and jdbc INTEGER</li>
 * <li>java LONG and jdbc BIGINT</li>
 * <li>java SHORT and jdbc SMALLINT</li>
 * <li>java STRING and jdbc CHAR, LONGNVARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, VARCHAR</li>
 * <li>java TIME and jdbc TIME</li>
 * <li>java TIMESTAMP and jdbc TIMESTAMP</li>
 * <ul>
 * <br/>
 * <br/>
 * 
 * 
 * <b>Example of MeasuredTupleConverter writing</b><br/>
 * <br/>
 * The following example shows how to create tuples in a table <code>TABLE_NAME</code> and how to
 * store them into a file called <code>dumpFile</code>. Each tuple contains two columns. The first
 * is an <code>INTEGER</code> (JDBC type) and the second is a <code>VARCHAR</code> (JDCBC type). The
 * example uses <code>ColumnMetaInfo</code> which is designed for quick using <i>RelationalType</i>.
 * Instead of this you can also use {@link xxl.core.relational.metaData.ColumnMetaData
 * ColumnMetaData}. After adding some tuples with values the example code
 * {@link xxl.core.io.converters.MeasuredTupleConverter#write(DataOutput, Tuple) writes} sequentially
 * each tuple content to <code>dumpFile</code>.
 * 
 * <code><pre>
 * 	File dumpFile = new File(DUMP_FILE_PATH);
 * 						
 * 	FileOutputStream fileOutputStream = new FileOutputStream(dumpFile);
 * 	DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
 * 	final String TABLE_NAME = "Students";
 * 
 * 	ColumnMetaInfo columnMetaData[] = new ColumnMetaInfo[] {  
 * 			new ColumnMetaInfo(RelationalType.INTEGER, "MatrNr", TABLE_NAME),
 * 			new ColumnMetaInfo(RelationalType.VARCHAR, "Name", TABLE_NAME) };
 * 
 * 	ColumnMetaDataResultSetMetaData metaData = new ColumnMetaDataResultSetMetaData(columnMetaData);
 * 			MeasuredTupleConverter tc = new MeasuredTupleConverter(metaData);
 * 
 * 	ArrayTuple tupleArray[] = new ArrayTuple[] { 
 * 			new ArrayTuple(new Integer(23), new String("Mustermann")),
 * 			new ArrayTuple(new Integer(24), new String("Musterfrau")),
 * 			new ArrayTuple(new Integer(25), new String("Doe"))  }; 
 * 
 * 	for (Tuple t : tupleArray) 
 * 			tc.write(dataOutputStream, t);
 * 				
 * </pre></code> <br/>
 * <br/>
 * 
 * 
 * <b>Example of MeasuredTupleConverter reading</b><br/>
 * <br/>
 * The following example shows how to restore tuples from a previous session from a file called
 * <code>dumpFile</code>. Each tuple contains two columns. The first is an <code>INTEGER</code>
 * (JDBC type) and the second is a <code>VARCHAR</code> (JDCBC type). Please make sure that the
 * <b>schema</b> for the new tuple is exactly the same as the schema of the written previously (as
 * it is in this example code).
 * 
 * The example uses <code>ColumnMetaInfo</code> which is designed for quick using
 * <i>RelationalType</i>. Instead of this you can also use
 * {@link xxl.core.relational.metaData.ColumnMetaData ColumnMetaData}. After setup this, the
 * MeasuredTupleConverter reads the <i>first</i> tuple from <code>dumpFile</code> and restore it
 * into a new object <code>tuple</code>. You can load all tuples by calling
 * {@link xxl.core.io.converters.MeasuredTupleConverter#read(DataInput) read(DataInput)}
 * sequentially.
 * 
 * <code><pre>
 * FileInputStream fileInputStream = new FileInputStream(new File("files/tupleDump.tmp"));
 * DataInputStream dataInputStream = new DataInputStream(fileInputStream);
 * final String TABLE_NAME = "Students";
 * 
 * ColumnMetaInfo columnMetaData[] = new ColumnMetaInfo[] { 
 * 		new ColumnMetaInfo(RelationalType.INTEGER, "MatNr", TABLE_NAME), 
 * 		new ColumnMetaInfo(RelationalType.VARCHAR, "Name", TABLE_NAME) };
 * 
 * ColumnMetaDataResultSetMetaData metaData = new ColumnMetaDataResultSetMetaData(columnMetaData);
 * MeasuredTupleConverter tc = new MeasuredTupleConverter(metaData);
 * 
 * Tuple readedTuple = tc.read(dataInputStream);
 * </pre></code>
 * 
 * @see xxl.core.relational.metaData.ColumnMetaData
 * @see xxl.core.relational.metaData.ColumnMetaInfo
 * @see xxl.core.relational.RelationalType
 * @see xxl.core.relational.JavaType
 * @see xxl.core.util.ConvertUtils
 * @see xxl.core.relational.tuples.Tuple
 * 
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 */
public final class MeasuredTupleConverter extends MeasuredConverter<Tuple> {

  private static final long serialVersionUID = 8652170728856255633L;

  /*
   * The meta information about the column types in tuple
   */
  private ExtendedResultSetMetaData mTupleMetaData;

  /**
   * Creates a new converter for objects of type Tuple. A tuple includes a lot of different typed
   * objects. Since a tuple does not know his own object types a description of the data type of the
   * contained objects will be needed here. <br/>
   * <b>Note:</b> Please check that meta data and tuples match in their object size (dimension) <br/>
   * 
   * @param tupleMetaData A type description of the contained objects within your tuples. You can
   *        use objects which implements {@link java.sql.ResultSetMetaData ResultSetMetaData}
   * 
   * @throws NullPointerException if tupleMetaData is null
   */
  public MeasuredTupleConverter(ExtendedResultSetMetaData tupleMetaData) {
    if (tupleMetaData == null)
      throw new NullPointerException(
          "MeasuredTupleConverter tupleMetaData is null");

    mTupleMetaData = tupleMetaData;
  }

  /*
   * For reason of measuring the length of data types a string value have to in fixed size style.
   * This method checks if a given String s of column with the given Index is smaller or equal to
   * the max supported fixed size
   */
  private void checkStringJustFitsMaxCharacterLength(String s, int columnIndex) {
    int maxSupportedLength = mTupleMetaData.getContentLength(columnIndex);
    if (s.length() > maxSupportedLength)
      throw new UnsupportedOperationException(
          "The tuple component value at index \""
              + columnIndex
              + "\" is too long. Maximum supported character count according to the meta data is \""
              + maxSupportedLength + "\".\nThe exception occures for \n\"" + s
              + "\"");

  }

  /**
   * Get the maximum size by the sum of all single sizes of all columns.
   */
  @Override
  public int getMaxObjectSize() {
    int tupleObjectSize = 0;

    try {
      for (int i = 0; i < mTupleMetaData.getColumnCount(); i++)
        tupleObjectSize +=
            getSingleObjectSize(mTupleMetaData.getColumnType(i + 1), i);
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return tupleObjectSize;
  }

  /*
   * Returns the size in bytes of a column entry. This is needed for I/O with Converters
   */
  private int getSingleObjectSize(int columnType, int columnIndex) {
    int typeSize = 0;

    RelationalType columnRelationalType =
        ConvertUtils.toRelationalType(columnType);
    JavaType columnJavaType = ConvertUtils.toJavaType(columnRelationalType);

    switch (columnJavaType) {
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
      case STRING:
        typeSize =
            FixedSizeStringConverter.calculateSize(mTupleMetaData
                .getContentLength(columnIndex + 1));
        break;
      case TIME:
        typeSize = TimeConverter.SIZE;
        break;
      case TIMESTAMP:
        typeSize = TimestampConverter.SIZE;
        break;
      default:
        throw new UnsupportedOperationException("Not implemented yet for \""
            + columnJavaType.toString() + "\"");
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
  public Tuple read(DataInput dataInput, Tuple t) throws IOException {
    // Read entire tuple object size which is the tuple dimension.
    // Example: Tuple(Integer, String) has dimension 2
    int tupleDimension = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);

    // A temporary vector holding the objects for converting this vector
    // into Object[] which is used in Tuple constructor. Reads each
    // column of singleTuple, find outs the column type and calls the
    // specific converter to read the type.
    ArrayList<Object> tempArray = new ArrayList<>();

    try {
      if (mTupleMetaData.getColumnCount() != tupleDimension)
        throw new RuntimeException(
            "Given tuple dimension and meta data dimension does not match.");

      for (int i = 0; i < tupleDimension; i++) {
        int columnType = mTupleMetaData.getColumnType(i + 1);
        tempArray.add(readWithType(dataInput, columnType, i));
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          "MeasuredTupleConverter::read SQLException of ResultSetMetaData tupleMetaData");
    }

    return new ColumnComparableArrayTuple(
        tempArray.toArray(new Comparable[tempArray.size()]));
  }

  /**
   * Reads a date from the <i>dataInput</i>. The metadata about the types of the individual
   * components of the tuples is known while constructing this TupleConverter. This method takes a
   * relational data type, translates it into a <i>Java language type<i> and reads an object of this
   * type from the input. The read object is then returned. <br/>
   * <br/>
   * For type converting, see <a href="http://db.apache.org/ojb/docu/guides/jdbc-types.html">JDBC
   * Types</a>
   * 
   * @param dataInput The input from which a date should be read <br/>
   * @param columnType The <i>relational</i> type of the object which should be read <br/>
   * @return The (according to the translation between Java language and relational language) typed
   *         into Object casted date.
   */
  public Object readWithType(DataInput dataInput, final int columnType,
      int columnIndex) {
    Object retval = null;

    RelationalType columnRelationalType =
        ConvertUtils.toRelationalType(columnType);
    JavaType columnJavaType = ConvertUtils.toJavaType(columnRelationalType);

    try {
      switch (columnJavaType) {
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
        case STRING:
          retval = StringConverter.DEFAULT_INSTANCE.read(dataInput);
          checkStringJustFitsMaxCharacterLength((String) retval,
              columnIndex + 1);
          break;
        case TIME:
          retval = TimeConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        case TIMESTAMP:
          retval = TimestampConverter.DEFAULT_INSTANCE.read(dataInput);
          break;
        default:
          throw new UnsupportedOperationException("Not implemented yet for \""
              + columnJavaType.toString() + "\"");
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
  public void write(DataOutput dataOutput, Tuple singleTuple)
      throws IOException {
    if (singleTuple == null)
      throw new NullPointerException(
          "MeasuredTupleConverter::write singleTuple is null");

    // Write entire tuple object size which is the tuple dimension.
    // Example: Tuple(Integer, String) has dimension 2
    int tupleDimension = singleTuple.getColumnCount();
    IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, tupleDimension);

    // Iterate through columns and use XXL built in converter to write
    // the column with specific type
    try {
      if (mTupleMetaData.getColumnCount() != singleTuple.getColumnCount())
        throw new RuntimeException(
            "Given tuple dimension and meta data dimension does not match.");

      for (int i = 0; i < tupleDimension; i++) {
        writeWithType(dataOutput, singleTuple, i,
            mTupleMetaData.getColumnType(i + 1));
      }

    } catch (SQLException e) {
      throw new RuntimeException(
          "MeasuredTupleConverter::read SQLException of ResultSetMetaData tupleMetaData");
    }

  }

  /**
   * Writes the value of the given column index into dataOutput. For this purpose, the type of the
   * column type is determined with the given MetaData and written with the specific XXL converter
   * for this type. <br/>
   * <br/>
   * For type converting, see <a href="http://db.apache.org/ojb/docu/guides/jdbc-types.html">JDBC
   * Types</a>
   * 
   * @param dataOutput The output in which the object at <i>columnIndex</i> of <i>tuple</i> should
   *        be written <br/>
   * @param tuple The tuple object <br/>
   * @param columnIndex The index of the tuples item which should be written into <i>dataOutput</i>
   */
  public void writeWithType(DataOutput dataOutput, final Tuple tuple,
      final int columnIndex, final int columnType) {
    try {
      RelationalType columnRelationalType =
          ConvertUtils.toRelationalType(columnType);
      JavaType columnJavaType = ConvertUtils.toJavaType(columnRelationalType);

      switch (columnJavaType) {
        case BOOLEAN:
          BooleanConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getBoolean(columnIndex + 1));
          break;
        case BYTE:
          ByteConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getByte(columnIndex + 1));
          break;
        case DATE:
          DateConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getDate(columnIndex + 1));
          break;
        case DOUBLE:
          DoubleConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getDouble(columnIndex + 1));
          break;
        case FLOAT:
          FloatConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getFloat(columnIndex + 1));
          break;
        case INT:
          IntegerConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getInt(columnIndex + 1));
          break;
        case LONG:
          LongConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getLong(columnIndex + 1));
          break;
        case SHORT:
          ShortConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getShort(columnIndex + 1));
          break;
        case STRING:
          String content = tuple.getString(columnIndex + 1);
          checkStringJustFitsMaxCharacterLength(content, columnIndex + 1);
          StringConverter.DEFAULT_INSTANCE.write(dataOutput, content);
          break;
        case TIME:
          TimeConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getTime(columnIndex + 1));
          break;
        case TIMESTAMP:
          TimestampConverter.DEFAULT_INSTANCE.write(dataOutput,
              tuple.getTimestamp(columnIndex + 1));
          break;
        default:
          throw new UnsupportedOperationException("Not implemented yet for \""
              + columnJavaType.toString() + "\"");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
