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

package xxl.core.relational.cursors;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.sources.io.InputStreamCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.AsciiConverter;
import xxl.core.io.converters.Converter;
import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.metaData.CompositeMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * This class constructs a metadata cursor for a given input stream, file or
 * URL.
 * 
 * <p>The first row of the input stream must contain all the column names
 * delimited by a string of <code>columnDelimiters</code> (the default column
 * delimiter is <tt>"\t"</tt>). Thereafter a string of
 * <code>rowDelimiters</code> must follow (the default row delimiter is
 * <tt>"\r\n"</tt>). The second row must contain the column types (also
 * delimited by a string of <code>columnDelimiters</code>). The possible column
 * types are:
 * <ul>
 *   <li><tt>NUMBER</tt> (<tt>NUMERIC</tt>),</li>
 *   <li><tt>SMALLINT</tt>,</li>
 *   <li><tt>INTEGER</tt>,</li>
 *   <li><tt>BIGINT</tt>,</li>
 *   <li><tt>DOUBLE</tt>,</li>
 *   <li><tt>DATE</tt>,</li>
 *   <li><tt>TIME</tt>,</li>
 *   <li><tt>TIMESTAMP</tt>,</li>
 *   <li><tt>BIT</tt>,</li>
 *   <li><tt>VARCHAR</tt> and</li>
 *   <li><tt>VARBINARY</tt>.</li>
 * </ul>
 * The type <tt>VARCHAR</tt> is set to <tt>VARCHAR(40)</tt> by default.
 * Thereafter two strings of row delimiters must follow, i.e., the relational
 * metadata definition is followed by an empty row.</p>
 * 
 * <p>The first three rows of the input stream are case insensitive (the
 * header). All following rows are case sensitive. Each of the following rows
 * represents a tuple of this relation. Values are delimited by the strings of
 * <code>columnDelimiters</code>, rows by the strings of
 * <code>rowDelimiters</code>. Note that every row (in particular the last row)
 * must be followed by a row delimiter to be read from the input stream. To
 * model a SQL <tt>NULL</tt> value one must use the
 * <code>nullRepresentation</code>. The string <code>nullRepresentation</code>
 * is by default set to <tt>"#"</tt>.</p>
 * 
 * <p>Example for a file:
 * <code><pre>
 *   key     NAME        first name  year of birth
 *   Number  Varchar     varChar     number
 *   
 *   1       Tiger       Scott       1957
 *   2       Smith       Scott       1961
 *   3       Smith       John        1948
 *   4       #           Test        #
 * </pre></code>
 * By passing the file name to the constructor, the specified file can be used
 * as a metadata cursor.</p>
 */
public class InputStreamMetaDataCursor extends InputStreamCursor<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {
	
	/**
	 * Returns a converter that is able to read and write result set metadata
	 * using the form specified above. The converter reads or writes three rows
	 * of metadata definition containing the column names, the column types and
	 * an empty row.
	 * 
	 * @param characterConverter the converter used for converting the
	 *        characters of the result set metadata's textual representation.
	 * @param columnDelimiters the strings that can be used for delimiting a
	 *        column.
	 * @param rowDelimiters the strings that can be used for delimiting a row.
	 * @return a converter that is able to read and write result set metadata
	 *         using the form specified above.
	 */
	public static Converter<ResultSetMetaData> getResultSetMetaDataConverter(final Converter<Character> characterConverter, final String[] columnDelimiters, final String[] rowDelimiters) {
		return new Converter<ResultSetMetaData>() {
			@Override
			public ResultSetMetaData read(DataInput dataInput, ResultSetMetaData resultSetMetaData) throws IOException {
				StringBuffer buffer = new StringBuffer();
				int column;
				boolean row = false;
				ArrayList<String> columnNames = new ArrayList<String>();
				
				do {
					buffer.append(characterConverter.read(dataInput));
					column = -1;
					for (int i = 0; i < columnDelimiters.length; i++)
						if (buffer.lastIndexOf(columnDelimiters[i]) >= 0) {
							column = columnDelimiters[i].length();
							break;
						}
					if (column < 0)
						for (int i = 0; i < rowDelimiters.length; i++)
							if (buffer.lastIndexOf(rowDelimiters[i]) >= 0) {
								column = rowDelimiters[i].length();
								row = true;
								break;
							}
					if (column >= 0) {
						columnNames.add(buffer.substring(0, buffer.length()-column));
						buffer = new StringBuffer();
					}
				}
				while (!row);
				
				ColumnMetaData[] columnMetaData = new ColumnMetaData[columnNames.size()];
				String columnName, columnType;
				int columnIndex = -1;
				row = false;
				
				do {
					buffer.append(characterConverter.read(dataInput));
					column = -1;
					for (int i = 0; i < columnDelimiters.length; i++)
						if (buffer.lastIndexOf(columnDelimiters[i]) >= 0) {
							column = columnDelimiters[i].length();
							break;
						}
					if (column < 0)
						for (int i = 0; i < rowDelimiters.length; i++)
							if (buffer.lastIndexOf(rowDelimiters[i]) >= 0) {
								column = rowDelimiters[i].length();
								row = true;
								break;
							}
					if (column >= 0) {
						if (++columnIndex == columnNames.size())
							throw new MetaDataException("meta data cannot be created because of missing columns");						columnName = columnNames.get(columnIndex).toUpperCase();
						columnType = buffer.substring(0, buffer.length()-column);
						buffer = new StringBuffer();
						if (columnType.equalsIgnoreCase("number")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, columnName, columnName, "", 9, 0, "", "", Types.NUMERIC, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("numeric")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, columnName, columnName, "", 9, 0, "", "", Types.NUMERIC, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("smallint")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 7, columnName, columnName, "", 7, 0, "", "", Types.SMALLINT, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("integer")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 9, columnName, columnName, "", 9, 0, "", "", Types.INTEGER, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("bigint")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 18, columnName, columnName, "", 18, 0, "", "", Types.BIGINT, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("double")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, true, 15, columnName, columnName, "", 15, 0, "", "", Types.DOUBLE, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("date")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, 10, columnName, columnName, "", 0, 0, "", "", Types.DATE, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("time")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, 11, columnName, columnName, "", 0, 0, "", "", Types.TIME, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("timestamp")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, 6, columnName, columnName, "", 6, 0, "", "", Types.TIMESTAMP, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("bit")) {
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, 1, columnName, columnName, "", 1, 0, "", "", Types.BIT,  true, false, false);
							continue;
						}
						if (columnType.toLowerCase().startsWith("varchar")) {
							int maxLength = 40;
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNullable, false, maxLength, columnName, columnName, "", maxLength, 0, "", "", Types.VARCHAR, true, false, false);
							continue;
						}
						if (columnType.equalsIgnoreCase("varbinary")) {
							int maxLength = 120;
							columnMetaData[columnIndex] = new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNullable, false, maxLength, columnName, columnName, "", maxLength, 0, "", "", Types.VARBINARY, true, false, false);
							continue;
						}
						throw new IllegalArgumentException("unsupported SQL data type");
							// ***** every VARCHAR columnIndex is here VARCHAR(40).  (hard coded)
					}
				}
				while (!row);
				
				if (++columnIndex != columnNames.size())
					throw new MetaDataException("meta data cannot be created because of missing columns");				
				column = -1;
				
				while (column < 0) {
					buffer.append(characterConverter.read(dataInput));
					for (int i = 0; i < rowDelimiters.length; i++)
						if (buffer.lastIndexOf(rowDelimiters[i]) >= 0) {
							column = rowDelimiters[i].length();
							break;
						}
					if (column >= 0 && buffer.length() != column)
						throw new IOException();
				}
				
				return new ColumnMetaDataResultSetMetaData(columnMetaData);
			}

			@Override
			public void write(DataOutput dataOutput, ResultSetMetaData resultSetMetaData) throws IOException {
				try {
					int columnCount = resultSetMetaData.getColumnCount();
					for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
						for (char character : resultSetMetaData.getColumnName(columnIndex).toCharArray())
							characterConverter.write(dataOutput, character);
						for (char character : (columnIndex < columnCount ? columnDelimiters : rowDelimiters)[0].toCharArray())
							characterConverter.write(dataOutput, character);
					}
					for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
						for (char character : resultSetMetaData.getColumnTypeName(columnIndex).toCharArray())
							characterConverter.write(dataOutput, character);
						for (char character : (columnIndex < columnCount ? columnDelimiters : rowDelimiters)[0].toCharArray())
							characterConverter.write(dataOutput, character);
					}
					for (char ch : rowDelimiters[0].toCharArray())
						characterConverter.write(dataOutput, ch);
				}
				catch (SQLException sqle) {
					throw new MetaDataException("the metadata cannot be accessed properly because of the following SQL exception: " + sqle.getMessage());
				}
			}
		};
	}
	
	/**
	 * Returns a converter that is able to read and write tuples using the form
	 * specified above. The converter reads or writes a row of tuple values.
	 * 
	 * @param characterConverter the converter used for converting the
	 *        characters of the tuples' textual representation.
	 * @param columnDelimiters the strings that can be used for delimiting a
	 *        column (i.e., a value of a tuple's column).
	 * @param rowDelimiters the strings that can be used for delimiting a row
	 *        (i.e., a tuple).
	 * @param nullRepresentation a textual representation for SQL <tt>NULL</tt>
	 *        values inside the given output stream.
	 * @param relationalMetaData the relational metadata of the tuples to be
	 *        converted.
	 * @param tupleFactory a function that maps a list of objects (column
	 *        values) and a to a new result tuple.
	 *        {@link ArrayTuple#FACTORY_METHOD} can be used as a default
	 *        factory method.
	 * @return a converter that is able to read and write tuples using the form
	 *         specified above.
	 */
	public static Converter<Tuple> getTupleConverter(final Converter<Character> characterConverter, final String[] columnDelimiters, final String[] rowDelimiters, final String nullRepresentation, final ResultSetMetaData relationalMetaData, final Function<Object, ? extends Tuple> tupleFactory) {
		return new Converter<Tuple>() {
			@Override
			public Tuple read(DataInput dataInput, Tuple tuple) throws IOException {
				try {
					StringBuffer buffer = new StringBuffer();
					int column, columnIndex = 1;
					boolean row = false;
					String stringValue;
					ArrayList<Object> columnValues = new ArrayList<Object>();
					
					do {
						buffer.append(characterConverter.read(dataInput));
						column = -1;
						for (int i = 0; i < columnDelimiters.length; i++)
							if (buffer.lastIndexOf(columnDelimiters[i]) >= 0) {
								column = columnDelimiters[i].length();
								break;
							}
						if (column < 0)
							for (int i = 0; i < rowDelimiters.length; i++)
								if (buffer.lastIndexOf(rowDelimiters[i]) >= 0) {
									column = rowDelimiters[i].length();
									row = true;
									break;
								}
						if (column >= 0) {
							stringValue = buffer.substring(0, buffer.length()-column);
							buffer = new StringBuffer();
							if (stringValue.equals(nullRepresentation))
								columnValues.add(null);
							else
								switch (relationalMetaData.getColumnType(columnIndex)) {
									case java.sql.Types.BIT:				// -7
										columnValues.add(Boolean.valueOf(stringValue));
										break;
									case java.sql.Types.BIGINT:				// -5
										columnValues.add(Long.valueOf(stringValue));
										break;
									case java.sql.Types.VARBINARY:			// -3
										StringTokenizer tokens = new StringTokenizer(stringValue, "[, ]");
										byte[] bytes = new byte[tokens.countTokens()];
										for (int j = 0; j < bytes.length; j++)
											bytes[j] = Byte.parseByte(tokens.nextToken());
										columnValues.add(bytes);
										break;
									case java.sql.Types.NUMERIC:			//  2 
										columnValues.add(new BigDecimal(stringValue));
										break;
									case java.sql.Types.INTEGER:			//  4
										columnValues.add(Integer.valueOf(stringValue));
										break;
									case java.sql.Types.SMALLINT:			//  5
										columnValues.add(Short.valueOf(stringValue));
										break;	
									case java.sql.Types.DOUBLE:				//  8
										columnValues.add(Double.valueOf(stringValue));
										break;
									case java.sql.Types.VARCHAR:			// 12
										columnValues.add(stringValue);
										break;
									case java.sql.Types.DATE:				// 91
										columnValues.add(Date.valueOf(stringValue));
										break;
									case java.sql.Types.TIME:				// 92
										columnValues.add(Time.valueOf(stringValue));
										break;
									case java.sql.Types.TIMESTAMP:			// 93
										columnValues.add(Timestamp.valueOf(stringValue));
										break;
									default:
										throw new IllegalArgumentException("unsupported SQL data type");
								}
							columnIndex++;
						}
					}
					while (!row);
					
					if (relationalMetaData.getColumnCount() != columnValues.size())
						throw new MetaDataException("tuple cannot be created because of missing columns");				
					
					return tupleFactory.invoke(columnValues);
				}
				catch (SQLException sqle) {
					throw new MetaDataException("the metadata cannot be accessed properly because of the following SQL exception: " + sqle.getMessage());
				}
			}

			@Override
			public void write(DataOutput dataOutput, Tuple tuple) throws IOException {
				try {
					int columnCount = relationalMetaData.getColumnCount();
					for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
						for (char character : (tuple.getObject(columnIndex) == null ? nullRepresentation : relationalMetaData.getColumnType(columnIndex) == Types.VARBINARY ? Arrays.toString((byte[])tuple.getObject(columnIndex)) : tuple.getObject(columnIndex).toString()).toCharArray())
							characterConverter.write(dataOutput, character);
						for (char character : (columnIndex < columnCount ? columnDelimiters : rowDelimiters)[0].toCharArray())
							characterConverter.write(dataOutput, character);
					}
				}
				catch (SQLException sqle) {
					throw new MetaDataException("the metadata cannot be accessed properly because of the following SQL exception: " + sqle.getMessage());
				}
			}
		};
		
	}
	
	/**
	 * Writes the given metadata cursor to the specified output stream using
	 * the given converters.
	 * 
	 * @param dataOutput the output stream the given cursor's data and
	 *        metadata is written to.
	 * @param resultSetMetaDataConverter the converter that is used to write
	 *        the given cursor's metadata.
	 * @param tupleConverter the converter that is used to write the given
	 *        cursor's data.
	 * @param cursor the metadata cursor providing the data and metadata to be
	 *        written.
	 * @throws IOException if an I/O exception occurs.
	 */
	public static void writeMetaDataCursor(DataOutput dataOutput, Converter<? super ResultSetMetaData> resultSetMetaDataConverter, Converter<? super Tuple> tupleConverter, MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor) throws IOException {
		resultSetMetaDataConverter.write(dataOutput, ResultSetMetaDatas.getResultSetMetaData(cursor));
		
		cursor.open();
		
		while (cursor.hasNext())
			tupleConverter.write(dataOutput, cursor.next());
		
		cursor.close();
	}
	
	/**
	 * Writes the given metadata cursor to the specified output stream using
	 * the given column and row delimiters and <tt>NULL</tt> representation.
	 * The content is written to the given output stream using the form
	 * specified above.
	 * 
	 * @param dataOutput the output stream the given cursor's data and
	 *        metadata is written to.
	 * @param columnDelimiters the strings that can be used for delimiting a
	 *        column (i.e., a value of a tuple's column).
	 * @param rowDelimiters the strings that can be used for delimiting a row
	 *        (i.e., a tuple).
	 * @param nullRepresentation a textual representation for SQL <tt>NULL</tt>
	 *        values inside the given output stream.
	 * @param cursor the metadata cursor providing the data and metadata to be
	 *        written.
	 * @throws IOException if an I/O exception occurs.
	 */
	public static void writeMetaDataCursor(DataOutput dataOutput, String[] columnDelimiters, String[] rowDelimiters, String nullRepresentation, MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor) throws IOException {
		writeMetaDataCursor(dataOutput, getResultSetMetaDataConverter(AsciiConverter.DEFAULT_INSTANCE, columnDelimiters, rowDelimiters), getTupleConverter(AsciiConverter.DEFAULT_INSTANCE, columnDelimiters, rowDelimiters, nullRepresentation, ResultSetMetaDatas.getResultSetMetaData(cursor), null), cursor);
	}
	
	/**
	 * Writes the given metadata cursor to the specified output stream using
	 * the default column and row delimiters and <tt>NULL</tt> representation
	 * described above. The content is written to the given output stream using
	 * the form specified above.
	 * 
	 * @param dataOutput the output stream the given cursor's data and
	 *        metadata is written to.
	 * @param cursor the metadata cursor providing the data and metadata to be
	 *        written.
	 * @throws IOException if an I/O exception occurs.
	 */
	public static void writeMetaDataCursor(DataOutput dataOutput, MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor) throws IOException {
		writeMetaDataCursor(dataOutput, new String[] {"\t"}, new String[] {"\r\n"}, "#", cursor);
	}
	
	/**
	 * A function that returns a new input stream providing this cursor's data.
	 */
	protected Function<?, ? extends InputStream> inputStreamFactory;
	
	/**
	 * The strings that can be used for delimiting a column (i.e., a value of a
	 * tuple's column).
	 */
	protected String[] columnDelimiters;
	
	/**
	 * The strings that can be used for delimiting a row (i.e., a tuple).
	 */
	protected String[] rowDelimiters;
	
	/**
	 * The textual representation for SQL <tt>NULL</tt> values inside the given
	 * output stream.
	 */
	protected String nullRepresentation;
	
	/**
	 * A function that maps a list of objects (column values) and a to a new
	 * result tuple. {@link ArrayTuple#FACTORY_METHOD} can be used as a default
	 * factory method.
	 */
	protected Function<Object, ? extends Tuple> tupleFactory;
	
	/**
	 * The converter that is used for reading the cursor's relational metadata
	 * from the underlying input stream.
	 */
	protected Converter<? extends ResultSetMetaData> resultSetMetaDataConverter;
	
	/**
	 * The metadata provided by the metadata cursor.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Constructs a metadata cursor that returns the data provided by the given
	 * input stream.
	 *
	 * @param inputStreamFactory a function that returns a new input stream
	 *        providing this cursor's data.
	 * @param columnDelimiters the strings that can be used for delimiting a
	 *        column.
	 * @param rowDelimiters the strings that can be used for delimiting a row.
	 * @param nullRepresentation a textual representation for SQL <tt>NULL</tt>
	 *        values inside the given output stream.
	 * @param tupleFactory a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 * @throws IOException if an I/O exception occurs.
	 */
	public InputStreamMetaDataCursor(Function<?, ? extends InputStream> inputStreamFactory, String[] columnDelimiters, String[] rowDelimiters, String nullRepresentation, Function<Object, ? extends Tuple> tupleFactory) throws IOException {
		super(new DataInputStream(inputStreamFactory.invoke()), null);
		this.inputStreamFactory = inputStreamFactory;
		this.columnDelimiters = columnDelimiters;
		this.rowDelimiters = rowDelimiters;
		this.nullRepresentation = nullRepresentation;
		this.tupleFactory = tupleFactory;
		
		resultSetMetaDataConverter = getResultSetMetaDataConverter(AsciiConverter.DEFAULT_INSTANCE, columnDelimiters, rowDelimiters);
		
		ResultSetMetaData relationalMetaData = resultSetMetaDataConverter.read(input);
		
		converter = getTupleConverter(AsciiConverter.DEFAULT_INSTANCE, columnDelimiters, rowDelimiters, nullRepresentation, relationalMetaData, tupleFactory);
		
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, relationalMetaData);
	}
	
	/**
	 * Constructs a metadata cursor that returns the data provided by the given
	 * URL.
	 *
	 * @param url the URL that provides this cursor's data.
	 * @param columnDelimiters the strings that can be used for delimiting a
	 *        column.
	 * @param rowDelimiters the strings that can be used for delimiting a row.
	 * @param nullRepresentation a textual representation for SQL <tt>NULL</tt>
	 *        values inside the given output stream.
	 * @param tupleFactory a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 * @throws IOException if an I/O exception occurs.
	 */
	public InputStreamMetaDataCursor(final URL url, String[] columnDelimiters, String[] rowDelimiters, String nullRepresentation, Function<Object, ? extends Tuple> tupleFactory) throws IOException {
		this(
			new AbstractFunction<Object, InputStream>() {
				@Override
				public InputStream invoke(){
					try {
						return url.openStream();
					}
					catch (IOException ioe) {
						throw new WrappingRuntimeException(ioe);
					}
				}
			},
			columnDelimiters,
			rowDelimiters,
			nullRepresentation,
			tupleFactory
		);
	}
	
	/**
	 * Constructs a metadata cursor that returns the data provided by the given
	 * URL. The content is written to the given output stream using the default
	 * values and the form specified above. 
	 *
	 * @param url the URL that provides this cursor's data.
	 * @throws IOException if an I/O exception occurs.
	 */
	public InputStreamMetaDataCursor(URL url) throws IOException {
		this(url, new String[] {"\t"}, new String[] {"\r\n"}, "#", ArrayTuple.FACTORY_METHOD);
	}
	
	/**
	 * Constructs a metadata cursor that returns the data provided by the
	 * specified file.
	 *
	 * @param filename the name of the file that provides this cursor's data.
	 * @param columnDelimiters the strings that can be used for delimiting a
	 *        column.
	 * @param rowDelimiters the strings that can be used for delimiting a row.
	 * @param nullRepresentation a textual representation for SQL <tt>NULL</tt>
	 *        values inside the given output stream.
	 * @param tupleFactory a function that maps a list of objects (column
	 *        values) and a to a new result Tuple.
	 *        {@link xxl.core.relational.tuples.ArrayTuple#FACTORY_METHOD} can be used
	 *        as a default factory method.
	 * @throws IOException if an I/O exception occurs.
	 */
	public InputStreamMetaDataCursor(final String filename, String[] columnDelimiters, String[] rowDelimiters, String nullRepresentation, Function<Object, ? extends Tuple> tupleFactory) throws IOException {
		this(
			new AbstractFunction<Object, FileInputStream>() {
				@Override
				public FileInputStream invoke(){
					try {
						return new FileInputStream(filename);
					}
					catch (FileNotFoundException fnfe) {
						throw new IllegalArgumentException("the specified file cannot be found because of the following reason : " + fnfe.getMessage());
					}
				}
			},
			columnDelimiters,
			rowDelimiters,
			nullRepresentation,
			tupleFactory
		);
	}

	/**
	 * Constructs a metadata cursor that returns the data provided by the
	 * specified file. The content is written to the given output stream using
	 * the default values and the form specified above. 
	 *
	 * @param filename the name of the file that provides this cursor's data.
	 * @throws IOException if an I/O exception occurs.
	 */
	public InputStreamMetaDataCursor(String filename) throws IOException {
		this(filename, new String[] {"\t"}, new String[] {"\r\n"}, "#", ArrayTuple.FACTORY_METHOD);
	}

	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and does not work for this
	 * cursor.</p>
	 * 
	 * @throws UnsupportedOperationException if the <code>reset</code>
	 *         operation is not supported by the cursor.
	 */
	@Override
	public void reset() throws UnsupportedOperationException {
		super.reset();
		try {
			input.close();
			input = new DataInputStream(inputStreamFactory.invoke());
			
			ResultSetMetaData relationalMetaData = resultSetMetaDataConverter.read(input);
			
			converter = getTupleConverter(AsciiConverter.DEFAULT_INSTANCE, columnDelimiters, rowDelimiters, nullRepresentation, relationalMetaData, tupleFactory);
			
			globalMetaData.replace(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, relationalMetaData);
		}
		catch (IOException ioe) {
			throw new WrappingRuntimeException(ioe);
		}
	}
	
	/**
	 * Returns <code>true</code> if the <code>reset</code> operation is
	 * supported by the cursor. Otherwise it returns <code>false</code>.
	 *
	 * @return <code>true</code> if the <code>reset</code> operation is
	 *         supported by the cursor, otherwise <code>false</code>.
	 */
	@Override
	public boolean supportsReset() {
		return true;
	}
	
	/**
	 * Returns the metadata information for this metadata-cursor as a composite
	 * metadata ({@link CompositeMetaData}).
	 *
	 * @return the metadata information for this metadata-cursor as a composite
	 *         metadata ({@link CompositeMetaData}).
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetaData;
	}
}
