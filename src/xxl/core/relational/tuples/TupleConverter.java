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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import xxl.core.collections.Lists;
import xxl.core.functions.Function;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters;
import xxl.core.relational.Types;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class is a converter for tuples which converts a tuple into a byte
 * value in order to read or write a tuple.
 */
public class TupleConverter extends Converter<Tuple> {
 	
 	/**
 	 * An array of converters for each column of a Tuple.
 	 */
 	protected List<Converter<Object>> converters;
 	
 	/**
 	 * A converter for boolean values is used to save, if the given column of
 	 * the tuple is a <code>null</code> value or not.
 	 */
 	protected BooleanConverter nullConverter;
	
	/**
	 * A factory that is used to construct a new tuple.
	 */
	protected Function<Object, ? extends Tuple> createResTuple;
	
	/** 
	 * Creates a new converter for tuples using the given converters to read
	 * and write the column values of tuples and the specified function for
	 * creating tuples by using these column values.
	 * 
	 * @param createResTuple a function that maps a list of objects (column
	 *        values) to a new result tuple. The factory methods of the tuple
	 *        default implementations {@link ArrayTuple} and {@link ListTuple}
	 *        can be used for this task. If <code>null</code> is passed, the
	 *        constructor will try to determine the type of tuple that is used
	 *        in the cursor. If it is possible, the appropriate factory method
	 *        is used. If it is not possible, {@link ArrayTuple#FACTORY_METHOD}
	 *        is used.
	 * @param considerNullValues determines whether the converter should
	 *        consider <code>null</code> values or not. If it is set to
	 *        <code>true</code> a boolean converter is used to convert the
	 *        information whether the value is <code>null</code> or not. When
	 *        the value is not <code>null</code> the converted value follows.
	 *        If it is set to <code>false</code> the specified converters must
	 *        deal with <code>null</code> values.
	 * @param converters an array of converters used for converting the column
	 *        values of the tuple to be converted.
	 */
	public TupleConverter(Function<Object, ? extends Tuple> createResTuple, boolean considerNullValues, Converter<Object>... converters) {
		this.createResTuple = createResTuple;
		this.converters = Arrays.asList(converters);
		nullConverter = considerNullValues ?
			BooleanConverter.DEFAULT_INSTANCE :
			new BooleanConverter() {
				@Override
				public Boolean read(DataInput dataInput, Boolean object) {
					return Boolean.FALSE;
				}

				@Override
				public void write(DataOutput dataOutput, Boolean object) {
					// nothing to be written
				}
			};
	}

	/** 
	 * Creates a new converter for tuples using the given result set metadata
	 * for retrieving converters to read and write the column values of tuples
	 * and the specified function for creating tuples by using these column
	 * values.
	 * 
	 * @param createResTuple a function that maps a list of objects (column
	 *        values) to a new result tuple. The factory methods of the tuple
	 *        default implementations {@link ArrayTuple} and {@link ListTuple}
	 *        can be used for this task. If <code>null</code> is passed, the
	 *        constructor will try to determine the type of tuple that is used
	 *        in the cursor. If it is possible, the appropriate factory method
	 *        is used. If it is not possible, {@link ArrayTuple#FACTORY_METHOD}
	 *        is used.
	 * @param considerNullValues determines whether the converter should
	 *        consider <code>null</code> values or not. If it is set to
	 *        <code>true</code> a boolean converter is used to convert the
	 *        information whether the value is <code>null</code> or not. When
	 *        the value is not <code>null</code> the converted value follows.
	 *        If it is set to <code>false</code> the specified converters must
	 *        deal with <code>null</code> values.
	 * @param metadata the result set metadata is used to retrieve converters
	 *        for reading and writing the column values of tuples.
	 */
	public TupleConverter(Function<Object, ? extends Tuple> createResTuple, boolean considerNullValues, ResultSetMetaData metadata) {
		this.createResTuple = createResTuple;
		
		try {
			int size = metadata.getColumnCount();
			converters = Lists.initializedList(null, size);
			// Construction of the Converter array
			for (int i = 1; i <= size; i++)
				setConverter(i, Converters.getConverterForJavaType(Types.getJavaTypeName(Types.getJavaType(metadata.getColumnType(i)))));
			nullConverter = considerNullValues ?
				BooleanConverter.DEFAULT_INSTANCE :
				new BooleanConverter() {
					@Override
					public Boolean read(DataInput dataInput, Boolean object) {
						return Boolean.FALSE;
					}

					@Override
					public void write(DataOutput dataOutput, Boolean object) {
						// nothing to be written
					}
				};
		}
		catch(SQLException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/** 
	 * Creates a new converter for tuples using the given converters to read
	 * and write the column values of tuples. The converter's <code>read</code>
	 * method returns array tuples generated by the
	 * {@link ArrayTuple#FACTORY_METHOD factory method} located in the class
	 * <code>ArrayTuple</code>.
	 * 
	 * @param considerNullValues determines whether the converter should
	 *        consider <code>null</code> values or not. If it is set to
	 *        <code>true</code> a boolean converter is used to convert the
	 *        information whether the value is <code>null</code> or not. When
	 *        the value is not <code>null</code> the converted value follows.
	 *        If it is set to <code>false</code> the specified converters must
	 *        deal with <code>null</code> values.
	 * @param converters an array of converters used for converting the column
	 *        values of the tuple to be converted.
	 */
	public TupleConverter(boolean considerNullValues, Converter<Object>... converters) {
		this(ArrayTuple.FACTORY_METHOD, considerNullValues, converters);
	}
	
	/** 
	 * Creates a new converter for tuples using the given result set metadata
	 * for retrieving converters to read and write the column values of tuples.
	 * The converter's <code>read</code> method returns array tuples generated
	 * by the {@link ArrayTuple#FACTORY_METHOD factory method} located in the
	 * class <code>ArrayTuple</code>.
	 * 
	 * @param considerNullValues determines whether the converter should
	 *        consider <code>null</code> values or not. If it is set to
	 *        <code>true</code> a boolean converter is used to convert the
	 *        information whether the value is <code>null</code> or not. When
	 *        the value is not <code>null</code> the converted value follows.
	 *        If it is set to <code>false</code> the specified converters must
	 *        deal with <code>null</code> values.
	 * @param metadata the result set metadata is used to retrieve converters
	 *        for reading and writing the column values of tuples.
	 */
	public TupleConverter(boolean considerNullValues, ResultSetMetaData metadata) {
		this(ArrayTuple.FACTORY_METHOD, considerNullValues, metadata);
	}
	
	/** 
	 * Creates a new converter for tuples using the given converters to read
	 * and write the column values of tuples. The converter's <code>read</code>
	 * method returns array tuples generated by the
	 * {@link ArrayTuple#FACTORY_METHOD factory method} located in the class
	 * <code>ArrayTuple</code>.
	 * 
	 * @param converters an array of converters used for converting the column
	 *        values of the tuple to be converted.
	 */
	public TupleConverter(Converter<Object>... converters) {
		this(ArrayTuple.FACTORY_METHOD, true, converters);
	}
	
	/** 
	 * Creates a new converter for tuples using the given result set metadata
	 * for retrieving converters to read and write the column values of tuples.
	 * The converter's <code>read</code> method returns array tuples generated
	 * by the {@link ArrayTuple#FACTORY_METHOD factory method} located in the
	 * class <code>ArrayTuple</code>.
	 * 
	 * @param metadata the result set metadata is used to retrieve converters
	 *        for reading and writing the column values of tuples.
	 */
	public TupleConverter(ResultSetMetaData metadata) {
		this(ArrayTuple.FACTORY_METHOD, true, metadata);
	}
	
	/**
	 * Sets a converter for the specified column of the tuple.
	 * 
	 * @param index the number of the column whose values should be converter
	 *        by the given converter.
	 * @param converter the converter used for reading and writing the values
	 *        of the specified column.
	 * @throws IndexOutOfBoundsException if the given column number less than
	 *         <code>1</code>.
	 */
	public void setConverter(int index, Converter<Object> converter) throws IndexOutOfBoundsException{
		for (int i = index - converters.size() - 1; i > 0; i--)
			converters.add(null);
		converters.set(index-1, converter);
	}
	
	/**
	 * Returns the converter used for converting the values of the specified
	 * column.
	 *
	 * @param index the number of the column whose data is converter by the
	 *        given converter.
	 * @return the converter used for converting the values of the specified
	 *         column.
	 * @throws IndexOutOfBoundsException if the given column number less than
	 *         <code>1</code> or greater than the number of registered
	 *         converters.
	 */
	public Converter<Object> getConverter(int index) throws IndexOutOfBoundsException{
		return converters.get(index-1);
	}
	
	/**
	 * Writes the byte value of a tuple to the specified data output stream.
	 * 
	 * @param output the output stream the byte value of the tuple is written
	 *        to.
	 * @param tuple the tuple to be written on the output stream.
	 * @throws IOException if an I/O error occurs. 
	 */	
	@Override
	public void write(DataOutput output, Tuple tuple) throws IOException {
		boolean isNull;
		for (int i = 1; i <= tuple.getColumnCount(); i++) {
		 	nullConverter.writeBoolean(output, isNull = tuple.isNull(i));
			if (!isNull)
				converters.get(i-1).write(output, tuple.getObject(i));
		}
	}
	
	/**
	 * Reads the tuple from the data input stream.
	 * 
	 * @param input the input stream containing the contents of a tuple.
	 * @param tuple the tuple to be restored. This implementation ignores the
	 *        given tuple.
	 * @return the tuple read from the data input stream.
	 * @throws IOException if an I/O error occurs. 
	 */
	@Override
	public Tuple read(DataInput input, Tuple tuple) throws IOException {
		Object[] columns = new Object[tuple != null ? tuple.getColumnCount() : converters.size()];
		for (int i = 0; i < columns.length; i++)	
			columns[i] = nullConverter.readBoolean(input) ?
				null :
				converters.get(i).read(input, tuple != null ? tuple.getObject(i+1) : columns[i]);
		return createResTuple.invoke(Arrays.asList(columns));
	}
}
