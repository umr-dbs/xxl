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

package xxl.core.io.converters;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import xxl.core.io.*;
import xxl.core.io.ByteBufferDataOutput;

/**
 * This class contains various methods connected to the serialization of
 * objects. The <code>toByteArray</code> methods return a serialized object as
 * byte array and the <code>sizeOf</code> methods determine the length of a
 * serialized object without materializing the bytes into the memory.
 * 
 * <p>The documentation of the methods contained in this class includes a brief
 * description of the implementation. Such descriptions should be regarded as
 * implementation notes, rather than parts of the specification. Implementors
 * should feel free to substitute other algorithms, as long as the
 * specification itself is adhered to.</p>
 *
 * @see ByteArrayOutputStream
 * @see DataOutputStream
 * @see IOException
 * @see OutputStream
 */
public class Converters {

    /**
     * Represents the different <tt>DataOutput</tt> resp. <tt>DataInput</tt> types
     * used for serialization resp. deserialization.
     */
    public static enum SerializationMode {
        /** Represents a ByteArrayOutputStream */
        BYTE_ARRAY,
        /** Represents an Unsafe object */
        UNSAFE,
        /** Represents a ByteBuffer */
        BYTE_BUFFER
    }

	/**
	 * Returns a converter that is able to read and write objects based on the
	 * given converter. Internally the returned converter casted the objects
	 * to be read or written to the type expected by the given converter.
	 * Therefore the returned converter will cause a
	 * <code>ClasscastException</code> when its methods are called with object
	 * that do not have the expected type.
	 * 
	 * @param <T> the type of the objects which can be read and written by the
	 *        given converter.
	 * @param converter the converter that is used to read and write the
	 *        objects internally.
	 * @return a converter that is able to read and write objects based on the
	 *         given comparator.
	 */
	public static <T> Converter<Object> getObjectConverter(final Converter<T> converter) {
		return new Converter<Object>() {
			@Override
			public Object read(DataInput dataInput, Object object) throws IOException {
				return converter.read(dataInput, (T)object);
			}

			@Override
			public void write(DataOutput dataOutput, Object object) throws IOException {
				converter.write(dataOutput, (T)object);
			}
			
		};
	}
	
	/**
	 * Returns a converter which can convert a java type. Only the name of the
	 * java type has to be given. If there is no converter available then this
	 * method returns a ConvertableConverter
	 * (and hopes that the object to be converted is convertable).
	 *
	 * @param javaTypeName String which represents a java classname.
	 * @return the desired converter
	 */
	public static Converter<Object> getConverterForJavaType(String javaTypeName) {
		if (javaTypeName.equals("java.lang.String"))
			return getObjectConverter(StringConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.math.BigDecimal"))
			return getObjectConverter(BigDecimalConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Integer"))
			return getObjectConverter(IntegerConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Short"))
			return getObjectConverter(ShortConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Boolean"))
			return getObjectConverter(BooleanConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Byte"))
			return getObjectConverter(ByteConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Long"))
			return getObjectConverter(LongConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Float"))
			return getObjectConverter(FloatConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.lang.Double"))
			return getObjectConverter(DoubleConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("byte[]"))
			return getObjectConverter(ByteArrayConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.sql.Date"))
			return getObjectConverter(DateConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.sql.Time"))
			return getObjectConverter(TimeConverter.DEFAULT_INSTANCE);
		if(javaTypeName.equals("java.sql.Timestamp"))
			return getObjectConverter(TimestampConverter.DEFAULT_INSTANCE);
		return getObjectConverter(ConvertableConverter.DEFAULT_INSTANCE);
	}
	
	/**
	 * Creates a newly allocated byte array that contains the serialization of
	 * the spezified object. Its size is the size of the serialized object as
	 * returned by <code>sizeOf(converter, object)</code>. If the creation of
	 * the byte array fails (e.g., an <code>IOException</code> is thrown) this
	 * method returns <code>null</code>.
	 * 
	 * <p>This implementation creates a new <code>ByteArrayOutputStream</code>
	 * that is wrapped by a new <code>DataOutputStream</code>. Thereafter the
	 * specified object is written to the <code>DataOutputStream</code> using
	 * its write method of the converter and the result of the
	 * <code>ByteArrayOutputStream</code>'s
	 * {@link ByteArrayOutputStream#toByteArray() toByteArray} method is
	 * returned.</p>
	 *
	 * @param <T> the type of the object to be converted.
	 * @param converter the converter to be used for converting the object.
	 * @param object the object to be converted.
	 * @return the serialized state (attributes) of the specified object, as a
	 *         byte array.
	 */
	public static <T> byte[] toByteArray(Converter<? super T> converter, T object) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream daos = new DataOutputStream(baos);
			converter.write(daos, object);
			return baos.toByteArray();
		}
		catch (IOException ie) {
			return null;
		}
	}

    /**
     * Creates a newly allocated byte array that contains the serialization of
     * the spezified object. Its size is the size of the serialized object as
     * returned by <code>sizeOf(converter, object)</code>. If the creation of
     * the byte array fails (e.g., an <code>IOException</code> is thrown) this
     * method returns <code>null</code>.
     *
     * <p>This implementation creates a new <code>ByteArrayOutputStream</code>
     * that is wrapped by a new <code>DataOutputStream</code>. Thereafter the
     * specified object is written to the <code>DataOutputStream</code> using
     * its write method of the converter and the result of the
     * <code>ByteArrayOutputStream</code>'s
     * {@link ByteArrayOutputStream#toByteArray() toByteArray} method is
     * returned.</p>
     *
     * @param <T> the type of the object to be converted.
     * @param converter the converter to be used for converting the object.
     * @param object the object to be converted.
     * @param serializationMode the type of <tt>DataOutput</tt> that should be used for serialization
     * @param buffer the size of the buffer used during deserialization
     * @return the serialized state (attributes) of the specified object, as a
     *         byte array.
     */
    public static <T> byte[] toByteArray(Converter<? super T> converter, T object, SerializationMode serializationMode, int buffer) {
        try {
            if (serializationMode == SerializationMode.BYTE_BUFFER) {
                ByteBufferDataOutput bbdo = new ByteBufferDataOutput(buffer);
                converter.write(bbdo, object);
                return bbdo.toByteArray();
            } else if (serializationMode == SerializationMode.UNSAFE) {
                UnsafeDataOutput udo = new UnsafeDataOutput(buffer);
                converter.write(udo, object);
                return udo.toByteArray();
            } else {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream daos = new DataOutputStream(baos);
                converter.write(daos, object);
                return baos.toByteArray();
            }
        }
        catch (IOException ie) {
            return null;
        }
    }

	/**
	 * Creates a newly allocated byte array that contains the serialization of
	 * the spezified object. Its size is the size of the serialized object as
	 * returned by <code>sizeOf(converter, object)</code>. If the creation of
	 * the byte array fails (e.g., an <code>IOException</code> is thrown) this
	 * method returns <code>null</code>.
	 * 
	 * <p>This implementation creates a new <code>ByteArrayOutputStream</code>
	 * that is wrapped by a new <code>DataOutputStream</code>. Thereafter the
	 * specified object is written to the <code>DataOutputStream</code> using
	 * its write method of the size converter and the result of the
	 * <code>ByteArrayOutputStream</code>'s
	 * {@link ByteArrayOutputStream#toByteArray() toByteArray} method is
	 * returned.</p>
	 *
	 * @param <T> the type of the object to be converted.
	 * @param converter the size converter to be used for converting the
	 *        object.
	 * @param object the object to be converted.
	 * @return the serialized state (attributes) of the specified object, as a
	 *         byte array.
	 */
	public static <T> byte[] toByteArray(SizeConverter<? super T> converter, T object) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(converter.getSerializedSize(object));
			DataOutputStream daos = new DataOutputStream(baos);
			converter.write(daos, object);
			return baos.toByteArray();
		}
		catch (IOException ie) {
			return null;
		}
	}

	/**
	 * Creates a newly allocated byte array that contains the serialization of
	 * the spezified convertable object. Its size is the size of the serialized
	 * object as returned by <code>sizeOf(convertable)</code>. If the creation
	 * of the byte array fails (e.g., an <code>IOException</code> is thrown)
	 * this method returns <code>null</code>.
	 * 
	 * <p>This implementation creates a new <code>ByteArrayOutputStream</code>
	 * that is wrapped by a new <code>DataOutputStream</code>. Thereafter the
	 * specified object is written to the <code>DataOutputStream</code> using
	 * its write method and the result of the
	 * <code>ByteArrayOutputStream</code>'s
	 * {@link ByteArrayOutputStream#toByteArray() toByteArray} method is
	 * returned.
	 *
	 * @param convertable the convertable object to be converted.
	 * @return the serialized state (attributes) of the specified object, as a
	 *         byte array.
	 */
	public static byte[] toByteArray(Convertable convertable) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream daos = new DataOutputStream(baos);
			convertable.write(daos);
			return baos.toByteArray();
		}
		catch (IOException ie) {
			return null;
		}
	}

	/**
	 * Returns the number of bytes needed to serialize the state (the
	 * attributes) of the specified object. If the measuring fails this method
	 * should return <code>-1.</code>
	 * 
	 * <p>This implementation creates an anonymous local class that implements
	 * the interface <code>OutputStream</code>. The <code>write</code> method
	 * of this local class only increases a counter and the
	 * <code>hashCode</code> method returns it. Thereafter the specified object
	 * is written to the <code>OutputStream</code> using the <code>write</code>
	 * method of the converter. If this call succeeds the hashCode of the
	 * <code>OutputStream</code> (the number of bytes written to the output
	 * stream) is returned otherwise <code>-1</code> is returned.</p>
	 *
	 * @param <T> the type of the object to be converted.
	 * @param converter the converter to be used for converting the object.
	 * @param object the object to be converted.
	 * @return the number of bytes needed to serialize the state (the
	 *         attributes) of the specified object or <code>-1</code> if the
	 *         measuring fails.
	 */
	public static <T> int sizeOf(Converter<? super T> converter, T object) {
		try {
			OutputStream output = new OutputStream() {
				int counter = 0;
				
				@Override
				public void write(int b) {
					counter++;
				}
				
				@Override
				public int hashCode() {
					return counter;
				}
			};
			converter.write(new DataOutputStream(output), object);
			return output.hashCode();
		}
		catch (IOException e) {
			return -1;
		}
	}

	/**
	 * Returns the number of bytes needed to serialize the state (the
	 * attributes) of the specified convertable object. If the measuring fails
	 * this method should return <code>-1</code>.
	 * 
	 * <p>This implementation creates an anonymous local class that implements
	 * the interface <code>OutputStream</code>. The <code>write</code> method
	 * of this local class only increases a counter and the
	 * <code>hashCode</code> method returns it. Thereafter the specified object
	 * is written to the <code>OutputStream</code> using its <code>write</code>
	 * method. If this call succeeds the hashCode of the
	 * <code>OutputStream</code> (the number of bytes written to the output
	 * stream) is returned otherwise <code>-1</code> is returned.</p>
	 *
	 * @param convertable the convertable object to be converted.
	 * @return the number of bytes needed to serialize the state (the
	 *         attributes) of the specified object or <code>-1</code> if the
	 *         measuring fails.
	 */
	public static int sizeOf(Convertable convertable) {
		try {
			OutputStream output = new OutputStream() {
				int counter = 0;

				@Override
				public void write(int b) {
					counter++;
				}

				@Override
				public int hashCode() {
					return counter;
				}
			};
			convertable.write(new DataOutputStream(output));
			return output.hashCode();
		}
		catch (IOException e) {
			return -1;
		}
	}

	/**
	 * Reads a single object from the given file and returns it. The specified
	 * converter is used for reading the object.
	 * 
	 * <p>This implementation opens a new data input stream on the given file
	 * and uses the specified converter to read the desired object. The
	 * specified object is passed to the read method of the converter.</p>
	 * 
	 * <p><b>Note</b> that every time this method is called, a new input stream
	 * is opened and closed on the given file. Therefore this method should not
	 * be used for continuous reading of objects.<p>
	 *
	 * @param <T> the type of the object to be read.
	 * @param file the file from which to read the object.
	 * @param object the object to read. If (<code>object==null</code>) the
	 *        converter should create a new object, else the specified object
	 *        is filled with the read data.
	 * @param converter the converter used for reading the object.
	 * @return the object which was read.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public static <T> T readSingleObject(File file, T object, Converter<T> converter) throws IOException {
		DataInputStream din = new DataInputStream(new FileInputStream(file));
		object = converter.read(din, object);
		din.close();
		return object;
	}

	/**
	 * Writes a single object to the given file. The specified converter is
	 * used for writing the object.
	 * 
	 * <p>This implementation opens a new data output stream on the given file
	 * and uses the specified converter to write the given object. The
	 * specified object is passed to the write method of the converter.</p>
	 * 
	 * <p><b>Note</b> that every time this method is called, a new output
	 * stream is opened and closed on the given file. Therefore this method
	 * should not be used for continuous writing of objects.</p>
	 *
	 * @param <T> the type of the object to be written.
	 * @param file the file to which to write the object.
	 * @param object the object to write.
	 * @param converter the converter used for writing the object.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public static <T> void writeSingleObject(File file, T object, Converter<? super T> converter) throws IOException {
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
		converter.write(dos, object);
		dos.close();
	}

	/**
	 * Reads a single convertable object from the given file and returns it.
	 * 
	 * <p>This implementation is equivalent to the call of
	 * <code>readSingleObject(file, object, ConvertableConverter.DEFAULT_INSTANCE)</code>.</p>
	 * 
	 * <p><b>Note</b> that every time this method is called, a new input stream
	 * is opened and closed on the given file. Therefore this method should not
	 * be used for continuous reading of objects.</p>
	 *
	 * @param file the file from which to read the object.
	 * @param object the object to read. If (<code>object==null</code>) the
	 *        converter should create a new object, else the specified object
	 *        is filled with the read data.
	 * @return the object which was read.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public static Convertable readSingleObject(File file, Convertable object) throws IOException {
		return readSingleObject(file, object, ConvertableConverter.DEFAULT_INSTANCE);
	}

	/**
	 * Writes a single convertable object to the given file.
	 * 
	 * <p>This implementation is equivalent to the call of
	 * <code>writeSingleObject(file, object, ConvertableConverter.DEFAULT_INSTANCE)</code>.</p>
	 * 
	 * <p><b>Note</b> that every time this method is called, a new output
	 * stream is opened and closed on the given file. Therefore this method
	 * should not be used for continuous writing of objects.</p>
	 *
	 * @param file the file to which to write the object.
	 * @param object the object to write.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public static void writeSingleObject (File file, Convertable object) throws IOException {
		writeSingleObject(file, object, ConvertableConverter.DEFAULT_INSTANCE);
	}
	
	/**
	 * This method transforms Converter to MeasuredConverter 
	 * 
	 * MaxObjectSize Parameter will be returned by method getMaxObjectSize() in  @see {@link MeasuredConverter}.
	 * 
	 * @param maxObjectSize getMaxObjectSize()
	 * @param objectConverter actual converter for object serialization
	 * @return MeasuredConverter
	 */
	public static <T> MeasuredConverter<T> createMeasuredConverter(final int maxObjectSize, final Converter<T> objectConverter){
		
		return new MeasuredConverter<T>(){

			@Override
			public int getMaxObjectSize() {
				return maxObjectSize;
			}

			@Override
			public T read(DataInput dataInput, T object) throws IOException {
				return objectConverter.read(dataInput, object);
			}

			@Override
			public void write(DataOutput dataOutput, T object)
					throws IOException {
				objectConverter.write(dataOutput, object);
			}
			
		};
	
	}
	
	/**
	 * This method transforms FixedSizeConverter @see {@link FixedSizeConverter} to MeasuredConverter @see {@link MeasuredConverter}.
	 * The method uses @see {@link MeasuredFixedSizeConverter} for wrapping the FixedSizeConverter
	 * 
	 * 
	 * @param objectConverter
	 * @return
	 */
	public static <T> MeasuredConverter<T> createMeasuredConverter(final FixedSizeConverter<T> objectConverter){
		return new MeasuredFixedSizeConverter<>(objectConverter);
	}
	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Converters() {
		// private access in order to ensure non-instantiability
	}

}
