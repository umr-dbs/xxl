/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2013 Prof. Dr. Bernhard Seeger
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

package xxl.core.io;

import sun.misc.Unsafe;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * This class provides a DataOutput implementation using java.lang.Unsafe.
 * The underlying unsafe object operates on an initially allocated final
 * byte array and is used for serialization of the primitive values.
 *
 * Additional to the methods defined in DataOutput, this implementation
 * offers direct access to the written bytes and can be reset.
 *
 * @see java.io.DataOutput
 * @see xxl.core.io.UnsafeDataInput
 */
public class UnsafeDataOutput implements DataOutput {

    /** Size of a serialized boolean value */
    private static final int SIZE_OF_BOOLEAN = 1;
    /** Size of a serialized byte value */
    private static final int SIZE_OF_BYTE = 1;
    /** Size of a serialized short value */
    private static final int SIZE_OF_SHORT= 2;
    /** Size of a serialized char value */
    private static final int SIZE_OF_CHAR= 2;
    /** Size of a serialized integer value */
    private static final int SIZE_OF_INT = 4;
    /** Size of a serialized float value */
    private static final int SIZE_OF_FLOAT = 4;
    /** Size of a serialized long value */
    private static final int SIZE_OF_LONG = 8;
    /** Size of a serialized double value */
    private static final int SIZE_OF_DOUBLE = 8;

    /** The unsafe object used for serialization */
    private static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe)field.get(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /** Offset of a byte array */
    private static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);

    /** The next write position in the byte buffer */
    private int pos = 0;
    /** The byte buffer used with the unsafe object */
    private final byte[] buffer;


    /**
     * Creates a new UsafeDataOutput object with the given underlying byte
     * buffer size
     *
     * @param size the size of the byte array used with Unsafe to serialize the data
     */
    public UnsafeDataOutput(int size) {
        buffer = new byte[size];
    }

    @Override
    public void write(int b) throws IOException {
        unsafe.putByte(buffer, byteArrayOffset + pos, (byte)b);
        pos += SIZE_OF_BYTE;
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b == null)
            throw new NullPointerException();
        else if (b.length > 0) {
            unsafe.copyMemory(b, byteArrayOffset,
                    buffer, byteArrayOffset + pos,
                    b.length);
            pos += b.length;
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null)
            throw new NullPointerException();
        else if (len < 0 || off < 0 || (off+len) > b.length)
            throw new IndexOutOfBoundsException();
        else if (len > 0) {
            unsafe.copyMemory(b, byteArrayOffset+off,
                    buffer, byteArrayOffset + pos,
                    len);
            pos += len;
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        unsafe.putBoolean(buffer, byteArrayOffset + pos, v);
        pos += SIZE_OF_BOOLEAN;
    }

    @Override
    public void writeByte(int v) throws IOException {
        unsafe.putByte(buffer, byteArrayOffset + pos, (byte)v);
        pos += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int v) throws IOException {
        unsafe.putByte(buffer,byteArrayOffset + pos,(byte)(0xff & (v >> 8)));
        unsafe.putByte(buffer,byteArrayOffset + pos,(byte)(0xff & v));
        pos += SIZE_OF_SHORT;
    }

    @Override
    public void writeChar(int v) throws IOException {
        unsafe.putChar(buffer, byteArrayOffset + pos, (char)v);
        pos += SIZE_OF_CHAR;
    }

    @Override
    public void writeInt(int v) throws IOException {
        unsafe.putInt(buffer, byteArrayOffset + pos, v);
        pos += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long v) throws IOException {
        unsafe.putLong(buffer, byteArrayOffset + pos, v);
        pos += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        unsafe.putFloat(buffer, byteArrayOffset + pos, v);
        pos += SIZE_OF_FLOAT;
    }

    @Override
    public void writeDouble(double v) throws IOException {
        unsafe.putDouble(buffer, byteArrayOffset + pos, v);
        pos += SIZE_OF_DOUBLE;
    }

    @Override
    public void writeBytes(String s) throws IOException {
        if (s == null)
            throw new NullPointerException();
        else if (s.length() > 0) {
            char[] result = new char[s.length()];
            s.getChars(0,result.length,result,0);
            for(int i = 0; i < result.length; i++)
                writeByte(result[i]);
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        if (s == null)
            throw new NullPointerException();
        else if (s.length() > 0) {
            char[] result = new char[s.length()];
            s.getChars(0,result.length,result,0);
            for(int i = 0; i < result.length; i++)
                writeChar(result[i]);
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        for(int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '\u0001' && c <= '\u007f') {
                writeByte((byte)c);
            } else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff')) {
                writeByte((byte)(0xc0 | (0x1f & (c >> 6))));
                writeByte((byte)(0x80 | (0x3f & c)));
            } else if (c >= '\u0800' && c <= '\uffff') {
                writeByte((byte)(0xe0 | (0x0f & (c >> 12))));
                writeByte((byte)(0x80 | (0x3f & (c >>  6))));
                writeByte((byte)(0x80 | (0x3f & c)));
            }
        }
    }

    /**
     * Returns the written bytes from the buffer.
     * @return the bytes written to the buffer
     */
    public byte[] toByteArray(){
        return java.util.Arrays.copyOf(buffer, pos);
    }

    /**
     * Resets this data output.
     */
    public void reset() {
        pos = 0;
    }
}
