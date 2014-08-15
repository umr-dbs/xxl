/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2014 Prof. Dr. Bernhard Seeger
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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This class provides a DataOutput implementation using java.nio.ByteBuffer.
 * The underlying byte buffer object operates on an initially allocated final
 * byte buffer and is used for serialization of the primitive values.
 *
 * Additional to the methods defined in DataOutput, this implementation
 * offers direct access to the written bytes and can be reset.
 *
 * @see java.io.DataOutput
 * @see ByteBufferDataInput
 */
public class ByteBufferDataOutput implements DataOutput {

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

    /** The byte buffer object used for serialization */
    private final ByteBuffer buffer;

    /** The next write position in the byte buffer */
    private int pos = 0;

    /**
     * Creates a new ByteBufferDataOutput object with the given underlying byte
     * buffer size
     *
     * @param size the size of the byte array used to serialize the data
     */
    public ByteBufferDataOutput(int size) {
        buffer = ByteBuffer.allocate(size);
    }

    @Override
    public void write(int b) throws IOException {
        ensureBuffer(SIZE_OF_BYTE);
        buffer.put((byte)b);
        pos += SIZE_OF_BYTE;
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b == null)
            throw new NullPointerException();
        else if (b.length > 0) {
            ensureBuffer(b.length);
            buffer.put(b);
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
            ensureBuffer(len);
            buffer.put(b,off,len);
            pos += len;
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        ensureBuffer(SIZE_OF_BOOLEAN);
        buffer.put((byte)(v?1:0));
        pos += SIZE_OF_BOOLEAN;
    }

    @Override
    public void writeByte(int v) throws IOException {
        ensureBuffer(SIZE_OF_BYTE);
        buffer.put((byte)v);
        pos += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int v) throws IOException {
        ensureBuffer(SIZE_OF_SHORT);
        buffer.put((byte) (0xff & (v >> 8)));
        buffer.put((byte)(0xff & v));
        pos += SIZE_OF_SHORT;
    }

    @Override
    public void writeChar(int v) throws IOException {
        ensureBuffer(SIZE_OF_CHAR);
        buffer.putChar((char) v);
        pos += SIZE_OF_CHAR;
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureBuffer(SIZE_OF_INT);
        buffer.putInt(v);
        pos += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureBuffer(SIZE_OF_LONG);
        buffer.putLong(v);
        pos += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        ensureBuffer(SIZE_OF_FLOAT);
        buffer.putFloat(v);
        pos += SIZE_OF_FLOAT;
    }

    @Override
    public void writeDouble(double v) throws IOException {
        ensureBuffer(SIZE_OF_DOUBLE);
        buffer.putDouble(v);
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
        // Calculate the UTF length
        int utflen = 0;
        for(int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '\u0001' && c <= '\u007f') {
                utflen++;
            } else if (c == '\u0000' || (c >= '\u0080' && c <= '\u07ff')) {
                utflen += 2;
            } else if (c >= '\u0800' && c <= '\uffff') {
                utflen += 3;
            }
        }

        // Store UTF length
        if (utflen > 65535)
            throw new UTFDataFormatException("String input too long!");
        else writeShort(utflen);

        // Write the data
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
     * Ensures that the buffer contains the requested amount of data.
    * 
     * @param size the size of the requested data
     * @throws IOException if there is not enough data left
     */
    private void ensureBuffer(int size) throws IOException {
        if (buffer.position()+size > buffer.capacity())
            throw new IOException("Buffer overflow: tried to append "+size+
                    " bytes to the buffer of capacity "+buffer.capacity()+
                    ", that already contains "+buffer.position()+" bytes");
    }

    /**
     * Returns the written bytes from the buffer.
     * @return the bytes written to the buffer
     */
    public byte[] toByteArray(){
        return Arrays.copyOf(buffer.array(), pos);
    }

    /**
     * Resets this data output.
     */
    public void reset() {
        pos = 0;
        buffer.clear();
    }
}
