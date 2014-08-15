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

import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;

/**
 * @author seidemann
 */
public class UnsafeDataInput implements DataInput {

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

    /** The unsafe object used for deserialization */
    private static final Unsafe unsafe;

    // Initialization of unsafe object
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

    /** The next read position in the byte buffer */
    private int pos = 0;
    /** The size of the buffer */
    private int bufferSize;
    /** The byte buffer used with the unsafe object */
    private final byte[] buffer;
    /** The input stream containing the input to be converted */
    private InputStream stream;

    /**
     * Creates a new UnsafeDataInput object for the given input
     * and the given buffer size.
     *
     * @param stream the input to be deserialized
     * @param bufferSize The size of the buffer to be used
     */
    public UnsafeDataInput(InputStream stream, int bufferSize) {
        this.stream = stream;
        this.buffer = new byte[bufferSize];
    }

    /**
     * Creates a new UnsafeDataInput object for the given input with
     * a fixed buffer of size 8192.
     * @param stream
     */
    public UnsafeDataInput(InputStream stream) {
        this(stream,8192);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        if (b == null)
            throw new NullPointerException();
        else if (b.length > 0) {
            for (int i = 0; i < b.length; i++){
                b[i] = readByte();
            }
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        if (b == null)
            throw new NullPointerException();
        else if (off < 0 || len < 0 || off+len > b.length)
            throw new IndexOutOfBoundsException();
        else if (b.length > 0) {
            for (int i = 0; i < len; i++){
                if (pos >= buffer.length)
                    throw new EOFException();
                else
                    b[off+i] = readByte();
            }
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        if (buffer.length - pos >= n)  {
            pos += n;
            return n;
        } else {
            int skipped = buffer.length - pos;
            pos = buffer.length;
            return skipped;
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        ensureBuffer(SIZE_OF_BOOLEAN);
        boolean value = unsafe.getBoolean(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_BOOLEAN;

        return value;
    }

    @Override
    public byte readByte() throws IOException {
        ensureBuffer(SIZE_OF_BYTE);
        byte value = unsafe.getByte(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_BYTE;

        return value;
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte()|0;
    }

    @Override
    public short readShort() throws IOException {
        ensureBuffer(SIZE_OF_SHORT);
        byte a = readByte();
        byte b = readByte();
        short value = (short)((a << 8) | (b & 0xff));
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        byte a = readByte();
        byte b = readByte();
        return (((a & 0xff) << 8) | (b & 0xff));
    }

    @Override
    public char readChar() throws IOException {
        ensureBuffer(SIZE_OF_CHAR);
        char value = unsafe.getChar(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_CHAR;

        return value;
    }

    @Override
    public int readInt() throws IOException {
        ensureBuffer(SIZE_OF_INT);
        int value = unsafe.getInt(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_INT;

        return value;
    }

    @Override
    public long readLong() throws IOException {
        ensureBuffer(SIZE_OF_LONG);
        long value = unsafe.getLong(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_LONG;

        return value;
    }

    @Override
    public float readFloat() throws IOException {
        ensureBuffer(SIZE_OF_FLOAT);
        float value = unsafe.getFloat(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_FLOAT;

        return value;
    }

    @Override
    public double readDouble() throws IOException {
        ensureBuffer(SIZE_OF_DOUBLE);
        double value = unsafe.getDouble(buffer, byteArrayOffset + pos);
        pos += SIZE_OF_DOUBLE;

        return value;
    }

    @Override
    public String readLine() throws IOException {
        StringBuilder builder = new StringBuilder();
        char character = (char) (readByte());
        while (character != '\n' && character != '\r') {
            builder.append(character);
            if (!ensureBuffer(SIZE_OF_BYTE,false))
                break;
            character = (char) unsafe.getByte(buffer, byteArrayOffset + pos);
            pos += SIZE_OF_BYTE;
        }
        return builder.toString();
    }

    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort(); //length of the utf string
        char[] outputBuffer = new char[utflen];
        int bufferPosition = 0;
        int inputCounter = 0;

        byte a,b,c;
        for (; inputCounter < utflen; ) {
            a = readByte();
            switch ( a >> 4) {
                // case 0xxxxxxx (0xxx) = 0-7 : One byte
                case 0:case 1: case 2:case 3:case 4:case 5:case 6:case 7:
                    inputCounter++;
                    if (inputCounter > utflen)
                        throw new UTFDataFormatException("invalid input: incomplete character at the end");
                    outputBuffer[bufferPosition++] = (char) a;
                    break;
                // case 110xxxxx (110x) = 12,13 : two bytes
                case 12:case 13:
                    inputCounter += 2;
                    if (inputCounter > utflen)
                        throw new UTFDataFormatException("invalid input: incomplete character at the end");
                    b = readByte();
                    if ((b >> 6) != 2)
                        throw new UTFDataFormatException("Expected byte has to be of the form 10xxxxxx");
                    else {
                        outputBuffer[bufferPosition++] = (char)(((a& 0x1f) << 6) | (b & 0x3f));
                    }
                    break;
                // case 1110xxxx (1110) : 14 three bytes
                case 14:
                    inputCounter += 3;
                    if (inputCounter > utflen)
                        throw new UTFDataFormatException("invalid input: incomplete character at the end");
                    b = readByte();
                    if ((b >> 6) != 2)
                        throw new UTFDataFormatException("Expected byte at position "+(inputCounter-2)+" has to be of the form 10xxxxxx");
                    else {
                        c = readByte();
                        if ((c >> 6) != 2)
                            throw new UTFDataFormatException("Expected byte at position "+(inputCounter-3)+" has to be of the form 10xxxxxx");
                        else {
                            outputBuffer[bufferPosition++] = (char)(((a & 0x0f) << 12) | ((b & 0x3f) << 6) | (c & 0x3f));
                        }
                    }
                    break;
                // case 1111xxxx (1111) : 15 UTFDataFormatException
                // case 10xxxxxx (10xx) : 8,9,10,11 UTFDataFormatException
                    default: throw new UTFDataFormatException("invalid input");
            }
        }
        return new String(outputBuffer);
    }


    private boolean ensureBuffer(int bytes, boolean strict) throws IOException {
        if (pos+bytes > bufferSize) {
            int offset = bufferSize-pos;
            if (offset > 0) {
                unsafe.copyMemory(buffer,byteArrayOffset+pos,buffer,byteArrayOffset,offset);
            }
            int bytesRead = stream.read(buffer,offset,buffer.length-offset);
            bufferSize = bytesRead+offset;
            pos = 0;
            if (bytesRead < bytes && strict)
                throw new IOException();
            else
                return bytesRead >= bytes;
        } else
            return true;
    }

    /** Ensures the buffer contains the required data
     *
     * @param bytes the number of bytes required
     * @throws IOException
     */
    private void ensureBuffer(int bytes) throws IOException{
        ensureBuffer(bytes,true);
    }

}
