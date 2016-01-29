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

package xxl.core.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import xxl.core.util.WrappingRuntimeException;

/**
 * 	Conversion routines: byte array &harr; short, int, long.
 *	<br>
 *	There are some catogories of methods:
 *	<ul>
 * 	<li>
 *		Methods with the suffix LE assume little endian byte ordering. Example for 4-Byte integers:<br>
 * 		<code>01 00 00 00 = 1, 00 00 00 01 = 65536, 00 00 00 80 = -2147483648</code>
 *	</li>
 *	<li>
 *		Methods with the suffix BE assume big endian byte ordering (so far not supported!). Example for 4-Byte integers:<br>
 * 		<code>01 00 00 00 = 16777216, 00 00 00 01 = 1, 00 00 00 80 = 128</code>
 *	</li>
 *	<li>
 *		Methods with the suffix Stream use streams to convert the types to byte arrays
 *		and back. These methode are usually much slower. Java uses big endian byte ordering.
 *	</li>
 *	<li>
 *		Other methods.
 *	</li>
 *	</ul>
 */
public class ByteArrayConversions
{
	/** 
	 * Do not allow instances of this class 
	 */
	private ByteArrayConversions() {}

	/**
	 * Converts the given byte to a short.
	 * @param b byte to convert.
	 * @return short.
	 */	
	public static short conv255(byte b)
	{
		return (b<0)?(short) (256+b):(short)b;
	}
	
	/**
	 * Converts the given byte to a short.
	 * @param b byte to convert.
	 * @return short.
	 */	
	public static short conv127(byte b)
	{
		return (short) (b&127);
	}
	
	/**
	 * Converts the byte array to a short. It is assumed that the array has a length of 2.
	 * @param b the byte array to convert.
	 * @return short.
	 */
	public static short convShortLE(byte b[])
	{
		// Variable kostet fast keine Zeit!
		int zw = (conv127(b[1])<<8) | conv255(b[0]);
		if (b[1]<0) // Vorzeichen
			return (short) (zw - (1<<15));
		else
			return (short) zw;
	}

	/**
	 * Converts the byte array to a short. It is assumed that the array has a length of 2.
	 * @param b the byte array to convert.
	 * @param offset start if the integer inside the array.
	 * @return short.
	 */
	public static short convShortLE(byte b[], int offset)
	{
		// Variable kostet fast keine Zeit!
		int zw = (conv127(b[offset+1])<<8) | conv255(b[offset]);
		if (b[offset+1]<0) // Vorzeichen
			return (short) (zw - (1<<15));
		else
			return (short) zw;
	}
	
	/**
	 * Converts the two given byte values to a short.
	 * @param b0 the 'left' byte value.
	 * @param b1 the 'right' byte value.
	 * @return short.
	 */
	public static short convShortLE(byte b0, byte b1)
	{
		// Variable kostet fast keine Zeit!
		int zw = (conv127(b1)<<8) | conv255(b0);
		if (b1 < 0) // Vorzeichen
			return (short) (zw - (1<<15));
		else
			return (short) zw;
	}
	
	/**
	 * Converts the given byte to a short value.
	 * @param b the byte to convert.
	 * @return short.
	 */
	public static short convShortLE(byte b)
	{
		return conv255(b);
	}
	
	/**
	 * Converts the byte array to a short value.
	 * @param b the byte array.
	 * @return short.
	 */	
	public static short convShortStream(byte b[])
	{
		try
		{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(output);
			out.write(b,0,b.length);
			ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
			DataInput in = new DataInputStream(input);
			return in.readShort();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Converts the given byte array to an int value. It is assumed that the byte
	 * array has a length of 4.
	 * @param b the byte array.
	 * @return int.
	 */	
	public static int convIntLE(byte b[])
	{
		int zw = (((((conv127(b[3])<<8) | conv255(b[2]))<<8) | conv255(b[1]))<<8) | conv255(b[0]);
		if (b[3]<0) // Vorzeichen
			return zw - (1<<31);
		else
			return zw;
	}

	/**
	 * Converts the given byte array to an int value. It is assumed that the byte
	 * array has a length of 4.
	 * @param b the byte array.
	 * @param offset start offset inside the array.
	 * @return int.
	 */	
	public static int convIntLE(byte b[], int offset)
	{
		int zw = (((((conv127(b[offset+3])<<8) | conv255(b[offset+2]))<<8) | conv255(b[offset+1]))<<8) | conv255(b[offset]);
		if (b[offset+3]<0) // Vorzeichen
			return zw - (1<<31);
		else
			return zw;
	}

	/**
	 * Convert the given bytes to an int value.
	 * @param b0 the 'outer left' byte value.
	 * @param b1 the byte value at the right of b0.
	 * @param b2 the byte value at the right of b1.
	 * @param b3 the byte value at the right of b2.
	 * @return int.
	 */
	public static int convIntLE(byte b0, byte b1, byte b2, byte b3)
	{
		int zw = (((((conv127(b3)<<8) | conv255(b2))<<8) | conv255(b1))<<8) | conv255(b0);
		if (b3<0) // Vorzeichen
			return zw - (1<<31);
		else
			return zw;
	}
	
	/**
	 * Convert the given bytes to an int value.
	 * @param b0 the 'left' byte value.
	 * @param b1 the 'right' byte value.
	 * @return int.
	 */
	public static int convIntLE(byte b0, byte b1)
	{
		return ((conv255(b1))<<8) | conv255(b0);
	}

	/**
	 * Convert the byte array to an int value.
	 * @param b the byte array.
	 * @return int.
	 */	
	public static int convIntStream(byte b[])
	{
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(output);
			out.write(b,0,b.length);
			ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
			DataInput in = new DataInputStream(input);
			return in.readInt();
		}
		catch (IOException e) {
			throw new xxl.core.util.WrappingRuntimeException(e);
			//return 0;
		}
	}

	/**
	 * Convert the given byte array to a long value. It is assumed that the
	 * byte array has a length of 8.
	 * @param b the byte array.
	 * @return long.
	 */	
	public static long convLongLE(byte b[])
	{
		long zw = ((long) conv127(b[7])<<56) | ((long) conv255(b[6])<<48) | ((long) conv255(b[5])<<40) | ((long) conv255(b[4])<<32) |
			((long) conv255(b[3])<<24) | ((long) conv255(b[2])<<16) | ((long) conv255(b[1])<<8) | conv255(b[0]);
		if (b[7]<0) // Vorzeichen
			return zw - (((long) 1)<<63);
		else
			return zw;
	}

	/**
	 * Convert the given byte array to a long value. It is assumed that the
	 * byte array has a length of 8.
	 * @param b the byte array.
	 * @param offset start if the integer inside the array.
	 * @return long.
	 */	
	public static long convLongLE(byte b[], int offset)
	{
		long zw = ((long) conv127(b[offset+7])<<56) | ((long) conv255(b[offset+6])<<48) | ((long) conv255(b[offset+5])<<40) | ((long) conv255(b[offset+4])<<32) |
			((long) conv255(b[offset+3])<<24) | ((long) conv255(b[offset+2])<<16) | ((long) conv255(b[offset+1])<<8) | conv255(b[offset]);
		if (b[offset+7]<0) // Vorzeichen
			return zw - (((long) 1)<<63);
		else
			return zw;
	}
	
	/**
	 * Convert the given byte values to a long value.
	 * @param b0 the 'outer left' byte value.
	 * @param b1 the byte value at the right of b0.
	 * @param b2 the byte value at the right of b1.
	 * @param b3 the byte value at the right of b2.
	 * @param b4 the byte value at the right of b3.
	 * @param b5 the byte value at the right of b4.
	 * @param b6 the byte value at the right of b5.
	 * @param b7 the byte value at the right of b6.
	 * @return long.
	 */
	public static long convLongLE(byte b0, byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7)
	{
		long zw = ((long) conv127(b7)<<56) | ((long) conv255(b6)<<48) | ((long) conv255(b5)<<40) | ((long) conv255(b4)<<32) |
			((long) conv255(b3)<<24) | ((long) conv255(b2)<<16) | ((long) conv255(b1)<<8) | conv255(b0);
		if (b7<0) // Vorzeichen
			return zw - (((long) 1)<<63);
		else
			return zw;
	}
	
	/**
	 * Convert the given byte values to a long value.
	 * @param b0 lowest byte (byte 0)
	 * @param b1 byte 1
	 * @param b2 byte 2
	 * @param b3 hightest byte (byte 3)
	 * @return long.
	 */
	public static long convLongLE(byte b0, byte b1, byte b2, byte b3)
	{
		return ((long) conv255(b3)<<24) | ((long) conv255(b2)<<16) | ((long) conv255(b1)<<8) | conv255(b0);
	}
	
	/**
	 * Convert the byte array to an long value.
	 * @param b the byte array.
	 * @return long.
	 */	
	public static long convLongStream(byte b[])
	{
		try {
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(output);
			out.write(b,0,b.length);
			ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
			DataInput in = new DataInputStream(input);
			return in.readLong();
		}
		catch (IOException e) {
			throw new xxl.core.util.WrappingRuntimeException(e);
			//return 0;
		}
	}

	/**
	 * Convert the integer to a byte array.
	 * @param i the integer value
	 * @param b array for the output.
	 */
	public static void convIntToByteArrayLE(int i, byte b[]) {
		b[0] = (byte) (i&255);
		i>>=8;
		b[1] = (byte) (i&255);
		i>>=8;
		b[2] = (byte) (i&255);
		i>>=8;
		b[3] = (byte) (i&255);
	}
	
	/**
	 * Convert the integer to a byte array.
	 * @param i the integer value
	 * @param b array for the output.
	 * @param offset determines the location to which the values are written inside the byte array.
	 */
	public static void convIntToByteArrayLE(int i, byte b[], int offset) {
		b[offset++] = (byte) (i&255);
		i>>=8;
		b[offset++] = (byte) (i&255);
		i>>=8;
		b[offset++] = (byte) (i&255);
		i>>=8;
		b[offset] = (byte) (i&255);
	}
	
	/**
	 * Convert the integer to a byte array.
	 * @param i the integer value
	 * @return byte array.
	 */
	public static byte[] convIntToByteArrayLE(int i) {
		byte b[] = new byte[4];
		convIntToByteArrayLE(i,b);
		return b;
	}
	
	/**
	 * Convert the integer to a byte array.
	 * @param i the integer value
	 * @return byte array.
	 */
	public static byte[] convIntToByteArrayStream(int i) {
		ByteArrayOutputStream bao = new ByteArrayOutputStream(4);
		DataOutputStream dos = new DataOutputStream(bao);
		try {
			dos.writeInt(i);
			dos.flush();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
		return bao.toByteArray();
	}
	
	/**
	 * Convert the long to a byte array.
	 * @param l the long value
	 * @param b array for the output.
	 */
	public static void convLongToByteArrayLE(long l, byte b[]) {
		b[0] = (byte) (l&255);
		l>>=8;
		b[1] = (byte) (l&255);
		l>>=8;
		b[2] = (byte) (l&255);
		l>>=8;
		b[3] = (byte) (l&255);
		l>>=8;
		b[4] = (byte) (l&255);
		l>>=8;
		b[5] = (byte) (l&255);
		l>>=8;
		b[6] = (byte) (l&255);
		l>>=8;
		b[7] = (byte) (l&255);
	}
	
	/**
	 * Convert the long to a byte array.
	 * @param l the long value
	 * @param b byte array for the output.
	 * @param offset determines the location to which the values are written inside the byte array.
	 */
	public static void convLongToByteArrayLE(long l, byte b[], int offset) {
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset++] = (byte) (l&255);
		l>>=8;
		b[offset] = (byte) (l&255);
	}

	/**
	 * Convert the long to a byte array.
	 * @param l the long value
	 * @return byte array.
	 */
	public static byte[] convLongToByteArrayLE(long l) {
		byte b[] = new byte[8];
		convLongToByteArrayLE(l,b);
		return b;
	}
	
	/**
	 * Retun the string representation of the byte array. Starting at offset from (inclusive)
	 * until the index to (exclusive).
	 * @param b the byte array.
	 * @param from index (inclusive).
	 * @param to index (exclusive).
	 * @return a string representation of the byte array.
	 */
	public static String byteToString(byte[] b, int from, int to)
	{
		try {
			//return new String(bpb, from, to-from, "US-ASCII");
			return new String(b, from, to-from, "ISO-8859-1");
			//return new String(bpb, from, from+2, "UTF-8");
			//return new String(bpb, from, to-from, "UTF-16BE");
		}
		catch (UnsupportedEncodingException e) {
			throw new WrappingRuntimeException(e);
		}
	}
	
	/**
	 * Retun the unicode string representation of the byte array. Starting at
	 * offset from (inclusive) until the index to (exclusive).
	 * @param b the byte array.
	 * @param from index (inclusive).
	 * @param to index (exclusive).
	 * @return a unicode  string representation of the byte array.
	 */
	public static String unicodeByteToString(byte[] b, int from, int to)
	{
		try {
			return new String(b, from, to-from, "UTF-16LE");
		}
		catch (UnsupportedEncodingException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Converts a litte endian representation into a big endian
	 * representation and vice versa.
	 * @param b input byte array.
	 * @return converted byte array.
	 */
	public static byte[] endianConversion(byte b[]) {
		byte bconv[] = new byte[b.length];
		for (int i=0 ; i<b.length ; i++)
			bconv[i] = b[b.length-1-i];
		return bconv;
	}
	
}
