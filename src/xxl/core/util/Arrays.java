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

package xxl.core.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.ArrayCursor;
import xxl.core.cursors.sources.Enumerator;
import xxl.core.io.ByteArrayConversions;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * The class <code>Arrays</code> contains methods which
 * are concerned with arrays of primitive Java types.
 */
public class Arrays {

	/**
	 * This class should never be instantiated
	 */
	private Arrays() {}

	/**
	 * Returns a new <code>boolean</code> array that is initialized according to
	 * the parameters. This method is useful especially in
	 * the relationalNEU package.
	 *
	 * @param size size of the array.
	 * @param initValue value that all boolean values get.
	 * @return returns a new boolean array
	 * 
	 */
	public static boolean[] newBooleanArray(int size, boolean initValue) {
		boolean[] b = new boolean[size];
		java.util.Arrays.fill(b,initValue);
		return b;
	}

	/**
	 * Returns a new <code>int</code> array that is initialized according to
	 * the parameters.
	 *
	 * @param size size of the array.
	 * @param initValue value that all int values get.
	 * @return returns a new int array
	 */
	public static int[] newIntArray(int size, int initValue) {
		int[] i = new int[size];
		java.util.Arrays.fill(i,initValue);
		return i;
	}

	/**
	 * Returns a new <code>byte</code> array that is initialized according to
	 * the parameters.
	 *
	 * @param size size of the array.
	 * @param byteValue value that all byte values get.
	 * @return returns a new byte array
	 */
	public static byte[] newByteArray(int size, byte byteValue) {
		byte[] b = new byte[size];
		java.util.Arrays.fill(b,byteValue);
		return b;
	}

	/**
	 * Returns a new <code>char</code> array that is initialized according to
	 * the parameters.
	 *
	 * @param size size of the array.
	 * @param charValue value that all char values get.
	 * @return returns a new char array
	 */
	public static char[] newCharArray(int size, char charValue) {
		char[] c = new char[size];
		java.util.Arrays.fill(c,charValue);
		return c;
	}

	/**
	 * Returns a new <code>int</code> array that is initialized according to
	 * the iterator.
	 *
	 * @param size size of the array.
	 * @param iterator Iterator containing the initialization int-values. The iterator
	 *	should contain exactly <code>size</code> elements. If that is not
	 *	true, then 0s are added (or the rest of the iterator may not be
	 *	traversed).
	 * @return returns a new int array
	 */
	public static int[] newIntArray(int size, Iterator iterator) {
		int[] i = new int[size];
		for (int counter = 0; counter<size ; counter++) {
			if (iterator.hasNext())
				i[counter] = ((Integer) iterator.next()).intValue();
			else
				i[counter]=0;
		}
		return i;
	}

	/**
	 * Copies a <code>boolean</code> array and returns the array of size toIndex-fromIndex.
	 *
	 * @param array boolean array.
	 * @param fromIndex the index of the first element (inclusive) to be copied.
	 * @param toIndex the index of the last element (exclusive) to be copied.
	 * @return returns a new array created
	 */
	public static boolean[] copy(boolean[] array,int fromIndex,int toIndex) {
		boolean[] array2 = new boolean[toIndex-fromIndex];
		for (int i=fromIndex; i<toIndex; i++)
			array2[i-fromIndex] = array[i];
		return array2;
	}

	/**
	 * Copies a <code>byte</code> array and returns the array of size toIndex-fromIndex.
	 *
	 * @param array byte array.
	 * @param fromIndex the index of the first element (inclusive) to be copied.
	 * @param toIndex the index of the last element (exclusive) to be copied.
	 * @return returns a new array created
	 */
	public static byte[] copy(byte[] array,int fromIndex,int toIndex) {
		byte[] array2 = new byte[toIndex-fromIndex];
		for (int i=fromIndex; i<toIndex; i++)
			array2[i-fromIndex] = array[i];
		return array2;
	}

	/**
	 * Copies an <code>int</code> array and returns the array of size toIndex-fromIndex.
	 *
	 * @param array int array.
	 * @param fromIndex the index of the first element (inclusive) to be copied.
	 * @param toIndex the index of the last element (exclusive) to be copied.
	 * @return returns a new array created
	 */
	public static int[] copy(int[] array,int fromIndex,int toIndex) {
		int[] array2 = new int[toIndex-fromIndex];
		for (int i=fromIndex; i<toIndex; i++)
			array2[i-fromIndex] = array[i];
		return array2;
	}

	/**
	 * Removes a part of a <code>byte</code> array and returns the resulting
	 * array. The size of the array depends on the shrink-Parameter.
	 *
	 * @param array byte array.
	 * @param fromIndex the index of the first element (inclusive) to be removed.
	 * @param toIndex the index of the last element (exclusive) to be removed.
	 * @param shrink determines if the returned array has length array.length-(toIndex-fromIndex)
	 *	(shrink==true) or array.length (shrink==false).
	 * @return returns the resulting array 
	 */
	public static byte[] remove(byte[] array, int fromIndex, int toIndex, boolean shrink) {
		int dataLength = array.length-(toIndex-fromIndex);
		byte[] array2;
		if (shrink)
			array2 = new byte[dataLength];
		else
			array2 = new byte[array.length];
		for (int i=0; i<fromIndex; i++)
			array2[i] = array[i];
		for (int i=fromIndex; i<dataLength ; i++)
			array2[i] = array[i+(toIndex-fromIndex)];
		return array2;
	}

	/**
	 * Removes a part of a <code>int</code> array and returns the resulting
	 * array. The size of the array depends on the shrink-Parameter.
	 *
	 * @param array int array.
	 * @param fromIndex the index of the first element (inclusive) to be removed.
	 * @param toIndex the index of the last element (exclusive) to be removed.
	 * @param shrink determines if the returned array has length array.length-(toIndex-fromIndex)
	 *	(shrink==true) or array.length (shrink==false).
	 * @return returns the resulting array 
	 */
	public static int[] remove(int[] array, int fromIndex, int toIndex, boolean shrink) {
		int dataLength = array.length-(toIndex-fromIndex);
		int[] array2;
		if (shrink)
			array2 = new int[dataLength];
		else
			array2 = new int[array.length];
		for (int i=0; i<fromIndex; i++)
			array2[i] = array[i];
		for (int i=fromIndex; i<dataLength ; i++)
			array2[i] = array[i+(toIndex-fromIndex)];
		return array2;
	}

	/**
	 * Returns a hashcode for a byte array. This mus be efficient, but 
	 * return many different values.
	 * @param b byte array
	 * @param offset start of the valid part of the array
	 * @param size size of the valid part inside the array
	 * @return a hash code
	 */
	public static int getHashCodeForByteArray(byte b[], int offset, int size) {
		if (size>=5)
			return 
				ByteArrayConversions.convIntLE(b, offset) ^
				ByteArrayConversions.convIntLE(b, offset+size-4);
		else {
			int ret=42;
			for (int i=offset; i<offset+size; i++) {
				ret <<= 8;
				ret = ret | b[i];
			}
			return ret;
		}

	}

	/**
	 * Outputs a boolean array to a PrintStream.
	 *
	 * @param array boolean array.
	 * @param out PrintStream to which the representation of the array is written.
	 */
	public static void print(boolean[] array,PrintStream out) {
		out.print("[");
		for (int i=0; i<array.length; i++)
			out.print(" "+array[i]);
		out.print("]");
	}

	/**
	 * Outputs an int array to a PrintStream.
	 *
	 * @param array int array.
	 * @param out PrintStream to which the representation of the array is written.
	 */
	public static void print(int[] array,PrintStream out) {
		out.print("[");
		for (int i=0; i<array.length; i++)
			out.print(" "+array[i]);
		out.print("]");
	}

	/**
	 * Outputs an Object-array to a PrintStream.
	 *
	 * @param array Object array.
	 * @param out PrintStream to which the representation of the array is written.
	 */
	public static void print(Object[] array,PrintStream out) {
		out.print("[");
		for (int i=0; i<array.length; i++)
			out.print(" "+array[i]);
		out.print("]");
	}

	/**
	 * Outputs an Object-array as a line to a PrintStream.
	 *
	 * @param array Object array.
	 * @param out PrintStream to which the representation of the array is written.
	 * @since 1.1
	 */
	public static void println(Object[] array,PrintStream out) {
		out.print("[ ");
		if (array.length>0) 
			out.print(array[0]);
		for (int i=1; i<array.length; i++)
			out.print(" , "+array[i]);
		out.println(" ]");
	}	

	/**
	 * Hex chars inside an array.
	 */
	public static char HEXCHARS[] = new char[]{'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

	/**
	 * Print the given byte as a hex number to a String.
	 * @param b byte value
	 * @return hexadecimal representation
	 */
	public static String printHex(byte b) {
		String res = "";
		res += HEXCHARS[((b >> 4) & 0x0F)];
		res += HEXCHARS[(b & 0x0F)];
		return res;
	}
	
	/**
	 * Print a given 32-bit value (as long) as a hex number to a String.
	 * @param l long value
	 * @return hexadecimal representation
	 */
	public static String printHex(long l) {
		StringBuffer res = new StringBuffer();
		res.append(HEXCHARS[(int)((l >> 32) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 28) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 24) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 20) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 16) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 12) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 8) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)((l >> 4) & 0x000000000000000F)]);
		res.append(HEXCHARS[(int)(l & 0x000000000000000F)]);
		return res.toString();
	}

	/**
	 * Print the array as a chain of hex numbers the start line number will be zero.
	 * @param arr byte array
	 * @param out PrintStream to which the representation of the array is written.
	 */
	public static void printHexArray(byte[] arr, PrintStream out) {
		printHexArray(arr, 0, out);
	}
	
	/**
	 * Print the given byte array as a chain of hex numbers. You can
	 * specify a line number so that the chain is numbered.
	 * @param arr byte array
	 * @param lineNum Number of the first output line.
	 * @param out PrintStream to which the representation of the array is written.
	 */
	public static void printHexArray(byte[] arr, long lineNum, PrintStream out) {
		for (int i=0; i <= arr.length/16; i++)
		{
			printHex(lineNum);
			out.print("h: ");
			for(int j=0; j < 16; j++)
			{
				if (i*16+j >= arr.length)
					return;
					
				printHex(arr[i*16 + j]);
				out.print(" ");
			}
			lineNum += 16;
			
			out.println();
		}
	}
	
	/**
	 * Print the array as a chain of hex numbers the start line number will be zero.
	 * @param arr byte array
	 * @return hexadecimal representation
	 */
	public static String printHexArrayString(byte[] arr) {
		return printHexArrayString(arr, 0);
	}
	
	/**
	 * Print the given byte array as a chain of hex numbers to a String. You can
	 * specifie a line number so that the chain is numbered.
	 * @param arr byte array
	 * @param lineNum the line numbler
	 * @return hexadecimal representation
	 */
	public static String printHexArrayString(byte[] arr, long lineNum) {
		StringBuffer result = new StringBuffer();
		for (int i=0; i <= arr.length/16; i++)
		{
			result.append(printHex(lineNum));
			result.append("h: ");
			for(int j=0; j < 16; j++)
			{
				if (i*16+j >= arr.length)
					return result.toString();
					
				result.append(printHex(arr[i*16 + j]));
				result.append(" ");
			}
			lineNum += 16;
			
			result.append("\n");
		}
		return result.toString();
	}
	
	/**
	 * The elements from array[0] to array[array.length-1] will be
	 * returned by this cursor.
	 * A <code>new Enumerator(array.length)</code> lets
	 * the elements be returned in the same order as they
	 * are contained in the given object array.
	 *
	 * @param array the object array delivering the elements.
	 * @return a cursor delivering the elements backed on the given array.
	 */
	public static Cursor arrayToCursor(Object[] array) {
		return new ArrayCursor(array);
	}

	/**
	 * The elements from array[begin] to array[end-1] will be
	 * returned by this cursor.
	 * A <code>new Enumerator(array.length)</code> lets
	 * the elements be returned in the same order as they
	 * are contained in the given object array.
	 *
	 * @param array the object array delivering the elements.
	 * @param begin index to start
	 * @param end index to stop (exclusive)
	 * @return a cursor delivering a subset of the elements backed on the given array.
	 */
	public static Cursor arrayToCursor(Object[] array, int begin, int end) {
		return new ArrayCursor(array, new Enumerator(begin, end));
	}

	/**
	 * The elements from array[0] to array[array.length-1] will be
	 * returned by this cursor.
	 *
	 * @param array the object array delivering the elements.
	 * @param indices iterator of indices; the indices of this
	 * 		iterator define the order the elements will be returned by
	 * 		the cursor.
	 * @return a cursor delivering the elements backed on the given array and
	 * 		the defined order.
	 */
	public static Cursor arrayToCursor(Object[] array, Iterator indices) {
		return new ArrayCursor(array, indices);
	}

	/**
	 * Returns an Iterator that iterates on an array of primitive bytes.
	 *
	 * @param array input byte array
	 * @return Iterator iterating over the array
	 */
	public static Iterator byteArrayIterator(final byte[] array) {
		return new Iterator() {
			int pos=0;
			public boolean hasNext() {
				return pos<array.length;
			}
			public Object next() {
				Byte b = new Byte(array[pos]);
				pos++;
				return b;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}

	/**
	 * Returns an Iterator that iterates on an array of primitive ints.
	 *
	 * @param array input int array
	 * @param size number of elements that are in the iteration.
	 * @return Iterator iterating over the array
	 */
	public static Iterator intArrayIterator(final int[] array, final int size) {
		return new Iterator() {
			int pos=0;
			public boolean hasNext() {
				return pos<size;
			}
			public Object next() {
				Integer b = new Integer(array[pos]);
				pos++;
				return b;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}

	/**
	 * Returns an Iterator that iterates on an array of primitive ints.
	 *
	 * @param array input int array
	 * @return Iterator iterating over the array
	 */
	public static Iterator intArrayIterator(final int[] array) {
		return intArrayIterator(array,array.length);
	}

	/**
	 * Returns an Iterator that iterates on an array of primitive shorts.
	 *
	 * @param array input short array
	 * @param size number of elements that are in the iteration.
	 * @return Iterator iterating over the array
	 */
	public static Iterator shortArrayIterator(final short[] array, final int size) {
		return new Iterator() {
			int pos=0;
			public boolean hasNext() {
				return pos<size;
			}
			public Object next() {
				Short b = new Short(array[pos]);
				pos++;
				return b;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}

	/**
	 * Returns an Iterator that iterates on an array of primitive shorts.
	 *
	 * @param array input short array
	 * @return Iterator iterating over the array
	 */
	public static Iterator shortArrayIterator(final short[] array) {
		return shortArrayIterator(array,array.length);
	}

	/**
	 * Returns an Iterator that iterates on an array of primitive longs.
	 *
	 * @param array input long array
	 * @param size number of elements that are in the iteration.
	 * @return Iterator iterating over the array
	 */
	public static Iterator longArrayIterator(final long[] array, final int size) {
		return new Iterator() {
			int pos=0;
			public boolean hasNext() {
				return pos<size;
			}
			public Object next() {
				Long b = new Long(array[pos]);
				pos++;
				return b;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}

	/**
	 * Returns an Iterator that iterates on an array of primitive longs.
	 *
	 * @param array input long array
	 * @return Iterator iterating over the array
	 */
	public static Iterator longArrayIterator(final long[] array) {
		return longArrayIterator(array,array.length);
	}

	/**
	 * Returns an Iterator that iterates on an array of primitive doubles.
	 *
	 * @param array input double array
	 * @param size number of elements that are in the iteration.
	 * @return Iterator iterating over the array
	 */
	public static Iterator doubleArrayIterator(final double[] array, final int size) {
		return new Iterator() {
			int pos=0;
			public boolean hasNext() {
				return pos<size;
			}
			public Object next() {
				Double b = new Double(array[pos]);
				pos++;
				return b;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}

	/**
	 * Returns an Iterator that iterates on an array of primitive doubles.
	 *
	 * @param array input double array
	 * @return Iterator iterating over the array
	 */
	public static Iterator doubleArrayIterator(final double[] array) {
		return doubleArrayIterator(array,array.length);
	}

	/**
	 * Returns an Iterator that iterates on an array of Strings.
	 *
	 * @param array input String array
	 * @param size number of elements that are in the iteration.
	 * @return Iterator iterating over the array
	 */
	public static Iterator stringArrayIterator(final String[] array, final int size) {
		return new Iterator() {
			int pos=0;
			public boolean hasNext() {
				return pos<size;
			}
			public Object next() {
				String b = new String(array[pos]);
				pos++;
				return b;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}

	/**
	 * Returns an Iterator that iterates on an array of Strings.
	 *
	 * @param array input String array
	 * @return Iterator iterating over the array
	 */
	public static Iterator stringArrayIterator(final String[] array) {
		return stringArrayIterator(array,array.length);
	}

	/**
	 * Wrapps a byte array to an outputstream. A call of the write() method on the
	 * returned outputStream inserts a byte into the array. With every call of the write()
	 * method the insert-position of enclosed array will be increased by one.
	 * @param content the byte array
	 * @param base the position in the array where to start writing
	 * @param end the position in the array where to stop writing
	 * @return OutputStream created
	 */
	public OutputStream byteArrayOutputStream (final byte[] content, final int base, final int end) {
		return new OutputStream () {
			int position = base;

			public void write (int b) throws IOException {
				if (position>=end)
					throw new IOException();
				if (position>=content.length)
					throw new IOException();
				content[position++] =  (byte) b;
			}
		};
	}

	/**
	 * Returns the index in the specified array of the first occurrence
	 * of the specified element, or -1 if the array does not contain
	 * this element. More formally, returns the lowest index i such that
	 * (element == null ? array[i] == null : element.equals(array[i])),
	 * or -1 if there is no such index.
	 * 
	 * @param array the array to be searched.
	 * @param element the element to search for.
	 * @return the index in the array of the first occurrence of the
	 *         specified element, or -1 if the array does not contain
	 *         this element.
	 */
	public static int indexOf(Object[] array, Object element) {
		for (int i = 0; i < array.length; i++)
			if (element == null ? array[i] == null : element.equals(array[i]))
				return i;
		return -1;
	}

	/**
	 * Returns the array index with the smallest element using a given 
	 * comparator.
	 * 
	 * @param array The array to be searched.
	 * @param comparator The comparator to compare two elements of the array.
	 * @return The index in the array of the smallest element or -1 if the 
	 * 	array does not have elements.
	 */
	public static int min(Object[] array, Comparator comparator) {
		if (array==null || array.length==0)
			return -1;
		
		int index=0;
		for (int i = 1; i < array.length; i++)
			if (comparator.compare(array[i],array[index])<0)
				index = i;
		
		return index;
	}

	/**
	 * Returns the array index with the biggest element using a given 
	 * comparator.
	 * 
	 * @param array The array to be searched.
	 * @param comparator The comparator to compare two elements of the array.
	 * @return The index in the array of the biggest element or -1 if the 
	 * 	array does not have elements.
	 */
	public static int max(Object[] array, Comparator comparator) {
		if (array==null || array.length==0)
			return -1;
		
		int index=0;
		for (int i = 1; i < array.length; i++)
			if (comparator.compare(array[i],array[index])>0)
				index = i;
		
		return index;
	}
	
	/**
	 * Returns an array containing an ascending sequence of <code>int</code>
	 * values within the specified range [<code>from</code>;<code>to</code>].
	 * 
	 * @param from the start of the returned <code>int</code> value sequence
	 *        (inclusive).
	 * @param to the end of the returned <code>int</code> value sequence
	 *        (exclusive).
	 * @return an array containing an ascending sequence of <code>int</code>
	 *         values within the specified range
	 *         [<code>from</code>;<code>to</code>].
	 */
	public static int[] enumeratedIntArray(int from, int to) {
		int[] array = new int[to - from];
		for (int i = 0, v = from; v < to; array[i++] = v++);
		return array;
	}

	/**
	 * Increments every column of the given array by the given increment and
	 * returns it.
	 * 
	 * @param array the array holding the <code>int</code> values to be
	 *        incremented.
	 * @param increment the value the array's columns are increased by.
	 * @return an array containing the incremented values of the given array.
	 */
	public static int[] incrementIntArray(int[] array, int increment) {
		int[] inc = new int[array.length];
		for (int i = 0; i < array.length; inc[i] = array[i++]+increment);
		return inc;
	}

	/**
	 * Decrements every column of the given array by the given decrement and
	 * returns it.
	 * 
	 * @param array the array holding the <code>int</code> values to be
	 *        decremented.
	 * @param decrement the value the array's columns are decreased by.
	 * @return an array containing the decremented values of the given array.
	 */
	public static int[] decrementIntArray(int[] array, int decrement) {
		return incrementIntArray(array, -decrement);
	}
	
	public static <I> Predicate<I[]> typedArrayEquals() {
		return new AbstractPredicate<I[]>() {
			@Override
			public boolean invoke(I[] a1, I[] a2) {
				if (a1==null || a2==null) {
					if (a1==null && a2==null)
						return true;
					return false;
				}
				if (a1.length != a2.length)
					return false;
				for (int i=0; i<a1.length; i++) {
					if (a1[i]==null || a2[i]==null) {
						if (a1[i]==null && a2[i]==null)
							continue;						
						return false;
					}
					if (!a1[i].equals(a2[i])) 
						return false;
				}
				return true;
			}
		};
	}
	
}
