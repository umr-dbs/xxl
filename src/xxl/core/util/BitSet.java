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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.Convertable;
import xxl.core.io.converters.SizeConverter;

/**
 * This class is a replacement for <tt>java.util.BitSet</tt> which neither
 * implements the interfaces <tt>Convertable</tt> nor <tt>Comparable</tt>. <br>
 *
 * This class implements a vector of bits. Each component of the bit set
 * has a <code>boolean</code> value.
 * The bits of a <code>BitSet</code> are indexed by nonnegative integers.
 * Individual indexed bits can be examined, set, or cleared. One
 * <code>BitSet</code> may be used to modify the contents of another
 * <code>BitSet</code> through logical AND, logical inclusive OR, and
 * logical exclusive OR operations.
 * Furthermore this class offers the opportunity to a convert a SDK's BitSet
 * to an instance of this class using a default precision of 64 bit.
 *
 * <p>
 * By default, all bits in the set initially have the value
 * <code>false</code>.
 * <p>
 * The array <code>bits</code> represents the bits of a BitSet.
 * BitSets are packed into arrays of "units."  Currently a unit is a long,
 * which consists of 64 bits, requiring 6 address bits.
 * The ith bit is stored in bits[i/64] at
 * bit position i % 64 (where bit position 0 refers to the most
 * significant bit and 63 refers to the least significant bit).
 * Every bit set has a precision, which determines the user-relevant
 * number of bits stored by this bit set. <br>
 * Note, that the precision is related to the implementation
 * of a bit set, so it may change with
 * implementation.
 * The dependence of the array <code>bits</code> and the user
 * specified <code>precision</code> in a constructor is as follows:
 * <br><br>
 * <code><pre>
 * 	bits = new long[ precision%64 == 0 ? (precision >>>6) : (precision>>>6)+1 ]
 * </code></pre>
 * The size of a bit set relates to logical length
 * of a bit set and is defined independently of implementation.
 * <p>
 * This class provides methods to read instances
 * of this class from the data input and also write them to the data
 * output.
 * Further different methods to compare two BitSets are supported, e.g.
 * only a specified sequence of bits can be compared or only one dimension
 * can be compared, which is especially useful if space filling curves
 * are represented by bit sets (e.g. z-code in {@link xxl.core.spatial spatial}).
 * <p>
 * <b>Example usage (1):</b>
 * <br><br>
 * <code><pre>
 * 	BitSet bitSet = new BitSet(62);
 * 	System.out.println("bitSet :" +bitSet);
 * 	System.out.println("bitSet's size:" +bitSet.size());
 * 	System.out.println("bitSet's precision:" +bitSet.precision());
 * 	System.out.println("Setting bits: 1, 19, 23, 54");
 * 	bitSet.set(1);
 * 	bitSet.set(19);
 * 	bitSet.set(23);
 * 	bitSet.set(54);
 * 	System.out.println("bitSet :" +bitSet);
 * 	System.out.println("Clearing bit with index 19.");
 * 	bitSet.clear(19);
 * 	System.out.println("bitSet :" +bitSet);
 * </code></pre>
 * This first example creates a new instance of a BitSet with a precision of 62 bits.
 * Initially all bits are <tt>false</tt>, and the size is 64 bits, because the internal
 * used array <code>bits</code> has length 1. <br>
 * The bits at position 1, 19, 23 and 54 are set. After that the bit with index
 * 19 is cleared.
 * So the final output of the bits that are set is: <tt>{1, 23, 54}</tt>.
 * <p>
 * <b>Example usage (2):</b>
 * <br><br>
 * <code><pre>
 * 	BitSet bitSet1 = new BitSet( new long[] {first, 7, 13, 42});
 * 	System.out.println("bitSet1: " +bitSet1);
 * 	System.out.println("bitSet1's size:" +bitSet1.size());
 * 	System.out.println("bitSet1's precision:" +bitSet1.precision());
 * 	System.out.println("bitSet2 gets a clone of bitSet1.");
 * 	BitSet bitSet2 = (BitSet)bitSet1.clone();
 * 	System.out.println("Clearing bit with index 125 of bitSet2. ");
 * 	bitSet2.clear(125);
 * 	System.out.println("bitSet2: " +bitSet2);
 * 	System.out.println("Determining first different bit between bitSet1 and bitSet2:");
 * 	System.out.println("first different bit: " +bitSet1.diff(bitSet2));
 * </code></pre>
 * In this example a BitSet is created initializing the attribute <code>bits</code>
 * with long values: 1l<<63(first), 7, 13, 42. So bits[0] = first, bits[1] = 7 and so on.
 * In this case the precision is 256 (bits.length * 64) and also the size.
 * Then bitSet1 is cloned and bit with index 125 is cleared. Therefore the following
 * comparison returns this index as the first different bit.
 * <p>
 * <b>Example usage (3):</b>
 * <br><br>
 * <code><pre>
 * 	bitSet1 = new BitSet( new long[] {first, 7, 13, 42}, 64);
 * 	System.out.println("bitSet1: " +bitSet1);
 * 	System.out.println("bitSet1's size:" +bitSet1.size());
 * 	System.out.println("bitSet1's precision:" +bitSet1.precision());
 * 	System.out.println("result of comparison between bitSet1 and bitSet2 (precision = 64): " +bitSet1.compare(bitSet2));
 * 	System.out.println("result of comparison between bitSet1 and bitSet2: " +bitSet1.compareTo(bitSet2));
 * 	System.out.println("comparing only the first 20 bits: " +bitSet1.compare(bitSet2, 20));
 * 	System.out.println("comparing the first two longs directly: " +bitSet1.compare2(bitSet2, 2));
 * </code></pre>
 * The last example creates nearly the same BitSet as example (2), but the precision
 * is only 64 bits. If the two BitSets, bitSet1 and bitSet2, are compared now, the
 * call <code>bitSet1.compare(bitSet2)</code> returns 0, that means that the
 * two BitSets are equal. But there is still a difference in the bit at position 125,
 * this difference does not matter, because the precision is set to only 64 bit.
 * If the same comparison is fulfilled with the method <code>compareTo</code> the
 * result is -1, because in this case all indices are compared.
 * Comparing only the first 20 bits delivers the same results as <code>compare()</code>,
 * but comparing the first two longs using the method <code>compare2()</code> returns
 * -1, because now the comparison is fulfilled with 128 bits.
 *
 * @see java.io.DataInput
 * @see java.io.DataOutput
 * @see java.lang.Cloneable
 * @see java.lang.Comparable
 * @see xxl.core.io.Convertable
 */
 public class BitSet implements Cloneable, Comparable<BitSet>, Convertable {

 	/**
 	 * Converter for BitSets.
 	 */
 	public static SizeConverter<BitSet> DEFAULT_CONVERTER = new SizeConverter<BitSet>() {
		/**
		 * Reads the state (the attributes) for an BitSet object from
		 * the specified data input and restores the calling object.
		 * In this case an Integer object is read at first, which contains the
		 * information about the BitSet's precision, i.e. the number of
		 * bits stored by this BitSet. After that the array <tt>bits</tt> is
		 * created and filled up with <code>bits.length</code> Long objects read
		 * from the specified data input.
		 * @param dataInput the stream to read data from in order to restore
		 *        the object.
		 * @throws IOException if I/O errors occur.
		 */
		@Override
		public BitSet read(DataInput dataInput, BitSet b) throws IOException {
			int precision = dataInput.readInt();
			if (b == null)
				b = new BitSet(precision);
			else {
				if (b.precision != precision)
					throw new RuntimeException("Precision of the object was not correct!");
			}
			
			for(int i = 0; i < b.bits.length; i++)
				b.bits[i] = dataInput.readLong();
			return b;
		}
		/**
		 * Writes the state (the attributes) of the calling object to the
		 * specified data output.
		 * At first an Integer object is written representing the BitSet's
		 * precision, i.e. the number of bits stored by this BitSet. After
		 * that the long array <code>bits</code> is written to the data output
		 * by calling the data output's method <code>writeLong</code>
		 * for each component of the array.
		 *
		 * @param dataOutput the stream to write the state (the attributes) of
		 *        the object to.
		 * @throws IOException includes any I/O exceptions that may occur.
		 */
		@Override
		public void write(DataOutput dataOutput, BitSet b) throws IOException {
			dataOutput.writeInt(b.precision);
			for(int i = 0; i < b.bits.length; i++)
				dataOutput.writeLong(b.bits[i]);
		}
		/**
		 * Returns the number of bytes used for serialization/deserialization.
		 * of a BitSet.
		 * @param b The BitSet of which the size is returned.
		 * @return the number of bytes.
		 */
		@Override
		public int getSerializedSize(BitSet b) {
			return 4 + b.bits.length * 8;
		}
 	};

	/**
	 * Returns the serialization size of a BitSet of a certain
	 * precision.
	 * @param precision Precision of the BitSet.
	 * @return Size in bytes.
	 */ 
 	public static int getSize(int precision) {
		return 4 + ((precision+63)/64)*8;
	}

	/**
	 * The bits in this BitSet. The ith bit is stored in bits[i/64] at
	 * bit position i % 64 (where bit position 0 refers to the most
	 * significant bit and 63 refers to the least significant bit).
	 */
	protected long[] bits;

	/**
	 * The user-relevant number of bits stored by this BitSet.
	 * This number can be chosen smaller than the size of a bit set,
	 * showing that the user does not need the whole precision.
	 * This possible loss of precision results in a better performance
	 * when comparing bit sets.
	 */
	protected int precision = 0;

	/**
	 * The BitSet's (first) highest bit is set to <tt>true</tt>
	 * using a 64 bit representation.
	 * All other bits are set to <tt>false</tt>.
	 */
	protected static final long first = 1l<<63;

	/**
	 * The given SDK's BitSet is converted to a {@link xxl.core.util.BitSet}
	 * using a default precision of 64 bits.
	 * So the resulting bit set is absolutely identical to the given bit set
	 * in each bit.
	 * @param bitSet input SDK's BitSet
	 * @return copy of input BitSet as {@link xxl.core.util.BitSet}
	 */
	public static BitSet convert (java.util.BitSet bitSet) {
		return new BitSet(bitSet);
	}

	/**
	 * Creates a new BitSet by setting <code>bits</code> to <tt>null</tt>
	 * and <code>precision</code> to 0.
	 */
	public BitSet(){
		this(null, 0);
	}

	/**
	 * Creates a new BitSet using a precision of 64 bits and
	 * an array containg only the specified long component.
	 *
	 * @param l component representing the vector of bits.
	 */
	public BitSet(long l){
		this(new long[]{l}, 64);
	}

	/**
	 * Creates a new BitSet using the given precision.
	 * The array <code>bits</code> is created as follows:
	 * <br><br>
	 * <code><pre>
	 * 	new long[ precision%64 == 0 ? (precision >>>6) : (precision>>>6)+1 ]
	 * </code></pre>
	 * That is the case, because the array's component type is <tt>long</tt>,
	 * i.e. 64 bits. So the number of bits to be stored (precision) has
	 * to be partioned in sections of 64 bits.
	 *
	 * @param precision the user-relevant number of bits stored by this BitSet.
	 */
	public BitSet(int precision){
		this(new long[ precision%64 == 0 ? (precision >>>6) : (precision>>>6)+1 ], precision);
	}

	/**
	 * Creates a new BitSet. (Copy-constructor)
	 * The new instance is created by cloning the array <code>bits</code>
	 * of the given BitSet b and calling the constructor
	 * {@link #BitSet(long[] bits, int precision)} with this cloned
	 * array and the precision of BitSet b.
	 *
	 * @param b the BitSet to be copied.
	 */
	public BitSet(BitSet b){
		this(b.bits.clone(), b.precision);
	}

	/**
	 * Creates a new BitSet using the specified array and
	 * precision that results in <tt>bits.length*64</tt>.
	 *
	 * @param bits the array representing a vector of bits.
	 */
	public BitSet(long[] bits){
		this(bits,bits.length<<6);
	}

	/**
	 * Creates a new BitSet using the given array and precision.
	 *
	 * @param bits the array representing a vector of bits.
	 * @param precision the user-relevant number of bits to be stored by this BitSet.
	 */
	public BitSet(long[] bits, int precision){
		this.bits = bits;
		this.precision = precision;
	}

	/**
	 * Creates a new BitSet using the given {@link java.util.BitSet}.
	 * A default precision of 64 bits is used.
	 * So the created bit set is absolutely identical to the given bit set
	 * in each bit.
	 *
	 * @param bitSet an instance of {@link java.util.BitSet}.
	 */
	public BitSet(java.util.BitSet bitSet) {
		this(bitSet.length());
		for (int i=0; i<bitSet.length(); i++)
			if(bitSet.get(i))
				this.set(i);
	}

	/**
	 * Returns the BitSet's precision, i.e. the number of bits
	 * stored by this BitSet.
	 *
	 * @return the precision of this BitSet.
	 */
	public int precision(){
		return precision;
	}

	/**
	 * Cloning this <code>BitSet</code> produces a new <code>BitSet</code>
	 * that is equal to it.
	 * The clone of the bit set is another bit set that has exactly the
	 * same bits set to <code>true</code> as this bit set and the same
	 * precision.
	 * <p>Overrides the <code>clone</code> method of <code>Object</code>.
	 * The copy-constructor {@link #BitSet(BitSet)} is called.
	 *
	 * @return a clone of this bit set.
	 */
	@Override
	public Object clone(){
		return new BitSet(this);
	}

	/**
	 * Sets the bit specified by the index to <code>true</code>.
	 *
	 * @param bitIndex a bit index.
	 * @throws IndexOutOfBoundsException if the specified index is negative.
	 */
	public void set(int bitIndex){
		bits[bitIndex>>>6] |= first >>> (bitIndex%64);
//		bits[bitIndex>>>6] |= 1l<<(bitIndex%64);
	}

	/**
	 * Returns the value of the bit with the specified index. The value
	 * is <code>true</code> if the bit with the index <code>bitIndex</code>
	 * is currently set in this <code>BitSet</code>; otherwise, the result
	 * is <code>false</code>.
	 *
	 * @param bitIndex the bit index.
	 * @return the value of the bit with the specified index.
	 * @throws IndexOutOfBoundsException if the specified index is negative.
	 */
	public boolean get(int bitIndex){
		return ((bits[bitIndex>>>6] << (bitIndex%64)) & first) == first;
//		return ((bits[bitIndex>>>6] >>> (bitIndex%64)) & 1l) == 1l;
	}

	/**
	 * Sets the bit specified by the index to <code>false</code>.
	 *
	 * @param bitIndex the index of the bit to be cleared.
	 * @throws IndexOutOfBoundsException if the specified index is negative.
	 */
	public void clear(int bitIndex){
		bits[bitIndex>>>6] &= ~(first >>>(bitIndex%64));
//		bits[bitIndex>>>6] &= ~(1l<<(bitIndex%64));
	}

	/**
	 * Returns the number of bits of space actually in use by this
	 * <code>BitSet</code> to represent bit values.
	 * The size is determined by <code>bits.length<<6</code>, i.e.
	 * bits.length*64, because the array's component type is <tt>long</tt>.
	 *
	 * @return the size of this bit set.
	 */
	public int size(){
		return bits.length<<6;
	}

	/**
	 * Returns a string representation of this bit set.
	 * For every index for which this <code>BitSet</code>
	 * contains a bit in the set state, the decimal representation
	 * of that index is included in the result.
	 * Such indeces are listed in order from lowest to
	 * highest, separated by ",$nbsp;" (a comma and a space) and
	 * surrounded by braces, resulting in the usual mathematical
	 * notation for a set of integers.<p>
	 * Overrides the <code>toString</code> method of <code>Object</code>.
	 *
	 * @return a string representation of this bit set.
	 */
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
			sb.append("bits in set state: {");
			String separator = "";
			for (int i=0; i<size(); i++) {
				if (get(i)) {
					sb.append(separator);
					separator = ", ";
					sb.append(i);
				}
			}
			sb.append("}");
		return new String(sb);
	}

	/** Alternate visualization of this String.
	 * @return resulting string
	*/
	public String toString2(){
		StringBuffer sb = new StringBuffer(size()+"\nsfc: ");
		if(size()<200){
			for(int i=0; i<size();i++)
				sb.append(get(i)?"1":"0");
			sb.append("\t\tprecision:\t"+precision+"\n     ");
			for(int i=0; i<size();i++)
				sb.append(i%10);
			sb.append("\n     ");
			for(int i=0; i<size();i+=10)
				sb.append((i/10)+"         ");
		}
		else
			sb.append("Error: String is too long!");
		return sb.toString();
	}

	/**
	 * Reads the state (the attributes) for an object of this class from
	 * the specified data input and restores the calling object. The state
	 * of the object before calling <tt>read</tt> will be lost.<br>
	 * In this case an Integer object is read at first, which contains the
	 * information about the BitSet's precision, i.e. the number of
	 * bits stored by this BitSet. After that the array <tt>bits</tt> is
	 * created and filled up with <code>bits.length</code> Long objects read
	 * from the specified data input.
	 *
	 * @param in the stream to read data from in order to restore
	 *        the object.
	 * @throws IOException if I/O errors occur.
	 */
	public void read(DataInput in) throws IOException{
		precision = in.readInt();
		bits = new long[ precision%64 == 0 ? (precision>>>6) : (precision>>>6)+1];//ensure that there is enough space in the array
		for(int i=0; i<bits.length; i++)
			bits[i] = in.readLong();/**/
	}

	/**
	 * Writes the state (the attributes) of the calling object to the
	 * specified data output.
	 * At first an Integer object is written representing the BitSet's
	 * precision, i.e. the number of bits stored by this BitSet. After
	 * that the long array <code>bits</code> is written to the data output
	 * by calling the data output's method <code>writeLong</code>
	 * for each component of the array.
	 *
	 * @param out the stream to write the state (the attributes) of
	 *        the object to.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void write(DataOutput out) throws IOException{
		out.writeInt(precision); //DEBUG
		for(int i=0; i<bits.length; i++)
			out.writeLong(bits[i]);/**/
	}

	/**
	 * Compares two BitSets and returns the index of the first bit
	 * that differs starting at bit index 0.
	 * A maximum of Integer.MAX_VALUE bits will be compared.
	 *
	 * @param b the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @return the index of the first bit that differs.
	 * @see #diff(BitSet, int)
	 */
	public int diff(BitSet b){
		return diff(b,Integer.MAX_VALUE);
	}

	/**
	 * Compares two BitSets concerning the specified number of bits
	 * <tt>nobits</tt> and returns the index of the first bit
	 * that differs starting at bit index 0.
	 * A maximum of <tt>min{<code>precision, b.precision, nobits</code>}</tt>
	 * bits will be compared.
	 *
	 * @param b the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @param nobits the maximum number of bits to be compared.
	 * @return the index of the first bit that differs.
	 */
	public int diff(BitSet b, int nobits){
		int bitIndex = 0;
		for(nobits = Math.min(Math.min(precision,b.precision),nobits); bitIndex < nobits; bitIndex++)
			if(get(bitIndex) != b.get(bitIndex) )
				return bitIndex;
		return bitIndex;
	}

	/**
	 * Compares the calling Bitset with the given BitSet <tt>b</tt>.
	 * The comparsion starts at bit index 0.
	 * A maximum of <tt>min{<code>precision, b.precision</code>}</tt>
	 * bits will be compared.
	 * This method returns a negative if this (the calling) BitSet is smaller than
	 * the given BitSet b, a positive value if BitSet b is smaller,
	 * otherwise 0 is returned.
	 *
	 * @param b the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @return a negative value if this (the calling) BitSet is smaller than
	 * 		the given BitSet b, a positive value if BitSet b is smaller,
	 * 		else 0 is returned.
	 * @see #compare(BitSet, int, int, int)
	 */
	public int compare(BitSet b){
		return compare(b,precision,0,1);
	}
	/**
	 * Compares the calling Bitset with the given BitSet <tt>b</tt> in
	 * the specified number of bits <tt>nobits</tt>.
	 * The comparsion starts at bit index 0.
	 * A maximum of <tt>min{<code>precision, b.precision, nobits</code>}</tt>
	 * bits will be compared.
	 * This method returns a negative if this (the calling) BitSet is smaller than
	 * the given BitSet b, a positive value if BitSet b is smaller,
	 * otherwise 0 is returned.
	 *
	 * @param b the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @param nobits the maximum number of bits to be compared.
	 * @return a negative value if this (the calling) BitSet is smaller than
	 * 		the given BitSet b, a positive value if BitSet b is smaller,
	 * 		else 0 is returned.
	 * @see #compare(BitSet, int, int, int)
	 */
	public int compare(BitSet b, int nobits){
		return compare(b,nobits,0,1);
	}

	/**
	 * Compares two BitSets with regard to only one dimension <tt>dim</tt>. <br>
	 * This method is implemented as follows:
	 * <br><br>
	 * <code><pre>
	 * 	int bitIndex = dim;
	 * 	for(nobits = Math.min( Math.min(precision,b.precision),nobits ); bitIndex < nobits; bitIndex += maxDim)
	 * 		if( get(bitIndex) != b.get(bitIndex) )
	 * 			return (get(bitIndex)?1:0) - (b.get(bitIndex)?1:0);
	 * 	return 0;
	 * </code></pre>
	 * A maximum of <tt>min{<code>precision, b.precision, nobits</code>}</tt>
	 * bits will be compared.
	 * The bits at bit index <tt>bitIndex</tt> will be compared starting with
	 * <code>bitIndex = dim</code>.
	 * Each time the comparison of two bits delivers <tt>true</tt> the
	 * bitIndex is incremented by <tt>maxDim</tt>. <br>
	 *
	 * This method returns a negative if this (the calling) BitSet is smaller than
	 * the given BitSet b, a positive value if BitSet b is smaller,
	 * otherwise 0 is returned.
	 *
	 * @param b the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @param nobits the maximum number of bits to be compared.
	 * @param dim the bit index the comparison starts.
	 * @param maxDim a value which is added after each comparsion of two
	 * 		bits to the current bit index.
	 * @return a negative value if this (the calling) BitSet is smaller than
	 * 		the given BitSet b, a positive value if BitSet b is smaller,
	 * 		else 0 is returned.
	 * @see xxl.core.spatial
	 */
	public int compare(BitSet b, int nobits, int dim, int maxDim){
		int bitIndex = dim;
		for(nobits = Math.min( Math.min(precision,b.precision),nobits ); bitIndex < nobits; bitIndex += maxDim)
			if( get(bitIndex) != b.get(bitIndex) )
				return (get(bitIndex)?1:0) - (b.get(bitIndex)?1:0);
		return 0;
	}

	/**
	 * Compares the calling Bitset with the given BitSet <tt>b</tt>
	 * in terms of their components, i.e. the longs will be compared
	 * directly. The given parameter <tt>noLongs</tt>, number of longs,
	 * specifies the maximum number of long components of the array
	 * <tt>bits</tt> to be compared.
	 *
	 * So this method is faster than {@link #compare(BitSet,int)}.
	 *
	 * @param b the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @param noLongs the number of longs to be compared, i.e.
	 * 		bits[0], ..., bits[noLongs] will be compared at the most.
	 * @return a negative value if this (the calling) BitSet is smaller than
	 * 		the given BitSet b, a positive value if BitSet b is smaller,
	 * 		else 0 is returned.
	 * @throws IndexOutOfBoundsException if <tt>noLongs</tt> is negative
	 * 		or it is larger than <tt>min{bits.length-1, b.bits.length-1}</tt>.
	 */
	public int compare2(BitSet b, int noLongs){
		for(int i=0 ; i < noLongs; i++){
			//kann man das folgende if eventuell sogar weglassen?
			if ((bits[i] & first) != (b.bits[i] & first))
				return bits[i] < b.bits[i] ? +1 : -1; //ACHTUNG: kehrt sich hier um!
			if (bits[i] < b.bits[i])
				return -1;
			if (bits[i] > b.bits[i])
				return +1;
		}
		return 0;
	}

	/**
	 * Compares the calling Bitset with the given Object <tt>o</tt> casted
	 * to a BitSet.
	 * The comparison returns a negative if this (the calling) BitSet is smaller than
	 * the given BitSet o, a positive value if BitSet o is smaller,
	 * otherwise 0 is returned.
	 *
	 * @param bs the BitSet to be compared with the BitSet
	 * 		calling this method.
	 * @return a negative value if this (the calling) BitSet is smaller than
	 * 		the given BitSet o, a positive value if BitSet o is smaller,
	 * 		else 0 is returned.
	 */
	public int compareTo(BitSet bs) {
		//compare longs directly
		int comp =  compare2(bs, Math.min( bits.length, bs.bits.length)); //compare BitSets
		return comp != 0 ? 	//equals 0 (i.e. BitSets are equal from bit 0 to bit <min>
			comp			//return comp
		:
			precision < bs.precision ? -1 : (precision > bs.precision ? +1 : 0); //the shortest BitSet is smaller/**/

/*		int comp =  compare(bs, Math.min( precision, bs.precision),0,1);		//compare BitSets
		return comp != 0 ? 								//equals 0 (i.e. BitSets are equal fomr bit 0 to bit <min>
			comp									//return comp
		:
			precision < bs.precision ? -1 : (precision > bs.precision ? +1 : 0);	//smaller is the shortest BitSet /**/
	}

	/**
	 * Tests if this is equal to the BitSet which is given as parameter.
	 * @param o second BitSet object.
	 * @return true iff both BitSets are equal. 
	 */
	@Override
	public boolean equals(Object o) {
		return compareTo((BitSet)o)==0;
	}

	/**
	 * Returns a hash code for the BitSet.
	 * @return the hash code.
	 */
	@Override
	public int hashCode() {
		if (bits == null || bits.length == 0)
			return 4711;
		return (int) bits[0];
	}
	
	/**
	 * Performs a logical <b>OR</b> of this bit set with the specified
	 * bit set. This bit set is modified so that a bit in it has the
	 * value <code>true</code> if and only if it either already had the
	 * value <code>true</code> or the corresponding bit in the bit set
	 * argument has the value <code>true</code>. <br>
	 * <b>Note</b>, this BitSet's capacity may change when executing
	 * this method, because the two BitSet are set to the larger
	 * length of the internal used array <tt>bits</tt>, so this
	 * array will have the following length: <br>
	  * <br><br>
	 * <code><pre>
	 * 	Math.max(bits.length, bitSet.bits.length)
	 * </code></pre>
	 *
	 * @param bitSet a bit set the operation should be fulfilled with.
	 */
	public void or (BitSet bitSet) {
		if (compareTo(bitSet) != 0) {
			// ensure capacity
			long[] result = new long[Math.max(bits.length, bitSet.bits.length)];

			int i; // position
			int min = Math.min(bits.length, bitSet.bits.length);

			// perform logical OR on bits in common
			for (i=0; i<min; i++)
				result[i] = bits[i] | bitSet.bits[i];

			BitSet set = (result.length == bits.length) ? this : bitSet; // larger bit set
			// copy any remaining bits
			for ( ; i<result.length; i++)
				result[i] = set.bits[i];
			this.bits = result; // assign the bit-array
		}
	}

	/**
	 * Performs a logical <b>AND</b> of this target bit set with the
	 * argument bit set. This bit set is modified so that each bit in it
	 * has the value <code>true</code> if and only if it both initially
	 * had the value <code>true</code> and the corresponding bit in the
	 * bit set argument also had the value <code>true</code>. <br>
	 * <b>Note</b>, this BitSet's capacity may change when executing
	 * this method, because the two BitSet are set to the larger
	 * length of the internal used array <tt>bits</tt>, so this
	 * array will have the following length: <br>
	 * <br><br>
	 * <code><pre>
	 * 	Math.max(bits.length, bitSet.bits.length)
	 * </code></pre>
	 *
	 * @param bitSet a bit set the operation should be fulfilled with.
	 */
	public void and (BitSet bitSet) {
		if (compareTo(bitSet) != 0) {
			// ensure capacity
			long[] result = new long[Math.max(bits.length, bitSet.bits.length)];

			int i; //position
			int min = Math.min(bits.length, bitSet.bits.length);
			// perform logical AND on bits in common
			for (i=0; i<min; i++)
				result[i] = bits[i] & bitSet.bits[i];

			// clear out remaining bits
			for ( ; i< result.length; i++)
				result[i] = 0;
			this.bits = result; //assign the bit-array
		}
	}

	 /**
	 * Performs a logical <b>XOR</b> of this bit set with the bit set
	 * argument. This bit set is modified so that a bit in it has the
	 * value <code>true</code> if and only if one of the following
	 * statements holds:
	 * <ul>
	 * <li>The bit initially has the value <code>true</code>, and the
	 *     corresponding bit in the argument has the value <code>false</code>.
	 * <li>The bit initially has the value <code>false</code>, and the
	 *     corresponding bit in the argument has the value <code>true</code>.
	 * </ul> <br>
	 * <b>Note</b>, this BitSet's capacity may change when executing
	 * this method, because the two BitSet are set to the larger
	 * length of the internal used array <tt>bits</tt>, so this
	 * array will have the following length: <br>
	 * <br><br>
	 * <code><pre>
	 * 	Math.max(bits.length, bitSet.bits.length)
	 * </code></pre>
	 *
	 * @param bitSet a bit set the operation should be fulfilled with.
	 */
	public void xor (BitSet bitSet) {
		// ensure capacity
		long[] result = new long[Math.max(bits.length, bitSet.bits.length)];

		if (compareTo(bitSet)!=0) {
			int i; //position
			int min = Math.min(bits.length, bitSet.bits.length);

			// perform logical XOR on bits in common
			for (i=0; i<min; i++)
				result[i] = bits[i] ^ bitSet.bits[i];

			BitSet set = (result.length == bits.length) ? this : bitSet; // larger bit set
			// copy any remaining bits
			for ( ; i<result.length; i++)
				result[i] = set.bits[i];
		}
		this.bits = result; //assign the bit-array
	}

	/**
	 * Performs a logical <b>NOT</b> of this bit set.
	 * This bit set is modified so that every bit is negated, i.e.
	 * a set bit is cleared and a bit set to <tt>false</tt> is set
	 * to <tt>true</tt>.
	 */
	public void not () {
		// perform logical NOT
		for (int i=0; i<bits.length; i++)
			bits[i] = ~bits[i];
	}
}
