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
package xxl.core.spatial;

import xxl.core.math.Maths;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.core.util.BitSet;

/** A collection of space filling curves.
 */
public class SpaceFillingCurves {
	
	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private SpaceFillingCurves() {}

	/**
	 * Computes the hilbert value for two given integers.
	 * The number of bits of x and y to be considered can be determined by setting the parameter mask appropriately.
	 * @param x the value of the first dimension
	 * @param y the value of the second dimension
	 * @param mask the bitmask containing exactly the highest bit to be considered.
	 *  	If all bits of x and y should be taken into account, choose the value 1<<31.
	 * @return the hilbert value for x and y
	 */
	public static long hilbert2d (int x, int y, int mask) {
		long hilbert = 0;
		int not_y = ~(y ^= x);
	
		do
			if ((y&mask)!=0)
				if ((x&mask)==0)
					hilbert = (hilbert<<2)|1;
				else {
					x ^= not_y;
					hilbert = (hilbert<<2)|3;
				}
			else
				if ((x&mask)==0) {
					x ^= y;
					hilbert <<= 2;
				}
				else
					hilbert = (hilbert<<2)|2;
		while ((mask >>>= 1)!=0);
		return hilbert;
	}

	/**
	 * Computes the hilbert value for two given integers.
	 * @param x the value of the first dimension
	 * @param y the value of the second dimension
	 * @return the hilbert value for x and y
	 */
	public static long hilbert2d (int x, int y) {
		return hilbert2d(x, y, 1<<31);
	}

	/**
	 * Computes for a given hilbert value the origin values for two dimensions resulting in that hilbert value.
	 * The number of bits of the resulting values to be considered can be determined by setting the parameter mask appropriately.
	 * @param hilbert the given hilbert value
	 * @param mask the bitmask containing exactly the highest bit of the resulting two values to be considered.
	 *  	If all bits should be taken into account, choose a value of 1<<31.
	 * @return an array containing the two values whose hilbert value is equal to the specified one
	 */
	public static int[] hilbert2d (long hilbert, int mask) {
		for (int x = 0, y = 0, shiftMask = 1;; shiftMask <<= 1)	{
			if ((hilbert&1)==0) {
				y |= shiftMask;
				if ((hilbert&2)==0)
					x ^= ~(y|-shiftMask);
				else
					x |= shiftMask;
			}
			else
				if ((hilbert&2)!=0)
					x ^= y|shiftMask;
			if (shiftMask==mask)
				return new int[]{x, (y^~x)&((mask<<1)-1)};
			hilbert >>>= 2;
		}
	}

	/**
	 * Computes for a given hilbert value the origin values for two dimensions resulting in that hilbert value.
	 * @param hilbert the given hilbert value
	 * @return an array containing the two values whose hilbert value is equal to the specified one
	 */
	public static int[] hilbert2d (long hilbert) {
		return hilbert2d(hilbert, 1<<31);
	}

	/**
	 * Computes the peano value for two given integers.
	 * The number of bits of x and y to be considered can be determined by setting the parameter mask appropriately.
	 * @param x the value of the first dimension
	 * @param y the value of the second dimension
	 * @param mask the bitmask containing exactly the highest bit to be considered.
	 *  	If all bits of x and y should be taken into account, choose the value 1<<31.
	 * @return the peano value for x and y
	 */
	public static long peano2d (int x, int y, int mask) {
		long peano = 0;
	
		for (; mask!=0; mask >>>= 1)
			if ((y&mask)==0)
				if ((x&mask)==0)
					peano <<= 2;
				else
					peano = (peano<<2)|2;
			else
				if ((x&mask)==0)
					peano = (peano<<2)|1;
				else
					peano = (peano<<2)|3;
		return peano;
	}

	/**
	 * Computes the peano value for two given integers.
	 * @param x the value of the first dimension
	 * @param y the value of the second dimension
	 * @return the peano value for x and y considering every bit
	 */
	public static long peano2d (int x, int y) {
		return peano2d(x, y, 1<<31);
	}

	/**
	 * Computes for a given peano value the values for two dimensions resulting in that peano value.
	 * The number of bits of the resulting values to be considered can be determined by setting the parameter mask appropriately.
	 * @param peano the given peano value
	 * @param mask the bitmask containing exactly the highest bit to be considered for the resulting two values.
	 *  	If all bits should be taken into account, choose the value 1<<31.
	 * @return an array containing the two values whose peano value is equal to the specified one
	 */
	public static int[] peano2d (long peano, int mask) {
		for (int x = 0, y = 0, shiftMask = 1;; shiftMask <<= 1)	{
			if ((peano&2)!=0)
				x |= shiftMask;
			if ((peano&1)!=0)
				y |= shiftMask;
			if (shiftMask==mask)
				return new int[] {x, y};
			peano >>>= 2;
		}
	}

	/**
	 * Computes for a given peano value the origin values for two dimensions resulting in that peano value.
	 * @param peano the given peano value
	 * @return an array containing the two values whose peano value is equal to the specified one
	 */
	public static int[] peano2d (long peano) {
		return peano2d(peano, 1<<31);
	}
		/**
		 * Computes the z-code of the given bit field, <tt>bitField</tt>, by merging the
		 * the bit-field-longs and returns it as a BitSet. <br>
		 * The BitSet's precision is set to:<br>
		 * <pre>
		 * 	componentPrecision*bitField.length
		 * </pre>
		 * So the basic used precision is the array's component precision.
		 *
		 * @param bitField a long array the z-code should be computed of.
		 * @param componentPrecision the bitField's component precision.
		 * @return a BitSet representing the z-code of the given <tt>bitField</tt>.
		 * @see xxl.core.util.BitSet#BitSet(int)
		 * @see #zCode(long[], int, int)
		 */
		public static BitSet zCode(final long[] bitField, int componentPrecision){
			return zCode(bitField, componentPrecision, 0);
	
	/*		BitSet zCode = new BitSet(componentPrecision*bitField.length);
			long mask = 0x1L << 62;		//first bit true, remainin bits: false
			int bitIndex = 0;		//bit-index in z-code
			for(int i=0; i<componentPrecision; i++){
				for(int d=0; d<bitField.length; d++){
					if((mask & bitField[d]) == mask)
						zCode.set(bitIndex);
					bitIndex++;
				}
				mask >>>= 1;
			}
			return zCode;*/
		}

	/**
	 * Computes the z-code of the given bit field, <tt>bitField</tt>, by merging
	 * the bit-field-longs and returns it as a BitSet. <br> (EXPERIMENTAL)
	 *
	 * @param bitField a long array the z-code should be computed of.
	 * @param precision the returned BitSet's full precision.
	 * @return a BitSet representing the z-code of the given <tt>bitField</tt>.
	 * @see xxl.core.util.BitSet#BitSet(int)
	 * @see #zCode(long[], int, int)
	 */
	public static BitSet zCode2(final long[] bitField, int precision){
		return zCode(bitField, precision/bitField.length, precision%bitField.length);
	}

	/**
	 * Computes the z-code of the given bit field, <tt>bitField</tt>, by merging
	 * the bit-field-longs and returns it as a BitSet. <br>
	 * The BitSet's precision is set to:<br>
	 * <pre>
	 * 	componentPrecision*bitField.length+additionalBits
	 * </pre>
	 * So the basic used precision is the array's component precision, but
	 * it can be increased by adding <tt>additionalBits</tt>.
	 *
	 * @param bitField a long array the z-code should be computed of.
	 * @param componentPrecision the bitField's component precision.
	 * @param additionalBits added bits, that should be taken into consideration
	 * 		too, i.e. the precision of the BitSet is incremented by this value.
	 * @return a BitSet representing the z-code of the given <tt>bitField</tt>.
	 * @see xxl.core.util.BitSet#BitSet(int)
	 */
	public static BitSet zCode(final long[] bitField, int componentPrecision, int additionalBits){
		BitSet zCode = new BitSet(componentPrecision*bitField.length+additionalBits);
	
		long mask = 0x1L << 62;		//first bit true, remainin bits: false
		int bitIndex = 0;		//bit-index in z-code
		for(int i=0; i<componentPrecision; i++){
			for(int d=0; d<bitField.length; d++){
				if((mask & bitField[d]) == mask)
					zCode.set(bitIndex);
				bitIndex++;
			}
			mask >>>= 1;
		}
	
		//Nachlauf:
			for(int d=0; d<additionalBits; d++){
				if((mask & bitField[d]) == mask)
					zCode.set(bitIndex);
				bitIndex++;
			}
	
		return zCode;
	}

	/** Alternative computation of z-codes. Computes the z-code
	 *	for a given rectangle using a quadtree based splitting
	 *	strategy.<br>
	 *	Precondition: MBR of the data subseteq [0;1)^dim.
	 *
	 * @param rectangle the rectangle for which the z-code should be computed
	 * @param maxLevel the highest partitioning level allowed
	 * @return a BitSet representing the z-code of the given <tt>rectangle</tt>.
	 * @see xxl.core.util.BitSet#BitSet(int)
	*/
	public static BitSet zCode (Rectangle rectangle, int maxLevel) {	//method by Tobias Schaefer
		long[] bitField = new long[rectangle.dimensions()];
		int minLevel = maxLevel;
		long[] ll = new long[rectangle.dimensions()];
		long[] ur = new long[rectangle.dimensions()];
	
		for (int i = 0; i < bitField.length; i++) {
			//scale to unit-cube:
			ll[i] = Maths.doubleToNormalizedLongBits( rectangle.getCorner(false).getValue(i) );
			ur[i] = Maths.doubleToNormalizedLongBits( rectangle.getCorner(true).getValue(i)  );
			long precision = 0;
			int level = 0;
			long mask = 1l<<(63-1); // long precision = 63 (eine 1 steht schon, also 63-1 mal shiften)
	
			//calculate level:
			while (mask > 0 && ((ll[i] & mask) == (ur[i] & mask))) {
				level++;
				precision += mask;
				mask >>>= 1;
			}
			bitField[i] = ll[i] & precision;
			minLevel = Math.min(minLevel, level);
		}
		return zCode(bitField, minLevel);
	}

}
