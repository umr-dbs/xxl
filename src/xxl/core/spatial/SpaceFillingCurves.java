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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


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

	

	/**
	 * 
	 * @param zPoint
	 * @param lPoint
	 * @param rPoint
	 * @return
	 */
	public static boolean containsInt(int[] zPoint, int[] lPoint, int[] rPoint) {
		for (int i = 0; i < zPoint.length; i++)
			if (zPoint[i] > rPoint[i] || zPoint[i] < lPoint[i])
				return false;
		return true;
	}
	
	
	/**
	 * 
	 * @param length dimension*number of bits pro dim
 	 * @param cycle
	 * @param dimension
	 * @return
	 */
	public static int[] createCyclicFunction(int length, int cycle, int dimension){
		int[] function = new int[length];
		for(int i = 0; i < function.length; i++ ){
			function[i] = (i/cycle) % dimension;
		}
		return function;
	}
	
	
	/**
	 * 
	 * @param point
	 * @param mask
	 * @return
	 */
	public static long computeZCode(int[] point, int bitsProdim) {
		long sfcKey = 0L;
		long mask = 1L;
		int dimension = point.length;
		int[] dimensionMasks = new int[point.length];
		// init masks
		Arrays.fill(dimensionMasks, 1 << (bitsProdim-1));
		// 
		for(int i = 0, k = dimension * bitsProdim; i < dimension * bitsProdim; i++, k--){
			// write first dim 
			int value = point[i % dimension];
			// extract 
			value = value & dimensionMasks[i % dimension];
			dimensionMasks[i % dimension] = dimensionMasks[i % dimension] >> 1;
			if (value != 0){
				mask = 1L << (k-1); 
				sfcKey |= mask; // write one 
			}
		}
		return sfcKey;
	}
	
	
	/**
	 * reverse function
	 */
	public static int[] computePointFromZKey(long zcode, int bitsProDim,
			int dimension) {
		int[] point = new int[dimension];
		Arrays.fill(point, 0);
		for (int i = 0; i < bitsProDim* dimension; i++) {
			int shift = i / point.length;
			int mask = 1 << shift;
			// test bit on position i
			long value =  zcode & (1L << i);
			if (value != 0) {
				point[point.length - 1 - (i % point.length)] |= mask;
			}
		}
		return point;
	}
	
	
	
	
	
	
	
	/**
	 * implements next point in box method
	 *  @see H. Tropf, H. Herzog: Multidimensional Range Search in Dynamically Balanced Trees, Angewandte Informatik, 2/1981, pp 71-77
	 *
	 * @param zcode
	 * @param bitsProDim number of bits which is needed to represent key
	 * @param dimension
	 * @return next value of z key within the query box
	 */
	public static long nextInBoxZValue(long zcode, int[] lPoint,
			int[] rPoint, int bitsProDim, int dimension) {
		long nextMatch = zcode;
		int[] zPoint = computePointFromZKey(zcode, bitsProDim, dimension);
		int[] indexLo = new int[dimension];
		int[] indexHi = new int[dimension];
		Arrays.fill(indexLo, -1);
		Arrays.fill(indexHi, -1);
		for (int i = 0; i < dimension; i++) { // step 2 check all Coordinates
			if (zPoint[i] > rPoint[i]) // check right point 
				indexHi[i] = (posCoordInt(zPoint[i], rPoint[i], bitsProDim) + 1)
						* dimension - (i + 1);
			if (zPoint[i] < lPoint[i]) // check left  point
				indexLo[i] = (posCoordInt(zPoint[i], lPoint[i], bitsProDim) + 1)
						* dimension - (i + 1);
		}
		int maxL = -1;// find max indexLo and indexHi
		int maxH = -1;
		for (int i = 0; i < dimension; i++) {
			maxL = (indexLo[i] > maxL) ? indexLo[i] : maxL;
			maxH = (indexHi[i] > maxH) ? indexHi[i] : maxH;
		}
		if (maxH > maxL) {
			for (int i = maxH + 1; i < dimension * bitsProDim; i++) {
				boolean testBit =  (nextMatch &(1L << i)) != 0;
				if (!testBit
						&& rPoint[dimension - (i % dimension) - 1] > zPoint[dimension
								- (i % dimension) - 1]) {
					indexLo[dimension - (i % dimension) - 1] = i;
					break;
				}
			}
		}
		int max = -1;
		for (int i = 0; i < dimension; i++) {// step 4
			if (indexLo[i] > -1)
				max = (indexLo[i] > max) ? indexLo[i] : max;
		}
		if (max > -1) {
			// test
			long flipMask = 1L << max;
			nextMatch = nextMatch ^ flipMask;// change to 0
			nextMatch = nextMatch >> max; // step 5
			nextMatch = nextMatch << max;
		}
		for (int i = 0; i < dimension; i++) {
			int value = readDimension(nextMatch, i, dimension, bitsProDim);
			if (value < lPoint[i]) {
				nextMatch = writeDimension(nextMatch, lPoint[i], i, bitsProDim,
						dimension);
			}
		}
		return nextMatch;
	}

	/**
	 * helper method
	 * 
	 * @param zcode
	 * @param dim
	 * @param bitProDim
	 * @param dimensions
	 * @return
	 */
	private static int readDimension(long zcode, int dim, int dimensions, int bitsProdim) {
		int[] reverseValue = computePointFromZKey(zcode, bitsProdim, dimensions);
		int val =  reverseValue[dim];
		return val;
	}

	/**
	 * helper method
	 * 
	 * @param zcode
	 * @param dim
	 * @param bitsProDim
	 * @param dimensions
	 */
	private static long writeDimension(long zcode, int value,
			int dim, int bitsProDim, int dimensions) {
		int[] reverseCode = computePointFromZKey(zcode, bitsProDim, dimensions);
		reverseCode[dim] = value;
		long code = computeZCode(reverseCode, bitsProDim);
		return code;
	}
	

	/**
	 * help method finds highest bit position that differs (2 second step)
	 * 
	 * @param c1
	 *            1-dim coordinate
	 * @param c2
	 *            1-dim coordinate
	 * @param n
	 *            resolution bits pro dim
	 * @return 
	 */
	private static int posCoordInt(int c1, int c2, int n) {
		int mask = 1 << (n - 1);
		int i;
		for (i = n - 1; ((mask & c1) ^ (mask & c2)) == 0 && i >= 0; i--, mask = mask >> 1)
			;
		return i;
	}
	
	/**
	 * Computes the list of ranges which runs through the query box for a Z-Curve. 
	 * This is a recursive algorithms, which cuts the box  along the separation dimension (first bit which is differs)
	 * until the whole box is within the region (equal prefix:11111...111, equal prefix:000000... 0000)    
	 * 
	 * @param lPoint
	 * @param rPoint
	 * @param bitsProDim
	 * @param dimension
	 * @return
	 */
	public static List<long[]> computeZBoxRanges(int[] lPoint,
			int[] rPoint, int bitsProDim, int dimension ){
		List<long[]> leftResult = null;
		List<long[]> rightResult = null;
		long zvalueLeft = computeZCode(lPoint, bitsProDim);
		long zvalueRight = computeZCode(rPoint, bitsProDim);
		if (zvalueLeft == zvalueRight ) {    // case the same point
			leftResult = new LinkedList<long[]>();
			leftResult.add(new long[]{zvalueLeft, zvalueRight});
			return  leftResult;
		}
       	long mask= 1L << 63;
		int suffix; // last position of common prefix
		int prefix = 0;
		for( suffix = 64; 
		((mask & zvalueLeft)  == (mask & zvalueRight)) & suffix  >= 0;
		suffix--, mask = mask >>> 1, prefix++ ){ 
//			System.out.println(Long.toBinaryString(mask)); 
		};
		long onesSuffix = (1L<<(suffix))-1;
		// cae prefix.1111111 := 1 << suffix 
		boolean ones = ( onesSuffix & zvalueRight ) == (onesSuffix);
		boolean nulls = (onesSuffix & zvalueLeft ) == 0L;
		boolean continuesSequence = ones && nulls;
		if(continuesSequence){
			leftResult = new LinkedList<long[]>();
			leftResult.add(new long[]{zvalueLeft, zvalueRight});
			return  leftResult;
		}
		int[] newLow = cut(zvalueLeft, zvalueRight,  bitsProDim,  dimension,  prefix, true);
		int[] newHigh = cut(zvalueLeft, zvalueRight,  bitsProDim,  dimension, prefix, false);
		leftResult = computeZBoxRanges(lPoint, newHigh, bitsProDim,  dimension);
		rightResult = computeZBoxRanges(newLow, rPoint, bitsProDim,  dimension);
		leftResult.addAll(rightResult);
		return leftResult;
	}
	
	/**
	 * helper method, which cuts the space in along the hyperplane
	 * @param zvalueLeft
	 * @param zvalueRight
	 * @param bitsProDim
	 * @param dimension
	 * @param prefix
	 * @param low
	 * @return
	 */
	private static int[] cut(long zvalueLeft,
			long zvalueRight, int bitsProDim, int dimension, int prefix, boolean low){
		int highestBitPosition = 64 - bitsProDim * dimension;
		prefix = prefix - highestBitPosition;
		int position = prefix; // Greatest dimension on which we cut the box;
		position =  position / dimension  ; // position in dimension from left to right
		// check to which dimension belong this value
		int dim = (prefix) % dimension;
		long output = 0L;
		if (low){ // compute left lower point of new box 
			output = zvalueLeft;
			// extract dimension 
			int dimvalue = readDimension(output, dim, dimension, bitsProDim);
			// set one on the position with a trailing 0 
			int mask = 1 << (bitsProDim -position-1);
			dimvalue = (dimvalue >> (bitsProDim -position-1)) << (bitsProDim -position-1);
		
			dimvalue |=mask; // write value
			output = writeDimension(output, dimvalue, dim, bitsProDim, dimension);
			return computePointFromZKey(output, bitsProDim, dimension);
		}
		output = zvalueRight;
		// extract dimension 
		int dimvalue = readDimension(output, dim, dimension, bitsProDim);
		// set 0 on the position with trailing 1;
		int mask = (1 << (bitsProDim -position-1))-1;
		dimvalue = (dimvalue >> (bitsProDim -position)) << (bitsProDim -position);
		dimvalue |=mask; // write value
		output = writeDimension(output, dimvalue, dim, bitsProDim, dimension);
		return computePointFromZKey(output, bitsProDim, dimension);
	}
	
	
	public static void main(String[] args) {
		int[] left = {1,0};
		int[] right = {7,7};
//		int[] left = {0,2};
//		int[] right = {5,7};
////		int[] left = {4,2};
////		int[] right = {5,7};
		int bitsProdim = 3;
		int dimension = 2;
		List<long[]> result = computeZBoxRanges(left,
				right, bitsProdim, dimension );
		for(long[] l : result){
			System.out.println(Arrays.toString(l));
		}
	}
	
}
