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

package xxl.core.indexStructures.keyRanges;

import java.math.BigInteger;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.BPlusTree.KeyRange;

/** 
 * An example for {@link xxl.core.indexStructures.BPlusTree.KeyRange} with BigInteger values.
 */
public class BigIntegerKeyRange extends KeyRange {
	
	/**
	 * An factory function providing a <tt>BigIntegerKeyRange</tt> if invoked
	 * with two {@link java.math.BigInteger}.
	 */
	public static final Function<BigInteger, BigIntegerKeyRange> FACTORY_FUNCTION = new AbstractFunction<BigInteger, BigIntegerKeyRange>() {
		@Override
		public BigIntegerKeyRange invoke(BigInteger min, BigInteger max) {
			return new BigIntegerKeyRange(min, max);
		}
	};

	/** Constructs a new <tt>BigIntegerKeyRange</tt>.
	 * 
	 * @param min the beginning of the range
	 * @param max the end of the range
	 */		
	public BigIntegerKeyRange(BigInteger min, BigInteger max) {
		super(min, max);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		return new BigIntegerKeyRange((BigInteger)minBound(), (BigInteger)maxBound());
	}
}
