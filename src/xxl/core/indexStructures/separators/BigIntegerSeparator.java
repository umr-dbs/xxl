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

package xxl.core.indexStructures.separators;

import java.math.BigInteger;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;

/** An example for {@link xxl.core.indexStructures.Separator} with BigInteger values.
 */
class BigIntegerSeparator extends Separator {

	/**
	 * An factory function providing an <tt>BigIntegerSeparator</tt> if invoked
	 * with an {@link java.math.BigInteger}.
	 */
	public static final Function<BigInteger, BigIntegerSeparator> FACTORY_FUNCTION = new AbstractFunction<BigInteger, BigIntegerSeparator>() {
		@Override
		public BigIntegerSeparator invoke(BigInteger key) {
			return new BigIntegerSeparator(key);
		}
	};
	
	/** Constructs a new <tt>BigIntegerSeparator</tt>.
	 * 
	 * @param key the BigInteger used for separating
	 */	
	public BigIntegerSeparator(BigInteger key) {
		super(key);
	}
		
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone() {
		if (!isDefinite())
			return new BigIntegerSeparator(null);
		return new BigIntegerSeparator((BigInteger)sepValue);
	}
}
