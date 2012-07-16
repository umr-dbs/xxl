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

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;

/**
 * An example for {@link xxl.core.indexStructures.Separator} with Integer values.
 */
public class IntSeparator extends Separator {

	/**
	 * An factory function providing an <tt>IntSeparator</tt> if invoked
	 * with an {@link java.lang.Integer}.
	 */
	public static final Function FACTORY_Function= 
		new AbstractFunction() {
			public Object invoke(Object key) {
				return new IntSeparator((Integer)key);
			}
		};
	
	/** Constructs a new <tt>IntSeparator</tt>.
	 * 
	 * @param key the intValue of the Integer used for separating
	 */	
	public IntSeparator(int key) {
		this(new Integer(key));
	}

	/** Constructs a new <tt>IntSeparator</tt>.
	 * 
	 * @param key the Integer used for separating
	 */	
	public IntSeparator(Integer key) {
		super(key);
	}
		
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public Object clone() {
		if(!isDefinite()) return new IntSeparator(null);
		return new IntSeparator(((Integer)sepValue).intValue());
	}
}
