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
 * 
 * An example for {@link xxl.core.indexStructures.Separator} with String values.
 *
 */
public class StringSeparator extends Separator{
	
	public static final Function<String, Separator> FACTORY_FUNCTION = new AbstractFunction<String, Separator>(){
		
		public Separator invoke(String key){
			return new StringSeparator((key==null) ? null : key);
		}
	};
	
	public StringSeparator(String sepValue) {
		super(sepValue);
	
	}

	@Override
	public Object clone() {
		return new StringSeparator((isDefinite()) ? (String)this.sepValue : null);
	}
	/**
	 * case insensitive 
	 * @param separator
	 * @return
	 */
	public int compareTo(Object sep){
		StringSeparator  stSep = (StringSeparator)sep;
		//check if indefinite
		if (!stSep.isDefinite() && !this.isDefinite()) return 0;
		if (!stSep.isDefinite()) return -1;
		if (!this.isDefinite()) return 1;
		return ((String)this.sepValue()).compareToIgnoreCase(((String)stSep.sepValue()));
	}

}
