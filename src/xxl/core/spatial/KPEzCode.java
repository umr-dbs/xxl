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

import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.LongConverter;
import xxl.core.math.Maths;
import xxl.core.util.BitSet;

/** 
	A KPE + zCode, i.e.&nbsp;(Descriptor, ID, BitSet, isReplicate-Flag), length = 4.

*/
public class KPEzCode extends KPE implements Comparable{



	/** Creates a new KPEzCode instance. 
	 * 
	 * @param data data conntained in the KPE
	 * @param ID id of the KPE
	 * @param zCode zCode of the data
	 * @param isReplicate flag indicating if this KPEzCode is a replicate
	 */
	public KPEzCode(Object data,  Object ID, BitSet zCode, boolean isReplicate){
		super(
			new Object[]{
				data,
				ID,
				zCode,
				isReplicate ? Boolean.TRUE : Boolean.FALSE 
			},
			new Converter[]{
				ConvertableConverter.DEFAULT_INSTANCE,
				// XXX: CHANGE 23.11.2005 Daniel Schaefer
				// IntegerConverter.DEFAULT_INSTANCE,
				
				// Default Container-Ids are of type Long
				LongConverter.DEFAULT_INSTANCE, 
				// END OF CHANGE
				ConvertableConverter.DEFAULT_INSTANCE,
				BooleanConverter.DEFAULT_INSTANCE,
			}
		);
	}

	/** Creates a new KPEzCode instance for a given KPE. 
	 *
	 * @param k the kpe to wrap 
	 * @param zCode zCode of the data
	 */
	public KPEzCode(KPE k, BitSet zCode){
		this(k.getData(), k.getID(), zCode, false);
	}

	/** Copy Constructor. Does only call <code>super(kpez);</code>. 
	 *
	 * @param kpez the KPE to use as a template for the new instance
	 */
	public KPEzCode(KPEzCode kpez){
		super(kpez);	
	}

	/** Creates a new KPEzCode instance. The ID is set to <tt>zero</tt>. This is not a replicate. 
	 * 
	 * @param data data conntained in the KPE
	 * @param zCode zCode of the data
	 */
	public KPEzCode(Object data, BitSet zCode){
		this(data, Maths.ZERO, zCode, false);
	}

	/** Creates a new KPEzCode instance. The ID is set to <tt>zero</tt>. 
	 * 
	 * @param data data conntained in the KPE
	 * @param zCode zCode of the data
	 * @param isReplicate flag indicating if this KPEzCode is a replicate
	 */
	public KPEzCode(Object data, BitSet zCode, boolean isReplicate){
		this(data, Maths.ZERO, zCode, isReplicate);
	}

	/** Creates a new KPEzCode instance. This is not a replicate. 
	 * 
	 * @param data data conntained in the KPE
	 * @param ID id of the KPE
	 * @param zCode zCode of the data
	 */
	public KPEzCode(Object data, Object ID, BitSet zCode){
		this(data, ID, zCode, false);
	}

	/** Creates a new KPEzCode instance without ID. The zCode is set to 0. 
	 * This is not a replicate. 
	 * 
	 * @param data data conntained in the KPE
	 */
	public KPEzCode(Object data){
		this(data, null, new BitSet(), false);
	}

	/**
	 * Creates and returns a copy of this object.
	 *
	 * @return a clone of this instance.
	 */
	public Object clone() {
		return new KPEzCode(this);
    }

	
	/** Returns zCode.
	 * 
	 * @return value of zCode 
	 */
	public BitSet getzCode(){
		return (BitSet) getObject(3);
	}

	/** Sets zCode.
	 * 
	 * @param zCode the new zCode
	 */
	public void setzCode(BitSet zCode){
		setObject(3, zCode);
	}

	/** Returns isReplicate.
	 *
	 * @return value of isReplicate 
	 */
	public boolean getIsReplicate(){
		return ((Boolean)getObject(4)).booleanValue();
	}

	/** Sets isReplicate.
	 * 
	 * @param isReplicate new value for isReplicate 
	 */
	public void setIsReplicate(boolean isReplicate){
		setObject(4, isReplicate ? Boolean.TRUE : Boolean.FALSE );
	}

	/** Compares this KPEzCode to another by comparing the zCodes.
	 * 
	 * @param o the KPEzCode to compare to
	 * @return comparison value of zCodes of this and the KPEzCode <tt>o</tt>
	 */
	public int compareTo(Object o){
		return getzCode().compareTo( ((KPEzCode)o).getzCode() );
	}

	/**
	 * Returns a string representation of the object. In general, the
	 * <tt>toString</tt> method returns a string that "textually represents" this
	 * object. The result should be a concise but informative representation that
	 * is easy for a person to read.
	 *
	 * @return  a string representation of the object.
	 */
	public String toString(){
		return super.toString() + "cell:\n" + getzCode().toString2();
	}
}
