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
import java.util.Random;

import xxl.core.io.converters.FixedSizeConverter;


public class Signature {
	/**
	 * Constructs a converter for a signature.
	 * @return The converter.
	 */
	public static FixedSizeConverter getConverter(final int sigLength, final int sigWeight) {
		return new FixedSizeConverter((sigLength+7)>>3) {
			Signature nullSignature = new Signature(sigLength);
			/**
			 * Serializes this Signature to the DataOutput-Stream.
			 * @param dataOutput the DataOutput-Stream
			 */
			public void write(DataOutput dataOutput, Object o) throws IOException {
				// if (o==null)
				//	dataOutput.write(nullSignature.s);
				// else
					dataOutput.write(((Signature)o).s);
			}
			/**
			 * Reconstructs the attributes from the DataInput-Stream.
			 * @param dataInput the DataInput-Stream that contains the serialized informations.
			 */
			public Object read(DataInput dataInput, Object o) throws IOException {
				if (o==null) { 
					byte b[] = new byte[(sigLength+7)>>3];
					dataInput.readFully(b);
					return new Signature(sigLength, b);
				}
				else {
					Signature sig = (Signature) o;
					sig.length = sigLength;
					dataInput.readFully(((Signature)o).s);
					return sig;
				}
			}
		};
	}

	protected byte[] s;
	protected int length;

	protected Signature(int length, byte s[]) {
		this.length = length;
		this.s = s;
	}

	protected Signature(int length) {
		this(length, new byte[(length+7)>>3]);
	}

	private static final void setBit(byte[] s, int i) {
		s[i>>3] |= (1<<(i&3)); 
	}

	private static final boolean getBit(byte[] s, int i) {
		return (((s[i>>3] >> (i&7))) &1)==1;
	}

	public static Signature createSignature(int length, int weight, Random random) {
		byte newSignature[] = new byte[(length+7)>>3];
		for (int i=0; i<weight; i++) {
			while (true) {
				int bitNumber = random.nextInt(length);
				if (!getBit(newSignature, bitNumber)) {
					setBit(newSignature, bitNumber);
					break;
				}
			}
		}
		return new Signature(length, newSignature);
	}

	public final boolean isInSignature(Signature inSig) {
		for (int i=0; i<s.length; i++)
			if ((s[i]&inSig.s[i])!=s[i])
				return false;
		return true;
	}

	public final Signature overlayWith(Signature s2) {
		byte[] b = Arrays.copy(s, 0, s.length);
		for (int i=0; i<s.length; i++)
			b[i] |= s2.s[i];
		return new Signature(length, b);
	}

	public Object clone() {
		return new Signature(length, Arrays.copy(s, 0, s.length));
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		for (int i=0; i<length; i++) {
			if (getBit(s,i))
				sb.append("1");
			else
				sb.append("0");
		}
		return sb.toString();
	}

	public boolean equals(Object o2) {
		if (o2==null)
			return false;
		Signature s2 = (Signature) o2;
		for (int i=0; i<s.length; i++)
			if (s[i]!=s2.s[i])
				return false;
		return true;
	}

	public int hashCode() {
		if (length<8)
			return s[0];
		else if (length<24)
			return (s[0]<<8) | s[1];
		else
			// 25 Bits ==> 4 Bytes available
			return (s[0]<<24) | (s[1]<<16) | (s[2]<<8) | s[3];
	}
}
