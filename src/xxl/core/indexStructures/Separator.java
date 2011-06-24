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

package xxl.core.indexStructures;

import java.util.Comparator;

	/**
	 * This class describes the known separtors of the B+ tree. A <tt>Separtor</tt> is 
	 * a simple key (for example an Integer). The <tt>Separator</tt> of a query is 
	 * a closed intervall [min, max]. With the contructors of the class Separator Separators of 
	 * every type can be created.   
	 * This class uses the interface {@link java.lang.Comparable} as a key class. Since 
	 * the interface {@link java.lang.Comparable} does not support the method clone() we have
	 * to let {@link Separator#clone() clone()} abstract here. 
	 * To use this class you only have to extend it to a concret class and implement the abstract method 
	 * {@link Separator#clone()}.
	 * For Example:
	 * <pre><code>
	  		public class IntSeparator extends BPlusTree.Separator {
	  			 			
	  			public IntSeparator(Integer key) {
	  				super(key);
	  			} 
	  			
	 				public Object clone() {
	 					return new IntSeparator(new Integer(((Integer)sepValue).intValue()));
	 				}
	  		}
	 	 </code></pre>
	 *	 
	 * @see Descriptor
	 * @see Comparable
	 */
	public abstract class Separator implements Descriptor, Comparable {
		
		/** This is the default Comparator for <tt>Separators</tt>. It is used to implement the method 
		 * {@link Separator#compareTo(Object)}.
		 */  
		public static final Comparator DEFAULT_COMPARATOR= new Comparator() {
			
			/** Compares two <tt>Separators</tt>. If a <tt>Separator</tt> is indefinite it is treated as a minimum.
			 *   
			 * @return a integer value which indicates how the both <tt>Separators</tt> lie to each other. 
			 * The result is as follows:
			 * <ul>
			 * <li> 0 if both <tt>Separators</tt> are indefinite or both are definite and the separation values are 
			 * equals.</li>
			 * <li> -1 if only the first <tt>Separator</tt> is indefinite or both are definite and the separation value of the 
			 * first <tt>Separator</tt> is smaller than the separation value of the second one. </li>
			 * <li> 1 if only the second <tt>Separator</tt> is indefinite or both are definite and the separation value of the 
			 * first <tt>Separator</tt> is bigger than the separation value of the second one.</li>
			 * <ul>
			 */
			public int compare(Object o1, Object o2) {
				Separator sep1=(Separator) o1;
				Separator sep2=(Separator) o2;
				if(!sep1.isDefinite()&& !sep2.isDefinite()) return 0;
				if(!sep1.isDefinite()) return -1;
				if(!sep2.isDefinite()) return 1;
				return sep1.sepValue.compareTo(sep2.sepValue);
			}			
		};
		
		/**The separation value.
		 */
		protected Comparable sepValue;
		
		/** Creates a new <tt>Separator</tt> with a given separation value.
		 * 
		 * @param sepValue the separation value of the new <tt>Separator</tt>
		 */
		public Separator(Comparable sepValue) {
			this.sepValue=sepValue;
		}
		
		/** Gives the separation value of this <tt>Separator</tt>.
		 * 
		 * @return the separation value of this <tt>Separator</tt>
		 */
		public Comparable sepValue() {
			return sepValue;
		}
		
		/** Updates the old separation value of this <tt>Separator</tt> by a new separation value.
		 * 
		 * @param newSepValue the new separation value
		 */
		public void updateSepValue(Comparable newSepValue) {
			this.sepValue=newSepValue;
		}
		
		/** Checks whether a given key has to be on the left or right side of this <tt>Separator</tt>.
		 * 
		 * @param key the key whose position has to be determined
		 * @return <tt>false</tt> if this <tt>Separator</tt> is definite and its separation value is smaller than the given key, <tt>true</tt> 
		 * otherwise
		 */ 
		public boolean isRightOf(Comparable key) {
			if(!isDefinite()) return false;
			return sepValue.compareTo(key)>=0;
		}
		
		/** Checks whether the separation value of this <tt>Separator</tt> is definite (not null).
		 * 
		 * @return <tt>true</tt> if the separation value is not <tt>null</tt>, <tt>false</tt> otherwise.
		 */
		public boolean isDefinite() {
			return sepValue!=null;
		}
		
		/** Compares the current <tt>Separator</tt> with another one by using the default <tt>Comparator</tt> 
		 * of this class.
		 * 
		 * @return a integer value which indicates how the both <tt>Separators</tt> lie to each other. 
		 * The result is as follows:
		 * <ul>
		 * <li> 0 if both <tt>Separators</tt> are indefinite or both are definite and the separation values are equals.</li>
		 * <li> -1 if only the current <tt>Separator</tt> is indefinite or both are definite and the separation value of the 
		 * current <tt>Separator</tt> is smaller than the separation value of the given one. </li>
		 * <li> 1 if only the given <tt>Separator</tt> is indefinite or both are definite and the separation value of the 
		 * current <tt>Separator</tt> is bigger than the separation value of the given one.</li>
		 * <ul>
		 * 
		 * @param sep separator to compare to
		 * @see #DEFAULT_COMPARATOR
		 */
		public int compareTo(Object sep) {
			return DEFAULT_COMPARATOR.compare(this, sep);
		}
		
		/** Unsupported operation.
		 *  
		 * @param descriptor unused
		 * @return never returns
		 * @throws UnsupportedOperationException
		 */
		public boolean overlaps(Descriptor descriptor) {
			throw new UnsupportedOperationException();
		}

		/** Unsupported operation. 

		 * 		 * @param descriptor unused
		 * @return never returns
		 * @throws UnsupportedOperationException
		 */
		public boolean contains(Descriptor descriptor){
			throw new UnsupportedOperationException();
		}
		
		/** Computes the union of this and a given <tt>Separator</tt>. It sets the separation value of the current 
		 * <tt>Separator</tt> to the minimum of both separation values.
		 * 
		 * @param descriptor has to be an instance of the class <tt>Separator</tt>
		 */
		public void union(Descriptor descriptor) {
			if(!(descriptor instanceof Separator)|| !isDefinite()) return;
			Separator sep=(Separator)descriptor;
			if(!sep.isDefinite()|| sepValue.compareTo(sep.sepValue)>0) this.sepValue=sep.sepValue; 
		}

		/** Checks whether the current <tt>Separator</tt> equals the given object by using the method 
		 * {@link Separator#compareTo(Object)}.
		 * 
		 * @param object the object to be compared for equality with this <tt>Seperator</tt>  
		 * @return <tt>true</tt> if the method {@link Separator#compareTo(Object)} return 0 and <tt>false</tt> otherwise.
		 */
		public boolean equals(Object object) {
			return compareTo(object)==0;	
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			StringBuffer sb=new StringBuffer("[");
			if(isDefinite()) sb.append(">="+sepValue);
			else sb.append("*");
			sb.append("]");
			return sb.toString(); 
		}
		
		/** Creates a physical copy of the current <tt>Separator</tt>.
		 * 
		 * @return a physical copy of the current <tt>Separator</tt>
		 * 
		 * @see java.lang.Object#clone()
		 */
		public abstract Object clone();		
	}
