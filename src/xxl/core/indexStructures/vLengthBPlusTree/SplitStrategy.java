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
package xxl.core.indexStructures.vLengthBPlusTree;


import java.util.Iterator;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Separator;
import xxl.core.predicates.AbstractPredicate;



/**
 * Skeleton class which provides basic functionality for implementing own split strategies for @see {@link VariableLengthBPlusTree} 
 * 
 * @param <D> Type of the data which is going to be stored in leaf nodes of B+Tree 
 * @param <K> Type of key objects
 */
public abstract class SplitStrategy<D,K> implements Splitter {
	
	/**
	 * function for computing the entry size 
	 */
	protected Function<D, Integer> getEntrySize;
	/**
	 * function for computing separator size 
	 */
	protected Function<Separator, Integer> getSeparatorSize;
	/**
	 * factory function for creating separtors
	 */
	protected Function<K, Separator> createSeparator;
	/**
	 * function for extracting keys from objects
	 */
	protected Function<D, K> createKey;
	/**
	 * size of the intern id of the back end container where the nodes of B+tree are stored 
	 */
	protected int containerIdSize;
	/**
	 * empty constructor. The strategy is parameterized and initialized through  @see {@link #initialize(Function, Function, Function, Function, int)} method  
	 */
	public SplitStrategy(){
		
	}
	/**
	 * 
	 * @param getEntrySize
	 * @param getSeparatorSize
	 * @param createKey
	 * @param createSeparator
	 * @return
	 */
	public SplitStrategy initialize(Function<D, Integer> getEntrySize, Function<Separator, Integer> getSeparatorSize,
			Function<D, K> createKey, Function<K, Separator> createSeparator, int containerIdSize){
		this.getEntrySize = getEntrySize;
		this.getSeparatorSize = getSeparatorSize;
		this.createSeparator = createSeparator;
		this.createKey = createKey;
		this.containerIdSize = containerIdSize;
		return this;
	}
	
	/**
	 * 
	 * @param dataObject
	 * @return
	 */
	public K getKey(D dataObject){
		return this.createKey.invoke(dataObject);
	}
	/**
	 * 
	 * @param separator
	 * @return
	 */
	public Separator createSeparator(K keyOfdataObject){
		return this.createSeparator.invoke(keyOfdataObject); 	
	}
	/**
	 * 
	 * @param dataObject
	 * @return
	 */
	public int getObjectSize(D dataObject){
		return this.getEntrySize.invoke(dataObject);
	}
	/**
	 * 
	 * @param separator
	 * @return
	 */
	public int getSeparatorSize(Separator separator){
		return this.getSeparatorSize.invoke(separator);
	}
	/**
	 * 
	 * @return
	 */
	public Cursor<Separator> filterRange(Iterator<Separator> entries, final int minLoad, final  int maxLoad){
		return new Filter<Separator>(entries, new AbstractPredicate<Separator>(){
			/**
			 * 
			 */
			int accLoad;
			
			public boolean invoke(Separator separator){
				accLoad += getSeparatorSize.invoke(separator);
				return accLoad > minLoad  &&  accLoad < maxLoad;
			}
		} );
	}
	
	
	
}
