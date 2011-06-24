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

package xxl.core.util.metaData;

import java.util.HashMap;

public abstract class AbstractMetaDataManagement<I,M> implements MetaDataManagement<I,M> {

	/**
	 * Object containing the metadata.
	 */
	protected CompositeMetaData<I,M> metaData;
		
	/**
	 * Maintains a counter for each metadata fragment.
	 */
	protected HashMap<I,Integer> frequencyCounter;
	
	
	public AbstractMetaDataManagement() {
		this.metaData = null;
		this.frequencyCounter = null;
	}
			
	public synchronized void initialize(CompositeMetaData<I,M> metaData) {
		if (this.metaData != null)
			throw new IllegalStateException("MetaDataManagement instance has already been initialized.");
		this.metaData = metaData;
		this.frequencyCounter = new HashMap<I,Integer>();
	}
				
	public synchronized boolean[] includeAll(I... metaDataIdentifiers) throws MetaDataException {
		boolean [] res = new boolean[metaDataIdentifiers.length];
		for (int i=0; i<metaDataIdentifiers.length; i++) 			
			res[i] = include(metaDataIdentifiers[i]);
		return res;
	}
	
	public synchronized boolean include(I metaDataIdentifier) throws MetaDataException {			
		if (metaData == null)
			initialize(new CompositeMetaData<I,M>());
		if (metaData.contains(metaDataIdentifier) || addMetaData(metaDataIdentifier)) {
			incrementFrequencyCounter(metaDataIdentifier);
			return true;
		}
		return false;
	}
	
	protected abstract boolean addMetaData(I metaDataIdentifier);
	
	public synchronized void excludeAll(I... metaDataIdentifiers) throws MetaDataException {
		for (int i=0; i<metaDataIdentifiers.length; i++) 		
			exclude(metaDataIdentifiers[i]);
	}
		
	public synchronized void exclude(I metaDataIdentifier) throws MetaDataException {
		if (metaData != null && metaData.contains(metaDataIdentifier)) {
			decrementFrequencyCounter(metaDataIdentifier);
			if (frequencyCounter.get(metaDataIdentifier) == 0 && !removeMetaData(metaDataIdentifier))
				throw new IllegalStateException("Unable to remove metadata: "+metaDataIdentifier);
			return;
		}
		throw new IllegalStateException("Unable to remove metadata: "+metaDataIdentifier);
	}
	
	protected abstract boolean removeMetaData(I metaDataIdentifier);
				
	public synchronized CompositeMetaData<I,M> getMetaData() {
		return metaData;
	}
	
	protected void incrementFrequencyCounter(I metaDataIdentifier) {
		if (frequencyCounter != null) {
			if (frequencyCounter.containsKey(metaDataIdentifier)) 
				frequencyCounter.put(metaDataIdentifier, frequencyCounter.get(metaDataIdentifier)+1);
			else 
				frequencyCounter.put(metaDataIdentifier, 1);
			return;
		}
		throw new MetaDataException("No frequency counter for metadata identifier "+metaDataIdentifier+" maintained.");
	}
	
	protected void decrementFrequencyCounter(I metaDataIdentifier) {
		if (frequencyCounter != null) {
			if (frequencyCounter.containsKey(metaDataIdentifier)) {
				frequencyCounter.put(metaDataIdentifier, frequencyCounter.get(metaDataIdentifier)-1);
				return;
			}
		}
		throw new MetaDataException("No frequency counter for metadata identifier "+metaDataIdentifier+" maintained.");
	}
	
}
