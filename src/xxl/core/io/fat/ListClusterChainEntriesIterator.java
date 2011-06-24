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

package xxl.core.io.fat;

import java.util.NoSuchElementException;

/**
 * This class iterates over all entries of the super class ClusterChainIterator.
 * The returned objects by the next method are DirectoryEntryInformation objects.
 * @see xxl.core.io.fat.DirectoryEntryInformation
 */
public class ListClusterChainEntriesIterator extends ClusterChainIterator
{	
	/**
	 * The actual directory entry.
	 */
	protected DirectoryEntryInformation dirEntInf;
	
	/**
	 * Indicates if this iterator has more elements
	 */
	protected boolean hasNext;
	
	/**
	 * An index inside the actual cluster.
	 */
	protected int index;

	/**
	 * The byte array representation of the directory entry
	 */
	protected byte[] directoryEntry;

	/**
	 * Creates an instance of this object. If the given clusterNumber belongs to
	 * the root directory in case of FAT12 or FAT16 the isRoot flag has to be true
	 * and the clusterNumber has to be direct. Otherwise the given clusterNumber has
	 * to be indirect.
	 * @param device object of FATDevice.
	 * @param clusterNumber number of cluster where the iteration should start.
	 * @param isRoot indicates if the clusterNumber belongs to the root directory.
	 */
	public ListClusterChainEntriesIterator(FATDevice device, long clusterNumber, boolean isRoot)
	{
		super(device, clusterNumber, isRoot);
		index = 0;
		directoryEntry = new byte[32];
	}


	/**
	 * Returns true if the iteration has more elements. (In other words,
	 * returns true if next would return an element rather than
     * throwing an exception.)
     * @return true if the iterator has more elements.
	 */
	public boolean hasNext()
	{
		if (hasNext)
			return true;

		if (super.hasNext())
		{
			System.arraycopy(cluster, index, directoryEntry, 0, 32);
			if (DIR.isLongEntry(directoryEntry))
			{
				int skip = DIR.getOrderNumber(directoryEntry);
				byte[] longEntry = new byte[(skip+1)*32];

				if (index+longEntry.length <= cluster.length)
				{
					System.arraycopy(cluster, index, longEntry, 0, longEntry.length);
					dirEntInf = new DirectoryEntryInformation(
						cluster,
						clusterNumber,
						index / device.getBytsPerSec(),
						index,
						longEntry,
						isRoot
					);
					index += 32*(skip+1);

					if (index >= cluster.length)
					{
						index = 0;
						super.next();
					}
					hasNext = true;
					return hasNext;
				}
				else	//cluster boundary
				{
					long oldClusterNumber = clusterNumber;
					byte[] cluster = (byte[])super.next();
					if (!super.hasNext())
					{
						hasNext = false;
						return hasNext;
					}

					System.arraycopy(cluster, index, longEntry, 0, cluster.length - index);
					System.arraycopy(	super.cluster,
										0,
										longEntry,
										cluster.length - index,
										index + longEntry.length - cluster.length);

					dirEntInf = new DirectoryEntryInformation(
						cluster,
						oldClusterNumber,
						index / device.getBytsPerSec(),
						index,
						longEntry,
						isRoot
					);

					index += longEntry.length - cluster.length;
					hasNext = true;
					return hasNext;
				}		//end cluster boundary
			}			//end if long entry
			else
			{
				dirEntInf = new DirectoryEntryInformation(
					cluster,
					clusterNumber,
					index/device.getBytsPerSec(),
					index,
					directoryEntry,
					isRoot
				);
				hasNext = true;
				index += 32;
				if (index >= cluster.length)
				{
					index = 0;
					super.next();
				}
				return hasNext;
			}
		}	//end if super.hasNext
		return false;
	}	//end next


	/**
	 * Returns the next element in the iteration.
	 * @return DirectoryEntryInformation which contains all necessary information
	 * about the actual directory entry.
	 * @throws NoSuchElementException in case iteration has no more elements
	 * @see xxl.core.io.fat.DirectoryEntryInformation
	 */
	public Object next() throws NoSuchElementException
	{
		if (hasNext || hasNext())
		{
			hasNext = false;
			return dirEntInf;
		}
		throw new NoSuchElementException();
	}	//end next


	/**
	 * Not implemented yet.
	 * @throws UnsupportedOperationException
	 */
	public void remove()
	{
		throw new UnsupportedOperationException();
	}	//end remove()
}	//end class
