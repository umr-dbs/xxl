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

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * This class iterates over a list of clusters. The object returned by the next method
 * is a byte array representation of the actual cluster.
 */
public class ClusterChainIterator implements Iterator
{
	/**
	 * Object of FATDevice.
	 */
	protected FATDevice device;
	
	/**
	 * Actual number of cluster.
	 */
	protected long clusterNumber;

	/**
	 * Indicates if the clusterNumber belongs to the root directory.
	 */
	protected boolean isRoot;

	/**
	 * Indicates if there is a next cluster.
	 */
	protected boolean hasNext;

	/**
	 * Actual cluster.
	 */
	protected byte[] cluster;


	/**
	 * Creates an instance of this object. If the given clusterNumber belongs to
	 * the root directory in case of FAT12 or FAT16 the isRoot flag has to be true
	 * and the clusterNumber has to be direct. Otherwise the given clusterNumber has
	 * to be indirect.
	 * @param device object of FATDevice.
	 * @param clusterNumber number of cluster where the iteration should start.
	 * @param isRoot indicates if the clusterNumber belongs to the root directory.
	 */
	public ClusterChainIterator(FATDevice device, long clusterNumber, boolean isRoot)
	{
		this.device = device;
		this.clusterNumber = clusterNumber;
		this.isRoot = isRoot;
		hasNext = true;
		if (isRoot && device.getFatType() != FAT.FAT32)
		{
			cluster = new byte[512];
			if (!device.readSector(cluster, clusterNumber))		//read only sector because the root directory is only accessible by sectors
				cluster = null;
		}
		else
		{
			cluster = device.readCluster(device.getFirstSectorNumberOfCluster(clusterNumber));
		}
	}	//end constructor


	/**
	 * Returns true if the iteration has more elements. (In other words,
	 * returns true if next would return an element rather than
     * throwing an exception.)
     * @return true if the iterator has more elements.
	 */
	public boolean hasNext()
	{
		if (hasNext)
			return hasNext;

		if (isRoot && device.getFatType() != FAT.FAT32)
		{
			if ((++clusterNumber) < device.getNumRootDirSectors() + device.getFirstRootDirSector())
			{
				cluster = new byte[512];
				if (!device.readSector(cluster,clusterNumber))		//read only sector because the root directory is only accessible by sectors
					cluster = null;
				hasNext = true;
				return true;
			}
		}
		else
		{
			clusterNumber = device.getFatContent(clusterNumber);
			if (!device.isLastCluster(clusterNumber))
			{
				cluster = device.readCluster(device.getFirstSectorNumberOfCluster(clusterNumber));
				hasNext = true;
				return true;
			}
			
		}
		
		return false;
	}	//end next


	/**
	 * Returns the next element in the iteration.
	 * @return the cluster as byte[]
	 * about the actual directory entry.
	 * @throws NoSuchElementException in case iteration has no more elements
	 * @see xxl.core.io.fat.DirectoryEntryInformation
	 */
	public Object next() throws NoSuchElementException
	{
		if (hasNext)
		{
			hasNext = false;
			return cluster;
		}
		else
		{
			if (!hasNext())
				throw new NoSuchElementException();
			else
			{
				hasNext = false;
				return cluster;
			}
		}
	}	//end next
	
	
	/**
	 * Not implemented yet.
	 * @throws UnsupportedOperationException
	 */
	public void remove()
	{
		throw new UnsupportedOperationException();
	}	//end remove()


	/**
	 * Return the clusterNumber.
	 * @return the clusterNumber.
	 */
	public long getClusterNumber()
	{
		return clusterNumber;
	}
	
	
	/**
	 * Return the flag that indicates if the actual cluster belongs to the root directory.
	 * @return true if the actual cluster belongs to the root directory; false otherwise.
	 */
	public boolean isRoot()
	{
		return isRoot;
	}
}	//end class ClusterChainIterator 


