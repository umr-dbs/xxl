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

import xxl.core.util.Arrays;

/**
 * This class holds information about a directory entry, like cluster, cluster number,
 * directory entry index and so on. It is used by methods of DIR to exchange information
 * between the methods.
 */
public class DirectoryEntryInformation
{
	/**
	 * Contains the cluster of the searched directory entry. If the directory entry
	 * span over more than one cluster, this field contains the first of the cluster
	 * chain.
	 */
	public byte[] cluster;
	
	/**
	 * Contains the cluster number of the searched directory entry. In case it is a
	 * cluster number of the root directory and the type of FAT is not FAT32 it is
	 * the direct cluster number otherwise it is the indirect cluster number which must
	 * be used with getFirstSectorNumberOfCluster(long) of FATDevice.
	 * directory
	 */
	public long clusterNumber;
	
	/**
	 * Contains the sector number within the cluster array of the searched directory entry.
	 */
	public long sectorNumber;
	
	/**
	 * Contains the index within the cluster of the searched directory entry.
	 */
	public long directoryEntryIndex;
	
	/**
	 * Contains the searched directoryEntry if it exist otherwise it's null.
	 */
	public byte[] directoryEntry;
	
	/**
	 * Indicates if the entry belongs to the root directory.
	 */
	public boolean isRoot;
	
	/**
	 * Create an instance of this object.
	 * @param cluster cluster of the directoryEntry.
	 * @param clusterNumber the number of the cluster.
	 * @param sectorNumber the number of the sector.
	 * @param directoryEntryIndex the index of the directory entry.
	 * @param directoryEntry the directory entry to store.
	 * @param isRoot flag which indicates if the directoryEntry belongs to the
	 * root directory.
	 */
	public DirectoryEntryInformation(byte[] cluster,
									 long clusterNumber,
									 long sectorNumber,
									 long directoryEntryIndex,
									 byte[] directoryEntry,
									 boolean isRoot)
	{
		this.cluster = cluster;
		this.clusterNumber = clusterNumber;
		this.sectorNumber = sectorNumber;
		this.directoryEntryIndex = directoryEntryIndex;
		this.directoryEntry = directoryEntry;
		this.isRoot = isRoot;
	}	//end constructor
	
	
	/**
	 * Clone the object.
	 * @return the cloned object.
	 */	
	protected Object clone()
	{
		byte[] clus = new byte[cluster.length];
		byte[] dirEnt = new byte[directoryEntry.length];
		System.arraycopy(cluster, 0, clus, 0, clus.length);
		System.arraycopy(directoryEntry, 0, dirEnt, 0, dirEnt.length);
		return new DirectoryEntryInformation(clus, clusterNumber, sectorNumber, directoryEntryIndex, dirEnt, isRoot);
	}
	
	
	/**
	 * Print some information about the variables of this class.
	 * @return some information about the variables of this class.
	 */
	public String toString()
	{
		String res = "DirectoryEntryInformation:\n";
		if (cluster != null)
		{
			res += "cluster is not null. cluster length "+cluster.length+"\n";
		}
		else
			res += "cluster is null.\n";
		res += "clusterNumber "+clusterNumber+"\n"+
			"sectorNumber "+sectorNumber+"\n"+
			"dirEntIndex "+directoryEntryIndex+"\n"+
			"is Root "+isRoot+"\n";
		
		return res+Arrays.printHexArrayString(directoryEntry);
	}
}	//end inner class DirectoryEntryInformation
