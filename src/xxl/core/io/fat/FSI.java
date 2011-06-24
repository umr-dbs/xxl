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

import xxl.core.io.fat.errors.InvalidValue;
import xxl.core.io.fat.errors.NotFSISector;
import xxl.core.io.fat.util.ByteArrayConversionsLittleEndian;

/**
 * This class implements the FSI (File System Info) Sector Structure and Backup Boot Sector.
 * On a FAT32 volume, the FAT can be a large data structure, unlike on FAT16 where it is limited
 * to a maximum of 128K woth of sectors and FAT12 where it is limited to a maximim of 6K worth
 * of sectors. For this reason, a provision is made to store the 'last known' free cluster count
 * on the FAT32 volume so that it does not have to be computed as soon as an API call is made to 
 * ask how much free space there is on the volume. The FSInfo sector number is the value in the
 * BPB.FSInfo filed; for Microsoft operating systems it is always set to 1.
 */
public class FSI
{
	/**
	 * The FSInfo sector as byte array.
	 */
	private byte[] fsInfo = new byte[512];
	
	/**
	 * Value 0x41615252. This lead signature is used to validate that this is in
	 * fact an FSInfo sector.
	 */
	protected final static long LEAD_SIG = 0x41615252;
	
	/**
	 * Another signature that is more localized in the sector to the location of the
	 * fields that are used.
	 */
	protected final static long STRUC_SIG = 0x61417272;
	
	/**
	 * Value 0xAA550000. This trail signature is used to validate that this is
	 * in fact an FSInfo sector. Note that the high 2 bytes of this value -
	 * which go into the bytes at offset 510 and 511 - match the signature
	 * bytes used at the same offset in sector 0.
	 */
	protected final static long TRAIL_SIG = 0xAA550000;
	
	/**
	 * Indicates if the free count value is unknown or not.
	 */
	protected final static long UNKNOWN_FREE_COUNT = 0xFFFFFFFF;
	
	/**
	 * Contains the last known free cluster count on the volume. If the value is 0xFFFFFFFF,
	 * then the free count is unknown and must be computed. Any other value can be used, but
	 * is not necessarily correct. It should be range checked at least to make sure it is 
	 * <= volume cluster count.
	 */
	protected long freeCount = UNKNOWN_FREE_COUNT;
	
	/**
	 * This is a hint for the FAT driver. It indicates the cluster number at
	 * which the driver should start looking for free clusters. Because a 
	 * FAT32 FAT is large, it can be rather time consuming if there are a
	 * lot of allocated clusters at the start of the FAT and the driver starts
	 * looking for free cluster starting at cluster 2. Typically this value is
	 * set to the last cluster number that the driver allocated. If the value
	 * is 0xFFFFFFFF, then there is no hint and the driver should start looking
	 * at cluster 2. Any other value can be used, but should be checked first
	 * to make sure it is a valid cluster number for the volume.
	 */
	protected long nextFree = UNKNOWN_FREE_COUNT;
	
	/**
	 * Object of ByteArrayConversionsLittleEndian. It supports transformations from a 
	 * different number of byte entries to short, int, long and vice versa.
	 * This transformations can either be for little endian systems or 
	 * for big endian systems.
	 */
	//protected ByteArrayConversionsLittleEndian ByteArrayConversionsLittleEndian = new ByteArrayConversionsLittleEndianLittleEndian();
	

	/**
	 * Create an instance of this object.
	 */	
	public FSI()
	{
	}	//end constructor
	
	
	/**
	 * Use this method only by an initialization of the file system, i.e.
	 * when you format your disk. 
	 * @return byte array which represents the FSInfo sector.
	 */
	public byte[] getFSI()
	{
		//the value should be 0x41615252
		//LeadSig
		fsInfo[0] = (byte)0x52;
		fsInfo[1] = (byte)0x52;
		fsInfo[2] = (byte)0x61;
		fsInfo[3] = (byte)0x41;
		
		//reserved for future expansion of FAT -> 0
		//Reserved1
		for (int i=4; i < 480; i++)
			fsInfo[i] = (byte)0;
			
		//StrucSig 0x61417272
		fsInfo[484] = (byte)0x72;
		fsInfo[485] = (byte)0x72;
		fsInfo[486] = (byte)0x41;
		fsInfo[487] = (byte)0x61;
		
		//FreeCount, contains the last known free cluster count on the volume.
		//If the value is 0xFFFFFFFF, then the free count is unknown and must
		//be computed. Any other value can be used, but is not necessarily correct.
		//It should be range checked at least to make sure it is <= volume cluster
		//count.
		fsInfo[488] = (byte)0xFF;
		fsInfo[489] = (byte)0xFF;
		fsInfo[490] = (byte)0xFF;
		fsInfo[491] = (byte)0xFF;
		
		//NxtFree. This is a hint for the FAT driver. It indicates the cluster
		//number at which the driver should start looking for free clusters.
		//Because a FAT32 FAT is large, it can be rather time consuming if 
		//there are a lot of allocated clusters at the start of the FAT and the
		//driver starts looking for a free cluster starting at cluster 2.
		//Typically this value is set to the last cluster number that the driver
		//allocated. If the value is 0xFFFFFFFF, then there is no hint and the
		//driver should start looking at cluster 2. Any other value can be used,
		//but should be checked first to make sure it is a valid cluster number
		//for the volume.
		nextFree = 2;
		fsInfo[492] = (byte)(nextFree);			//(byte)0xFF;
		fsInfo[493] = (byte)(nextFree >> 8);	//(byte)0xFF;
		fsInfo[494] = (byte)(nextFree >> 16);	//(byte)0xFF;
		fsInfo[495] = (byte)(nextFree >> 32);	//(byte)0xFF;
			
		//reserved for future expansion of FAT -> 0
		//Reserved2
		for (int i=496; i < 496+12; i++)
			fsInfo[i] = (byte)0;
			
		//TrailSig. Value 0xAA550000. This trail signature is used to validate that
		//this is in fact an FSInfo sector. Note that the high 2 bytes of this
		//value -- which go into the bytes at offsets 510 and 511 -- match the
		//signature bytes used at the same offsets in sector 0.
		fsInfo[508] = (byte)0x00;
		fsInfo[509] = (byte)0x00;
		fsInfo[510] = (byte)0x55;
		fsInfo[511] = (byte)0xAA;
		
		return fsInfo;
	}	//end getFSI()
	
	
	/**
	 * Set the fsInfo byte array. If the given fsiSector doesn't contain
	 * the right signatures or if the length is not equal 512 an exception
	 * is thrown.
	 * @param fsiSector the FSInfo Sector.
	 * @throws NotFSISector in case the three signatures or the length of the array are wrong.
	 */
	public void initializeFSI(byte[] fsiSector) throws NotFSISector
	{
		long leadSig = ByteArrayConversionsLittleEndian.convLong(fsiSector[0], fsiSector[1], fsiSector[2], fsiSector[3]);
		long strucSig = ByteArrayConversionsLittleEndian.convLong(fsiSector[484], fsiSector[485], fsiSector[486], fsiSector[487]);
		long trailSig = ByteArrayConversionsLittleEndian.convLong(fsiSector[508], fsiSector[509], fsiSector[510], fsiSector[511]);

		if (leadSig != LEAD_SIG || strucSig != STRUC_SIG || trailSig != trailSig || fsiSector.length != 512)
			throw new NotFSISector(fsiSector);
		
		System.arraycopy(fsiSector, 0, fsInfo, 0, fsiSector.length);
				
		freeCount = ByteArrayConversionsLittleEndian.convLong(fsiSector[488], fsiSector[489], fsiSector[490], fsiSector[491]);
		nextFree = ByteArrayConversionsLittleEndian.convLong(fsiSector[492], fsiSector[493], fsiSector[494], fsiSector[495]);
	}	//end initializeFSI(byte[] fsiSector)
	
	
	/**
	 * Return the number free clusters.
	 * @return the number of free clusters.
	 * @throws InvalidValue if the number of free clusters is unknown.
	 */
	public long getFreeCount() throws InvalidValue
	{
		if (freeCount == 0xFFFFFFFF)
			throw new InvalidValue("The number of free clusters is unknown.", freeCount);
		return freeCount;
	}	//end getFreeCount()
	
		
	/**
	 * Set the number of free clusters to the given value newFreeCount.
	 * @param newFreeCount is the new free count value.
	 */
	protected void setFreeCount(long newFreeCount)
	{		
		freeCount = newFreeCount;
	}	//end setFreeCount(freeCount)


	/**
	 * Decrease the number of free clusters by the given value decFreeCount.
	 * @param decFreeCount is the value that is substract from freeCount.
	 */
	protected void decFreeCount(long decFreeCount)
	{
		freeCount -= decFreeCount;
	}	//end decFreeCount(freeCount)


	/**
	 * Increase the number of free clusters by the given value incFreeCount.
	 * @param incFreeCount is the value that is add to freeCount.
	 */
	protected void incFreeCount(long incFreeCount)
	{
		freeCount += incFreeCount;
	}	//end incFreeCount(freeCount)


	/**
	 * Return the next free cluster number.
	 * 
	 * @return the next free cluster number.
	 */
	public long getNextFree()
	{
		//if nextFree eqauls 0xFFFFFFFF the nextFree value is not valid
		//and a search has to start at fat-index 2
		if (nextFree == 0xFFFFFFFF)
			return 2;
		return nextFree;
	}	//end getNextFree()


	/**
	 * Set the next free variable to the given newValue. The next free
	 * value indicates the file system where to start the search for
	 * free clusters in the FAT.
	 * @param newValue the new next free value.
	 */
	protected void setNextFree(long newValue)
	{
		nextFree = newValue;
	}	//end setNextFree(long newValue)


	/**
	 * Return the updated FSI-sector. Use this method to save the changes
	 * of the FSI-sector after work with the file system.
	 * @return byte array that represents the FSI.
	 */
	public byte[] getUsedFSI()
	{
		//update the fsi with the changed values

		//free count
		fsInfo[492] = (byte)(freeCount & 0x00000000000000FF);
		fsInfo[493] = (byte)((freeCount & 0x000000000000FF00) >> 8);
		fsInfo[494] = (byte)((freeCount & 0x0000000000FF0000) >> 16);
		fsInfo[495] = (byte)((freeCount & 0x00000000FF000000) >> 24);

		//nextFree
		fsInfo[492] = (byte)(nextFree);
		fsInfo[493] = (byte)(nextFree >> 8);
		fsInfo[494] = (byte)(nextFree >> 16);
		fsInfo[495] = (byte)(nextFree >> 32);

		return fsInfo;
	}	//end getUsedFSI()
}	//end class FSI
