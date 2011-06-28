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
import java.util.LinkedList;
import java.util.List;

import xxl.core.io.fat.errors.InitializationException;
import xxl.core.io.fat.errors.InvalidValue;
import xxl.core.io.fat.errors.NotEnoughMemory;
import xxl.core.io.fat.errors.NotFSISector;
import xxl.core.io.fat.errors.WrongFATType;
import xxl.core.io.fat.util.ByteArrayConversionsLittleEndian;
import xxl.core.io.fat.util.MyMath;


/**
 * This class represents the FAT (File Allocation Table) this can be
 * a FAT12, FAT16, or FAT32.
 */
public class FAT
{
	/**
	 * Object of FATDevice.
	 */
	private FATDevice device;
	
	/**
	 * Object of the BIOS Parameter Block.
	 */
	private BPB bpb;
	
	/**
	 * Object of the FSI. It's only valid in case of FAT32.
	 */
	private FSI fsi;
	
	/**
	 * Object for free memory management.
	 */
	private FreeMemoryManagement freeMemoryManagement;
	
	/**
	 * Indicates the type of FAT, i.e. FAT12, FAT16, FAT32
	 */
	private byte fatType = 0;
	
	/**
	 * Typically there is more than one FAT, the fatNumber indicates
	 * which one should be used.
	 */
	private int fatNumber = 0;	//0 is fat number one, 1 is fat number two and so on
	
	/**
	 * Contains the number of sectors for one FAT.
	 */
	private long FATSz;
	
	/**
	 * Contains the number of defect sectors that are not data sectors. These
	 * are all sectors until the first data sector (bpb.getFirstDataSector()).
	 * All other defect sectors are known by the FAT-structure.
	 * @see xxl.core.io.fat.BPB#getFirstDataSector()
	 */
	private List defectSectorNumbers = new LinkedList();
	
	/**
	 * Byte array which can hold one sector.
	 */
	private byte[] sectorBuffer;

	/**
	 * Constant variable which indicates a bad cluster on a FAT12 file system.
	 */
	public static final int BAD_CLUSTER_FAT12 = 0x0FF7;
	
	/**
	 * Constant variable which indicates a bad cluster on a FAT16 file system.
	 */
	public static final int BAD_CLUSTER_FAT16 = 0xFFF7;
	
	/**
	 * Constant variable which indicates a bad cluster on a FAT32 file system.
	 */
	public static final long BAD_CLUSTER_FAT32 = 0x0FFFFFF7;
	
	/**
	 * Constant variable which indicates a EOC_MARK (end of chain) on a FAT12 file system.
	 */
	public static final int EOC_MARK12 = 0x0FFF;
	
	/**
	 * Constant variable which indicates a EOC_MARK (end of chain) on a FAT16 file system.
	 */
	public static final int EOC_MARK16 = 0xFFFF;
	
	/**
	 * Constant variable which indicates a EOC_MARK (end of chain) on a FAT32 file system.
	 */
	public static final long EOC_MARK32 = 0x0FFFFFFF;
	
	/**
	 * Constant variable which indicates a FAT12 file system.
	 */
	public static final byte FAT12 = 0;
	
	/**
	 * Constant variable which indicates a FAT16 file system.
	 */
	public static final byte FAT16 = 1;

	/**
	 * Constant variable which indicates a FAT32 file system.
	 */
	public static final byte FAT32 = 2;

	/**
	 * Constant variable which indicates an unknown file system.
	 */
	public static final byte UNKNOWN_FAT = 3;
		
	/**
	 * If bit is 1, volume is clean. If bit is 0 volume is dirty. This
	 * indicates that the file system driver did not dismount properly the volume
	 * the last time it was mounted. You should run
	 * Scandisk on it.
	 */
	public static final int CLN_SHUT_BIT_MASK_FAT16 = 0x8000;
	
	/**
	 * If bit is 1, volume is clean. If bit is 0 volume is dirty. This
	 * indicates that the file system driver did not dismount properly the volume
	 * the last it was mounted. You should run
	 * Scandisk on it.
	 */
	public static final long CLN_SHUT_BIT_MASK_FAT32 = 0x08000000;
	
	/**
	 * If this bit is 1, no disk read/write errors were encountered. If
	 * this bit is 0, the file system driver encountered a disk I/O error
	 * on the volume the last time it was mounted, which is an indicator
	 * that some sectors may have gone bad on the volume. You should run
	 * Scandisk on it.
	 */
	public static final int HRD_ERR_BIT_MASK_FAT16 = 0x4000;
	
	/**
	 * If this bit is 1, no disk read/write errors were encountered. If
	 * this bit is 0, the file system driver encountered a disk I/O error
	 * on the volume the last time it was mounted, which is an indicator
	 * that some sectors may have gone bad on the volume. You should run
	 * Scandisk on it.
	 */
	public static final long HRD_ERR_BIT_MASK_FAT32 = 0x04000000;
	
	/**
	 * Flag which indicates that the checkdisk/scandisk method should
	 * scan for bad clusters.
	 */
	public static final byte CHECK_BAD_CLUSTERS = 0;
	

	/**
	 * This class is used to make the free memory management.
	 */
	protected class FreeMemoryManagement
	{	
		/**
		 * Contains Long objects which represents the cluster numbers that are free.
		 */
		protected LinkedList freeClusters;
			
		/**
		 * Create an instance of this class.
		 */
		public FreeMemoryManagement()
		{
			freeClusters = null;
		}	//end constructor
		
		
		/**
		 * Create an instance of this class.
		 * @param freeClusters list of Long objects which represent the free cluster numbers.
		 */
		public FreeMemoryManagement(LinkedList freeClusters)
		{
			this.freeClusters = freeClusters;
		}	//end constructor
		
		
		/**
		 * Return a list with cluster numbers that are free.
		 * @param numOfFreeClusters is the number of free clusters.
		 * @return list with cluster numbers that are free. The list is
		 * empty if there are not enough free clusters.
		 * @throws NotEnoughMemory if there are not enough clusters left to
		 * support the query.
		 */
		public List getFreeClusters(long numOfFreeClusters) throws NotEnoughMemory
		{
			List list = new LinkedList();
			
			if (getFatType() == FAT32)
			{	
				if (numOfFreeClusters == 0)
					return list;		
				long lastFatCluster = bpb.getLastFatCluster();
				for (long i = fsi.getNextFree(); i < lastFatCluster; i++)
				{
					if (getFatContent(i) == 0)
					{
						list.add(new Long(i));
						if (list.size() >= numOfFreeClusters)
						{
							fsi.decFreeCount(list.size());
							fsi.setNextFree(i+1);
							return list;
						}
					}
				}
				
				//if we reach this part, we have not enough memory
				//return the ordered memory and throw exception
				long numClusters = list.size();
				addFreeClusters(list);
				throw new NotEnoughMemory(numClusters, numOfFreeClusters);
			}
			else	//FAT12 and FAT16
			{
				for (int i=0; i < numOfFreeClusters; i++)
				{
					if (!freeClusters.isEmpty())
						list.add(freeClusters.remove(0));
					else
					{
						long numClusters = list.size();
						freeClusters.addAll(0, list);
						throw new NotEnoughMemory(numClusters, numOfFreeClusters);
					}
				}
			}
			return list;
		}	//end getFreeClusters(numOfFreeClusters)


		/**
		 * Add the cluster with number clusterNumber to the free cluster list.
		 * @param clusterNumber is the number of the cluster that should be added
		 * to the free cluster list.
		 */
		public void addFreeCluster(long clusterNumber)
		{
			if (getFatType() == FAT32)
			{
				setFatContent(clusterNumber, 0);	//mark clusterNumber as free

				//update fsi
				fsi.incFreeCount(1);
				if (fsi.getNextFree() > clusterNumber)
					fsi.setNextFree(clusterNumber);
			}
			else
			{
				Long tmp = new Long(clusterNumber);
				if (!freeClusters.contains(tmp))
					freeClusters.add(tmp);	//einfuegen nicht gerade geistreich
			}
		}	//end addFreeCluster(clusterNumber)


		/**
		 * Add all cluster numbers of the list freeClusters to the free
		 * cluster list.
		 * @param freeClusters list which contains cluster numbers that
		 * should be added to the free cluster list.
		 */
		public void addFreeClusters(List freeClusters)
		{
			for (int i=0; i < freeClusters.size(); i++)
				addFreeCluster(((Long)freeClusters.get(i)).longValue());
		}	//end addFreeClusters(List freeClusters)


		/**
		 * Return the number of free clusters.
		 * @return the number of free clusters.
		 */
		public long getNumFreeClusters()
		{
			if (getFatType() == FAT32)
			{
				try
				{
					return fsi.getFreeCount();
				}
				catch (InvalidValue e)
				{
					long result = determineNumOfFreeClusters();
					fsi.setFreeCount(result);
					return result;
				}
			}
			return freeClusters.size();
		}	//end getNumFreeClusters()


		/**
		 * This method determines the number of free clusters by
		 * scanning the FAT, this might be time consuming.
		 * @return the number of free clusters.
		 */
		private long determineNumOfFreeClusters()
		{
			byte[] fat = readFAT();
			long freeClusters = 0;
			long lastClusterNumber = bpb.getLastFatCluster();
			for (long index = 2; index < lastClusterNumber; index++)
			{
				//if entry is free put it on the free cluster list
				if (getFatContent(index, fat) == 0)
					freeClusters++;
			}
			return freeClusters;
		}	//end determineNumOfFreeClusters()

	}	//end inner class FreeMemoryManagement

	/**
	 * Create an instance of this object. The constructor will boot
	 * the fat and initialize the free memory management with all found free
	 * clusters.
	 * @param device an object of the FATDevice where this FAT is active.
	 * @param bpb the BPB of the device.
	 */
	public FAT(FATDevice device, BPB bpb)
	{
		this.device = device;
		this.bpb = bpb;
		this.fatType = bpb.getFatType();
		//number of sectors for one fat
		FATSz  = (fatType == FAT12) ? bpb.FATSz16 : ((fatType == FAT16) ? bpb.FATSz16 : bpb.FATSz32);

		//must be done after the constructor because of checkdisk
		//initializeFat();
	}	//end constructor boot


	/**
	 * Create an instance of this object. This constructor will format
	 * the fat blocks and initialize the free memory management with all
	 * free clusters.
	 * @param device an object of the FATDevice where this FAT is active.
	 * @param bpb the BPB of the device.
	 * @param fatType the type of fat that should be created.
	 * @throws InitializationException in case this object couldn't be initialized.
	 */
	public FAT(FATDevice device, BPB bpb, byte fatType) throws InitializationException
	{
		this.device = device;
		this.bpb = bpb;
		this.fatType = fatType;

		byte[] dataBlock = new byte[bpb.BytsPerSec];

		//clear the block
		for (int i=0; i < dataBlock.length; i++)
			 dataBlock[i] = 0;

		int firstFatSector = bpb.RsvdSecCnt;
		long numFatSectors = bpb.getNumFatSectors();
		//number of sectors for one fat
		FATSz  = (fatType == FAT12) ? bpb.FATSz16 : ((fatType == FAT16) ? bpb.FATSz16 : bpb.FATSz32);

		//before the file system can be formatted the surface of the disk
		//must be checked for defect sectors/cluster
		if (!checkDiskSurface())
		{
			throw new InitializationException("The disk surface is out of order. Couldn't initialize FAT.");
		}

		//the first two entries of the FAT have specific values
		//the first entry equals the BPB.Media at the low 8 bits
		//the rest of the bits are set to 1 and the
		//second entry is set to EOC-MARK (depends on fatType).
		//For example, if the BPB.Media value is 0xF8, for FAT12
		//FAT[0] = 0x0FF8, for FAT16 FAT[0] = 0xFFF8, and for
		//FAT32 FAT[0] = 0x0FFFFFF8.
		if (fatType == FAT12)
		{
			//hard wired the BPB.Media value with filled 1 bit entries and the EOC_MARK
			dataBlock[0] = (byte)bpb.Media;
			dataBlock[1] = (byte)0xFF;
			dataBlock[2] = (byte)0xFF;

			//first writes the first sector in each fat
			for (int i=0; i < bpb.NumFATs; i++)
				writeSector(dataBlock, firstFatSector + i*numFatSectors);

			//initialize everything else with 0
			dataBlock[0] = 0;
			dataBlock[1] = 0;
			dataBlock[2] = 0;

			for (int j=0; j < bpb.NumFATs; j++)
				for (int i=firstFatSector + 1; i < firstFatSector + numFatSectors; i++)
					writeSector(dataBlock, i + j*numFatSectors);


		}
		else if (fatType == FAT16)
		{
			//hard wired the BPB.Media value with filled 1 bit entries and the EOC_MARK
			dataBlock[0] = (byte)bpb.Media;
			dataBlock[1] = (byte)0xFF;
			dataBlock[2] = (byte)0xFF;
			//the high two bits of the second entry in the fat are used to indicate a dirty
			//volume, see CLN_SHUT_BIT_MASK_FAT16 and HRD_ERR_BIT_MASK_FAT16.
			//At the beginning everything is fine when set to EOC_MARK.
			dataBlock[3] = (byte)0xFF;

			//first writes the first sector in each fat
			for (int i=0; i < bpb.NumFATs; i++)
				writeSector(dataBlock, firstFatSector + i*numFatSectors);

			//initialize everything else with 0
			dataBlock[0] = 0;
			dataBlock[1] = 0;
			dataBlock[2] = 0;
			dataBlock[3] = 0;

			for (int j=0; j < bpb.NumFATs; j++)
				for (int i=firstFatSector + 1; i < firstFatSector + numFatSectors; i++)
					writeSector(dataBlock, i + j*numFatSectors);
		}
		else if (fatType == FAT32)
		{
			//initialize the FSI
			if (fatType == FAT.FAT32)
			{
				fsi = new FSI();
				try
				{
					//write the formatted FileSystemInfo at the specified sector number to disk
					writeSector(fsi.getFSI(), bpb.getFSInfoSectorNumber());
				}
				catch(WrongFATType e)
				{
					throw new InitializationException("Couldn't initialize FAT, because of: "+e);
				}
			}

			//hard wired the BPB.Media value with filled 1 bit entries and the EOC_MARK
			dataBlock[0] = (byte)bpb.Media;
			dataBlock[1] = (byte)0xFF;
			dataBlock[2] = (byte)0xFF;
			dataBlock[3] = (byte)0x0F;

			dataBlock[4] = (byte)0xFF;
			dataBlock[5] = (byte)0xFF;
			dataBlock[6] = (byte)0xFF;
			//the high two bits of the second entry in the fat are used to indicate a dirty
			//volume, see CLN_SHUT_BIT_MASK_FAT32 and HRD_ERR_BIT_MASK_FAT32.
			//At the beginning everything is fine when set to EOC_MARK.
			dataBlock[7] = (byte)0x0F;

			//first writes the first sector in each fat
			for (int i=0; i < bpb.NumFATs; i++)
				writeSector(dataBlock, firstFatSector + i*numFatSectors);

			//initialize everything else with 0
			dataBlock[0] = 0;
			dataBlock[1] = 0;
			dataBlock[2] = 0;
			dataBlock[3] = 0;
			dataBlock[4] = 0;
			dataBlock[5] = 0;
			dataBlock[6] = 0;
			dataBlock[7] = 0;

			for (int j=0; j < bpb.NumFATs; j++)
				for (int i=firstFatSector + 1; i < firstFatSector + numFatSectors; i++)
					writeSector(dataBlock, i + j*numFatSectors);
		}
		
		//set the found bad clusters in the FAT
		for (int i = 0; i < defectSectorNumbers.size(); i++)
			setBadMark(((Long)defectSectorNumbers.get(i)).longValue());
		
		//must be done after the constructor because of checkdisk
		//initializeFat();
	}	//end constructor format


	/**
	 * Initialize the FAT, i.e. the free list of data. This method is not
	 * automatically called by constructors, because of the checkdisk() method.
	 * The checkdisk method needs ready initialized objects.
	 * @throws InitializationException in case the FAT couldn't be initialized.
	 */
	protected void initializeFat() throws InitializationException
	{
		sectorBuffer = new byte[bpb.BytsPerSec];
		long lastClusterNumber = bpb.getLastFatCluster();
		long fatContent;

		byte[] fat = null;

		if (fatType == FAT12)
			fat = readFAT();

		//check the CLN_SHUT_BIT and the HRD_ERR_BIT which indicates if we need a check/scan-disk.
		//This is only by FAT16 and FAT32 supported
		if (fatType == FAT16)
		{
			fat = readFAT();
			fatContent = getFatContent(1);
			if ((fatContent & HRD_ERR_BIT_MASK_FAT16) != HRD_ERR_BIT_MASK_FAT16)
			{
				//Volume is dirty. The file system driver encountered a disk I/O error
				//the last time the volume was mounted.
				//start checkdisk to scan for bad clusters.
				device.out.println("Start checkdisk because of HRD_ERR_BIT");
				checkdisk(fat, CHECK_BAD_CLUSTERS);
			}
			else if ((fatContent & CLN_SHUT_BIT_MASK_FAT16) != CLN_SHUT_BIT_MASK_FAT16)
			{	//Volume is dirty. The volume was not dismounted properly the last time.
				//start checkdisk
				device.out.println("Start checkdisk because of CLN_SHUT_BIT");
				checkdisk(fat);
			}
		}
		else if (fatType == FAT32)
		{
			//get the FSI
			fsi = new FSI();
			try
			{
				fsi.initializeFSI(readSector(bpb.getFSInfoSectorNumber()));
			}
			catch(WrongFATType e)
			{
				throw new InitializationException("Couldn't initialize FAT, because of: "+e);
			}
			catch(NotFSISector e)
			{
				throw new InitializationException("Couldn't initialize FAT, because of: "+e);
			}
			catch(Exception e)
			{
				//couldn't read the fsi, try the backup
				try
				{
					fsi.initializeFSI(readSector(bpb.getFSInfoSectorNumber() + bpb.BkBootSec));
				}
				catch(WrongFATType e1)
				{
					throw new InitializationException("Couldn't initialize FAT, because of: "+e1);
				}
				catch(NotFSISector e1)
				{
					throw new InitializationException("Couldn't initialze FAT, because of: "+e1);
				}
				catch(Exception e1)
				{
					throw new InitializationException("Couldn't initialze FAT, because of: "+e1);
				}
			}

			//since we read the FAT only if we need to run checkdisk
			//we set the fat number to 0
			fatNumber = 0;
			fatContent = getFatContent(1);
			if ((fatContent & HRD_ERR_BIT_MASK_FAT32) != HRD_ERR_BIT_MASK_FAT32)
			{
				//Volume is dirty. The file system driver encountered a disk I/O error
				//the last time the volume was mounted.
				//start checkdisk to scan for bad clusters.
				device.out.println("Start checkdisk because of HRD_ERR_BIT.");
				checkdisk(readFAT(), CHECK_BAD_CLUSTERS);
			}
			else if ((fatContent & CLN_SHUT_BIT_MASK_FAT32) != CLN_SHUT_BIT_MASK_FAT32)
			{	//Volume is dirty. The volume was not dismounted properly the last time
				//start checkdisk
				device.out.println("Start checkdisk because of CLN_SHUT_BIT.");
				checkdisk(readFAT());
			}
		}

		//scan FAT for free entries (only by FAT12 and FAT16)
		if (fatType == FAT12 || fatType == FAT16)
		{
			LinkedList freeClusters = new LinkedList();

			//the first two entries of the FAT are reserved
			for (int i=2; i < lastClusterNumber; i++)
			{
				//if entry is free put it on the free cluster list
				if (getFatContent(i, fat) == 0)
					freeClusters.addLast(new Long(i));
			}
			freeMemoryManagement = new FreeMemoryManagement(freeClusters);
		}
		else
			freeMemoryManagement = new FreeMemoryManagement();

		setCLN_SHUT_BITToError();		//set error bits, this is only done if it's necessary
	}	//end initializeFat()


	/**
	 * Reinitialize the FAT structure only the entries that are marked
	 * as bad will remain, all others will be set to zero.
	 */
	protected void fastFormat()
	{
		LinkedList freeClusters = new LinkedList();
		long lastFatCluster = bpb.getLastFatCluster();
		long fatContent;

		for (long i=2; i < lastFatCluster; i++)
		{
			fatContent = getFatContent(i);
			if (!isBadMark(fatContent))
			{
				freeClusters.add(new Long(i));
				setFatContent(i, 0);
			}
		}

		if (fatType != FAT.FAT32)
			freeMemoryManagement = new FreeMemoryManagement(freeClusters);
		else
		{
			freeMemoryManagement = new FreeMemoryManagement();
			try
			{
				fsi = new FSI();
				fsi.initializeFSI(fsi.getFSI());
				fsi.setNextFree(2);
				fsi.setFreeCount(freeClusters.size());
				//write the new formatted FileSystemInfo at the specified sector number to disk
				writeSector(fsi.getUsedFSI(), bpb.getFSInfoSectorNumber());
			}
			catch (WrongFATType e)
			{
				device.out.println(e);
			}
			catch (NotFSISector ee)
			{
				device.out.println(ee);
			}
		}

		setCLN_SHUT_BITToError();
	}	//end fastFormat()


	/**
	 * Set the CLN_SHUT_BIT to error. This is done only, if
	 * the fat type is FAT16 or FAT32.
	 */
	private void setCLN_SHUT_BITToError()
	{
		//set the CLN_SHUT_BIT to error, this is necessary because it might be
		//that the PC lose his energy during an FAT-operation which cause the
		//FAT to be inconsistent.
		//In case the user unmount the FATDevice the CLN_SHIT_BIT is set back
		//to no error, indicating that the FAT is consistent.
		if (fatType == FAT16)
		{
			//IMPORTANT: the made change must be written to disk, it is not enough
			//if the made change exist only in RAM.
			sectorBuffer = readSector(bpb.RsvdSecCnt + fatNumber*FATSz);
			long fatContent = getFatContent(1, sectorBuffer);
			fatContent &= 0x7FFF;
			setFatContent(1, fatContent, sectorBuffer);
			device.rawAccess.write(sectorBuffer, bpb.RsvdSecCnt + fatNumber*FATSz);
		}
		else if (fatType == FAT32)
		{
			//IMPORTANT: the made change must be written to disk, it is not enough
			//if the made change exist only in RAM.
			sectorBuffer = readSector(bpb.RsvdSecCnt + fatNumber*FATSz);
			long fatContent = getFatContent(1, sectorBuffer);
			fatContent &= 0xF7FFFFFF;
			setFatContent(1, fatContent);
			device.rawAccess.write(sectorBuffer, bpb.RsvdSecCnt + fatNumber*FATSz);
		}
	}	//end setCLN_SHUT_BITToError()


	/**
	 * If one or more sectors couldn't be read/write properly
	 * the HRD_ERR_BIT should be set to error, so that the next
	 * time the device is booted a scandisk will be made.
	 * This method sets the HRD_ERR_BIT to error.
	 */
	protected void setHRD_ERR_BITToError()
	{
		if (fatType == FAT16)
		{
			sectorBuffer = readSector(bpb.RsvdSecCnt + fatNumber*FATSz);
			long fatContent = getFatContent(1, sectorBuffer);
			fatContent &= 0xBFFF;
			setFatContent(1, fatContent, sectorBuffer);
			device.rawAccess.write(sectorBuffer, bpb.RsvdSecCnt + fatNumber*FATSz);
		}
		if (fatType == FAT32)
		{
			sectorBuffer = readSector(bpb.RsvdSecCnt + fatNumber*FATSz);
			long fatContent = getFatContent(1, sectorBuffer);
			fatContent &= 0xFBFFFFFF;
			setFatContent(1, fatContent, sectorBuffer);
			device.rawAccess.write(sectorBuffer, bpb.RsvdSecCnt + fatNumber*FATSz);
		}
	}


	/**
	 * Check if the given content equals a BAD_CLUSTER_X mark.
	 * @param content a value from the FAT structure.
	 * @return true in case content equals a BAD_CLUSTER_X mark; false otherwise.
	 */
	public boolean isBadMark(long content)
	{
		if (fatType == FAT.FAT12)
			return content == BAD_CLUSTER_FAT12;
		else if (fatType == FAT.FAT16)
			return content == BAD_CLUSTER_FAT16;
		else if (fatType == FAT.FAT32)
			return content == BAD_CLUSTER_FAT32;
		else
			return false;
	}	//end isBadMark

	/**
	 * Return the complete FAT as byte array or null in case the FAT and all copies
	 * of the FAT couldn't be read. The method will also determine the fat number
	 * that should be used and reports defect FAT-sectors.
	 * @return the FAT as byte array if readable otherwise null.
	 */
	private byte[] readFAT()
	{
		int numFatSectors = (int)bpb.getNumFatSectors();
		int firstFatSecNum = bpb.RsvdSecCnt;
		byte[] fat = new byte[numFatSectors * bpb.BytsPerSec];
				
		//try to read the fat, if one fat is defect try the next one.
		for (fatNumber=0; fatNumber < bpb.NumFATs; fatNumber++)
		{
			for (int sectorNumber=firstFatSecNum; sectorNumber < firstFatSecNum + numFatSectors; sectorNumber++)
			{
				try
				{
					sectorBuffer = readSector(sectorNumber + fatNumber*FATSz);
					System.arraycopy(sectorBuffer, 0, fat, (sectorNumber - firstFatSecNum)*bpb.BytsPerSec, bpb.BytsPerSec);
				}
				catch (Exception e)
				{
					device.out.println("FAT number "+fatNumber+" is defect, because of one or more defect sector(s)!");
					if (!defectSectorNumbers.contains(new Long(sectorNumber)))
						defectSectorNumbers.add(new Long(sectorNumber));
				}
			}
			//we could read one complete FAT, so we are done.
			return fat;
		}
		
		//we could not read one FAT
		device.out.println("\nFAILURE: Sorry but there is no FAT readable. The usage of this device is not given.\n");
		return null;
	}	//end readFAT()
	
	
	/**
	 * Unmount the fat. This method is called by the file system, never
	 * call this method directly.
	 * @see xxl.core.io.fat.FileSystem#shutDown()
	 * @see xxl.core.io.fat.FileSystem#unmount(java.lang.String deviceName)
	 */
	protected void unmount()
	{
		//writes everything ok flag in FAT's. Only needed by FAT16 and FAT32
		long fatContent = getFatContent(1);
		if (fatType == FAT16)
			if ((fatContent & HRD_ERR_BIT_MASK_FAT16) == HRD_ERR_BIT_MASK_FAT16)
				setFatEocMark(1);	//there was no read/write error -> mark fat as OK
		
		if (fatType == FAT32)
			if ((fatContent & HRD_ERR_BIT_MASK_FAT32) == HRD_ERR_BIT_MASK_FAT32)
				setFatEocMark(1);	//there was no read/write error -> mark fat as OK
				
		//write the used FAT to all other not used FAT's
		//but only if it's not FAT32 or if FAT32 is not mirrored at runtime
		boolean fat32IsMirrored = false;
		try
		{
			fat32IsMirrored = bpb.isFAT32Mirrored();
		}
		catch (WrongFATType e)
		{
		}
		if (fatType != FAT32 || (fatType == FAT32 && !fat32IsMirrored))
		{
			long firstFatSecNum = bpb.RsvdSecCnt;
			long numFatSectors = (int)bpb.getNumFatSectors();
			long startSectorNumber = firstFatSecNum + fatNumber*FATSz;
			
			for (long sectorNumber = 0; sectorNumber < numFatSectors; sectorNumber++)
			{
				sectorBuffer = readSector(sectorNumber + startSectorNumber);
				for (int tmpFatNumber=0; tmpFatNumber < bpb.NumFATs; tmpFatNumber++)	
				{
					if (tmpFatNumber == fatNumber)
						continue;	//don't copy the FAT to itself
					try
					{	//write the actual sector to the not used FAT fatNumber
						writeSector(sectorBuffer, firstFatSecNum + tmpFatNumber*FATSz + sectorNumber);
					}
					catch (Exception e)
					{
						device.out.println(e);
						device.out.println("In unmount of class FAT. The copy of the used FAT to FAT-Number "+tmpFatNumber+" to sector "+(firstFatSecNum + tmpFatNumber*FATSz + sectorNumber)+" failed, because of an error.");
					}
				}	//end for all fatNumbers
			}		//end for all sectors
		}
		
		//in case of FAT32 write the maybe changed FSI
		if (fatType == FAT32)
		{
			sectorBuffer = fsi.getUsedFSI();
			try
			{
				device.writeSector(sectorBuffer, bpb.getFSInfoSectorNumber());
				//write also the backup
				device.writeSector(sectorBuffer, bpb.getFSInfoSectorNumber() + bpb.BkBootSec);
			}
			catch (WrongFATType e)
			{
				device.out.println(e);
				device.out.println("Either FSI-sector or FSI-backup-sector was not written.");
			}
		}
	}	//end unmount()
	

	/**
	 * Return the sector given by sectorNumber.
	 * 
	 * @param sectorNumber the number of the sector to be read.
	 * @return the sector as byte array.
	 */
	private byte[] readSector(long sectorNumber)
	{
		byte b[] = new byte[512];
		if (!device.readSector(b, sectorNumber))
			return null;
		else
			return b;
	}	//end readSector(long sectorNumber)


	/**
	 * Write one sector to disk.
	 * @param sector the data to write.
	 * @param sectorNumber the number where the sector should be written.
	 */
	private void writeSector(byte[] sector, long sectorNumber)
	{
		device.writeSector(sector, sectorNumber);
	}	//end writeSector(byte[] sector, long sectorNumber)


	/**
	 * This method is the same as getFatContent(clusterNumber) except that 
	 * the whole fat byte array is known. It's used by the initialization
	 * method to speed up the boot process.
	 * @param clusterNumber the number of cluster.
	 * @param fatBuffer the FAT as byte array.
	 * @return the value at the FAT indexed by clusterNumber.
	 */
	protected long getFatContent(long clusterNumber, byte[] fatBuffer)
	{			
		int thisFATEntOffset;
		if (fatType == FAT12)
			thisFATEntOffset = (int)(clusterNumber + (clusterNumber >> 1));
		else if (fatType == FAT16)
			thisFATEntOffset = (int)(clusterNumber << 1);
		else
			thisFATEntOffset = (int)(clusterNumber << 2);
			
		long fatContent;	
				
		if (fatType == FAT12)
		{
			if ((clusterNumber & 0x0001) == 1)
			{
				fatContent = ByteArrayConversionsLittleEndian.convInt(
					fatBuffer[thisFATEntOffset],
					fatBuffer[thisFATEntOffset+1]
				);
				fatContent >>= 4;
			}
			else
			{
				fatContent = ByteArrayConversionsLittleEndian.convInt(
					fatBuffer[thisFATEntOffset],
					fatBuffer[thisFATEntOffset+1]
				);
				fatContent = fatContent & 0x0FFF;
			}
		}
	 	else if (fatType == FAT16)
	 	{
	 		fatContent = ByteArrayConversionsLittleEndian.convInt(
	 			fatBuffer[thisFATEntOffset],
				fatBuffer[thisFATEntOffset+1]
			);
		}
		else
		{
			fatContent = ByteArrayConversionsLittleEndian.convLong(
				fatBuffer[thisFATEntOffset],
				fatBuffer[thisFATEntOffset+1],
				fatBuffer[thisFATEntOffset+2],
				fatBuffer[thisFATEntOffset+3]
			);
			fatContent &= 0x000000000FFFFFFF;
		}
			
		return fatContent;
	}	//end getFatContent(clusterNumber, byte[] fat)
	

	/**
	 * Get the FAT content for the given clusterNumber.
	 * @param clusterNumber is the index in the FAT
	 * @return the FAT content for the given clusterNumber.
	 */
	public long getFatContent(long clusterNumber)
	{	
		return getFatContent(clusterNumber, fatNumber);
	}	//end getFatContent(long clusterNumber)
		
	
	/**
	 * Get the FAT content for the given clusterNumber from the given fatNumber.
	 * @param clusterNumber is the index in the FAT
	 * @param fatNumber is the number of the FAT that is used.
	 * @return the FAT content for the given clusterNumber and fatNumber.
	 */
	protected long getFatContent(long clusterNumber, int fatNumber)
	{	
		long fatOffset = 0;
		long fatContent = 0;
		
		if (fatType == FAT12)
			fatOffset = clusterNumber + (clusterNumber >> 1);
		else if (fatType == FAT16)
			fatOffset = clusterNumber << 1;
		else if (fatType == FAT32)
			fatOffset = clusterNumber << 2;
		
		//calculate the sector number of the FAT sector that contains the entry
		//for cluster clusterNumber in the first FAT. If the second FAT is needed
		//than add FATSz to thisFATSecNum and 2*FATSz for the third FAT.
		long thisFATSecNum = bpb.RsvdSecCnt + (fatOffset / bpb.BytsPerSec) + FATSz*fatNumber;
		int thisFATEntOffset = (int)(fatOffset % bpb.BytsPerSec);
		
		//read the sector of the FAT that contains the FAT-Entry
		sectorBuffer = readSector(thisFATSecNum);
		
		//only for FAT12 needed
		//check for sector boundary
		if (thisFATEntOffset == (bpb.BytsPerSec - 1))
		{
			/*
			This cluster access spans a sector boundary in the FAT
			There are a number of strategies to handling this. The
			easiest is to always load FAT sectors into memory
			in pairs if the volume is FAT12 (if you want to load
			FAT sector N, you also load FAT sector N+1 immediately
			following it in memory unless sector N is the last FAT
			sector).
			*/
			byte[] sectorBuffer2 = new byte[bpb.BytsPerSec];
			sectorBuffer2 = readSector(thisFATSecNum+1);
			
			if ((clusterNumber & 0x0001) == 1)
			{
				fatContent = ByteArrayConversionsLittleEndian.convInt(
					sectorBuffer[thisFATEntOffset],
					sectorBuffer2[0]
				);
				fatContent >>= 4;
			}
			else
			{
				fatContent = ByteArrayConversionsLittleEndian.convInt(
					sectorBuffer[thisFATEntOffset],
					sectorBuffer2[0]
				);
				fatContent = fatContent & 0x0FFF;
			}
			
			return fatContent;
		}	//end if boundary case of FAT12
						
		
		if (fatType == FAT12)
		{		
			if ((clusterNumber & 0x0001) == 1)
			{
				fatContent = ByteArrayConversionsLittleEndian.convInt(
					sectorBuffer[thisFATEntOffset],
					sectorBuffer[thisFATEntOffset+1]
				);
				fatContent >>= 4;
			}
			else
			{
				fatContent = ByteArrayConversionsLittleEndian.convInt(
					sectorBuffer[thisFATEntOffset],
					sectorBuffer[thisFATEntOffset+1]);
				fatContent = fatContent & 0x0FFF;
			}
		}
	 	else if (fatType == FAT16)
	 	{
	 		fatContent = ByteArrayConversionsLittleEndian.convInt(
	 			sectorBuffer[thisFATEntOffset],
				sectorBuffer[thisFATEntOffset+1]
			);
		}
		else //fatType == FAT32
		{
			fatContent = ByteArrayConversionsLittleEndian.convLong(
				sectorBuffer[thisFATEntOffset],
				sectorBuffer[thisFATEntOffset+1],
				sectorBuffer[thisFATEntOffset+2],
				sectorBuffer[thisFATEntOffset+3]
			);
			fatContent &= 0x000000000FFFFFFF;
		}
			
		return fatContent;
	}	//end getFatContent(clusterNumber)
	
	
	/**
	 * Set the entry of FAT given by clusterNumber with EOC_MARK.
	 * @param clusterNumber the index of the FAT that should set with the
	 * EOC_MARK.
	 */
	protected void setFatEocMark(long clusterNumber)
	{
		if (fatType == FAT12)
			setFatContent(clusterNumber, EOC_MARK12);
		else if (fatType == FAT16)
			setFatContent(clusterNumber, EOC_MARK16);
		else if (fatType == FAT32)
			setFatContent(clusterNumber, EOC_MARK32);
		else
			device.out.println("Error in setFatEocMark: wrong fatType");	
	}	//end setFatEocMark(clusterNumber)
	
	
	/**
	 * Mark the index given by clusterNumber as bad. From now on the
	 * cluster number associated with this index is not usable.
	 * @param clusterNumber the number of cluster.
	 */
	protected void setBadMark(long clusterNumber)
	{
		if (fatType == FAT12)
			setFatContent(clusterNumber, BAD_CLUSTER_FAT12);
		else if (fatType == FAT16)
			setFatContent(clusterNumber, BAD_CLUSTER_FAT16);
		else if (fatType == FAT32)
			setFatContent(clusterNumber, BAD_CLUSTER_FAT32);
		else
			device.out.println("Error in setBadMark: wrong fatType");	
	}	//end setBadMark(long clusterNumber)

	
	/**
	 * Set the given content of the FAT at the given cluster number.
	 * @param clusterNumber is the cluster number at which the content is set.
	 * @param content is data to store in the FAT at clusterNumber.
	 */
	protected void setFatContent(long clusterNumber, long content)
	{
		try
		{
			if (fatType != FAT.FAT32 || !bpb.isFAT32Mirrored())
				//FAT32 is not mirrored or it's FAT12 or FAT16
				setFatContent(clusterNumber, content, fatNumber);
			else 
			{	//FAT32 is mirrored at runtime
				for (int i=0; i < bpb.NumFATs; i++)
					setFatContent(clusterNumber, content, i);
			}
		}
		catch (WrongFATType e)
		{
			device.out.println(e);
		}
	}	//end setFatContent(long clusterNumber, long content)

	
	/**
	 * Set the given content of the FAT at the given cluster number at the
	 * given fatNumber.
	 * @param clusterNumber is the cluster number at which the content is set.
	 * @param content is data to store in the FAT at clusterNumber.
	 * @param fatNumber the number of the FAT.
	 */
	private void setFatContent(long clusterNumber, long content, int fatNumber)
	{
		long fatOffset = 0;

		
		if (fatType == FAT12)
			fatOffset = clusterNumber + (clusterNumber >> 1);
		else if (fatType == FAT16)
			fatOffset = clusterNumber << 1;
		else if (fatType == FAT32)
			fatOffset = clusterNumber << 2;
		
		//calculate the sector number of the FAT sector that contains the entry
		//for cluster clusterNumber in the first FAT. If the second FAT is needed
		//than add FATSz to thisFATSecNum and 2*FATSz for the third FAT.
		long thisFATSecNum = bpb.RsvdSecCnt + (fatOffset / bpb.BytsPerSec) + FATSz*fatNumber;
		int thisFATEntOffset = (int)(fatOffset % bpb.BytsPerSec);
		
		//read the sector of the FAT that contains the FAT-Entry
		sectorBuffer = readSector(thisFATSecNum);

		//only for FAT12 needed
		//check for sector boundary
		if (thisFATEntOffset == (bpb.BytsPerSec - 1))
		{
			/*
			This cluster access spans a sector boundary in the FAT
			There are a number of strategies to handling this. The
			easiest is to always load FAT sectors into memory
			in pairs if the volume is FAT12 (if you want to load
			FAT sector N, you also load FAT sector N+1 immediately
			following it in memory unless sector N is the last FAT
			sector). It is assumed that this is the strategy used here
			which makes this if test for a sector boundary span
			unnecessary.
			*/
			byte[] sectorBuffer2 = new byte[bpb.BytsPerSec];
			sectorBuffer2 = readSector(thisFATSecNum + 1);
			if ((clusterNumber & 0x0001) == 1)
			{
				content <<= 4;
				//free 12 bits for the new entry
				sectorBuffer2[0] &= 0x00;
				sectorBuffer[thisFATEntOffset] &=0x0F;
			}
			else
			{
				//only the low 12 bits are important
				content &= 0x0000000000000FFF;
				//free the low 12 bits for the new entry
				sectorBuffer2[0] &= 0xF0;
				sectorBuffer[thisFATEntOffset] &= 0x00;
			}
			//store the new entry 
			sectorBuffer2[0] |= ((content & 0x000000000000FF00) >> 8);
			sectorBuffer[thisFATEntOffset] |= (content & 0x00000000000000FF);
			
			//save changes
			writeSector(sectorBuffer, thisFATSecNum);
			writeSector(sectorBuffer2, thisFATSecNum + 1);
			
			//we are done
			return;
		}	//end boundary case for FAT12
				
		if (fatType == FAT12)
		{
			if ((clusterNumber & 0x0001) == 1)
			{
				content <<= 4;
				//free 12 bits for the new entry
				sectorBuffer[thisFATEntOffset+1] &= 0x00;
				sectorBuffer[thisFATEntOffset] &=0x0F;
			}
			else
			{
				//only the low 12 bits are important
				content &= 0x0000000000000FFF;
				//free the low 12 bits for the new entry
				sectorBuffer[thisFATEntOffset+1] &= 0xF0;
				sectorBuffer[thisFATEntOffset] &= 0x00;
			}
			//store the new entry 
			sectorBuffer[thisFATEntOffset+1] |= ((content & 0x000000000000FF00) >> 8);
			sectorBuffer[thisFATEntOffset] |= (content & 0x00000000000000FF);
		}
		else if (fatType == FAT16)
		{
			
			sectorBuffer[thisFATEntOffset+1] = (byte)(content >> 8);
			sectorBuffer[thisFATEntOffset] = (byte)(content);
		}
		else if (fatType == FAT32)
		{
			content &= 0x0FFFFFFF;	//clear the 4 reserved bits
			
			//clear the existing entry except the reserved 4 bits
			sectorBuffer[thisFATEntOffset+3] &= 0xF0;
			sectorBuffer[thisFATEntOffset+2] = 0;
			sectorBuffer[thisFATEntOffset+1] = 0;
			sectorBuffer[thisFATEntOffset] = 0;
			//write the new entry to the fat
			sectorBuffer[thisFATEntOffset+3] |= (byte)(content >> 24);
			sectorBuffer[thisFATEntOffset+2] |= (byte)(content >> 16);
			sectorBuffer[thisFATEntOffset+1] |= (byte)(content >> 8);
			sectorBuffer[thisFATEntOffset] |= (byte)content;
		}

		writeSector(sectorBuffer, thisFATSecNum);
	}	//end setFatContent(long clusterNumber, long content, int fatNumber)


	/**
	 * Set the given content of the FAT at the given cluster number to the
	 * given fatBuffer. This method is used by checkdisk to speed up the process.
	 * @param clusterNumber is the cluster number at which the content is set.
	 * @param content is data to store in the FAT at clusterNumber.
	 * @param fatBuffer is the FAT where the content is stored.
	 */
	private void setFatContent(long clusterNumber, long content, byte[] fatBuffer)
	{			
		int thisFATEntOffset;
		if (fatType == FAT12)
			thisFATEntOffset = (int)(clusterNumber + (clusterNumber >> 1));
		else if (fatType == FAT16)
			thisFATEntOffset = (int)(clusterNumber << 1);
		else
			thisFATEntOffset = (int)(clusterNumber << 2);
				
		if (fatType == FAT12)
		{
			if ((clusterNumber & 0x0001) == 1)
			{
				content <<= 4;
				//free 12 bits for the new entry
				fatBuffer[thisFATEntOffset+1] &= 0x00;
				fatBuffer[thisFATEntOffset] &=0x0F;
			}
			else
			{
				//only the low 12 bits are important
				content &= 0x0000000000000FFF;
				//free the low 12 bits for the new entry
				fatBuffer[thisFATEntOffset+1] &= 0xF0;
				fatBuffer[thisFATEntOffset] &= 0x00;
			}
			//store the new entry 
			fatBuffer[thisFATEntOffset+1] |= ((content & 0x000000000000FF00) >> 8);
			fatBuffer[thisFATEntOffset] |= (content & 0x00000000000000FF);
		}
		else if (fatType == FAT16)
		{			
			fatBuffer[thisFATEntOffset+1] = (byte)(content >> 8);
			fatBuffer[thisFATEntOffset] = (byte)(content);
		}
		else if (fatType == FAT32)
		{
			content &= 0x0FFFFFFF;	//clear the 4 reserved bits
			
			//clear the existing entry except the reserved 4 bits
			fatBuffer[thisFATEntOffset+3] &= 0xF0;
			fatBuffer[thisFATEntOffset+2] = 0;
			fatBuffer[thisFATEntOffset+1] = 0;
			fatBuffer[thisFATEntOffset] = 0;
			//write the new entry to the fat
			fatBuffer[thisFATEntOffset+3] |= (byte)(content >> 24);
			fatBuffer[thisFATEntOffset+2] |= (byte)(content >> 16);
			fatBuffer[thisFATEntOffset+1] |= (byte)(content >> 8);
			fatBuffer[thisFATEntOffset] |= (byte)content;
		}
	}	//end setFatContent(clusterNumber, long content, byte[] fat)
	
	
	/**
	 * Mark the sector given by sectorNumber as no longer usable.
	 * @param sectorNumber the number of sector.
	 */
	protected void markSectorAsBad(long sectorNumber)
	{
		setHRD_ERR_BITToError();
		if (sectorNumber < bpb.getFirstDataSector())
			//the sector number belongs to a stored data-structure
			defectSectorNumbers.add(new Long(sectorNumber));
		else
		{
			//the sector number belongs to the data-sectors
			long clusterNumber = MyMath.roundUp((sectorNumber-bpb.getFirstDataSector())/(float)bpb.SecPerClus);
			setBadMark(clusterNumber);
		}
	}	//end markSectorAsBad(long sectorNumber)
	
	
	/**
	 * Mark the cluster given by clusterNumber as no longer usable.
	 * @param clusterNumber number of cluster.
	 */
	protected void markClusterAsBad(long clusterNumber)
	{
		setBadMark(clusterNumber);
	}	//end markClusterAsBad(long clusterNumber)
	
	
	/**
	 * Add a list of free cluster numbers to the free memory management. All cluster
	 * numbers will be marked as free.
	 * @param freeClusterList a list of Long objects.
	 */
	public void addFreeClusters(List freeClusterList)
	{
		//mark all clusters of the list as free (in the fat)
		for (int i=0; i < freeClusterList.size(); i++)
		{
			setFatContent(((Long)freeClusterList.get(i)).longValue(), 0);
		}

		//add the free clusters to the free memory management
		freeMemoryManagement.addFreeClusters(freeClusterList);
	}	//end addFreeClusters(List freeClusterList)


	/**
	 * Mark cluster numbers as free. The cluster number startClusterNumber gets the EOC_MARK
	 * all next numOfClusters clusters belonging to the cluster chain will be marked as free.
	 * @param startClusterNumber the cluster number to start.
	 */
	protected void addFreeClustersMarkFirstAsEOC(long startClusterNumber)
	{
		if (startClusterNumber < 2 || getFatContent(startClusterNumber) == 0)
			return;	//nothing to do
			
		//create the list of free clusters and mark them as free.
		List freeClusters = new LinkedList();
		long actualClusterNumber = getFatContent(startClusterNumber);
		long nextClusterNumber;
		do
		{
			nextClusterNumber = getFatContent(actualClusterNumber);
			setFatContent(actualClusterNumber, 0);
			freeClusters.add(new Long(actualClusterNumber));
			if (!isLastCluster(nextClusterNumber))
				actualClusterNumber = nextClusterNumber;
			else
				break;
		}while (true);
		
		//add the free clusters to the free memory management
		freeMemoryManagement.addFreeClusters(freeClusters);
		
		//set EOC_MARK to startClusterNumber
		setFatEocMark(startClusterNumber);
	}	//end addFreeClustersMarkFirstAsEOC(long startClusterNumber)
	
	
	/**
	 * Mark cluster numbers as free beginning with the given startClusterNumber.
	 * @param startClusterNumber the cluster number to start.
	 */
	protected void addFreeClusters(long startClusterNumber)
	{
		if (startClusterNumber < 2 || getFatContent(startClusterNumber) == 0)
			return;	//nothing to do
			
		//create the list of free clusters and mark them as free.
		List freeClusters = new LinkedList();
		long actualClusterNumber = startClusterNumber;
		long nextClusterNumber;
		freeClusters.add(new Long(startClusterNumber));
		do
		{
			nextClusterNumber = getFatContent(actualClusterNumber);
			setFatContent(actualClusterNumber, 0);
			freeClusters.add(new Long(actualClusterNumber));
			if (!isLastCluster(nextClusterNumber))
				actualClusterNumber = nextClusterNumber;
			else
				break;
		}
		while (true);
		
		//add the free clusters to the free memory management
		freeMemoryManagement.addFreeClusters(freeClusters);
	}	//end addFreeClusters(long startClusterNumber)
	
	
	/**
	 * Get a list of free cluster numbers as entries with a size of
	 * numOfFreeClusters. All allocated cluster
	 * numbers are automatically stored in the fat.
	 * In case there is not enough memory an exception is thrown.
	 * @param numOfFreeClusters is the number of free clusters.
	 * @return list with free cluster numbers as entries.
	 * @throws NotEnoughMemory in case there are not enough free clusters
	 * to support the query.
	 */
	public List getFreeClusters(long numOfFreeClusters) throws NotEnoughMemory
	{
		return getFreeClusters(numOfFreeClusters, 0);
	}	//end getFreeClusters(long numOfFreeClusters)


	/**
	 * Get a list of free cluster numbers as entries with a size of
	 * numOfFreeClusters. All allocated cluster
	 * numbers are automatically stored in the fat started at lastClusterNumber.
	 * In case there is not enough memory an exception is thrown.
	 * @param numOfFreeClusters the number of free clusters.
	 * @param lastClusterNumber the last cluster number.
	 * @return list with free cluster numbers as entries.
	 * @throws NotEnoughMemory if there is not enough memory to support the query.
	 */
	public List getFreeClusters(long numOfFreeClusters, long lastClusterNumber) throws NotEnoughMemory
	{
		List list = freeMemoryManagement.getFreeClusters(numOfFreeClusters);
		if (list.isEmpty())
			return list;
			
		Iterator it = list.iterator();
		long clusterNumber, content;
		
		if (lastClusterNumber == 0)	//there is no lastClusterNumber
		{			
			clusterNumber = ((Long)it.next()).longValue();
		}
		else
		{
			//override the content of the fat entry indexed by last cluster number
			//with the next clusterNumber of the freeClusterList
			content = ((Long)it.next()).longValue();
			setFatContent(lastClusterNumber, content);
			clusterNumber = content;
		}
		
		while(it.hasNext())
		{
			content = ((Long)it.next()).longValue();
			setFatContent(clusterNumber, content);
			clusterNumber = content;
		}
		
		setFatEocMark(clusterNumber);
		
		return list;
	}	//end getFreeClusters(long numOfFreeClusters, long lastClusterNumber)

	
	/**
	 * Determine if the given clusterNumber points to an EOC_MARK,
	 * this depends on the fatType.
	 * @param clusterNumber the number of the cluster.
	 * @return true if the given clusterNumber equals an EOC_MARK otherwise false.
	 */
	public boolean isLastCluster(long clusterNumber)
	{
		if (fatType == FAT12)
			return clusterNumber == EOC_MARK12;
		else if (fatType == FAT16)
			return clusterNumber == EOC_MARK16;
		else
			return clusterNumber == EOC_MARK32;
	}	//end isLastCluster(long clusterNumber)


	/**
	 * Return the type of FAT. The possible FAT types are FAT12, FAT16, or FAT32.
	 * @return the fat type.
	 */
	public byte getFatType()
	{
		return fatType;
	}	//end getFatType()
	
	
	/**
	 * Check the cluster chains in the FAT and repair them. 
	 * @param fat the FAT as byte array.
	 * @return true if the FAT could be repaired otherwise false.
	 */
	protected boolean checkdisk(byte[] fat)
	{
		if (fat == null)
			return false;
		try
		{
			DIR directory = new DIR(device, bpb, bpb.getFatType());
			byte[] directoryEntry = null;
			long fileLength = 0;
			long clusterNumber;
			long numOfClusters = 0;	//the number of clusters that are really allocated for the file
			long clusterNumberTmp = 0;
			byte[] usedClusters = new byte[fat.length];
			boolean fatCorrected = true;	//is set to false if we get a non removable error.
	
			DirectoryEntryInformation dirEntInf;
			EntriesFilterIterator it;
			if (bpb.getFatType() == FAT.FAT12)
				it = directory.getEntries(DIR.LIST_FAT12_ENTRY);
			else if (bpb.getFatType() == FAT.FAT16)
				it = directory.getEntries(DIR.LIST_FAT16_ENTRY);
			else
				it = directory.getEntries(DIR.LIST_FAT32_ENTRY);
			
			while (it.hasNext())
			{
				dirEntInf = (DirectoryEntryInformation)it.next();
				directoryEntry = dirEntInf.directoryEntry;
				clusterNumber = DIR.getClusterNumber(directoryEntry);
				fileLength = DIR.getFileLength(directoryEntry);
				
				//if no cluster allocated but file length not zero we found an error.
				if (clusterNumber == 0 && fileLength != 0)
				{
					device.out.println("Found file that has a length not zero but no cluster allocated. Set its length to zero.");
					DIR.setFileLength(dirEntInf.directoryEntry, 0);
					directory.writeDirectoryEntry(dirEntInf);
					numOfClusters = 0;
					continue;
				}
			
				if (clusterNumber != 0 && fileLength == 0 && DIR.isFile(dirEntInf.directoryEntry))
				{
					device.out.println("Found file that has length zero but points to a cluster. Set its cluster number to zero.");
					DIR.setClusterNumber(dirEntInf.directoryEntry, 0L);
					directory.writeDirectoryEntry(dirEntInf);
					numOfClusters = 0;
					continue;
				}
			
				//follow cluster chain and mark each used FAT-entry and 
				//count the number of allocated clusters
				do
				{						
					//must checked because it is possible that we have a
					//corrupted directory entry
					if (clusterNumber == 0 || isLastCluster(clusterNumber))
						break;
						
					if (getFatContent(clusterNumber, usedClusters) != 0)
					{
						device.out.println("Two files share one fat entry!");
					}
					
					setFatContent(clusterNumber, getFatContent(clusterNumber, fat), usedClusters);
					clusterNumber = getFatContent(clusterNumber, fat);
					
					if (clusterNumber != 0)
						numOfClusters++;
				}
				while(!isLastCluster(clusterNumber));
				
				//check if the length of the file matches the number of allocated clusters
				long numOfUsedClustersTmp = 0;
				boolean errorCorrected = true;
				if (fileLength > numOfClusters*bpb.BytsPerSec*bpb.SecPerClus)
				{
					errorCorrected = false;
					device.out.println("File length is bigger than the allocated clusters in the FAT!");
					
					//check if the second fat contains the correct cluster chain.
					long content1, content2;
					for (int fatNumberTmp = 0; fatNumberTmp < bpb.NumFATs; fatNumberTmp++)
					{
						numOfUsedClustersTmp = 0;
						clusterNumberTmp = DIR.getClusterNumber(directoryEntry);
						
						if (fatNumberTmp == fatNumber)
							continue;	//we use this fatNumber already
						boolean fatSharing = false;		//is true if a fat entry is shared by more than one file.
						do
						{							
							content1 = getFatContent(clusterNumberTmp, fat);
							content2 = getFatContent(clusterNumberTmp, fatNumberTmp);
												
							if (content1 != content2 && content2 != 0)
							{
								if (getFatContent(clusterNumberTmp, usedClusters) != 0)
								{
									device.out.println("One or more FAT entries are used by different files. Couldn't correct the FAT.");
									fatSharing = true;
									break;
								}
								setFatContent(clusterNumberTmp, content2, usedClusters);
							}
							clusterNumberTmp = content2;
							
							if (content1 != 0 || content2 != 0)
								numOfUsedClustersTmp++;
						}
						while (!isLastCluster(content2) && content2 != 0);

						long rightLength = MyMath.roundUp((double)fileLength / (bpb.BytsPerSec*bpb.SecPerClus));
						if (numOfUsedClustersTmp == rightLength && !fatSharing)
						{
							errorCorrected = true;
							break;	//allright we correct this error
						}
					}	//end for fatNumberTmp
				}	//end fileLength is bigger than allocated cluster chain

				if (!errorCorrected)
				{
					//the file length was bigger than the allocated clusters
					//and we couldn't correct this failure
					//therefore we set the file length to the really used space
					//set file length to numOfUsedClustersTmp
					fatCorrected = false;
					DIR.setFileLength(dirEntInf.directoryEntry, numOfUsedClustersTmp*bpb.SecPerClus*bpb.BytsPerSec);
					directory.writeDirectoryEntry(dirEntInf);
				}

				//reset numOfClusters
				numOfClusters = 0;
			}

			//write the new FAT to disk
			//first set the first two FAT-entries
			if (fatType == FAT12)
			{
				setFatContent(0, (byte)bpb.Media + 0x0F, usedClusters);
				setFatContent(1, EOC_MARK12, usedClusters);
			}
			else if (fatType == FAT16)
			{
				setFatContent(0, (byte)bpb.Media + 0xFF00, usedClusters);
				//at the beginning the CLN_SHUT_BIT is set to error
				setFatContent(1, (EOC_MARK16 & 0x7FFF), usedClusters);
			}
			else
			{
				setFatContent(0, (byte)bpb.Media + 0x0FFFFF00, usedClusters);
				//at the beginning the CLN_SHUT_BIT is set to error
				setFatContent(1, (EOC_MARK32 & 0xF7FFFFFF), usedClusters);
			}
			//second the copy and write process
			int firstFatSecNum = bpb.RsvdSecCnt;
			for (int fatNumberTmp=0; fatNumberTmp < bpb.NumFATs; fatNumberTmp++)
				for (int numFatSectors=0; numFatSectors < bpb.getNumFatSectors(); numFatSectors++)
				{
					//copy usedClusters (contains the corrected FAT) to fat
					System.arraycopy(usedClusters, numFatSectors*bpb.BytsPerSec, fat, numFatSectors*bpb.BytsPerSec, bpb.BytsPerSec);
					//copy usedClusters to sectorBuffer to write it to disk
					System.arraycopy(usedClusters, numFatSectors*bpb.BytsPerSec, sectorBuffer, 0, bpb.BytsPerSec);
					writeSector(sectorBuffer, numFatSectors + firstFatSecNum + fatNumberTmp*FATSz);
				}

			return fatCorrected;
		}
		catch (xxl.core.io.fat.errors.DirectoryException e)
		{
			device.out.println(e);
			return false;
		}
		catch (Exception e)
		{
			device.out.println("An "+e+" occur! Couldn't correct the FAT. It might be that some files are no longer correct accessible!");
			return false;
		}
	}	//end checkdisk(byte[] fat)


	/**
	 *
	 * @param fat the FAT as byte array.
	 * @param flag the flag indicates what kind of checkdisk is to do.
	 * Use CHECK_BAD_CLUSTERS as flag to check the disk surface for failers.
	 * @return true if the fat is corrected otherwise false.
	 */
	protected boolean checkdisk(byte[] fat, byte flag)
	{
		if (fat == null)
			return false;
		checkdisk(fat);
		if ((flag & CHECK_BAD_CLUSTERS) != 0)
		{
			return checkDiskSurface();
		}

		return true;
	}	//end checkdisk(byte[] fat, byte flag)


	/**
	 * Check the surface of the disk.
	 * @return true if no disk error could be found or in case of an error the
	 * system will work properly with it, otherwise false.
	 */
	public boolean checkDiskSurface()
	{
		device.out.println("Check the disk surface, this may take some minutes!");

		long percent;
		long oldPercent = 0;
		long lastCluster = bpb.getLastFatCluster();

		//perform a read test to each sector of bpb, fat, root directory, and fsi
		long lastSectorNumber = bpb.getFirstDataSector();
		for (long sectorNumber = 0; sectorNumber < lastSectorNumber; sectorNumber++)
		{
			try
			{
				readSector(sectorNumber);

				percent = MyMath.roundDown(((double)sectorNumber / (lastCluster))*100);
				if (Math.abs(oldPercent - percent) > 4)
				{
					device.out.print(percent+"%, ");
					oldPercent = percent;
				}
			}
			catch (Exception e)
			{
				if (sectorNumber == 0)
				{
					device.out.println("The bpb sector is defect, the system could not work without it!");
					return false;
				}
				if (sectorNumber >= bpb.getFirstRootDirSecNum() &&
					sectorNumber < bpb.getFirstRootDirSecNum() + bpb.getRootDirSectors())
				{
					device.out.println("One or more root directory sector(s) are defect, the system could not work without it!");
					return false;
				}
				try
				{
					if (fatType == FAT32 && sectorNumber == bpb.getFSInfoSectorNumber())
					{
						device.out.println("The FSInfo sector is defect, the system could not work without it!");
						return false;
					}
				}
				catch (WrongFATType error)
				{
					device.out.println(error);
				}
				device.out.println("Found defect sector: "+sectorNumber);
				defectSectorNumbers.add(new Long(sectorNumber));
			}
		}

		//perform a read test to each data cluster
		for (long i=bpb.getFirstDataSector(); i < lastCluster; i++)
		{
			try
			{
				device.readCluster(i);
				
				percent = MyMath.roundDown(((double)i / (lastCluster))*100);
				if (Math.abs(oldPercent - percent) > 4)
				{
					device.out.print(percent+"%, ");
					oldPercent = percent;
				}
			}
			catch (Exception e)
			{
				device.out.println("Found defect cluster: "+i);
				setBadMark(i - bpb.getFirstDataSector() + 2);
			}
		}

		return true;
	}	//end checkDiskSurface()


	/**
	 * Return the total number of free bytes.
	 * @return the total number of free bytes.
	 */
	public long getNumberOfFreeBytes()
	{
		return freeMemoryManagement.getNumFreeClusters()*bpb.BytsPerSec*bpb.SecPerClus;
	}	//end getNumberOfFreeBytes()


	/**
	 * Return the FSI object.
	 * @return the FSI object.
	 */
	protected FSI getFSI()
	{
		return fsi;
	}	//end getFSI()

	//////////////////////////////////////////////
	// DEBUG - METHODS also used by RawExplorer //
	//////////////////////////////////////////////

	
	/**
	 * Debug method.
	 * Return the start sector number for the given fatNumber.
	 * 
	 * @param fatNumber the given fatNumber.
	 * @return the start sector number.
	 */
	public long getFATSectorNumber(int fatNumber)
	{
		return fatNumber*FATSz + bpb.RsvdSecCnt;
	}

	
	/**
	 * Debug method.
	 * Return the FAT fatNumber as byte array. 
	 * 
	 * @param fatNumber the given fatNumber.
	 * @return the FAT as byte array.
	 */
	public byte[] getFAT(int fatNumber)
	{
		int firstFatSecNum = bpb.RsvdSecCnt;
		int numFatSectors = (int)bpb.getNumFatSectors();
		
		byte[] fat = new byte[numFatSectors * bpb.BytsPerSec];
		byte[] sectorBuffer = new byte[bpb.BytsPerSec];
				
		//try to read the fat, if one fat is defect try the next one.
		for (int sectorNumber=firstFatSecNum; sectorNumber < firstFatSecNum + numFatSectors; sectorNumber++)
		{
			try
			{
				sectorBuffer = readSector(sectorNumber + fatNumber*FATSz);
				System.arraycopy(sectorBuffer, 0, fat, (sectorNumber - firstFatSecNum)*bpb.BytsPerSec, bpb.BytsPerSec);
			}
			catch (Exception e)
			{
				device.out.println("FAT number "+fatNumber+" is defect, because of one or more defect sector(s)!");
				if (!defectSectorNumbers.contains(new Long(sectorNumber)))
					defectSectorNumbers.add(new Long(sectorNumber));
			}
		}
		return fat;
	}

}	//end class FAT
