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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import xxl.core.io.fat.errors.DirectoryException;
import xxl.core.io.fat.errors.FileDoesntExist;
import xxl.core.io.fat.errors.IllegalName;
import xxl.core.io.fat.errors.NameAllreadyExists;
import xxl.core.io.fat.errors.NotEnoughMemory;
import xxl.core.io.fat.util.ByteArrayConversionsLittleEndian;
import xxl.core.io.fat.util.MyMath;
import xxl.core.io.fat.util.StringOperations;
import xxl.core.util.Arrays;


/**
 * This class represents a directory for the FAT. One directory entry consist
 * of 32 bytes. The order of numbers is little endian.
 */
public class DIR
{
	
	/**
	 * Indicates that the entry is marked as free.
	 */
	public static final byte FREE_ENTRY = (byte)0xE5;
	
	/**
	 * Indicates that the entry is marked as free and all next entries are free, too.
	 */
	public static final byte FREE_ENTRY_AND_NEXTS_FREE = (byte)0x00;
	
	/**
	 * Indicates that the entry is marked as free.
	 */
	public static final byte FREE_ENTRY_KANJI = (byte)0x05;
	
	/**
	 * Indicates that file is read only.
	 */
	public static final byte ATTR_READ_ONLY	= (byte)0x01;
	
	/**
	 * Indicates that normal directory listings should not show this file.
	 */
	public static final byte ATTR_HIDDEN	= (byte)0x02;
	
	/**
	 * Indicates that this is an operating system file.
	 */
	public static final byte ATTR_SYSTEM	= (byte)0x04;
	
	/**
	 * There should only be one "file" on the volume that has this
	 * attribute set, and that file must be in the root directory.
	 * This name of this file is actually the label for the volume.
	 * firstClusterHI and firstClusterLO must always be 0 for the 
	 * volume label (no data clusters are allocated to the volume label
	 * file).
	 */
	public static final byte ATTR_VOLUME_ID	= (byte)0x08;
	
	/**
	 * Indicates that this file is actually a container for other files.
	 */
	public static final byte ATTR_DIRECTORY	= (byte)0x10;
	
	/**
	 * This attribute supports backup utilities. This bit is set by the FAT
	 * file system driver when a file is created, renamed, or written to.
	 * Backup utilities may use this attribute to indicate which files on 
	 * the volume have been modified since the last time that a backup
	 * was performed.
	 */
	public static final byte ATTR_ARCHIVE 	= (byte)0x20;
	
	/**
	 * Indicates that the "file" is actually part of the long name entry
	 * for some other file.
	 */
	public static final byte ATTR_LONG_NAME	= (byte)(ATTR_READ_ONLY | ATTR_HIDDEN | ATTR_SYSTEM | ATTR_VOLUME_ID);
	
	/**
	 * A mask for determining whether an entry is a long-name
	 * sub-component.
	 */
	public static final byte ATTR_LONG_NAME_MASK = (byte)(ATTR_READ_ONLY | ATTR_HIDDEN | ATTR_SYSTEM | ATTR_VOLUME_ID | ATTR_DIRECTORY | ATTR_ARCHIVE);
	
	/**
	 * Indicates that the "entry" is the last part of the long name entry
	 * set.
	 */
	public static final byte LAST_LONG_ENTRY = (byte)0x40;	
	
	/**
	 * Indicates that only the root entry should be listed, all other directory entries will be filtered.
	 */
	public static final int LIST_ROOT_ENTRY = 1;
	
	/**
	 * Indicates that only the dot and dot dot entries should be listed,
	 * all other directory entries will be filtered.
	 */
	public static final int LIST_DOT_AND_DOTDOT_ENTRY = 2;
	
	/**
	 * Indicates that only directory entries (entries with the set ATTR_DIRECTORY flag) should
	 * be listed, all other entries will be filtered.
	 */
	public static final int LIST_DIRECTORY_ENTRY = 4;
	
	/**
	 * Indicates that only file entries should be listed, all other directory entries will be filtered.
	 */
	public static final int LIST_FILE_ENTRY = 8;
	
	/**
	 * Indicates that only the free entries should be listed, all other directory entries will be filtered.
	 */
	public static final int LIST_FREE_ENTRY = 16;
	
	/**
	 * Indicates that only the entries which have an entry in the FAT12 will be listed,
	 * all other directory entries will be filtered.
	 */ 
	public static final int LIST_FAT12_ENTRY = LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY;
	
	/**
	 * Indicates that only the entries which have an entry in the FAT16 will be listed,
	 * all other directory entries will be filtered.
	 */ 
	public static final int LIST_FAT16_ENTRY = LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY;
	
	/**
	 * Indicates that only the entries which have an entry in the FAT32 will be listed,
	 * all other directory entries will be filtered.
	 */ 
	public static final int LIST_FAT32_ENTRY = LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY | LIST_ROOT_ENTRY;
	
	/**
	 * Indicates that all entries will be listed except the free directory entries.
	 */
	public static final int LIST_ALL_EXCEPT_FREE = LIST_ROOT_ENTRY | LIST_DOT_AND_DOTDOT_ENTRY | LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY;
	
	/**
	 * Indicates that every entry should be listed. No entry is filtered.
	 */
	public static final int LIST_EVERYTHING_ENTRY = LIST_ROOT_ENTRY | LIST_DOT_AND_DOTDOT_ENTRY | LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY | LIST_FREE_ENTRY;
		
	/**
	 * Object of the FATDevice;
	 */
	private FATDevice device = null;
	
	/**
	 * Object of the BPB.
	 */
	private BPB bpb = null;

	/**
	 * Contains the first sector number of the root directory.
	 */
	private long firstRootDirSecNum;
	
	/**
	 * Contains the number of root directory sectors.
	 */
	private long numRootDirSectors;
	
	/**
	 * Byte array to store one sector.
	 */
	private byte[] sectorBuffer;
	
	/**
	 * Byte array to store one cluster.
	 */
	private byte[] clusterBuffer;
	
	/**
	 * Root directory as byte array.
	 */
	private byte[] rootDirectory;
	
	//private LinkedList directory;
	
	/**
	 * Calendar to get actual time and date.
	 */
	private GregorianCalendar calendar = new GregorianCalendar();

	/**
	 * The name of a directory entry has some restrictions. This set contains
	 * characters that are not allowed to be in a short name.
	 */
	public static final HashSet illicitValues = new HashSet(30, 0.8f);

	//initialize the hashtable with values that are illicit
	static
	{
		illicitValues.add(new Short((short)0x22));
		illicitValues.add(new Short((short)0x2A));
		illicitValues.add(new Short((short)0x2B));
		illicitValues.add(new Short((short)0x2C));
		illicitValues.add(new Short((short)0x2E));
		illicitValues.add(new Short((short)0x2F));
		illicitValues.add(new Short((short)0x3A));
		illicitValues.add(new Short((short)0x3B));
		illicitValues.add(new Short((short)0x3C));
		illicitValues.add(new Short((short)0x3D));
		illicitValues.add(new Short((short)0x3E));
		illicitValues.add(new Short((short)0x3F));
		illicitValues.add(new Short((short)0x5B));
		illicitValues.add(new Short((short)0x5C));
		illicitValues.add(new Short((short)0x5D));
		illicitValues.add(new Short((short)0x7C));
	}
	
	
	/**
	 * This class contains the information about one or more free directory entries.
	 */
	protected class FreeInformation
	{
		/**
		 * Index to the start position of the ordered free directory entry.
		 */
		int freeEntryIndex;

		/**
		 * Contains the ordered list of clusters where the ordered free directory entry can be stored.
		 * The first cluster number in the list is the one for the free entry index.
		 * The stored cluster numbers are all direct. See documentation for the meaning
		 * of direct and indirect addressing.
		 */
		List freeClusters;

		/**
		 * Is the first cluster of the free cluster list, all other clusters must be loaded.
		 */
		byte[] cluster;

		/**
		 * Indicates if the information of this class belongs to the root directory.
		 * Notice that the cluster numbers stored at the freeClusters list are always
		 * direct, so you don't have to transform with device.getFirstSectorNumberOfCluster(...).
		 */
		boolean isRoot;

		/**
		 * Creates an instance of this object.
		 * @param freeEntryIndex the index of the free entry.
		 * @param freeClusters a list of free clusters, this clusters must be direct accessible.
		 * @param cluster byte array representation of a cluster.
		 * @param isRoot indicates if this free information belongs to the root directory.
		 */
		public FreeInformation(int freeEntryIndex, List freeClusters, byte[] cluster, boolean isRoot)
		{
			this.freeEntryIndex = freeEntryIndex;
			this.freeClusters = freeClusters;
			this.cluster = cluster;
			this.isRoot = isRoot;
		}

		/**
		 * Return a String with the information about the variables of this class.
		 * @return String with the information.
		 */
		public String toString()
		{
			String res = "FreeInformation:\n";
			res += "freeEntryIndex "+freeEntryIndex;
			res += ", list length "+freeClusters.size()+", ";
			res += freeClusters +"\n";
			res += "isRoot "+isRoot+"\n";
			return res;
		}
	}	//end inner class FreeInformation
	
	
	/**
	 * This class contains information about a number of a cluster.
	 * The old song: If the clusterNumber belongs to the root directory
	 * in case of FAT12 or FAT16 the clusterNumber is direct and isRoot
	 * has to be true. Otherwise the clusterNumber is indirect.
	 */
	protected class ClusterTuple
	{		
		/**
		 * Contains a number of a cluster.
		 */
		public long clusterNumber;
		
		/**
		 * Indicates if the clusterNumber belongs to the root directory of FAT12 or FAT16.
		 * For a FAT32 file system this flag must be false also for the root directory.
		 */
		public boolean isRoot;
		
		/**
		 * Creates an instance of this object.
		 * @param clusterNumber the number of a cluster.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 */
		public ClusterTuple(long clusterNumber, boolean isRoot)
		{
			this.clusterNumber = clusterNumber;
			this.isRoot = isRoot;
		}	//end constructor
		
		
		/**
		 * Print the information about the variables of this class.
		 * @return information about the variables of this class.
		 */
		public String toString()
		{
			String res = "ClusterTuple:\n";
			res += "cn "+clusterNumber+" isRoot "+isRoot+"\n";
			return res;			
		}	//end toString()
	}	//end inner class ClusterTuple
	
	
	/**
	 * Inner class represents one entry of the directory. One entry consists  
	 * of 32 bytes. Since the naming convention is no longer restricted to
	 * 8.3 one entry may be consist of x*32 bytes. Where x is a positive non
	 * zero integer.
	 */
	protected class DirectoryStructure
	{
		/**
		 * Short name representation of the directory name. The format is
		 * 8.3.
		 */
		protected String shortName;
		
		/**
		 * Long name representation of the directory name, in case the given
		 * directory name is not storable as shortName.
		 */
		protected String longName;
		
		/**
		 * Attributes of the directory entry.
		 */
		protected short attributes;
		
		/**
		 * Reserved for use by Windows NT. Value is set to 0 when a file is created.
		 */
		private short ntReserved;
		
		/**
		 * Millisecond stamp at file creation time. This field actually contains a count
		 * of tenths of a second. The granularity of the seconds part of creationTime is 
		 * 2 seconds so this field is a count of tenths of a second and its value range is
		 * 0-199 inclusive.
		 */
		protected short creationTimeTenth;
		
		/**
		 * The time stamp the file was created. A time stamp is a 16-bit field
		 * that has a granularity of 2 seconds. Bit 0 is the LSB of the 16-bit word,
		 * bit 15 is the MSB of the 16-bit word.
		 * Bits 0-4: 2-second count, valid range 0-29 inclusive (0-58 seconds).
		 * Bits 5-10: Minutes, valid value range 0-59 inclusive.
		 * Bits 11-15: Hours, valid range 0-23 inclusive.
		 */
		protected int creationTime;

		/**
		 * The date stamp the file was created. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 */
		protected int creationDate;
		
		/**
		 * The date stamp the file was last accessed. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 */
		protected int lastAccessDate;
		
		/**
		 * The high part of the cluster number stored at the directory entry.
		 */
		protected int firstClusterHI;
		
		/**
		 * The time stamp the file was last written. A time stamp is a 16-bit field
		 * that has a granularity of 2 seconds. Bit 0 is the LSB of the 16-bit word,
		 * bit 15 is the MSB of the 16-bit word.
		 * Bits 0-4: 2-second count, valid range 0-29 inclusive (0-58 seconds).
		 * Bits 5-10: Minutes, valid value range 0-59 inclusive.
		 * Bits 11-15: Hours, valid range 0-23 inclusive.
		 */
		protected int writeTime;
		
		/**
		 * The date stamp the file was last written. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 */
		protected int writeDate;
		
		/**
		 * The low part of the cluster number stored at the directory entry.
		 */
		protected int firstClusterLO;
		
		/**
		 * The length of the file stored at the directory entry.
		 */
		protected long fileSize;
		
		/**
		 * The number of the cluster where the directory entry should be stored.
		 */
		protected long clusterNumber;		//this is the cluster number where the directory entry should be stored
		
		/**
		 * Indicates if the clusterNumber belongs to the root directory.
		 */
		protected boolean isRoot;
		
		/**
		 * Contains the number of entries that this directory entry uses.
		 */	
		protected int numOfEntries = 0;	
		
		
		/**
		 * Creates an instance of this object.
		 * @param name the name stored at the directory entry.
		 * @param clusterNumber the number of the cluster where the directory should be stored.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 * @throws IllegalName in case the given name is not legal.
		 */
		public DirectoryStructure(String name, long clusterNumber, boolean isRoot) throws IllegalName
		{
			this(name, 0, DIR.ATTR_ARCHIVE, 0, clusterNumber, isRoot);
		}	//end constructor
		
		
		/**
		 * Creates an instance of this object.
		 * @param name the name stored at the directory entry.
		 * @param attribute the attribute stored at the directory entry.
		 * @param clusterNumber the number of the cluster where the directory should be stored.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 * @throws IllegalName in case the given name is not legal.
		 */
		public DirectoryStructure(	String name,
									byte attribute,
									long clusterNumber,
									boolean isRoot) throws IllegalName
		{
			this(name, 0, attribute, 0, clusterNumber, isRoot);
		}	//end constructor
		
		
		/**
		 * Creates an instance of this object.
		 * @param name the name stored at the directory entry.
		 * @param entryClusterNumber the number of the cluster stored at the directory entry.
		 * @param clusterNumber the number of the cluster where the directory should be stored.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 * @throws IllegalName in case the given name is not legal.
		 */
		public DirectoryStructure(	String name,
									long entryClusterNumber,
									long clusterNumber,
									boolean isRoot) throws IllegalName
		{
			this(name, entryClusterNumber, DIR.ATTR_ARCHIVE, 0, clusterNumber, isRoot);
		}	//end constructor
				
		
		/**
		 * Creates an instance of this object.
		 * @param name the name stored at the directory entry.
		 * @param entryClusterNumber the number of the cluster stored at the directory entry.
		 * @param attribute the attribute stored at the directory entry.
		 * @param fileSize the size of the file stored at the directory entry.
		 * @param clusterNumber the number of the cluster where the directory should be stored.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 * @throws IllegalName in case the given name is not legal.
		 */
		public DirectoryStructure(	String name,
									long entryClusterNumber,
									byte attribute,
									long fileSize,
									long clusterNumber,
									boolean isRoot) throws IllegalName
		{			
			this(	name,
					attribute,
					calendar.get(Calendar.HOUR_OF_DAY),		//crtTimeHour
					calendar.get(Calendar.MINUTE),			//crtTimeMinute
					calendar.get(Calendar.SECOND),			//crtTimeSecond
					calendar.get(Calendar.MILLISECOND),		//crtTimeMillisecond
					calendar.get(Calendar.DAY_OF_MONTH),	//crtDateDayOfMonth
					calendar.get(Calendar.MONTH) + 1,		//crtDateMonth, the month count starts at 0
					calendar.get(Calendar.YEAR),			//crtDateYear,
					entryClusterNumber,
					fileSize,
					clusterNumber,
					isRoot);
		}	//end constructor
		
		
		/**
		 * Creates an instance of this object.
		 * @param name the name stored at the directory entry.
		 * @param attribute the attribute stored at the directory entry.
		 * @param crtTimeHour creation time hour valid range form 1 to 24.
		 * @param crtTimeMinute creation time minute valid range from 0 to 59.
		 * @param crtTimeSecond creation time second valid range from 0 to 59.
		 * @param crtTimeMillisecond creation time millisecond valid range from 0 to 999.
		 * @param crtDateDayOfMonth creation day valid range from 1 to 31.
		 * @param crtDateMonth creation month valid range from 1 to 12.
		 * @param crtDateYear creation year.
		 * @param entryClusterNumber the number of the cluster stored at the directory entry.
		 * @param fileSize the size of the file stored at the directory entry.
		 * @param clusterNumber the number of the cluster where the directory should be stored.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 * @throws IllegalName in case the given name is not legal.
		 */
		public DirectoryStructure(	String name,
									byte attribute,
									int crtTimeHour,
									int crtTimeMinute,
									int crtTimeSecond,
									int crtTimeMillisecond,
									int crtDateDayOfMonth,
									int crtDateMonth,
									int crtDateYear,
									long entryClusterNumber, 
									long fileSize,
									long clusterNumber,
									boolean isRoot) throws IllegalName
		{
			this.clusterNumber = clusterNumber;
			this.isRoot = isRoot;
			setName(name);
			setAttribute(attribute);
			ntReserved = 0;
			setCreationTime(crtTimeHour - 1, crtTimeMinute, (crtTimeSecond >> 1), ((crtTimeMillisecond / 10) << 1));
			setCreationDate(crtDateDayOfMonth, crtDateMonth, crtDateYear-1980);
			setLastAccessDate(crtDateDayOfMonth, crtDateMonth, crtDateYear-1980);
			setLastWriteTime(crtTimeHour - 1, crtTimeMinute, (crtTimeSecond >> 1));
			setLastWriteDate(crtDateDayOfMonth, crtDateMonth, crtDateYear-1980);
			
			firstClusterHI = (int)((entryClusterNumber >> 16) & 0x000000000000FFFF);
			firstClusterLO = (int)(entryClusterNumber & 0x000000000000FFFF);
			
			this.fileSize = fileSize;
		}	//end constructor
				
		
		/**
		 * Initialize this directory entry with the given byte array.
		 * All variables will be initialized.
		 * @param dirStruc a directory entry as byte array, the length has to be a
		 * multiple of 32 byte.
		 * @param clusterNumber is the number of the cluster where the directory entry is taken from.
		 * It is needed because of the numeric tail of the short name.
		 * @param isRoot indicates if the given clusterNumber belongs to the root directory.
		 */
		public DirectoryStructure(byte[] dirStruc, long clusterNumber, boolean isRoot)
		{
			this.clusterNumber = clusterNumber;
			this.isRoot = isRoot;
			
			//extract the short name together with the numeric tail algorithm
			String str = ByteArrayConversionsLittleEndian.byteToString(dirStruc, dirStruc.length - 32 + 0, dirStruc.length - 32 + 11); 
			shortName = numericTailGeneration(str, new ClusterChainIterator(device, clusterNumber, isRoot));
			
			longName = getLongName(dirStruc);
			
			attributes = ByteArrayConversionsLittleEndian.convShort(dirStruc[dirStruc.length - 32 + 11]);
			ntReserved = ByteArrayConversionsLittleEndian.convShort(dirStruc[dirStruc.length - 32 + 12]);
			creationTimeTenth = ByteArrayConversionsLittleEndian.convShort(dirStruc[dirStruc.length - 32 + 13]);
			creationTime = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 14], dirStruc[dirStruc.length - 32 + 15]);
			creationDate = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 16], dirStruc[dirStruc.length - 32 + 17]);
			lastAccessDate = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 18], dirStruc[dirStruc.length - 32 + 19]);
			firstClusterHI = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 20], dirStruc[dirStruc.length - 32 + 21]);
			writeTime = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 22], dirStruc[dirStruc.length - 32 + 23]);
			writeDate = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 24], dirStruc[dirStruc.length - 32 + 25]);
			firstClusterLO = ByteArrayConversionsLittleEndian.convInt(dirStruc[dirStruc.length - 32 + 26], dirStruc[dirStruc.length - 32 + 27]);
			fileSize = ByteArrayConversionsLittleEndian.convLong(dirStruc[dirStruc.length - 32 + 28], dirStruc[dirStruc.length - 32 + 29], dirStruc[dirStruc.length - 32 + 30], dirStruc[dirStruc.length - 32 + 31]);
		}	//end constructor
				
		
		/**
		 * Set the name of a file or directory. There are some restrictions 
		 * on the name.
		 * @param str the name, it can be either a short name with 8.3 naming convention or a
		 * long name.
		 * @throws IllegalName in case the name is not legal.
		 */
		public void setName(String str) throws IllegalName
		{
			if (str.equals(System.getProperty("file.separator")))
			{
				shortName = str+"          ";
				numOfEntries = 1;
				return;
			}
			
			if (str.equals(".          ") || str.equals("..         "))
			{
				shortName = str;
				numOfEntries = 1;
				return;
			}
			
			shortName = numericTailGeneration(str, new ClusterChainIterator(device, clusterNumber, isRoot));
						
			int dotIndex = shortName.lastIndexOf(".");
			String shortFileName = "";
			String shortFileExtension = "";
			if (dotIndex >= 0)
			{
				shortFileName = shortName.substring(0, dotIndex);
				if (DIR.containsIllicitValues(shortFileName.getBytes()))
					throw new IllegalName("");
				shortFileExtension = shortName.substring(dotIndex + 1, shortName.length());
				if (DIR.containsIllicitValues(shortFileExtension.getBytes()))
					throw new IllegalName("");
			}
			else
				shortFileName = shortName;
			
					
			for (int i=shortFileName.length(); i < 8; i++)
				shortFileName += " ";
				
			for (int i=shortFileExtension.length(); i < 3; i++)
				shortFileExtension += " ";
			
			numOfEntries = 1;			
			
			if (!isShortName(str) || !str.toUpperCase().equals(str))
			{
				longName = str;
				numOfEntries += MyMath.roundUp(str.length() / 13.0);
			}
	
			shortName = shortFileName + shortFileExtension;
			//all short name entries must be in upper case
			shortName = shortName.toUpperCase();
		}	//end setName
	
	
		/**
		 * Set the cluster number of this entry to the given clusterNumber.
		 * @param clusterNumber number of cluster to set.
		 */
		public void setClusterNumber(long clusterNumber)
		{
			firstClusterHI = (int)((clusterNumber >> 16) & 0x000000000000FFFF);
			firstClusterLO = (int)(clusterNumber & 0x000000000000FFFF);
		}	//end setClusterNumber(long clusterNumber)
		
				
		/**
		 * Set the attribute. Only the first 6 bits are free to change. The
		 * upper 2 bit (of the byte type) are reserved.
		 * @param attribute the attribute for the directory entry.
		 */
		public void setAttribute(short attribute)
		{
			//upper two bits are reserved
			attributes &= 0x00C0;		//clear the first 6 bits
			attribute &= 0x003F;		//only the first 6 bits should have a value
			attributes |= attribute;	//set the first 6 bits
		}	//end setAttribute
		
		
		/**
		 * Set the creationTime to the actual time. A time stamp is a 16-bit field
		 * that has a granularity of 2 seconds. Bit 0 is the LSB of the 16-bit word,
		 * bit 15 is the MSB of the 16-bit word.
		 * Bits 0-4: 2-second count, valid range 0-29 inclusive (0-58 seconds).
		 * Bits 5-10: Minutes, valid value range 0-59 inclusive.
		 * Bits 11-15: Hours, valid range 0-23 inclusive.
		 */
		public void setCreationTime()
		{
			setCreationTime(
				calendar.get(Calendar.HOUR_OF_DAY) - 1,
				calendar.get(Calendar.MINUTE),
				(calendar.get(Calendar.SECOND) >> 1),
				(calendar.get(Calendar.MILLISECOND) / 10) << 1
			);
		}	//end setCreationTime()
		
		
		/**
		 * Set the creationTime to the given parameters. A time stamp is a 16-bit field
		 * that has a granularity of 2 seconds. Bit 0 is the LSB of the 16-bit word,
		 * bit 15 is the MSB of the 16-bit word.
		 * Bits 0-4: 2-second count, valid range 0-29 inclusive (0-58 seconds).
		 * Bits 5-10: Minutes, valid value range 0-59 inclusive.
		 * Bits 11-15: Hours, valid range 0-23 inclusive.
		 * All parameter must be in the valid range, no conversion will be done.
		 * @param crtTimeHour creation time hour.
		 * @param crtTimeMinute creation time minute.
		 * @param crtTimeSecond creation time second.
		 * @param crtTimeMillisecond creation time millisecond
		 */
		public void setCreationTime(int crtTimeHour,
									int crtTimeMinute,
									int crtTimeSecond,
									int crtTimeMillisecond)
		{
			creationTime = 0;
			creationTime = crtTimeSecond >> 1;
			creationTime |= (crtTimeMinute << 5);
			creationTime |= (crtTimeHour << 11);
						
			creationTimeTenth = (short)crtTimeMillisecond;
		}	//end setCreationTime(...)
		
		
		/**
		 * Set the creationDate to the actual date. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 */
		public void setCreationDate()
		{
			setCreationDate(
				calendar.get(Calendar.DAY_OF_MONTH),
				calendar.get(Calendar.MONTH) + 1,
				calendar.get(Calendar.YEAR) - 1980
			);
		}	//end setCreationDate()
		
		
		/**
		 * Set the creationDate to the actual date. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 * All parameter must be in the valid range, no conversion will be done.
		 * @param crtDateDayOfMonth creation day.
		 * @param crtDateMonth creation month.
		 * @param countOfYears the count of years since 1980.
		 */
		public void setCreationDate(int crtDateDayOfMonth, int crtDateMonth, int countOfYears)
		{
			creationDate = 0;
			creationDate = (crtDateDayOfMonth);
			creationDate |= (crtDateMonth << 5);
			creationDate |= (countOfYears << 9);
		}	//end setCreationDate(...)
		
		
		/**
		 * Set the last access to the actual date. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 * All parameter must be in the valid range, no conversion will be done.
		 * @param dayOfMonth last access day.
		 * @param month last access month.
		 * @param year last access year.
		 */
		public void setLastAccessDate(int dayOfMonth, int month, int year)
		{
			lastAccessDate = 0;
			lastAccessDate = (dayOfMonth);
			lastAccessDate |= (month << 5);
			lastAccessDate |= (year << 9);
		}	//end setLastAccessDate(...)
		
		
		/**
		 * Set the last access date with the actual time.
		 */
		public void setLastAccessDate()
		{
			setLastAccessDate(	calendar.get(Calendar.DAY_OF_MONTH),
								calendar.get(Calendar.MONTH)+1,
								calendar.get(Calendar.YEAR)-1980);
		}	//end lastAccessDate()
		
		
		/**
		 * Set the last write date to the actual date. A date stamp is a 16-bit field
		 * that that is basically a date relative to the MS-DOS epoch of 01/01/1980.
		 * Bit 0 is the LSB of the 16-bit word, bit 15 is the MSB of the 16-bit word:
		 * Bits 0-4: Day, valid range 1-31 inclusive.
		 * Bits 5-8: Month, 1=January, valid range 1-12 inclusive.
		 * Bits 9-15: Count of years from 1980, valid range 0-127 inclusive (1980-2107).
		 * All parameter must be in the valid range, no conversion will be done.
		 * @param dayOfMonth last write day.
		 * @param month last write month.
		 * @param year last write year.
		 */
		public void setLastWriteDate(int dayOfMonth, int month, int year)
		{
			writeDate = 0;
			writeDate = (dayOfMonth);
			writeDate |= (month << 5);
			writeDate |= (year << 9);
		}	//end setLastAccessDate(...)
	
		
		/**
		 * Set the last write date with the actual date.
		 */
		public void setLastWriteDate()
		{
			setLastAccessDate(	calendar.get(Calendar.DAY_OF_MONTH),
								calendar.get(Calendar.MONTH)+1,
								calendar.get(Calendar.YEAR)-1980);
			setLastWriteDate(	calendar.get(Calendar.DAY_OF_MONTH),
								calendar.get(Calendar.MONTH)+1,
								calendar.get(Calendar.YEAR)-1980);
		}
		
		
		/**
		 * Set the last write time to the given parameters. A time stamp is a 16-bit field
		 * that has a granularity of 2 seconds. Bit 0 is the LSB of the 16-bit word,
		 * bit 15 is the MSB of the 16-bit word.
		 * Bits 0-4: 2-second count, valid range 0-29 inclusive (0-58 seconds).
		 * Bits 5-10: Minutes , valid value range 0-59 inclusive.
		 * Bits 11-15: Hours, valid range 0-23 inclusive
		 * All parameter must be in the valid range, no conversion will be done.
		 * @param hour last write time hour.
		 * @param minute last write time minute.
		 * @param second last write time second.
		 */
		public void setLastWriteTime(int hour, int minute, int second)
		{
			writeTime = 0;
			writeTime = second >> 1;
			writeTime |= (minute << 5);
			writeTime |= (hour << 11);
		}	//end setLastWriteTime(...)
		
		
		/**
		 * Set the last write time with the actual time.
		 */
		public void setLastWriteTime()
		{
			setLastWriteTime(	calendar.get(Calendar.HOUR) - 1,
								calendar.get(Calendar.MINUTE),
								calendar.get(Calendar.SECOND) >> 1
							);
		}	//end setLastWriteTime()
	
	
		/**
		 * Return the representation of this class as byte array.
		 * @return the directory entry as byte array. The length is a multiple
		 * of 32 byte.
		 */
		public byte[] getByte()
		{
			byte[] arr = new byte[32*numOfEntries];
			
			//long name
			if (arr.length > 32)
			{
				byte[] temp;
				byte checksum = (byte)checksum(shortName.getBytes());
				int longNameIndex = 0;	//index inside the string long name
				for (int i=1; i < numOfEntries; i++)
				{
					//set order number
					arr[arr.length - 32*(i+1)] = (byte)i;
					
					//set attribute long name
					arr[arr.length - 32*(i+1) + 11] = ATTR_LONG_NAME;
					
					//set type
					arr[arr.length - 32*(i+1) + 12] = (byte)0;	//indicates that this long directory entry is a sub-component of a long name
					
					//set checksum
					arr[arr.length - 32*(i+1) + 13] = checksum;
					
					//set first cluster low
					arr[arr.length - 32*(i+1) + 26] = (byte)0;	//must be zero
					arr[arr.length - 32*(i+1) + 27] = (byte)0;
					
					try
					{
						//set the first 5 characters
						if (longNameIndex+5 <= longName.length())
						{
							temp = longName.substring(longNameIndex, longNameIndex+5).getBytes("UTF-16LE");
							System.arraycopy(temp, 0, arr, arr.length - 32*(i+1) + 1, temp.length);
						}
						else
						{
							temp = longName.substring(longNameIndex, longName.length()).getBytes("UTF-16LE");
							System.arraycopy(temp, 0, arr, arr.length - 32*(i+1) + 1, temp.length);
							
							int index = arr.length - 32*(i+1) + 1 + temp.length;
							arr[index] = (byte)0;
							arr[++index] = (byte)0;
							for(index++; index < 11; index++)
								arr[index] = (byte)0xFF;
							
							for(index=14; index < 26; index++)
								arr[index] = (byte)0xFF;
								
							for(index=28; index < 32; index++)
								arr[index] = (byte)0xFF;
							
							break;
						}
						longNameIndex += 5;
						
						if (longNameIndex+6 <= longName.length())
						{
							//set the next 6 charecters
							temp = longName.substring(longNameIndex, longNameIndex+6).getBytes("UTF-16LE");
							System.arraycopy(temp, 0, arr, arr.length - 32*(i+1) + 14, temp.length);
						}
						else
						{
							temp = longName.substring(longNameIndex, longName.length()).getBytes("UTF-16LE");
							System.arraycopy(temp, 0, arr, arr.length - 32*(i+1) + 14, temp.length);
							
							int index = arr.length - 32*(i+1) + 14 + temp.length;
							arr[index] = (byte)0;
							arr[++index] = (byte)0;
							for(index++; index < 26; index++)
								arr[index] = (byte)0xFF;
								
							for(index=28; index < 32; index++)
								arr[index] = (byte)0xFF;
							
							break;
						}
						longNameIndex += 6;
						
						if(longNameIndex+2 <= longName.length())
						{
							//set the next 2 characters
							temp = longName.substring(longNameIndex, longNameIndex+2).getBytes("UTF-16LE");
							System.arraycopy(temp, 0, arr, arr.length - 32*(i+1) + 28, temp.length);
						}
						else
						{
							temp = longName.substring(longNameIndex, longName.length()).getBytes("UTF-16LE");
							System.arraycopy(temp, 0, arr, arr.length - 32*(i+1) + 28, temp.length);
							
							int index = arr.length - 32*(i+1) + 28 + temp.length;
							arr[index] = (byte)0;
							arr[++index] = (byte)0;
							for(index++; index < 32; index++)
								arr[index] = (byte)0xFF;
							break;
						}
						longNameIndex += 2;
					}
					catch(UnsupportedEncodingException e)
					{
						System.out.println(e);
						continue;
					}
				}
				
				//set last long entry flag
				arr[0] |= (byte)0x40;
			}	//end long name

			//short name
			System.arraycopy(shortName.getBytes(), 0, arr, arr.length - 32 + 0, 11);
			
			//attr
			arr[arr.length - 32 + 11] = (byte)attributes;
			
			//ntReserved
			arr[arr.length - 32 + 12] = (byte)ntReserved;
			
			//creationTimeTenth
			arr[arr.length - 32 + 13] = (byte)creationTimeTenth;
			
			//creationTime
			arr[arr.length - 32 + 14] = (byte)creationTime;
			arr[arr.length - 32 + 15] = (byte)(creationTime >> 8);
			
			//creationDate
			arr[arr.length - 32 + 16] = (byte)creationDate;
			arr[arr.length - 32 + 17] = (byte)(creationDate >> 8);
						
			//lastAccessDate
			arr[arr.length - 32 + 18] = (byte)(lastAccessDate);
			arr[arr.length - 32 + 19] = (byte)(lastAccessDate >> 8);

			//firstClusterHI
			arr[arr.length - 32 + 20] = (byte)firstClusterHI;
			arr[arr.length - 32 + 21] = (byte)(firstClusterHI >> 8);
			
			//writeTime
			arr[arr.length - 32 + 22] = (byte)writeTime;
			arr[arr.length - 32 + 23] = (byte)(writeTime >> 8);
			
			//writeDate
			arr[arr.length - 32 + 24] = (byte)writeDate;
			arr[arr.length - 32 + 25] = (byte)(writeDate >> 8);
			
			//firstClusterLO
			arr[arr.length - 32 + 26] = (byte)(firstClusterLO);
			arr[arr.length - 32 + 27] = (byte)(firstClusterLO >> 8);
			
			//fileSize
			arr[arr.length - 32 + 28] = (byte)(fileSize);
			arr[arr.length - 32 + 29] = (byte)(fileSize >> 8);
			arr[arr.length - 32 + 30] = (byte)(fileSize >> 16);
			arr[arr.length - 32 + 31] = (byte)(fileSize >> 24);
			
			return arr;
		}	//end getByte()
				

		/**
		 * Return how much 32 byte entries are needed to store all the information of this directory entry.
		 * @return the number of 32 byte entries of this directory entry.
		 */
		public int getNumOfEntries()
		{
			return numOfEntries;
		}	//end getNumOfEntries()
		
		
		/**
		 * Return the cluster number.
		 * @return cluster number.
		 */
		public long getClusterNumber()
		{
			return ((long)(firstClusterHI) << 16) | firstClusterLO;
		}	//end getClusterNumber()
		

		/**
		 * Return the length of the file associated with this directory entry. If
		 * it's a directory the file length is 0.
		 * @return the length of the file.
		 */
		public long length()
		{
			return fileSize;
		}	//end length()
		
	}	//end inner class DirectoryStructure
	
	
	/**
	 * Creates an instance of this object. The constructor will 
	 * create a new directory structure. Use this constructor
	 * for a format process.
	 * @param device instance of FATDevice.
	 * @param bpb instance of BPB.
	 */
	public DIR(FATDevice device, BPB bpb)
	{
		this.device = device;
		this.bpb = bpb;

		//initialize calendar with actual time
		calendar.setTime(new Date());

		sectorBuffer = new byte[bpb.BytsPerSec];
		clusterBuffer = new byte[bpb.BytsPerSec*bpb.SecPerClus];

		//this value is valid for all three FAT's
		firstRootDirSecNum = bpb.getFirstRootDirSecNum();

		//this value is only valid in case of FAT12 or FAT16
		numRootDirSectors = bpb.getRootDirSectors();

		//create an empty directory structure with only the root directory.
		//In case of FAT12 or FAT16 the byte array contains as much sector as needed to store the
		//the bpb.RootEntCnt.
		//In case of FAT32 the byte array has a size of one cluster.
		rootDirectory = getDir(numRootDirSectors, firstRootDirSecNum);
		if (device.getFatType() != FAT.FAT32)
		{
			for (int i=(int)firstRootDirSecNum; i < firstRootDirSecNum + numRootDirSectors; i++)
			{
				System.arraycopy(
					rootDirectory,
					(int)(i - firstRootDirSecNum)*bpb.BytsPerSec,
					sectorBuffer,
					0,
					bpb.BytsPerSec
				);
				writeSector(sectorBuffer, i);
			}
		}
		else
		{
			//write cluster to disk
			writeCluster(device.getFirstSectorNumberOfCluster(firstRootDirSecNum), rootDirectory);
			//mark this cluster number as used in FAT
			device.setFatContent(firstRootDirSecNum, FAT.EOC_MARK32);
		}
	}	//end constructor format
	
	
	/**
	 * Creates an instance of this object. The constructor will
	 * boot the directory structure.
	 * @param device instance of FATDevice.
	 * @param bpb instance of BPB.
	 * @param fatType the type of FAT.
	 */
	public DIR(FATDevice device, BPB bpb, byte fatType)
	{
		this.device = device;
		this.bpb = bpb;
				
		//initialize calendar with actual time
		calendar.setTime(new Date());
				
		sectorBuffer = new byte[bpb.BytsPerSec];
		clusterBuffer = new byte[bpb.BytsPerSec*bpb.SecPerClus];

		//this value is valid for all FAT's		
		firstRootDirSecNum = bpb.getFirstRootDirSecNum();
		
		//this value is only valid for FAT32
		numRootDirSectors = bpb.getRootDirSectors();
		
		if (fatType != FAT.FAT32)
		{
			rootDirectory = new byte[(int)numRootDirSectors*bpb.BytsPerSec];
			for (int i=(int)firstRootDirSecNum; i < firstRootDirSecNum + numRootDirSectors; i++)
			{
				sectorBuffer = readSector(i);
				System.arraycopy(sectorBuffer, 0, rootDirectory, (int)(i - firstRootDirSecNum)*bpb.BytsPerSec, bpb.BytsPerSec);
			}
				
			if (FileSystem.debug)
			{
				System.out.println("\n\tDirectory\n");
				Arrays.printHexArray(rootDirectory, firstRootDirSecNum * bpb.BytsPerSec, System.out);
			}	
		}
		else
		{
			//read all cluster of the cluster chain beginning at the first root directory cluster number
			LinkedList clusterList = new LinkedList();
			LinkedList clusterNumberList = new LinkedList();	//only for debug and information purposes.
			long clusterNumber = firstRootDirSecNum;
			do
			{
				clusterNumberList.addLast(new Long(clusterNumber));
				clusterList.addLast(readCluster(device.getFirstSectorNumberOfCluster(clusterNumber)));
				clusterNumber = device.getFatContent(clusterNumber);
			}while(!device.isLastCluster(clusterNumber));
			
			int clusterLength = ((byte[])clusterList.get(0)).length;
			rootDirectory = new byte[clusterLength*clusterList.size()];
			for (int i=0; i < clusterList.size(); i++)
			{
				System.arraycopy((clusterList.get(i)), 0, rootDirectory, i*clusterLength, clusterLength);
				if (FileSystem.debug)
					Arrays.printHexArray(
							((byte[])clusterList.get(i)),
							device.getFirstSectorNumberOfCluster(((Long)clusterNumberList.get(i)).longValue())*bpb.BytsPerSec,
							System.out
					);
							
			}
		}
	}	//end constructor boot
	
	
	/**
	 * Return a byte array of the directory. Use this method only if you
	 * format your file-system. In case of FAT12 or FAT16 the returned byte
	 * array consists of enough sectors to store the BPB.RootEntCnt. In case of FAT32
	 * the returned byte array is as big as a cluster.
	 * @param rootDirSectors the number of the sectors of the root directory.
	 * @param firstRootDirSecNum the cluster number stored at the root directory
	 * entry. In case of FAT12 or fAT16 this value should be 0. In case of FAT32
	 * this value must be cluster number where the root directory is stored.
	 * @return the root directory as a byte array.
	 */
	protected byte[] getDir(long rootDirSectors, long firstRootDirSecNum)
	{
		int length = 0;
		if (bpb.getFatType() != FAT.FAT32)
			length = (int)rootDirSectors*bpb.BytsPerSec;
		else
			length = bpb.SecPerClus*bpb.BytsPerSec;
			
		byte[] dir = new byte[length];
		
		//first entry is root directory
		byte[] rootEntry = createRootDirectory((byte)(ATTR_VOLUME_ID | ATTR_ARCHIVE), firstRootDirSecNum);
		System.arraycopy(rootEntry, 0, dir, 0, 32);
		
		//all other entrys marked as free
		for (int i=32; i < length; i += 32)
			dir[i] = FREE_ENTRY_AND_NEXTS_FREE;
			
		return dir;
	}	//end getDir(int rootDirSectors)
	
	
	/**
	 * Read the cluster indexed by cluster number.
	 * @param clusterNumber the number of the cluster.
	 * @return the cluster as byte array.
	 */
	protected byte[] readCluster(long clusterNumber)
	{
		return device.readCluster(clusterNumber);
	}	//end readCluster(long clusterNumber)
	
	
	/**
	 * Read the sector indexed by sector number.
	 * @param sectorNumber the number of the sector.
	 * @return the sector as byte array.
	 */
	protected byte[] readSector(long sectorNumber)
	{
		byte b[] = new byte[bpb.BytsPerSec];
		if (!device.readSector(b,sectorNumber))
			return null;
		else
			return b;
	}	//end readSector(long sectorNumber)
	
	
	/**
	 * Write the sector indexed by sector number to disk.
	 * @param sector the sector to write.
	 * @param sectorNumber the number of the sector.
	 */
	protected void writeSector(byte[] sector, long sectorNumber)
	{
		device.writeSector(sector, sectorNumber);
	}	//end writeSector(byte[] sector, long sectorNumber)
	
	
	/**
	 * Write the cluster indexed by cluster number to disk.
	 * @param cluster the cluster to write.
	 * @param clusterNumber the number of the cluster.
	 */
	protected void writeCluster(long clusterNumber, byte[] cluster)
	{
		device.writeCluster(clusterNumber, cluster);
	}	//end writeCluster(long clusterNumber, byte[] cluster)


	/**
	 * Write the directory entry of dirEntInf at the positions given by dirEntInf to disk.
	 * @param dirEntInf information about the directory entry.
	 * @throws DirectoryException in case the directory entry can not be written.
	 */
	protected void writeDirectoryEntry(DirectoryEntryInformation dirEntInf) throws DirectoryException
	{
		//check for boundary case
		if (dirEntInf.directoryEntryIndex + dirEntInf.directoryEntry.length >= dirEntInf.cluster.length)
		{	//boundary case
			//write the first part
			System.arraycopy(	dirEntInf.directoryEntry,
								0,
								dirEntInf.cluster,
								(int)dirEntInf.directoryEntryIndex,
								dirEntInf.cluster.length - (int)dirEntInf.directoryEntryIndex);
			if (dirEntInf.isRoot && device.getFatType() != FAT.FAT32)
				writeSector(dirEntInf.cluster, dirEntInf.clusterNumber);
			else
				writeCluster(device.getFirstSectorNumberOfCluster(dirEntInf.clusterNumber), dirEntInf.cluster);

			long clusterNumber = dirEntInf.clusterNumber;

			int index = (int)(dirEntInf.cluster.length - dirEntInf.directoryEntryIndex);
			if (dirEntInf.isRoot && device.getFatType() != FAT.FAT32)	//root directory
			{
				long lastRootDirClusterNumber = firstRootDirSecNum + MyMath.roundUp(numRootDirSectors/(float)bpb.SecPerClus);
				for (;;)
				{
					clusterNumber++;
					if (clusterNumber >= lastRootDirClusterNumber)
						throw new DirectoryException("");

					byte[] cluster = readSector(clusterNumber);
					int length = dirEntInf.directoryEntry.length-index >= cluster.length ? cluster.length : dirEntInf.directoryEntry.length - index;
					System.arraycopy(dirEntInf.directoryEntry, index, cluster, 0, length);
					writeSector(cluster, clusterNumber);
					index += length;
					if (index >= dirEntInf.directoryEntry.length)
						return;	//we are done
				}
			}
			else		//not root directory or FAT32
			{
				do
				{
					clusterNumber = device.getFatContent(clusterNumber);
					if (device.isLastCluster(clusterNumber))
						throw new DirectoryException("");

					byte[] cluster = readCluster(device.getFirstSectorNumberOfCluster(clusterNumber));
					int length = dirEntInf.directoryEntry.length - index >= cluster.length ?
						cluster.length :
						dirEntInf.directoryEntry.length - index;

					System.arraycopy(dirEntInf.directoryEntry, index, cluster, 0, length);
					writeCluster(device.getFirstSectorNumberOfCluster(clusterNumber), cluster);
					index += length;
					if (index >= dirEntInf.directoryEntry.length)
						return;	//we are done
				}while(true);
			}
		}
		else	//no boundary
		{
			System.arraycopy(dirEntInf.directoryEntry, 0, dirEntInf.cluster, (int)dirEntInf.directoryEntryIndex, dirEntInf.directoryEntry.length);
			if (dirEntInf.isRoot && device.getFatType() != FAT.FAT32)
				writeSector(dirEntInf.cluster, dirEntInf.clusterNumber);	//root directory is only accessible by sector
			else
				writeCluster(device.getFirstSectorNumberOfCluster(dirEntInf.clusterNumber), dirEntInf.cluster);
		}
	}	//end writeDirectoryEntry(...)


	/**
	 * Write the directory entry at the position given by freeInformation to disk.
	 * @param freeInformation information about directory entry.
	 * @param directoryEntry the directory entry to write.
	 */
	private void writeDirectoryEntry(FreeInformation freeInformation, byte[] directoryEntry)
	{
		List freeClusters = freeInformation.freeClusters;
		//copy first part of directory entry and write to disk
		byte[] cluster;
		if (freeInformation.cluster != null)
			cluster = freeInformation.cluster;
		else
		{
			if (freeInformation.isRoot && device.getFatType() != FAT.FAT32)
				cluster = readSector(((Long)freeClusters.get(0)).longValue());
			else
				cluster = readCluster(((Long)freeClusters.get(0)).longValue());
		}

		System.arraycopy(
			directoryEntry,
			0,
			cluster,
			freeInformation.freeEntryIndex,
			(directoryEntry.length + freeInformation.freeEntryIndex > cluster.length) ?
				cluster.length - freeInformation.freeEntryIndex :
				directoryEntry.length
		);

		if (freeInformation.isRoot && device.getFatType() != FAT.FAT32)
			writeSector(cluster, ((Long)freeClusters.get(0)).longValue());
		else
			writeCluster(((Long)freeClusters.get(0)).longValue(), cluster);

		int directoryEntryIndex = cluster.length - freeInformation.freeEntryIndex;
		int restLength = directoryEntry.length - directoryEntryIndex;

		//if directory entry spans over cluster boundary copy the rest of
		//directory entry and write the remaining cluster
		for (int i=1; i < freeClusters.size(); i++)
		{
			if (freeInformation.isRoot && device.getFatType() != FAT.FAT32)
				cluster = readSector(((Long)freeClusters.get(i)).longValue());
			else
				cluster = readCluster(((Long)freeClusters.get(i)).longValue());
			System.arraycopy(
				directoryEntry,				//source
				directoryEntryIndex,		//source index
				cluster,					//destination
				0,							//destination index
				((cluster.length > restLength) ? restLength : cluster.length)	//length
			);

			if (freeInformation.isRoot && device.getFatType() != FAT.FAT32)
				writeSector(cluster, ((Long)freeClusters.get(i)).longValue());
			else
				writeCluster(((Long)freeClusters.get(i)).longValue(), cluster);

			directoryEntryIndex += cluster.length;
			restLength -= cluster.length;
		}
	}	//end writeDirectoryEntry(FreeInformation freeInformation, byte[] directoryEntry)


	/**
	 * Create a new entry in the directory.
	 * In case the directory structure to the new entry doesn't exist a FileDoesntExist exception
	 * will be thrown. In case there is already an entry with the same name a NameAllreadyExists
	 * exception will be thrown. In case there is not enough memory to store the entry
	 * a NotEnoughMemory exception is thrown.
	 * @param directoryName is the name of the new file.
	 * @return the cluster number stored at the new entry.
	 * @throws DirectoryException in case the entry couldn't be created.
	 */
	public long createFile(String directoryName) throws DirectoryException
	{
		return createFile(directoryName, ATTR_ARCHIVE, 0);
	}	//end createFile(String directoryName)


	/**
	 * Create a new entry in the directory. The following values are initialized to the new entry:
	 * The name is the last component of directoryName, attribute, and fileSize.
	 * There will be enough space ordered to suffice the given size of the file.
	 * In case the directory structure to the new entry doesn't exist a FileDoesntExist exception
	 * will be thrown. In case there is already an entry with the same name a NameAllreadyExists
	 * exception will be thrown. In case there is not enough memory to store the entry or to suffice
	 * the given size of the file a NotEnoughMemory exception is thrown.
	 * @param directoryName is the name of the new file.
	 * @param fileSize is the size of the file of the new entry.
	 * @return the cluster number stored at the new entry.
	 * @throws DirectoryException in case the entry couldn't be created.
	 */
	public long createFile(String directoryName, long fileSize) throws DirectoryException
	{
		return createFile(directoryName, ATTR_ARCHIVE, fileSize);
	}	//end createFile(String directoryName, long fileSize)


	/**
	 * Create a new entry in the directory. The following values are initialized to the new entry:
	 * The name is the last component of directoryName, attribute, and fileSize.
	 * There will be enough space ordered to suffice the given size of the file.
	 * In case the directory structure to the new entry doesn't exist a FileDoesntExist exception
	 * will be thrown. In case there is already an entry with the same name a NameAllreadyExists
	 * exception will be thrown. In case there is not enough memory to store the entry or to suffice
	 * the given size of the file a NotEnoughMemory exception is thrown.
	 * @param directoryName is the name of the new file.
	 * @param attribute is the attribute of the new entry.
	 * @param fileSize is the size of the file of the new entry.
	 * @return the cluster number stored at the new entry.
	 * @throws DirectoryException in case the entry couldn't be created.
	 */
	public long createFile(String directoryName, byte attribute, long fileSize) throws DirectoryException
	{
		ClusterTuple ct = getCluster(StringOperations.extractPath(directoryName));
		return createFile(directoryName, attribute, fileSize, ct);
	}	//end createFile(String directoryName, byte attribute, long fileSize)


	/**
	 * Create a new entry in the directory. The following values are initialized to the new entry:
	 * The name is the last component of directoryName, attribute, and fileSize.
	 * The new entry is stored at the position given by clusterTuple. There will be enough space
	 * ordered to suffice the given size of the file.
	 * In case the directory structure to the new entry doesn't exist a FileDoesntExist exception
	 * will be thrown. In case there is already an entry with the same name a NameAllreadyExists
	 * exception will be thrown. In case there is not enough memory to store the entry or to suffice
	 * the given size of the file a NotEnoughMemory exception is thrown.
	 * @param directoryName is the name of the new file.
	 * @param attribute is the attribute of the new entry.
	 * @param fileSize is the size of the file of the new entry.
	 * @param clusterTuple contains information about the position the new entry is stored.
	 * @return the cluster number stored at the new entry.
	 * @throws DirectoryException in case the entry couldn't be created.
	 */
	private long createFile(String directoryName, byte attribute, long fileSize, ClusterTuple clusterTuple) throws DirectoryException
	{
		//get enough space to store the data of the file
		List freeClusterList = device.getFreeClusters(
			MyMath.roundUp(fileSize / ((float)bpb.SecPerClus*bpb.BytsPerSec))
		);

		return createFile(
			directoryName,
			attribute,
			fileSize,
			(freeClusterList != null && freeClusterList.size() > 0) ?
				((Long)freeClusterList.get(0)).longValue() :
				0,
			clusterTuple
		);
	}	//end createFile(String directoryName, byte attribute, long fileSize, ClusterTuple clusterTuple)


	/**
	 * Create a new entry in the directory. The following values are initialized to the new entry:
	 * The name is the last component of directoryName, attribute, fileSize, and clusterNumber.
	 * The new entry is stored at the position given by clusterTuple. There will be enough space
	 * ordered to suffice the given size of the file.
	 * In case the directory structure to the new entry doesn't exist a FileDoesntExist exception
	 * will be thrown. In case there is already an entry with the same name a NameAllreadyExists
	 * exception will be thrown. In case there is not enough memory to store the entry or to suffice
	 * the given size of the file a NotEnoughMemory exception is thrown.
	 * @param directoryName is the name of the new file.
	 * @param attribute is the attribute of the new entry.
	 * @param fileSize is the size of the file of the new entry.
	 * @param clusterNumber is the number of the cluster of the new entry.
	 * @param clusterTuple contains information about the position the new entry is stored.
	 * @return the cluster number stored at the new entry.
	 * @throws DirectoryException in case the entry couldn't be created.
	 */
	private long createFile(String directoryName,
							byte attribute,
							long fileSize,
							long clusterNumber,
							ClusterTuple clusterTuple) throws DirectoryException
	{
		if (clusterTuple == null)
			throw new FileDoesntExist("The directory structure to the file "+directoryName+" doesn't exist.");

		String fileName = StringOperations.extractFileName(directoryName);

		DirectoryEntryInformation dirEntInf = findDirectoryEntry(
			clusterTuple.clusterNumber,
			fileName,
			clusterTuple.isRoot
		);
		if (dirEntInf != null && dirEntInf.directoryEntry != null)
			throw new NameAllreadyExists(directoryName);

		DirectoryStructure dirEntry = new DirectoryStructure(
			fileName,		//name of the file that is stored at the directory entry
			clusterNumber,	//number of cluster that is stored at the directory entry
			attribute,		//attribute that is stored at the directory entry
			fileSize,		//size of file that is stored at directory entry
							//cluster number at which the directory entry is stored
			clusterTuple.clusterNumber,
			clusterTuple.isRoot	//belongs the clusterNumber to the root directory?
		);

		//get enough space to store the entry
		FreeInformation freeInformation = getFreeDirectoryEntryIndex(
			clusterTuple.clusterNumber,
			dirEntry.getNumOfEntries(),
			clusterTuple.isRoot
		);
		writeDirectoryEntry(freeInformation, dirEntry.getByte());

		return dirEntry.getClusterNumber();
	}	//end createFile(String directoryName, byte attribute, long fileSize, long clusterNumber, ClusterTuple clusterTuple)


	/**
	 * Return the directory entry information about directoryName or null in case
	 * the file doesn't exist.
	 * @param directoryName the name of the directory.
	 * @return information about the searched directory entry or null in case
	 * it doesn't exist.
	 */
	public DirectoryEntryInformation findDirectoryEntry(String directoryName)// throws FileDoesntExist
	{
		ClusterTuple clusterTuple = getCluster(StringOperations.extractPath(directoryName));
			
		if (clusterTuple == null)
			return null;
				
		//extract the name from directoryName
		String fileName = StringOperations.extractFileName(directoryName);

		return findDirectoryEntry(clusterTuple.clusterNumber, fileName, clusterTuple.isRoot);
	}	//end findDirectoryEntry(String directoryName)
	
	
	/**
	 * Find the directory entry with the given fileName in the cluster chain started at the given
	 * cluster number.
	 * @param clusterNumber the first cluster number of the cluster chain the entry should be searched.
	 * In case the clusterNumber belongs to the root directory only by FAT12 and FAT16 it has to be
	 * direct otherwise indirect.
	 * @param fileName the name of the file without the path or device name.
	 * @param isRoot indicates if the cluster number belongs to the root directory.
	 * @return information about the searched directory entry and the entry itself or null in case
	 * the searched entry doesn't exist.
	 */
	protected DirectoryEntryInformation findDirectoryEntry(long clusterNumber, String fileName, boolean isRoot)
	{	
		Iterator entriesIterator = new EntriesFilterIterator(
			new ListClusterChainEntriesIterator(
				device,
				clusterNumber,
				isRoot),
			LIST_ALL_EXCEPT_FREE
		);
		while (entriesIterator.hasNext())
		{
			DirectoryEntryInformation dirEntInf = (DirectoryEntryInformation)entriesIterator.next();
			if (getName(dirEntInf.directoryEntry).equals(fileName))
				return dirEntInf;
		}
		
		return null;
	}	//end findDirectoryEntry(byte[] cluster, String fileName)
	
	
	/**
	 * Return the information about the given path, that are the first cluster number of the cluster
	 * chain which belongs to the given path and a flag which indicates if the path points to
	 * the root directory or not. For example if path equals "\\" or "" path points to the root directory
	 * in this case the first cluster number is the first cluster number of the root directory.
	 * if the path points to an other directory the first cluster number is the cluster number
	 * of the last directory from path of directory entry.
	 * In case the path doesn't exist or the last component is not a directory null is returned.
	 * @param path the path through the directory structure.
	 * @return information about the path or null in case the path doesn't exist or it's not a directory.
	 */
	private ClusterTuple getCluster(String path)
	{
		if (path.equals(System.getProperty("file.separator")) || path.equals(""))	//return root
			return new ClusterTuple(firstRootDirSecNum, true);
		
		StringTokenizer stringTokenizer = new StringTokenizer(path, System.getProperty("file.separator"));
		
		if (!stringTokenizer.hasMoreTokens())
			return new ClusterTuple(firstRootDirSecNum, true);
		
		String str = stringTokenizer.nextToken();
						
		//start the search with the cluster of the root directory entry
		long clusterNumber = firstRootDirSecNum;
				
		DirectoryEntryInformation dirEntInf = findDirectoryEntry(clusterNumber, str, true);
		if (dirEntInf == null)
			return null;
			
		byte[] directoryEntry = dirEntInf.directoryEntry;
			
		if (isDirectory(directoryEntry))
		{
			if (stringTokenizer.hasMoreTokens())
			{
				//continue search, but from now on with cluster chain except the root directory
				return getCluster(getClusterNumber(directoryEntry), stringTokenizer);
			}
			else
			{
				//we found the entry and it is a directory
				return new ClusterTuple(getClusterNumber(directoryEntry), false);
			}
		}
		
		//sorry, but searched entry is not a directory
		return null;
	}	//end getCluster(String fileName)
	
	
	/**
	 * Is used by getCluster(String path). The method follows the path through the directory structure
	 * given by cluster number and the string tokenizer. See documentation of getCluster(String path)
	 * for details.
	 * @param fatIndex the index of the FAT.
	 * @param stringTokenizer string tokenizer which contains the directory path.
	 * @return information about the path or null in case the path doesn't exist or it's not a directory.
	 */
	private ClusterTuple getCluster(long fatIndex, StringTokenizer stringTokenizer)
	{		
		if (!stringTokenizer.hasMoreTokens())
			return null;
		
		String str = stringTokenizer.nextToken();
		DirectoryEntryInformation dirEntInf = findDirectoryEntry(fatIndex, str, false);
		if (dirEntInf == null)
			return null;
		byte[] directoryEntry = dirEntInf.directoryEntry;
				
		if (isDirectory(directoryEntry) && stringTokenizer.hasMoreTokens())
		{
			return getCluster(getClusterNumber(directoryEntry), stringTokenizer);
		}
		else if (stringTokenizer.hasMoreTokens())
			//it's not a directory but there are more tokens
			return null;
		
		if (isDirectory(directoryEntry))
		{
			//there are no more tokens and it is a directory
			return new ClusterTuple(getClusterNumber(directoryEntry), false);
		}
		else	//there are no more tokens and it is not a directory
		{
			return null;
		}
	}	//end getCluster((long clusterNumber, StringTokenizer stringTokenizer)
		

	/**
	 * Create a new directory (folder). One cluster is allocated to the directory
	 * (unless it is the root directory on a FAT16/FAT12 volume). All bytes in that
	 * cluster are initialized to 0.  There are two special entries in the first two 
	 * 32-byte
	 * directory entries of the directory (the first two 32 bytes entries in the data
	 * region of the cluster that is allocated). The first entry is the dot entry, the
	 * second entry is the dot dot entry. The dot entry points to itself and the
	 * dot dot entry points to the starting cluster of the parent of this directory.
	 * @param directoryName the name of the directory.
	 * @throws DirectoryException in case the directory couldn't be created.
	 */
	public void createDirectory(String directoryName) throws DirectoryException	
	{
		createDirectory(directoryName, ATTR_DIRECTORY);
	}	//end createDirectory(String directoryName)

	
	/**
	 * Create a new directory (folder). One cluster is allocated to the directory
	 * (unless it is the root directory on a FAT16/FAT12 volume). All bytes in that
	 * cluster are initialized to 0. There are two special entries in the first two 32-byte
	 * directory entries of the directory (the first two 32 bytes entries in the data
	 * region of the cluster that is allocated). The first entry is the dot entry, the
	 * second entry is the dot dot entry. The dot entry points to itself and the
	 * dot dot entry points to the starting cluster of the parent of this directory.
	 * @param directoryName the name of the directory.
	 * @param attributes the attributes for the new entry.
	 * @throws DirectoryException in case the directory couldn't be created.
	 */
	public void createDirectory(String directoryName, byte attributes) throws DirectoryException
	{
		createDirectory(
			getCluster(StringOperations.extractPath(directoryName)),
			directoryName,
			attributes,
			calendar.get(Calendar.HOUR_OF_DAY),
			calendar.get(Calendar.MINUTE),
			calendar.get(Calendar.SECOND),
			calendar.get(Calendar.MILLISECOND),
			calendar.get(Calendar.DAY_OF_MONTH),
			calendar.get(Calendar.MONTH) + 1,
			calendar.get(Calendar.YEAR)
		);
	}	//end createDirectory(String directoryName, byte attributes)
	

	/**
	 * Create a new directory (folder). One cluster is allocated to the directory
	 * (unless it is the root directory on a FAT16/FAT12 volume). All bytes in that
	 * cluster are initialized to 0. There are two special entries in the first two 32-byte
	 * directory entries of the directory (the first two 32 bytes entries in the data
	 * region of the cluster that is allocated). The first entry is the dot entry, the
	 * second entry is the dot dot entry. The dot entry points to itself and the
	 * dot dot entry points to the starting cluster of the parent of this directory.
	 * @param clusterTuple contains information where the new directory should be stored.
	 * @param directoryName the name of the directory.
	 * @param attributes the attributes for the new entry.
	 * @param hourOfDay the hour of the time, valid range from 1 to 24.
	 * @param minutes the minute of the time, valid range from 0 to 59.
	 * @param seconds the seconds of the time, valid range from 0 to 59.
	 * @param milliseconds the milliseconds of the time, valid range from 0 to 999.
	 * @param dayOfMonth the day of month, valid range from 1 to 31.
	 * @param monthOfYear the month of the year, valid range from 1 to 12.
	 * @param countOfYears the count of years, valid range from 1980 to 2107.
	 * @throws DirectoryException in case the directory couldn't be created.
	 */
	private void createDirectory(
			ClusterTuple clusterTuple,
			String directoryName,
			byte attributes,
			int hourOfDay,
			int minutes,
			int seconds,
			int milliseconds,
			int dayOfMonth,
			int monthOfYear,
			int countOfYears
	) throws DirectoryException
	{
		//get one cluster of space for the new directory
		List freeClusterList = device.getFreeClusters(1);
		long fatFreeIndex = ((Long)freeClusterList.get(0)).longValue();
		
		//create the directory entry, this can be done with createFile(...)		
		createFile(directoryName, attributes, 0, fatFreeIndex, clusterTuple);
		
		//now the content of the directory will be created, that is the dot and dot dot entry.
		
		//cluster number where the directory entry will be stored
		long clusterNumberOfDirectoryEntry = (
			(clusterTuple.isRoot &&device.getFatType() != FAT.FAT32) ?
				clusterTuple.clusterNumber :
				device.getFirstSectorNumberOfCluster(clusterTuple.clusterNumber)
		);
		
		//cluster number where the directory with dot and dot dot entry will be created
		long clusterNumberOfDirectory = device.getFirstSectorNumberOfCluster(fatFreeIndex);
		
		//initialize the cluster with 0 values
		byte[] clusterBuffer = new byte[bpb.SecPerClus*bpb.BytsPerSec];
		for (int i=0; i < clusterBuffer.length; i++)
			clusterBuffer[i] = 0;
				
		//create Dot-Entry
		DirectoryStructure dot = null, dotdot = null;
		try
		{
			 dot = new DirectoryStructure(	".          ",
											attributes,
											hourOfDay,
											minutes,
											seconds,
											milliseconds,
											dayOfMonth,
											monthOfYear,
											countOfYears,
											clusterNumberOfDirectory,	//cluster number stored in directory entry
																		//points to itself
											0,				//file size
											clusterNumberOfDirectory,	//start cluster number where the whole entry is stored
											false			//the clusterNumberOfDirectory does not belong to the root directory
											);


			//create Dot-Dot-Entry
			dotdot = new DirectoryStructure("..         ",
											attributes,
											hourOfDay,
											minutes,
											seconds,
											milliseconds,
											dayOfMonth,
											monthOfYear,
											countOfYears,
											//dayOfYear,
											clusterNumberOfDirectoryEntry,	//pointer to parent directory
											0,									//file size
											clusterNumberOfDirectory,	//start cluster number where the whole entry is stored
											false					//the clusterNumberOfDirectory does not belong to the root directory
											);
		}
		catch(Exception e)
		{
			System.out.println(e);
		}

		byte[] directoryEntry = dot.getByte();
		System.arraycopy(directoryEntry, 0, clusterBuffer, 0, directoryEntry.length);

		directoryEntry = dotdot.getByte();
		System.arraycopy(directoryEntry, 0, clusterBuffer, directoryEntry.length, directoryEntry.length);

		//write cluster because it's a directory and not the root directory
		writeCluster(clusterNumberOfDirectory, clusterBuffer);
	}	//end createDirectory(ClusterTuple clusterTuple,String directoryName,byte attributes,int hourOfDay,int minutes,int seconds,int milliseconds,int dayOfMonth,int monthOfYear,int countOfYears,int dayOfYear)


	/**
	 * Create root directory entry.
	 * @param attributes attributes for the root directory.
	 * @param firstRootDirSecNum the cluster number stored at the root directory
	 * entry. In case of FAT12 or fAT16 this value should be 0. In case of FAT32
	 * this value must be cluster number where the root directory is stored.
	 * @return root directory entry as byte array.
	 */
	private byte[] createRootDirectory(byte attributes, long firstRootDirSecNum)
	{
		DirectoryStructure dirStructure = null;
		try
		{
			if (device.getFatType() != FAT.FAT32)
				dirStructure = new DirectoryStructure(
					System.getProperty("file.separator"),
					attributes,
					firstRootDirSecNum,		//cn where the directory is stored
					true
				);
			else
				dirStructure = new DirectoryStructure(
					System.getProperty("file.separator"),
					firstRootDirSecNum,		//cn stored at the entry		
					attributes,				//attributes
					0,						//fileSize
					firstRootDirSecNum,		//cn where the directory is stored
					false
				);
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
		
		return dirStructure.getByte();
	}	//end createRootDirectory
	
		
	/**
	 * Check if there exists a file with the given directory name.
	 * @param directoryName the name of the file.
	 * @return true in case such a file exists.
	 */
	public boolean exists(String directoryName)
	{
		try
		{
			return getDirectoryEntry(directoryName) != null;
		}
		catch(FileDoesntExist e)
		{
			return false;
		}
	}	//end exists(String directoryName)
		
	
	/**
	 * Return the index to the first free directory entry that is found in the cluster chain
	 * started at cluster number and where the next (numOfFreeEntries - 1) are also free.
	 * If no such entry exists a DirectoryException is thrown.
	 * @param clusterNumber the first cluster number of the cluster chain.
	 * @param numOfFreeEntries the number of free continuous entries.
	 * @param isRoot indicates if the cluster number belongs to the root directory.
	 * @return information about the searched free index.
	 * @throws DirectoryException in case no such entry exists.
	 */
	private FreeInformation getFreeDirectoryEntryIndex(long clusterNumber, int numOfFreeEntries, boolean isRoot) throws DirectoryException
	{
		return getFreeDirectoryEntryIndex(
			(isRoot && device.getFatType() != FAT.FAT32) ?
				readSector(clusterNumber) :
				readCluster(device.getFirstSectorNumberOfCluster(clusterNumber)),
			clusterNumber,
			0,
			numOfFreeEntries,
			isRoot
		);
	}	//end getFreeDirectoryEntryIndex(long clusterNumber, int numOfFreeEntries, boolean isRoot)


	/**
	 * Is used by getFreeDirectoryEntryIndex(long clusterNumber, int numOfFreeEntries, boolean isRoot).
	 * See that documentation.
	 * @param cluster where the free entries are searched.
	 * @param clusterNumber cluster number of the given cluster.
	 * @param clusterIndex index inside the cluster at which the search starts.
	 * @param numOfFreeEntries the number of free entries that are searched.
	 * @param isRoot indicates if the cluster number belongs to the root directory.
	 * @return information about the searched free index.
	 * @throws DirectoryException in case no such entry exists.
	 */
	private FreeInformation getFreeDirectoryEntryIndex(
		byte[] cluster,
		long clusterNumber,
		int clusterIndex,
		int numOfFreeEntries,
		boolean isRoot) throws DirectoryException
	{
		byte[] directoryEntry = new byte[32];
		int counter;
		int startIndex;
		long startClusterNumber;

		for (; clusterIndex < cluster.length; clusterIndex += 32)
		{
			System.arraycopy(cluster, clusterIndex, directoryEntry, 0, 32);

			counter = 0;

			//if we find a free entry there are different possibilities:
			//1. this and the next free following entries are not enough to
			//support this operation.
			//2. this and the next following entries are enough to support this operation
			//3. this and the next following entries of this cluster are not enough to
			//support this operation:
			//3.1 the entries of the following cluster are enough to support this operation
			//3.2 the entries of the following cluster are not enough to support this operation.
			if (isFree(directoryEntry))
			{
				//check if there are enough entries left on this cluster to support the operation
				if (clusterIndex + 32*numOfFreeEntries <= cluster.length)
				{
					startIndex = clusterIndex;
					do
					{
						counter++;
						if (counter >= numOfFreeEntries)
						{
							List freeClusters = new LinkedList();
							freeClusters.add(
								new Long((isRoot && device.getFatType() != FAT.FAT32) ?
									clusterNumber :
									device.getFirstSectorNumberOfCluster(clusterNumber))
							);
							return new FreeInformation(startIndex, freeClusters, cluster, isRoot);
						}

						clusterIndex += 32;
						System.arraycopy(cluster, clusterIndex, directoryEntry, 0, 32);
					}
					while(isFree(directoryEntry));
				}	//end if
				else	//this cluster contains not enough (free) entries
				{
					//save the position, index and cluster where the first free entry was found
					//and copy the cluster to the buffer
					startIndex = clusterIndex;
					if (isRoot && device.getFatType() != FAT.FAT32)
					{
						startClusterNumber = clusterNumber;
						System.arraycopy(cluster, 0, sectorBuffer, 0, cluster.length);
					}
					else
					{
						startClusterNumber = device.getFirstSectorNumberOfCluster(clusterNumber);
						System.arraycopy(cluster, 0, clusterBuffer, 0, cluster.length);
					}
	
					List freeClusters = new LinkedList();
					freeClusters.add(new Long(startClusterNumber));

					do
					{
						counter++;
						if (counter >= numOfFreeEntries)
						{
							if (isRoot && device.getFatType() != FAT.FAT32)
								return new FreeInformation(startIndex, freeClusters, sectorBuffer, isRoot);
							else
								return new FreeInformation(startIndex, freeClusters, clusterBuffer, isRoot);
						}
						clusterIndex += 32;
						if (clusterIndex < cluster.length)
							System.arraycopy(cluster, clusterIndex, directoryEntry, 0, 32);
						else	//the end of actual cluster is reached
						{
							//get the next cluster
							if (isRoot && device.getFatType() != FAT.FAT32)
							{
								if ((++clusterNumber) >= firstRootDirSecNum + numRootDirSectors)
								{
									//there is not enough space in the root directory
									int numOfNeededClusters = MyMath.roundUp(((numOfFreeEntries - counter)*32.0f) / (bpb.SecPerClus*bpb.BytsPerSec));
									throw new NotEnoughMemory(0, numOfNeededClusters);
								}
								cluster = readSector(clusterNumber);	//root directory is only accessible by sector
								freeClusters.add(new Long(clusterNumber));
							}
							else	//try to get at least one cluster more of space
							{
								long fatContent = device.getFatContent(clusterNumber);
								if (device.isLastCluster(fatContent))
								{
									long numOfNeededClusters = MyMath.roundUp(((numOfFreeEntries - counter)*32.0f) / (bpb.SecPerClus*bpb.BytsPerSec));
									List moreFreeClusters = device.getFreeClusters(numOfNeededClusters, clusterNumber);
									//clear the tempBuffer
									byte[] tempBuffer = new byte[cluster.length];
									for (int l=0; l < tempBuffer.length; l++)
										tempBuffer[l] = 0;
										
									//update the entries of the list
									//they are indirect but we need direct ones for the FreeInformation object
									for (int k=0; k < moreFreeClusters.size(); k++)
									{
										Long content = (Long)moreFreeClusters.get(k);
										moreFreeClusters.set(
											k,
											new Long(device.getFirstSectorNumberOfCluster(content.longValue()))
										);

										//it's important that all of this new ordered cluster are initialized with
										//zero values, otherwise the directory contains 'ghost entries'

										//it's not the root directory of FAT12 or FAT16
										writeCluster(
											device.getFirstSectorNumberOfCluster(content.longValue()),
											tempBuffer
										);
									}
									freeClusters.addAll(moreFreeClusters);
									return new FreeInformation(startIndex, freeClusters, clusterBuffer, isRoot);
								}	//end if device.isLastCluster(...)

								clusterNumber = fatContent;
								//it's not the root directory of FAT12 or FAT16
								cluster = readCluster(device.getFirstSectorNumberOfCluster(fatContent));
								freeClusters.add(new Long(device.getFirstSectorNumberOfCluster(clusterNumber)));
							}

							System.arraycopy(cluster, 0, directoryEntry, 0, 32);
							clusterIndex = 0;
						}		//end else we need the next cluster
					}
					while(isFree(directoryEntry));

					//if we reach this part, the operation could not be supported
					//start a new search at the actual cluster
					return getFreeDirectoryEntryIndex(
						cluster,
						clusterNumber,
						clusterIndex,		//we searched this cluster already until this index
						numOfFreeEntries,
						isRoot
					);
				}	//end else: this cluster contains not enough (free) entries
			}	//end if(isFree(directoryEntry))
		}		//end for all entries of the cluster

		if (isRoot && device.getFatType() != FAT.FAT32)
		{
			if ((++clusterNumber) >= firstRootDirSecNum + numRootDirSectors)
				throw new NotEnoughMemory("There is no more space for an other entry in this directory");
			return getFreeDirectoryEntryIndex(
				readSector(clusterNumber),
				clusterNumber,
				0,
				numOfFreeEntries,
				isRoot
			);

		}
		else
		{
			long fatContent = device.getFatContent(clusterNumber);
			
			//if the fatContent points to the EOC_MARK then we reached the
			//end of this directory, the only thing to do now is to order
			//enough space to support this operation. In case there is not
			//enough space left a NotEnoughMemoryException is thron.
			if (device.isLastCluster(fatContent))
			{
				long numOfNeededClusters = MyMath.roundUp((numOfFreeEntries*32.0f) / (bpb.SecPerClus*bpb.BytsPerSec));
				List moreFreeClusters = device.getFreeClusters(numOfNeededClusters, clusterNumber);
				//clear the tempBuffer
				byte[] tempBuffer = new byte[bpb.SecPerClus*bpb.BytsPerSec];
				for (int l=0; l < tempBuffer.length; l++)
					tempBuffer[l] = 0;
					
				//update the entries of the list
				//they are indirect but we need direct ones for the FreeInformation object
				for (int k=0; k < moreFreeClusters.size(); k++)
				{
					Long content = (Long)moreFreeClusters.get(k);
					moreFreeClusters.set(
						k,
						new Long(device.getFirstSectorNumberOfCluster(content.longValue()))
					);

					//it's important that all of this new ordered cluster are initialized with
					//zero values, otherwise the directory contains 'ghost entries'

					//it's not the root directory of FAT12 or FAT16
					writeCluster(
						device.getFirstSectorNumberOfCluster(content.longValue()),
						tempBuffer
					);
				}
				return new FreeInformation(0, moreFreeClusters, tempBuffer, isRoot);
			}

			return getFreeDirectoryEntryIndex(
				//it's not the root directory of FAT12 or FAT16
				readCluster(device.getFirstSectorNumberOfCluster(fatContent)),
				fatContent,
				0,
				numOfFreeEntries,
				isRoot
			);
		}
	}	//end getFreeDirectoryEntryIndex(byte[] cluster, long clusterNumber, int clusterIndex, int numOfFreeEntries, boolean isRoot)


	/**
	 * Check if the given fileName is a directory or not. If the file name
	 * doesn't exist the method will return false.
	 * @param fileName the name of the directory
	 * @return true is the fileName is a directory otherwise return false.
	 */
	public boolean isDirectory(String fileName)
	{
		try
		{
			return isDirectory(getDirectoryEntry(fileName));
		}
		catch(FileDoesntExist e)
		{
			return false;
		}
	}	//end isDirectory(fileName)
	
	
	/**
	 * Check if the given fileName is a file or not. If the file name 
	 * doesn't exist the method will return false.
	 * @param fileName the name of the file.
	 * @return true is the fileName is a file otherwise return false.
	 */
	public boolean isFile(String fileName)
	{
		try
		{
			return isFile(getDirectoryEntry(fileName));
		}
		catch(FileDoesntExist e)
		{
			return false;
		}
	}	//end isFile(fileName)
	
	
	/**
	 * Check if the given file name is in 8.3 naming convention.
	 * @param fileName the name of the file.
	 * @return true in case the file name is in 8.3 naming convention.
	 */
	public static boolean isShortName(String fileName)
	{
		//check if long name fits in a 8.3 naming convention
		int dotIndex = fileName.lastIndexOf(".");
		if (dotIndex >= 0)
		{
			String firstPart = fileName.substring(0, dotIndex);
			String secondPart = fileName.substring(dotIndex + 1, fileName.length());
			return 	(	!(containsIllicitValues(firstPart.getBytes())) &&
						(firstPart.length() <= 8) &&
						!(containsIllicitValues(secondPart.getBytes())) &&
						(secondPart.length() <= 3)
					);
		}
		else
		{
			return (!(containsIllicitValues(fileName.getBytes())) && (fileName.length() <= 8));
		}
	}	//end isShortName(String fileName)
	
	
	/**
	 * Return true if the byte array contains illegal values. That are
	 * all values smaller than 0x20 except 0x05 and also all values
	 * that are in the illicit-set.
	 * @param name the name.
	 * @return true if the byte array contains an illegal value otherwise
	 * false.
	 */
	public static boolean containsIllicitValues(byte[] name)
	{
		for (int i=0; i < name.length; i++)
		{
			if (name[i]	< 0x20 && name[i] != 0x05)
				return true;

			if (illicitValues.contains(new Short(ByteArrayConversionsLittleEndian.convShort(name[i]))))
				return true;
		}

		return false;
	}	//containsIllegalValues(byte[] name)


	/**
	 * Return the recreated file name from a string stored as a directoryEntry.
	 * This method will always create a dot in the returned string so don't use
	 * this method with files that have the directory attribute set. The dot and
	 * dot dot strings will be returned without blanks at the end.
	 * @param buffer holds the information of the file name.
	 * @param from the start index inclusive.
	 * @param to the end index exclusive.
	 * @return the file name in 8.3 naming convention.
	 */
	public static String recreateFileName(byte[] buffer, int from, int to)
	{
		String str = ByteArrayConversionsLittleEndian.byteToString(buffer, from, to);
		if (str.equals(".          "))
			return ".";
		else if (str.equals("..         "))
			return "..";
		
		String firstPart = str.substring(0, 8);
		String lastPart = str.substring(8, 11);
		
		//clear all last blanks of both substrings
		for (int i=0; i < 3; i++)
			if (lastPart.endsWith(" "))
				lastPart = lastPart.substring(0, lastPart.length() - 1);
			else
				break;
				
		for (int i=0; i < 8; i++)
			if (firstPart.endsWith(" "))
				firstPart = firstPart.substring(0, firstPart.length() - 1);
			else
				break;
					
		return firstPart + "." + lastPart;
	}	//end getFileName(byte[] buffer, int from, int to)
	
	
	/**
	 * Return the directory entry which matches the given file name.
	 * @param fileName the name of the file.
	 * @return byte array representation of the directory entry.
	 * @throws FileDoesntExist exception in case the file doesn't exist.
	 */
	public byte[] getDirectoryEntry(String fileName) throws FileDoesntExist
	{
		DirectoryEntryInformation dirEntInf = findDirectoryEntry(fileName);
		if (dirEntInf != null && dirEntInf.directoryEntry != null)
			return dirEntInf.directoryEntry;

		throw new FileDoesntExist(fileName);
	}	//end getDirectoryEntry(fileName)
	
	
	/**
	 * Return the clusterNumber of the directory entry specified
	 * by fileName.
	 * @param fileName the name of the file.
	 * @return the cluster number.
	 * @throws FileDoesntExist exception in case the file doesn't exist.
	 */
	public long getClusterNumber(String fileName) throws FileDoesntExist
	{		
		return getClusterNumber(getDirectoryEntry(fileName));
	}	//end getClusterNumber(String fileName)
	

	/**
	 * Update the cluster number of the file given by directory name with the given 
	 * cluster number. In case the file doesn't exist a FileDoesntExist exception
	 * will be thrown.
	 * @param directoryName the name of the file.
	 * @param clusterNumber the new cluster number.
	 * @throws DirectoryException in case of a directory error.
	 */	
	protected void setClusterNumber(String directoryName, long clusterNumber) throws DirectoryException
	{
		DirectoryEntryInformation dirEntInf = findDirectoryEntry(directoryName);
		if (dirEntInf == null || dirEntInf.directoryEntry == null)
			throw new FileDoesntExist(directoryName);
		
		setClusterNumber(dirEntInf.directoryEntry, clusterNumber);
		writeDirectoryEntry(dirEntInf);
	}	//end setClusterNumber(String directoryName, long clusterNumber)
	
	
	/**
	 * Return the length of the file given by file name. If the file doesn't exist
	 * 0 is returned.
	 * @param fileName the name of the file.
	 * @return the length of the file, if it doesn't exist 0 is returned.
	 */
	public long length(String fileName)// throws FileDoesntExist
	{		
		try
		{		
		 	return getFileLength(getDirectoryEntry(fileName));
		}
		catch(Exception e)
		{
			return 0;
		}
	}	//end getSize(String fileName)
	
	
	/**
	 * Return the last write time stored at the directory entry given
	 * by the fileName. The returned value is the number of milliseconds
	 * since January 1, 1970, 00:00:00 GMT.
	 * @param fileName the name of the file.
	 * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 * until the last write time.
	 */
	public long getLastWriteTime(String fileName)
	{
		try
		{
			byte[] directoryEntry = getDirectoryEntry(fileName);
			DirectoryTime time = getWriteTime(directoryEntry);
			DirectoryDate date = getWriteDate(directoryEntry);
			Calendar temp = new GregorianCalendar(
				date.year,
				date.month - 1,
				date.day,
				time.hour,
				time.minute,
				time.second
			);
			return temp.getTime().getTime();	//access to Date first and from there to getTime() to return the long value
		}
		catch (FileDoesntExist e)
		{
			return 0;
		}
	}	//end getLastWriteTime(String fileName)
	
	
	/**
	 * Return the creation time stored at the directory entry given
	 * by the fileName. The returned value is the number of milliseconds
	 * since January 1, 1970, 00:00:00 GMT.
	 * @param fileName the name of the file.
	 * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT,
	 * until the creation time.
	 */	
	public long getCreationTime(String fileName)
	{
		try
		{
			byte[] directoryEntry = getDirectoryEntry(fileName);
			DirectoryTime time = getCreationTime(directoryEntry);
			DirectoryDate date = getCreationDate(directoryEntry);
			Calendar temp = new GregorianCalendar(
				date.year,
				date.month - 1,
				date.day,
				time.hour,
				time.minute,
				time.second
			);
			return temp.getTime().getTime();	//access to Date first and from there to getTime() to return the long value
		}
		catch (FileDoesntExist e)
		{
			return 0;
		}
	}	//end getCreationTime(String fileName)
	
	
	/**
	 * Returns the attribute stored at the directory entry given by fileName.
	 * @param fileName the name of the file.
	 * @return the attribute of the directory entry given by fileName.
	 */
	public byte getAttribute(String fileName)
	{
		try
		{
			byte[] directoryEntry = getDirectoryEntry(fileName);
			return getAttribute(directoryEntry);
		}
		catch (FileDoesntExist e)
		{
			return 0;
		}
	}
	
	
	/**
	 * Renames the file denoted by directoryName.
	 * @param directoryName the name of the file to rename
	 * @param newName the new name of the file.
	 * @return true if and only if the renaming succeeded; false otherwise.
	 */
	protected boolean renameTo(String directoryName, String newName)
	{
		DirectoryEntryInformation dirEntInf1 = null;
		try
		{			
			String path = StringOperations.extractPath(directoryName);
							
			//check if the path exists
			ClusterTuple clusterTuple = getCluster(path);
			if (clusterTuple == null)
				return false;
				
			String fileName = StringOperations.extractFileName(directoryName);
			
			//extract path and name of newName
			String path2 = StringOperations.extractPath(newName);
			String newFileName = StringOperations.extractFileName(newName);
			
			//get directory entry of the file directoryName
			dirEntInf1 = findDirectoryEntry(
				clusterTuple.clusterNumber,
				fileName,
				clusterTuple.isRoot
			);
			if (dirEntInf1 == null || dirEntInf1.directoryEntry == null)
				return false;
									
			//create the directory structure to newName in case it doesn't exist.
			StringTokenizer stringTokenizer = new StringTokenizer(path2, System.getProperty("file.separator"));
			String tempPath = "";
			while (stringTokenizer.hasMoreTokens())
			{
				tempPath += stringTokenizer.nextToken() + System.getProperty("file.separator");
				if (!exists(tempPath))
					createDirectory(tempPath);					
			}
			
			//remove directoryName must be done before the creation of the new entry
			//otherwise it might be that the new entry will be deleted also.
			if (!isDirectory(dirEntInf1.directoryEntry) ||
							//the content of a directory is never stored at the root directory
							//except for the root directory itself, but this directory is not
							//deletable
				(isDirectory(dirEntInf1.directoryEntry) && 
				directoryIsEmpty(getClusterNumber(dirEntInf1.directoryEntry), false)))	
			{
				//mark the directory entry as free
				setFree(dirEntInf1.directoryEntry);
				writeDirectoryEntry(dirEntInf1);
			}
						
			//get the clusterTuple for the destination file
			ClusterTuple clusterTuple2 = getCluster(path2);
															
			//create a new directory structure entry 
			DirectoryTime creationTime = getCreationTime(dirEntInf1.directoryEntry);
			DirectoryDate creationDate = getCreationDate(dirEntInf1.directoryEntry);
			short creationTimeTenth = getCreationTimeTenth(dirEntInf1.directoryEntry);
										
			//if the old entry was a directory we have to create new dot and dot dot entries.
			if (isDirectory(dirEntInf1.directoryEntry))
			{
				createDirectory(
					clusterTuple2,
					newName,
					ATTR_DIRECTORY,
					creationTime.hour,
					creationTime.minute,
					creationTime.second,
					creationTimeTenth,
					creationDate.day,
					creationDate.month,
					creationDate.year
				);
			}
			else
			{			
				DirectoryStructure dirEnt = new DirectoryStructure(
					newFileName, 
					getAttribute(dirEntInf1.directoryEntry),
					creationTime.hour,
					creationTime.minute,
					creationTime.second,
					getCreationTimeTenth(dirEntInf1.directoryEntry),
					creationDate.day,
					creationDate.month,
					creationDate.year,
					getClusterNumber(dirEntInf1.directoryEntry),	//the cluster number stored at the new entry
					getFileLength(dirEntInf1.directoryEntry),
					clusterTuple2.clusterNumber,	//the cluster number where the new entry should be stored
					clusterTuple2.isRoot
				);
				
				//order space to store the new entry
				FreeInformation freeInformation = getFreeDirectoryEntryIndex(
					clusterTuple2.clusterNumber,
					dirEnt.getNumOfEntries(),
					clusterTuple2.isRoot
				);
				
				//write the new entry to disk
				writeDirectoryEntry(freeInformation, dirEnt.getByte());
			}
		}
		catch(FileDoesntExist e)
		{
			return false;
		}
		catch(NotEnoughMemory e)
		{
			try
			{
				if (dirEntInf1 != null)
					writeDirectoryEntry(dirEntInf1);
			}
			catch (Exception exe)
			{
			}
			return false;
		}
		catch(IllegalName e)
		{
			return false;
		}
		catch(DirectoryException e)
		{
			try
			{
				if (dirEntInf1 != null)
					writeDirectoryEntry(dirEntInf1);
			}
			catch (Exception exe)
			{
			}
			return false;
		}
					
		return true;
	}	//end renameTo(...)


	/**
	 * Deletes the file or directory denoted by the given filename. If
	 * this pathname denotes a directory, then the directory must be empty in
	 * order to be deleted.
	 *
	 * @param fileName the given filename
	 * @return true if and only if the file or directory is successfully
	 * deleted; false otherwise.
	 */
	public boolean delete(String fileName)
	{
		return delete(findDirectoryEntry(fileName));
	}	//end delete(String fileName)


	/**
	 * Deletes the file or directory denoted by dirEntInf. If dirEntInf
	 * denotes a directory, then the directory must be empty in
	 * order to be deleted.
	 *
	 * @param dirEntInf an information denoting the file or directory to be
	 *        deleted.
	 * @return true if and only if the file or directory is successfully
	 * deleted; false otherwise.
	 */
	private boolean delete(DirectoryEntryInformation dirEntInf)
	{
		try
		{
			if (dirEntInf == null || dirEntInf.directoryEntry == null)
			{
				return false;
			}
			if (isDirectory(dirEntInf.directoryEntry) &&
							//the content of a directory is never stored at the root directory
							//except for the root directory itself, but this directory is not
							//deletable
				!directoryIsEmpty(getClusterNumber(dirEntInf.directoryEntry), false))	
			{
				return false;
			}

			//mark the directory entry as free
			setFree(dirEntInf.directoryEntry);
			writeDirectoryEntry(dirEntInf);

			//mark all FAT entries that are used by the directory or file as free
			long clusterNumber = getClusterNumber(dirEntInf.directoryEntry);
			if (clusterNumber >= 2)
				device.addFreeClusters(clusterNumber);

			return true;
		}
		catch(DirectoryException e)
		{
			return false;
		}
	}	//end delete(String fileName)
	
	
	/**
	 * Return true if the file associated with the given file name is hidden.
	 * If no such file exists false is returned.
	 * @param fileName the name of the file.
	 * @return true if the file is marked as hidden.
	 */
	public boolean isHidden(String fileName)
	{
		try
		{
			return isHidden(getDirectoryEntry(fileName));
		}
		catch(FileDoesntExist e)
		{
			return false;
		}		
	}	//end isHidden(fileName)
	
	
	/**
	 * Return true if the file associated with the given file name is read only.
	 * If no such file exists true is returned.
	 * @param fileName the name of the file.
	 * @return true if the file is marked as read only or if the file doesn't exist.
	 */
	public boolean isReadOnly(String fileName)
	{
		try
		{
			return isReadOnly(getDirectoryEntry(fileName));
		}
		catch(FileDoesntExist e)
		{
			return true;
		}		
	}	//end isReadOnly(fileName)
	

	/**
	 * Set the last write time of the directory entry given by
	 * fileName.
	 * @param fileName the name of the file.
	 * @param time is the last write time in milliseconds since
	 * January 1, 1970, 00:00:00 GMT.
	 * @return true if the last write time could be set otherwise false.
	 */
	protected boolean setLastWriteTime(String fileName, long time)
	{
		try
		{
			DirectoryEntryInformation dirEntInf = findDirectoryEntry(fileName);
			if (dirEntInf == null || dirEntInf.directoryEntry == null)
				return false;
		
			setWriteTime(dirEntInf.directoryEntry, time);
			writeDirectoryEntry(dirEntInf);
			
			return true;
		}
		catch(DirectoryException e)
		{
			return false;
		}
	}	//end setLastWriteTime(String fileName, long time)
	
	
	/**
	 * Set the last write date of the directory entry given by fileName.
	 * @param fileName the name of the file.
	 * @param year the year of the last write date.
	 * @param month the month of the last write date.
	 * @param day the day of the last write date.
	 * @return true if the last date could be set; false otherwise.
	 */
	protected boolean setLastWriteDate(String fileName, int year, int month, int day)
	{
		try
		{
			DirectoryEntryInformation dirEntInf = findDirectoryEntry(fileName);
			if (dirEntInf == null || dirEntInf.directoryEntry == null)
				return false;
		
			setLastWriteDate(dirEntInf.directoryEntry, year, month, day);
			writeDirectoryEntry(dirEntInf);
			
			return true;
		}
		catch(DirectoryException e)
		{
			return false;
		}
	}	//end setLastWriteDate(String fileName, int year, int month, int day)

	
	/**
	 * Set the last access date of the directory entry given by fileName.
	 * @param fileName the name of the file.
	 * @param year the year of the last write date.
	 * @param month the month of the last write date.
	 * @param day the day of the last write date.
	 * @return true if the last access date could be set; false otherwise.
	 */
	protected boolean setLastAccessDate(String fileName, int year, int month, int day)
	{
		try
		{
			DirectoryEntryInformation dirEntInf = findDirectoryEntry(fileName);
			if (dirEntInf == null || dirEntInf.directoryEntry == null)
				return false;
		
			setLastAccessDate(dirEntInf.directoryEntry, year, month, day);
			writeDirectoryEntry(dirEntInf);
			
			return true;
		}
		catch(DirectoryException e)
		{
			return false;
		}
	}	//end setLastAccessDate(String fileName, int year, int month, int day)
	

	/**
	 * Update the length of the file associated by the given file name.
	 * @param fileName the name of the file.
	 * @param fileLength the new length.
	 * @return true in case the length could be set; false otherwise.
	 */
	protected boolean writeLength(String fileName, long fileLength)// throws DirectoryException
	{
		try
		{
			DirectoryEntryInformation dirEntInf = findDirectoryEntry(fileName);
		
			if (dirEntInf == null || dirEntInf.directoryEntry == null)
				return false;
						
			setFileLength(dirEntInf.directoryEntry, fileLength);
			writeDirectoryEntry(dirEntInf);
			return true;
		}
		catch(DirectoryException e)
		{
			return false;
		}
	}	//end writeLength(String fileName, long fileLength)
	
	
	/**
	 * Returns an array of strings naming the files and directories in the
	 * directory denoted by this fileName.
	 *
	 * If this fileName does not denote a directory, then this
	 * method returns null. Otherwise an array of strings is
	 * returned, one for each file or directory in the directory. Names
	 * denoting the directory itself and the directory's parent directory are
	 * not included in the result. Each string is a file name rather than a
	 * complete path.
	 *
	 * There is no guarantee that the name strings in the resulting array
	 * will appear in any specific order; they are not, in particular,
	 * guaranteed to appear in alphabetical order.
	 *
	 * @param fileName the given filename
	 * @return  An array of strings naming the files and directories in the
	 *          directory denoted by this fileName.  The array will be
	 *          empty if the directory is empty.  Returns null if
	 *          this fileName does not denote a directory, or if an
	 *          I/O error occurs.
	 *
	 */
	public String[] list(String fileName)
	{
		try
		{
			if (fileName.equals(System.getProperty("file.separator")) || fileName.equals(""))
				return listRoots();
				
			byte[] directoryEntry = getDirectoryEntry(fileName);
			if (!isDirectory(directoryEntry))
				return null;
				
			long clusterNumber = getClusterNumber(directoryEntry);
			
			ArrayList result = new ArrayList();
			Iterator entriesIterator = new EntriesFilterIterator(
				new ListClusterChainEntriesIterator(device,
													clusterNumber,
													false	//it can not be the root directory
													),
				LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY
			); 
			
			while(entriesIterator.hasNext())
			{
				DirectoryEntryInformation dirEntInf = (DirectoryEntryInformation)entriesIterator.next();
				result.add(getName(dirEntInf.directoryEntry));
			}
			
			return (String[])(result.toArray(new String[0]));
		}
		catch(FileDoesntExist e)
		{
			return null;
		}
	}	//end list(String fileName)


	/**
	 * List the files of the root directory.
	 * @return an array of strings denoting the files and directories of the root
	 * directory or an empty array in case there is no entry in the root directory.
	 */
	public String[] listRoots()
	{
		ArrayList result = new ArrayList();
		Iterator entriesIterator = new EntriesFilterIterator(
			new ListClusterChainEntriesIterator(
				device,
				firstRootDirSecNum,
				true
			),
			LIST_DIRECTORY_ENTRY | LIST_FILE_ENTRY
		); 

		while(entriesIterator.hasNext())
		{
			DirectoryEntryInformation dirEntInf = (DirectoryEntryInformation)entriesIterator.next();
			result.add(getName(dirEntInf.directoryEntry));
		}
		
		return (String[])(result.toArray(new String[0]));
	}	//end listRoots()
		
	
	/**
	 * Return a short name made of the given long name for the directory
	 * that is associated with the given cluster number. In case there are
	 * more than one short names in a directory that equals each other they
	 * get a numeric tail in order to distinguish them. Of course this
	 * is only possible if the long names for this short names are not equal
	 * to each other. In case the short name is unique for this directory and
	 * the longname fits the 8.3 naming convention (except the upper case rule)
	 * there will be no numeric tail generated.
	 * @param longName the long name.
	 * @param clusterIterator iterator that iterates over the cluster chain where
	 * the the given longName is stored.
	 * @return the unique short name of the given long name for the directory given
	 * by clusterIterator.
	 */
	protected String numericTailGeneration(String longName, Iterator clusterIterator)
	{
		String basisName = getBasisName(longName);

		//copy the cluster chain in one byte array
		LinkedList clusterList = new LinkedList();
		while(clusterIterator.hasNext())
			clusterList.addLast(clusterIterator.next());

		if (clusterList.size() < 1)
			return basisName;	//cluster list is empty -> no collision between names

		int clusterLength = ((byte[])clusterList.get(0)).length;
		byte[] cluster = new byte[clusterLength*clusterList.size()];
		for (int i=0; i < clusterList.size(); i++)
		{
			System.arraycopy(
				(clusterList.get(i)),
				 0,
				 cluster,
				 i*clusterLength,
				 clusterLength
			);
		}

		//search basis-name in cluster and find a free numeric tail
		//in case the basis-name exist
		BitSet usedNumbers = new BitSet(20);
		byte[] directoryEntry = new byte[32];	//findDirectoryEntry(cluster, basisName);
		String nameTmp;
		boolean nameExist = false;
		for (int i=0; i < cluster.length; i += 32)
		{
			System.arraycopy(cluster, i, directoryEntry, 0, 32);
			nameTmp = getName(directoryEntry);
			if (nameTmp.equals(basisName) && !isLongEntry(directoryEntry))
			{
				usedNumbers.set((int)StringOperations.extractNumericTail(nameTmp));
				nameExist = true;
			}
		}

		int freeNumber = 1;
		int numberOfDigits = 1;
		if (!nameExist)
		{
			if (isShortName(longName))
				return longName;	//the short name is only the basis-name without the numeric tail
		}
		else	//allright there is an other entry with the same short name
		{
			//search the usedNumbers for a free number
			for (; freeNumber < usedNumbers.size(); freeNumber++)
				if (!usedNumbers.get(freeNumber))
					break;	//we found a free number

			//calculate how many digits freeNumber has
			while(freeNumber % (10*numberOfDigits) != freeNumber)
				numberOfDigits++;
		}

		//insert the number in the basis name so that the naming convention holds

		//split basis-name in two strings

		int dotIndex = basisName.indexOf(".", 0);
		String firstPart = "";
		String fileExtension = "";
		if (dotIndex != -1)
		{
			firstPart = basisName.substring(0, dotIndex);
			fileExtension = basisName.substring(dotIndex, basisName.length());	//inclusive dot
		}
		else
			firstPart = basisName.substring(0);

		if (8 - numberOfDigits - 1 > firstPart.length())
			return firstPart + "~" + new Long(freeNumber).toString() + fileExtension;
		else
			return firstPart.substring(0, 8-numberOfDigits-1) + "~" + new Long(freeNumber).toString() + fileExtension;

	}	//end numericTailGeneration(String longName, Iterator clusterIterator)


	/**
	 * Create the short name (8.3 name) from a given long name.
	 * @param longName the name of a file or directory for which the
	 * 8.3 naming convention doesn't hold.
	 * @return the basis name of the given longName.
	 */
	public static String getBasisName(String longName)
	{
		//UNICODE name is converted to upper case
		//longName.toUpperCase();
		longName = longName.toUpperCase();

		//normally at this point the long name has to be converted
		//from UNICODE to OEM, but this can not be done, since
		//then all the other operations will go wrong.
		//Therefore we do this step at the end.

		//strip all leading and embedded spaces from the long name.
		//search the last character that is not a space.
		int i = longName.length()-1;
		for (; i >= 0 ; i--)
			if (!longName.substring(i, i+1).equals(" "))
				break;
		String firstPart = longName.substring(0, i+1);
		String secondPart = longName.substring(i+1, longName.length());
		//remove all spaces from firstPart
		StringTokenizer stringTokenizer = new StringTokenizer(firstPart, " ");
		firstPart = "";
		while (stringTokenizer.hasMoreTokens())
		{
			firstPart += stringTokenizer.nextToken();
		}
		longName = firstPart+secondPart;	//concatenate the strings

		//strip all leading periods (.) from the long name
		int lastIndex = longName.lastIndexOf(".");
		if (lastIndex != -1)
		{
			firstPart = longName.substring(0, lastIndex);
			secondPart = longName.substring(lastIndex, longName.length());
			//remove all periods in the firstPart
			longName = "";
			stringTokenizer = new StringTokenizer(firstPart, ".");
			while (stringTokenizer.hasMoreTokens())
			{
				longName += stringTokenizer.nextToken();
			}
			longName += secondPart;
		}

		//copy the first eight characters to the result
		String basisName = "";
		for (i=0; i < 8 && i < longName.length(); i++)
		{
			if (longName.substring(i, i+1).equals("."))
				break;
			basisName += longName.substring(i, i+1);
		}

		//insert a dot at the end of the primary components of the basis-name
		//iff the basis (long?) name has an extension after the last period
		//in the name
		if (longName.lastIndexOf(".") != -1 && !longName.endsWith("."))
		{
			basisName += ".";

			//copy the extension from the long name to the basis name
			int start = longName.lastIndexOf(".") + 1;
			for (i=start; i < longName.length() && i < start + 3; i++)
			{
				basisName += longName.substring(i, i+1);
			}
		}

		int index = basisName.lastIndexOf(".");
		firstPart = basisName.substring(0, (index != -1) ? index : basisName.length());
		String lastPart = "";
		if (index != -1)
		{
			lastPart = basisName.substring(index + 1, basisName.length());
			basisName = convertToOEM(firstPart) + "." + convertToOEM(lastPart);
		}
		else
			basisName = convertToOEM(firstPart);

		return basisName.toUpperCase();
	}	//end getBasisName(String longName)


	/**
	 * Converts the given string to OEM. All characters that are
	 * not allowed to be in a short name are replaced by an
	 * underscore '_'.
	 * @param str the String to convert.
	 * @return the converted String.
	 */
	public static String convertToOEM(String str)
	{
		//convert UNICODE to OEM
		String result = "";
		byte[] tempArr = str.getBytes();
		short[] tempName = new short[tempArr.length];
		for (int i=0; i < tempArr.length; i++)
		{
			char c = str.charAt(i);
			tempName[i] = ByteArrayConversionsLittleEndian.convShort(tempArr[i]);
			if ((tempName[i] < 0x20 && tempName[i] != 0x05) ||
				DIR.illicitValues.contains( new Short(tempName[i]) ))
				c = '_';
			result += c;
		}
		return result;
	}	//end convertToOEM(String str)


	/**
	 * Returns the checksum of the given name. The checksum is stored in long directory
	 * entries. The name must have a length of 11 and it must be in the format of a
	 * MS-DOS directory entry. See the paper: "Hardware White Paper. Microsoft Extensible
	 * Firmware Initiative FAT32 File System specification. Version 1.03 DEcember 6, 2000"
	 * for more details.
	 * @param name assumed to be 11 bytes long.
	 * @return the checksum.
	 */
	public static short checksum(byte[] name)
	{
		//even the checksum is only a byte the calculation must be
		//with short otherwise the result is wrong but the calculation
		//doesn't use more than 8 bit to store the temporary result.
		short sum = 0;
		short tmp;
		for (int i=0; i < 11; i++)
		{
			if ((sum & 1) == 1)
				tmp = 0x80;
			else
				tmp = 0;
			
			sum = (short)((tmp + (sum >> 1) + name[i]) & 0xFF);
		}
		return sum;
	}	//end checksum(byte[] name)
		
	
	/**
	 * Check if the directory given by clusterNumber is empty.
	 * @param clusterNumber the number of the cluster.
	 * @param isRoot indicates if the clusterNumber belongs to the root directory.
	 * @return return true if the directory is empty; false otherwise.
	 */
	protected boolean directoryIsEmpty(long clusterNumber, boolean isRoot)
	{
		Iterator entriesIterator = new EntriesFilterIterator(
			new ListClusterChainEntriesIterator(
				device,
				clusterNumber,
				isRoot
			),
			LIST_ALL_EXCEPT_FREE
		);
		int counter = 0;
		while (entriesIterator.hasNext())
		{
			//DirectoryEntryInformation dirEntInf = (DirectoryEntryInformation)entriesIterator.next();
			entriesIterator.next();

			//you might want to check if the entries are something else than dot and dot dot
			counter++;
		}
		return counter == 2;
	}


	////////////////////////////////////
	//                                //
	// byte[] directoryEntry methods  //
	//                                //
	////////////////////////////////////
		
	
	/**
	 * Set the cluster number of the given directory entry to the given cluster number.
	 * The method will only update the directory entry, but will not
	 * save the information in the directory structure.
	 * @param directoryEntry the given directory entry.
	 * @param clusterNumber the new cluster number.
	 */
	public static void setClusterNumber(byte[] directoryEntry, long clusterNumber)
	{
		//firstClusterHI
		directoryEntry[directoryEntry.length-32+20] = (byte)(clusterNumber >> 16);
		directoryEntry[directoryEntry.length-32+21] = (byte)(clusterNumber >> 24);
					
		//firstClusterLO
		directoryEntry[directoryEntry.length-32+26] = (byte)(clusterNumber);
		directoryEntry[directoryEntry.length-32+27] = (byte)(clusterNumber >> 8);
	}	//end setClusterNumber(byte[] directoryEntry, long clusterNumber)
	
	
	/**
	 * Return the cluster number of the given directoryEntry.
	 * @param directoryEntry the given directory entry.
	 * @return the cluster number stored in directoryEntry.
	 */
	public static long getClusterNumber(byte[] directoryEntry)
	{
		long firstClusterHI = ByteArrayConversionsLittleEndian.convInt(
			directoryEntry[directoryEntry.length-32+20],
			directoryEntry[directoryEntry.length-32+21]
		);
		long firstClusterLO = ByteArrayConversionsLittleEndian.convInt(
			directoryEntry[directoryEntry.length-32+26],
			directoryEntry[directoryEntry.length-32+27]
		);
		return firstClusterLO | (firstClusterHI << 16);
	}	//end getClusterNumber(directoryEntry)
		
	
	/**
	 * Return the creation time of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return DirectoryTime contains the hour, minute, and second the entry was made.
	 */
	public static DirectoryTime getCreationTime(byte[] directoryEntry)
	{
		short index14 = (short)(directoryEntry.length - 32 + 14);
		short index15 = (short)(directoryEntry.length - 32 + 15);
		byte hour = (byte)(ByteArrayConversionsLittleEndian.convShort(directoryEntry[index15]) >> 3);
		int tmp = ByteArrayConversionsLittleEndian.convInt(directoryEntry[index14], directoryEntry[index15]);
		byte minute = (byte)((tmp >> 5) & 0x003F);
		byte second = (byte)((directoryEntry[index14] & 0x1F) << 1);
		
		return new DirectoryTime((byte)(hour + 1), minute, second);
	}	//end getCreationTime(byte[] directoryEntry)
	
	
	/**
	 * Return the creation time tenth (of a second) of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return the tenth of the second as the entry was made.
	 */
	public static short getCreationTimeTenth(byte[] directoryEntry)
	{
		//index 13
		return (short)((ByteArrayConversionsLittleEndian.convShort(directoryEntry[directoryEntry.length - 32 + 13]) * 10) / 2);
	}	//end getCreationTimeTenth(byte[] directoryEntry)
	
	
	/**
	 * Return the last write time of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return DirectoryTime contains the hour, minute, and second of the last write access.
	 */
	public static DirectoryTime getWriteTime(byte[] directoryEntry)
	{
		short index22 = (short)(directoryEntry.length - 32 + 22);
		short index23 = (short)(directoryEntry.length - 32 + 23);
		byte hour = (byte)(ByteArrayConversionsLittleEndian.convShort(directoryEntry[index23]) >> 3);
		int tmp = ByteArrayConversionsLittleEndian.convInt(directoryEntry[index22], directoryEntry[index23]);
		byte minute = (byte)((tmp >> 5) & 0x003F);
		byte second = (byte)((directoryEntry[index22] & 0x1F) << 1);
		
		return new DirectoryTime((byte)(hour + 1), minute, second);
	}	//end getCreationTime(byte[] directoryEntry)
		
	
	/**
	 * Sets the last-modified time of the given directoryEntry. 
	 * @param directoryEntry the given directory entry.
	 * @param time the new last-modified time, measured in milliseconds since
	 * the epoch (00:00:00 GMT, January 1, 1970).
	 */	 
	public static void setWriteTime(byte[] directoryEntry, long time)
	{		
		GregorianCalendar writeTime = new GregorianCalendar();
		writeTime.setTime(new Date(time));
		
		int writeTimeTemp = 0;
		writeTimeTemp = writeTime.get(Calendar.SECOND) >> 1;
		writeTimeTemp |= ((writeTime.get(Calendar.MINUTE)) << 5);
		writeTimeTemp |= ((writeTime.get(Calendar.HOUR_OF_DAY) - 1) << 11);
		directoryEntry[directoryEntry.length - 32 + 22] = (byte)writeTimeTemp;
		directoryEntry[directoryEntry.length - 32 + 23] = (byte)(writeTimeTemp >> 8);
	}	//end setWriteTime(byte[] directoryEntry, long time)
	
	
	/**
	 * Sets the last write date to the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @param year the last write year, valid range from 1980 to 2107.
	 * @param month the last write month, valid range from 1 to 12.
	 * @param day the last write day, valid range from 1 to 31.
	 */	
	public static void setLastWriteDate(byte[] directoryEntry, int year, int month, int day)
	{
		year -= 1980;
		int writeDate = 0;
		writeDate = day;
		writeDate |= (month << 5);
		writeDate |= (year << 9);
		directoryEntry[directoryEntry.length - 32 + 24] = (byte)writeDate;
		directoryEntry[directoryEntry.length - 32 + 25] = (byte)(writeDate >> 8);
	}	//end setLastWriteDate(byte[] directoryEntry,  int year, int month, int day)
		
	
	/**
	 * Sets the last access date to the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @param year the last access year, valid range from 1980 to 2107.
	 * @param month the last access month, valid range from 1 to 12.
	 * @param day the last access day, valid range from 1 to 31.
	 */	
	public static void setLastAccessDate(byte[] directoryEntry, int year, int month, int day)
	{
		year -= 1980;
		int accessDate = 0;
		accessDate = day;
		accessDate |= (month << 5);
		accessDate |= (year << 9);
		directoryEntry[directoryEntry.length - 32 + 18] = (byte)accessDate;
		directoryEntry[directoryEntry.length - 32 + 19] = (byte)(accessDate >> 8);
	}	//end setLastAccessDate(byte[] directoryEntry,  int year, int month, int day)
	
	
	/**
	 * Returns the creation date of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return DirectoryDate contains the day, month, and year this entry was made.
	 */
	public static DirectoryDate getCreationDate(byte[] directoryEntry)
	{
		short index16 = (short)(directoryEntry.length - 32 + 16);
		short index17 = (short)(directoryEntry.length - 32 + 17);
		byte year = (byte)(ByteArrayConversionsLittleEndian.convShort(directoryEntry[index17]) >> 1);
		int tmp = ByteArrayConversionsLittleEndian.convInt(directoryEntry[index16], directoryEntry[index17]);
		byte month = (byte)((tmp >> 5) & 0x000F);
		byte day = (byte)(directoryEntry[index16] & 0x1F);
		
		return new DirectoryDate(year + 1980, month, day);
	}	//end getCreationDate(byte[] directoryEntry)
	
	
	/**
	 * Returns the last access date of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return DirectoryDate contains the day, month, and year of the last access.
	 */
	public static DirectoryDate getLastAccessDate(byte[] directoryEntry)
	{
		short index18 = (short)(directoryEntry.length - 32 + 18);
		short index19 = (short)(directoryEntry.length - 32 + 19);
		byte year = (byte)(ByteArrayConversionsLittleEndian.convShort(directoryEntry[index19]) >> 1);
		int tmp = ByteArrayConversionsLittleEndian.convInt(directoryEntry[index18], directoryEntry[index19]);
		byte month = (byte)((tmp >> 5) & 0x000F);
		byte day = (byte)(directoryEntry[index18] & 0x1F);
		
		return new DirectoryDate(year + 1980, month, day);
	}	//end getLastAccessDate(byte[] directoryEntry)
	
	
	/**
	 * Returns the date of the last write of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return DirectoryDate contains the day the month and the year of the last write.
	 */
	public static DirectoryDate getWriteDate(byte[] directoryEntry)
	{
		short index25 = (short)(directoryEntry.length - 32 + 25);
		short index24 = (short)(directoryEntry.length - 32 + 24);
		byte year = (byte)(ByteArrayConversionsLittleEndian.convShort(directoryEntry[index25]) >> 1);
		int tmp = ByteArrayConversionsLittleEndian.convInt(directoryEntry[index24], directoryEntry[index25]);
		byte month = (byte)((tmp >> 5) & 0x000F);
		byte day = (byte)(directoryEntry[index24] & 0x1F);
		
		return new DirectoryDate(year + 1980, month, day);
	}	//end getWriteDate(byte[] directoryEntry)
		
		
	/**
	 * Returns true if the given directoryEntry is marked as hidden.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is marked as hidden; false otherwise.
	 */
	public static boolean isHidden(byte[] directoryEntry)
	{
		//index 11
		return (directoryEntry[directoryEntry.length - 32 + 11] & ATTR_HIDDEN) == ATTR_HIDDEN;
	}	//end isHidden(byte[] directoryEntry)
	
	
	/**
	 * Returns true if the given directoryEntry is marked as read only.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is marked as read only; false otherwise.
	 */
	public static boolean isReadOnly(byte[] directoryEntry)
	{
		//index 11
		return (directoryEntry[directoryEntry.length - 32 + 11] & ATTR_READ_ONLY) == ATTR_READ_ONLY;
	}	//end isReadOnly(byte[] directoryEntry)
	
	
	/**
	 * Checks if the given directory entry is marked as a directory.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is a directory.
	 */
	public static boolean isDirectory(byte[] directoryEntry)
	{		
		if (((directoryEntry[directoryEntry.length - 32 + 11] & ATTR_LONG_NAME_MASK) != ATTR_LONG_NAME) &&
			(directoryEntry[directoryEntry.length - 32 + 0] != FREE_ENTRY))
			return (directoryEntry[directoryEntry.length - 32 + 11] & (ATTR_DIRECTORY | ATTR_VOLUME_ID)) == ATTR_DIRECTORY;
		return false;
	}
	
	
	/**
	 * Checks if the given directory entry is the dot entry.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is a dot entry.
	 */
	public static boolean isDotEntry(byte[] directoryEntry)
	{		
		String str = ByteArrayConversionsLittleEndian.byteToString(
			directoryEntry,
			directoryEntry.length - 32,
			directoryEntry.length - 32 + 11
		);
		return str.equals(".          ") && isDirectory(directoryEntry);
	}	//end isDotEntry(byte[] directoryEntry)
	
	
	/**
	 * Checks if the given directory entry is the dot dot entry.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is the dot dot entry.
	 */
	public static boolean isDotDotEntry(byte[] directoryEntry)
	{		
		String str = ByteArrayConversionsLittleEndian.byteToString(
						directoryEntry,
						directoryEntry.length - 32,
						directoryEntry.length - 32 + 11
		);
		return str.equals("..         ") && isDirectory(directoryEntry);
	}	//end isDotDotEntry(byte[] directoryEntry)

	
	/**
	 * Checks if the given directory entry is a file entry.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directoryEntry is marked as a file.
	 */	
	public static boolean isFile(byte[] directoryEntry)
	{
		if (((directoryEntry[directoryEntry.length - 32 + 11] & ATTR_LONG_NAME_MASK) != ATTR_LONG_NAME) &&
			(directoryEntry[directoryEntry.length - 32 + 0] != FREE_ENTRY))
			return (directoryEntry[directoryEntry.length - 32 + 11] & (ATTR_DIRECTORY | ATTR_VOLUME_ID)) == 0x00;
		return false;
	}	//end isFile(byte[] directoryEntry)
	
	
	/**
	 * Checks if the given directory entry is the root directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is the root directory entry.
	 */
	public static boolean isRootDirectory(byte[] directoryEntry)
	{
		if (((directoryEntry[directoryEntry.length - 32 + 11] & ATTR_LONG_NAME_MASK) != ATTR_LONG_NAME) &&
			(directoryEntry[directoryEntry.length - 32 + 0] != FREE_ENTRY))
			return (directoryEntry[directoryEntry.length - 32 + 11] & (ATTR_DIRECTORY | ATTR_VOLUME_ID)) == ATTR_VOLUME_ID;
		return false;
	}	//end isRootDirectory(byte[] directoryEntry)
	
	
	/**
	 * Returns true if the given directory entry is a part of the set
	 * of entries for a long directory entry. To be true, the attribute
	 * ATTR_LONG_NAME must be set.
	 * @param directoryEntry the given directory entry.
	 * @return true in case the directory entry is a long entry.
	 */
	public static boolean isLongEntry(byte[] directoryEntry)
	{
		return (((directoryEntry[11] & ATTR_LONG_NAME_MASK) == ATTR_LONG_NAME) && (directoryEntry[0] != FREE_ENTRY));
	}	//end isLongEntry(byte[] directoryEntry)
	
	
	/**
	 * Checks if the given directory entry is the last long entry of the set
	 * of directory entries for one file.
	 * Use this method only on long directory entries.
	 * @param directoryEntry the given directory entry.
	 * @return true if the directory entry is the last entry of the set 
	 * of the directory entries for one file.
	 */
	public static boolean isLastLongEntry(byte[] directoryEntry)
	{
		//index 11
		return ((directoryEntry[directoryEntry.length - 32 + 11] & 0x40) == 0x40);
	}	//end isLastLongEntry(byte[] directoryEntry)
	
	
	/**
	 * Returns the order number of the given directory entry. The last
	 * order value flag is not returned. Use this method only on
	 * long directory entries.
	 * @param directoryEntry the given directory entry.
	 * @return the order number.
	 */
	public static short getOrderNumber(byte[] directoryEntry)
	{
		return ByteArrayConversionsLittleEndian.convShort((byte)(directoryEntry[0] & 0xBF));
	}	//end getOrderNumber(byte[] directoryEntry)
	
	
	/**
	 * Checks if the given directory entry is marked as free entry.
	 * @param directoryEntry the given directory entry.
	 * @return true if the given directory entry is marked as free.
	 */
	public static boolean isFree(byte[] directoryEntry)
	{		
		byte value = directoryEntry[0];
		return (value == FREE_ENTRY ||
				value == FREE_ENTRY_AND_NEXTS_FREE ||
				value == FREE_ENTRY_KANJI);
	}	//end isFree(byte[] directoryEntry)
	
	
	/**
	 * Checks if the given directory entry is the last entry. If a directory entry
	 * is marked as last entry the file system can use this information to accelerate
	 * the access.
	 * @param directoryEntry the given directory entry.
	 * @return true if the given directory entry is marked as last entry.
	 */
	public static boolean isLastEntry(byte[] directoryEntry)
	{		
		return ByteArrayConversionsLittleEndian.convShort(directoryEntry[0]) == FREE_ENTRY_AND_NEXTS_FREE;
	}	//end isLastEntry(byte[] directoryEntry)
	
	
	/**
	 * Returns the name contained in the given directoryEntry.
	 * @param directoryEntry the given directory entry.
	 * @return the name.
	 */
	public static String getName(byte[] directoryEntry)
	{
		if (isLongEntry(directoryEntry))
			return getLongName(directoryEntry);
		else
			return getShortName(directoryEntry);		
	}	//end getName(byte[] directoryEntry)
	
	
	/**
	 * Returns the short name of the directory entry without the numeric tail.
	 * @param directoryEntry the given directory entry.
	 * @return the short name without the numeric tail.
	 */
	public static String getShortName(byte[] directoryEntry)
	{
		if (isDirectory(directoryEntry) || isRootDirectory(directoryEntry))
		{
			String str = ByteArrayConversionsLittleEndian.byteToString(
				directoryEntry,
				directoryEntry.length - 32 + 0,
				directoryEntry.length - 32 + 11
			);
			int length = str.length();
			//clear all last blanks
			for (int i=0; i < length; i++)
				if (str.endsWith(" "))
					str = str.substring(0, str.length() - 1);
				else
					break;
			
			return str;
		}
		else
			return recreateFileName(
				directoryEntry,
				directoryEntry.length - 32 + 0,
				directoryEntry.length - 32 + 11
			);
	}	//getShortName(byte[] directoryEntry)
	
	
	/**
	 * Returns the name of the file of a set of long name directory entries.
	 * the longEntry must be the whole set of long name directory entries
	 * (inclusive the short directory entry).
	 * @param longEntry a set of directory entries that together are a long
	 * name entry.
	 * @return the name of the long directory entry. In case the long name
	 * is not valid the null string is returned.
	 */
	public static String getLongName(byte[] longEntry)
	{
		String result = "";
		
		//calculate the checksum of the basis-name. If there is one member of the
		// the set of long directory entries that has not this checksum set. The
		// long name is no longer valid.
		byte[] basisName = new byte[11];
		System.arraycopy(longEntry, longEntry.length-32, basisName, 0, 11);
		
		byte checksum = (byte)checksum(basisName);
		
		//extract the name		
		for (int i=longEntry.length-64; i >= 0; i-=32)
		{
			if (checksum != longEntry[i+13])
			{
				//for debug purposes
				throw new RuntimeException ("Checksum doesn't match: "+Arrays.printHexArrayString(longEntry));
			}
			
			//get the first part
			for (int j=i+1; j < i+11; j += 2)
				if (longEntry[j] == 0 && longEntry[j+1] == 0)
					return result + ByteArrayConversionsLittleEndian.unicodeByteToString(longEntry, i+1, j);
			result += ByteArrayConversionsLittleEndian.unicodeByteToString(longEntry, i+1, i+11);
			
			//get the second part
			for (int j=i+14; j < i+26; j += 2)
				if (longEntry[j] == 0 && longEntry[j+1] == 0)
					return result + ByteArrayConversionsLittleEndian.unicodeByteToString(longEntry, i+14, j);
			result += ByteArrayConversionsLittleEndian.unicodeByteToString(longEntry, i+14, i+26);
			
			//get the third part
			for (int j=i+28; j < i+32; j += 2)
				if (longEntry[j] == 0 && longEntry[j+1] == 0)
					return result + ByteArrayConversionsLittleEndian.unicodeByteToString(longEntry, i+28, j);
			result += ByteArrayConversionsLittleEndian.unicodeByteToString(longEntry, i+28, i+32);
		}
		
		return result;
	}	//end getLongName((byte[] longEntry)
	
	
	/**
	 * Return the attribute byte of the given directory entry.
	 * @param directoryEntry the given directory entry.
	 * @return the attribute.
	 */	
	public static byte getAttribute(byte[] directoryEntry)
	{
		//index 11
		return directoryEntry[directoryEntry.length - 32 + 11];
	}	//end getAttribute(byte[] directoryEntry)
	
	
	/**
	 * Return the length of the file stored in the given directory
	 * entry.
	 * @param directoryEntry the given directory entry.
	 * @return the length of the file stored in the given directory
	 * entry.
	 */
	public static long getFileLength(byte[] directoryEntry)
	{		
		//index 28 - 31
		return ByteArrayConversionsLittleEndian.convLong(
			directoryEntry[directoryEntry.length - 32 + 28],
			directoryEntry[directoryEntry.length - 32 + 29],
			directoryEntry[directoryEntry.length - 32 + 30],
			directoryEntry[directoryEntry.length - 32 + 31]
		);
	}	//end getSize(byte[] directoryEntry)
	
		
	/**
	 * Set the length of the given directory entry to the given length.
	 * The method will only update the directory entry, but will not
	 * save the information in the directory structure.
	 * @param directoryEntry the given directory entry.
	 * @param fileLength the new length.
	 */
	public static void setFileLength(byte[] directoryEntry, long fileLength)
	{
		//index 28 - 31
		directoryEntry[directoryEntry.length - 32 + 31] = (byte)((fileLength >> 32) & 0x00000000000000FF);
		directoryEntry[directoryEntry.length - 32 + 30] = (byte)((fileLength >> 16) & 0x00000000000000FF);
		directoryEntry[directoryEntry.length - 32 + 29] = (byte)((fileLength >> 8) & 0x00000000000000FF);
		directoryEntry[directoryEntry.length - 32 + 28] = (byte)(fileLength & 0x00000000000000FF);
	}	//end setFileLength(byte[] directoryEntry, long fileLength)
	
		
	/**
	 * Mark the given directory entry as free.
	 * @param directoryEntry the given directory entry.
	 */
	public static void setFree(byte[] directoryEntry)
	{
		for (int i=0; i < directoryEntry.length; i+= 32)
			directoryEntry[i] = FREE_ENTRY;
	}	//end setFree(byte[] directoryEntry)
	
	
	/**
	 * Return an iterator which lists all entries of the directory structure
	 * depending on the given list entry attribute. The iterator will return
	 * DirectoryEntryInformation objects, which contains all important information.
	 * @param listEntryAttribute specifies the kind of entries that should
	 * be returned within the iterator.
	 * @return Iterator which returns 32 byte arrays.
	 * @see xxl.core.io.fat.DirectoryEntryInformation
	 */
	public EntriesFilterIterator getEntries(int listEntryAttribute)
	{
		return new EntriesFilterIterator(
			new ListEntriesIterator(
				device,
				bpb.getFirstRootDirSecNum(),
				true
			),
			listEntryAttribute
		);
	}	//end getAllEntries(int listEntryAttribute)
		
	
	///////////////////////////////////
	//          DEBUG_METHODS        //
	///////////////////////////////////
	
	
	/**
	 * Return the root directory as a byte array.
	 * @return the root directory as a byte array.
	 */
	public byte[] getRootDir()
	{		
		if (bpb.getFatType() != FAT.FAT32)
		{	
			byte[] result = new byte[bpb.getRootDirSectors()*bpb.BytsPerSec*bpb.SecPerClus];
			Iterator clusters = new ClusterChainIterator(device, bpb.getFirstRootDirSecNum(), true);
			int index = 0;
			while (clusters.hasNext())
			{
				byte[] cluster = (byte[])clusters.next();
				System.arraycopy(cluster, 0, result, index, cluster.length);
				index += bpb.BytsPerSec*bpb.SecPerClus;
			}
			return result;
		}
		else
		{
			//first determines the number of bytes used for the root directory entry.
			int clusterLength = 0;
			Iterator clusters = new ClusterChainIterator(device, bpb.getFirstRootDirSecNum(), true);
			while (clusters.hasNext())
				clusterLength += ((byte[])clusters.next()).length;
				
			//now copy each cluster of the root directory in one byte array
			byte[] result = new byte[clusterLength];
			int index = 0;
			clusters = new ClusterChainIterator(device, bpb.getFirstRootDirSecNum(), true);
			while (clusters.hasNext())
			{
				byte[] cluster = (byte[])clusters.next();
				System.arraycopy(cluster, 0, result, index, cluster.length);
				index += cluster.length;
			}
			return result;
		}
	}	//end getRootDir()
	
}	//end class DIR
