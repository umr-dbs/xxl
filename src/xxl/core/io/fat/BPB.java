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

import xxl.core.io.fat.errors.InitializationException;
import xxl.core.io.fat.errors.WrongFATType;
import xxl.core.io.fat.errors.WrongLength;
import xxl.core.io.fat.util.ByteArrayConversionsLittleEndian;
import xxl.core.io.fat.util.MyMath;
import xxl.core.util.Arrays;

/**
 * This class represents the BIOS Parameter Block. Normally it is located in the first 
 * sector of the volume in the reserved region. It's assumed that all bytes of the bpb
 * on the disk are Little Endian encoded. This means the decimal value 65450 = 0xFFAA
 * is stored in the following order: 0xAA, 0xFF. Of course this is true only for numbers
 * but not for strings or other byte chains that are not interpreted as numbers.
 */
public class BPB
{

	//I assume the following table
	//byte				= 1 byte
	//short				= 2 byte
	//int				= 4 byte
	//long				= 8 byte
	//float				= 4 byte
	//double			= 8 byte
	
	/**
	 * Jump instruction to boot code. This field has two allowed forms:
	 * jmpBoot[0] = 0xEB, jmpBoot[1] = 0x??, jmpBoot[2] = 0x90
	 * and
	 * jmpBoot[0] = 0xE9, jmpBoot[1] = 0x??, jmpBoot[2] = 0x??
	 * 0x?? indicates that any 8-bit value is allowed in that byte. What this
	 * forms is a three-byte Intel x86 unconditional branch (jump)
	 * instruction that jumps to the start of the operating system bootstrap
	 * code. This code typically occupies the rest of sector 0 of the volume
	 * following the BPB and possibly other sectors. Either of these forms
	 * is acceptable. JmpBoot[0] = 0xEB is the more frequently used
	 * format.
	 */
	public short[] jmpBoot = new short[3];
	
	
	/**
	 * "MSWIN4.1" There are many misconceptions about this field. It is
	 * only a name string. Microsoft operating systems don't pay any
	 * attention to this field. Some FAT drivers do. This is the reason that
	 * the indicated string, "MSWIN4.1", is the recommended setting,
	 * because it is the setting least likely to cause compatibility problems.
	 * If you want to put something else in here, that is your option, but
	 * the result may be that some FAT drivers might not recognize the
	 * volume. Typically this is some indication of what system formatted
	 * the volume.
	 */
	public String OEMName;

	/**
	 * Count of bytes per sector. This value may take on only the following values:
	 * 512, 1024, 2048, or 4096. If maximum compatibility with old implementatiosn is
	 * desired, only the value 512 should be used. There is a lot of FAT code in the
	 * world that is basically "hard wired" to 512 bytes per sector and doesn't
	 * bother to check this field to make sure it is 512. 
	 * Note: Do not misinterpret these statements about maximum compatibility. If the
	 * media being recorded has a physical sector size N, you must use N and this must 
	 * still be less than or equal to 4096. Maximum compatibility is achieved by only
	 * using media with specific sector sizes.
	 */
	public int BytsPerSec = 1024;
	
	/**
	 * Number of sectors per allocation unit. This value must be a power of 2 that is 
	 * greater 0. The legal values are 1, 2, 4, 8, 16, 32, 64, and 128. Note however, 
	 * that a value never should be used, if the "bytes per cluster" value results 
	 * (BPB_BytsPerSec * BPB_SecPerClus) greater than 32K (32 * 1024). There is a
	 * misconception that values greater than this are OK. Values that cause a cluster
	 * size greater than 32K bytes do not work properly; do not try to define one. Some
	 * versions of some systems allow 64K bytes per cluster value. Many application 
	 * setup programs will not work correctly on such a FAT volume.
	 */
	public short SecPerClus = 4;
	
	/**
	 * Number of reserved sectors in the Reserved region of the volume starting at the
	 * first sector of the volume. This field must not be 0. For FAT12 and FAT16 volumes,
	 * this value should never be anything other than 1. For FAT32 volumes, this value
	 * is typically 32. There is a lot of FAT code in the world "hard wired" to 1 
	 * reserved sector for FAT12 and FAT16 volumes and that doesn't bother to check this
	 * field to make sure it is 1. Microsoft operating systems will properly support any
	 * non-zero value in this field.
	 */
	public int RsvdSecCnt = 32;
	
	/**
	 * The count of FAT data structures on the volume. This field should always contain
	 * the value 2 for any FAT volume of any type. Although any value greater than or
	 * equal to 1 is perfectly valid, many software programs and a few operating systems
	 * FAT file system drivers may not function properly if the value is something other
	 * than 2. All Microsoft file system drivers will support a value other than 2, but
	 * it is still highly recommended that no value other than 2 be used in this field.
	 * The reason the standard value for this field is 2 is to provide redundancy for the
	 * FAT data structure so that if a sector goes bad in one of the FATs, that data is not
	 * lost because it is duplicated in the other FAT. On non-disk-based media, such as 
	 * FLASH memory cards, where such redundancy is a useless feature, a value of 1 may be
	 * used to save the space that a second copy of the FAT uses, but some FAT file system
	 * drivers might not recognize such a volume properly.
	 */
	public short NumFATs = 2;
	
	/**
	 * For FAT12 and FAT16 volumes, this field contains the count of 32-byte directory
	 * entries in the root directory. For FAT32 volumes, this field must be set to 0. For
	 * FAT12 and FAT16 volumes, this value should always specify a count that when multiplied
	 * by 32 results in an even multiple of BPB_BytsPerSec. For maximum compatibility, FAT16
	 * volumes should use the value 512.
	 */
	public int RootEntCnt = 0;
	
	/**
	 * This field is the old 16-bit total count of sectors on the volume. This count 
	 * includes the count of all sectors in all four regions of the volume. This field can
	 * be 0; if it is 0, then BPB_TotSec32 must be non-zero. For FAT32 volumes, this field
	 * must be 0. For FAT12 and FAT16 volumes, this field contains the sector count, and
	 * BPB_TotSec32 is 0 if the total sector count "fits" (is less than 0x10000).
	 * If the total sector count doesn't fits the value is stored in BPB_TotSec32.
	 */
	public int TotSec16 = 0;
	
	/**
	 * 0xF8 is the standard value for "fixed" (non-removable) media. For removable media,
	 * 0xF0 is frequently used. The legal values for this field are 0xF0, 0xF8, 0xF9, 0xFA,
	 * 0xFB, 0xFC, 0xFD, 0xFE, and 0xFF. The only other important point is that whatever value
	 * is put in here must also be put in the low byte of the FAT[0] entry. This dates back to
	 * the old MS-DOS 1.x media determination noted earlier and is no longer usually used for
	 * anything.
	 */
	public short Media = (byte)0x00F8;
	
	/**
	 * This field is the FAT12/FAT16 16-bit count of sectors occupied by ONE FAT. On FAT32
	 * volumes this field must be 0, and BPB_FATSz32 contains the FAT size count.
	 */
	public int FATSz16 = 0;
	
	/**
	 * Sectors per track for interrupt 0x13. This field is only relevant for media that have
	 * a geometry (volume is broken down into tracks by multiple heads and cylinders) and are
	 * visible on interrupt 0x13. This field contains the "sectors per track" geometry value.
	 */
	public int SecPerTrk = 0;
	
	/**
	 * Number of heads for interrupt 0x13. This field is relevant as discussed earlier for
	 * BPB_SecPerTrk. This field contains the one based "count of heads". For example, on a
	 * 1.44 MB 3.5-inch floppy drive this value is 2.
	 */
	public int NumHeads = 0;

	/**
	 * Count of hidden sectors preceding the partition that contains this FAT volume. This
	 * field is generally only relevant for media visible on interrupt 0x13. This field should
	 * always be zero on media that are not partitioned. Exactly what value is appropriate is
	 * operating system specific.
	 */

	public long HiddSec = 0;	
	
	/**
	 * This field is the new 32-bit total count of sectors on the volume. This count includes
	 * the count of all sectors in all four regions of the volume. This field can be 0; if it
	 * is 0, then BPB_TotSec16 must be non-zero. For FAT32 volumes, this field must be non-zero.
	 * For FAT12/FAT16 volumes, this field contains the sector count if BPB_TotSec16 is 0 (count
	 * is greater than or equal to 0x10000).
	 */
	public long TotSec32 = 0;
	
	
	
	
	//////////////////////////
	//// FAT12 & FAT16    ////
	//////////////////////////
	
	
	
	/**
	 * Int 0x13 drive number (e.g. 0x80). This field supports MS-DOS
	 * bootstrap and is set to the INT 0x13 drive number of the media
	 * (0x00 for floppy disks, 0x80 for hard disks).
	 * NOTE: This field is actually operating system specific.
	 */
	public short DrvNum = 0x80;
		
	/**
	 * Reserved (used by Windows NT). Code that formats FAT volumes
	 * should always set this byte to 0.
	 */
	public short Reserved1 = 0;
	
	/**
	 * Extended boot signature (0x29). This is a signature byte that
	 * indicates that the following three fields in the boot sector are
	 * present.
	 */
	public short BootSig = 0x29;
	
	/**
	 * Volume serial number. This field, together with BS_VolLab,
	 * supports volume tracking on removable media. These values allow
	 * FAT file system drivers to detect that the wrong disk is inserted in a
	 * removable drive. This ID is usually generated by simply combining
	 * the current date and time into a 32-bit value.
	 */
	public long VolID;
	
	/**
	 * Volume label. This field matches the 11-byte volume label
	 * recorded in the root directory.
	 * NOTE: FAT file system drivers should make sure that they update
	 * this field when the volume label file in the root directory has its
	 * name changed or created. The setting for this field when there is no
	 * volume label is the string "NO NAME ".
	 */
	public String VolLab;
	
	/**
	 * One of the strings "FAT12   ", "FAT16    ", or "FAT     ".
	 * NOTE: Many people think that the string in this field has
	 * something to do with the determination of what type of FAT-
	 * FAT12, FAT16, or FAT32-that the volume has. This is not true.
	 * You will note from its name that this field is not actually part of the
	 * BPB. This string is informational only and is not used by Microsoft
	 * file system drivers to determine FAT type, because it is frequently
	 * not set correctly or is not present. See the FAT Type Determination
	 * section of this document. This string should be set based on the
	 * FAT type though, because some non-Microsoft FAT file system
	 * drivers do look at it.
	 */
	public String FilSysType;
		
		
		
	//////////////////////
	////   FAT32     /////
	//////////////////////

	
	

	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * This field is the FAT32 32-bit count of sectors occupied by ONE FAT. BPB_FATSz16 must be 0.
	 */	
	public long FATSz32 = 0;
	
	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * Bits 0-3 -- Zero-based number of active FAT. Only valid if mirroring is disabled.
	 * Bits 4-6 -- Reserved.
	 * Bit 7 -- 0 means the FAT is mirrored at runtime into all FATs.
	 *       -- 1 means only one FAT is active; it is the one referenced in bits 0-3.
	 * Bits 8-15 -- Reserved.
	 */
	public int ExtFlags = 0;
	
	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * High byte is major revision number. Low byte is minor revision number. This is the version
	 * number of the FAT32 volume. This supports the ability to extend the FAT32 media type in 
	 * the future without worrying about old FAT32 drivers mounting the volume. This document
	 * defines the version to 0:0. If this field is non-zero, back-level Windows versions will
	 * not mount the volume. NOTE: Disk utilities should respect this field and not operate on
	 * volumes with a higher major or minor version number than that for which they were designed
	 * . FAT32 file system drivers must check this field and not mount the volume if it does not
	 * contain a version number that was defined at the time the driver was written.
	 */
	public int FSVer = 0;
	
	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * This is set to the cluster number of the first cluster of the root directory, usually 2
	 * but not required to be 2. NOTE: Disk utilities that change the location of the root
	 * directory should make every effort to place the first cluster of the root directory in
	 * the first non-bad cluster on the drive (i.e., in cluster 2, unless it's marked bad).
	 * This is specified so that disk repair utilities can easily find the root directory if
	 * this field accidentally gets zeroed.
	 */
	public long RootClus = 2;
	
	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * Sector number of FSINFO structure in the reserved area of the FAT32 volume. Usually 1.
	 * NOTE: There will be a copy of the FSINFO structure in BackupBoot, but only the copy
	 * pointed to by this field will be kept up to date (i.e., both the primary and backup boot
	 * record will point to the same FSINFO sector).
	 */
	public int FSInfo = 1;
	
	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * If non-zero, indicates the sector number in the reserved area of the volume of a copy of
	 * the boot record. Usually 6. No value other than 6 is recommended.
	 */
	public int BkBootSec = 6;
	
	/**
	 * This field is only defined for FAT32 media and does not exist on FAT12 and FAT16 media.
	 * Reserved for future expansion. Code that formats FAT32 volumes should always set all of
	 * the bytes of this field to 0.
	 * Important: The space for this field is 12 byte.
	 */
	public short[] Reserved = new short[12];
	
	/////////////////////////////////////
	//  end of specific values of BPB  //
	/////////////////////////////////////
	
	/**
	 * Instance of FATDevice.
	 */
	protected FATDevice device;
	
	/**
	 * Indicates the type of FAT.
	 */
	private byte fatType = FAT.UNKNOWN_FAT;
	
	/**
	 * Number of clusters of FAT, see Microsoft paper.
	 */
	protected long countOfClusters;

	//////////////////////////////////////////////
	//   for initialization of an empty device  //
	//////////////////////////////////////////////

	
	/**
	 * This is the table for FAT16 drives. NOTE that this table includes
	 * entries for disk sizes larger than 512 MB even though typically
	 * only the entries for disks smaller than 512 MB in size are used.
	 * The way this table is accessed is to look for the first entry
	 * in the table for which the disk size is less than or equal
	 * to the DiskSize field in that table entry. For this table to
	 * work properly BPB_RsvdSecCnt must be 1, BPB_NumFATs
	 * must be 2, and BPB_RootEntCnt must be 512. Any of these values
	 * being different may require the first table entries DiskSize value
	 * to be changed otherwise the cluster count may be to low for FAT16.
	 */
	protected static final DiskSizeToSectorPerCluster[] diskTableFAT16 = new DiskSizeToSectorPerCluster[]
	{
		new DiskSizeToSectorPerCluster(8400L,		(byte)0),		//disks up to 4.1 MB, the 0 value for secPerClusVal trips an error
		new DiskSizeToSectorPerCluster(32680L,		(byte)2),		//disks up to 16 MB, 1k cluster
		new DiskSizeToSectorPerCluster(262144L,		(byte)4),		//disks up to 128 MB, 2k cluster
		new DiskSizeToSectorPerCluster(524288L,		(byte)8),		//disks up to 256 MB, 4k cluster
		new DiskSizeToSectorPerCluster(1048576L,	(byte)16),		//disks up to 512 MB, 8k cluster
		
		//the entries after this point are not used unless FAT16 is forced
		
		new DiskSizeToSectorPerCluster(2097152L,	(byte)32),		//disks up to 1 GB, 16k cluster
		new DiskSizeToSectorPerCluster(4194304L,	(byte)64),		//disks up to 2 GB, 32k cluster
		new DiskSizeToSectorPerCluster(0xFFFFFFFFL,	(byte)0),		//any disks greater than 2 GB, 0 value for secPerClusVal trips an error
	};
	
	
	/**
	 * This is the table for FAT32 drives. NOTE that this table includes
	 * entries for disk sizes smaller than 512 MB even though typically
	 * only the entries for disks >= 512 MB in size are used.
	 * The way this table is accessed is to look for the first entry
	 * in the table for which the disk size is less than or equal
	 * to the DiskSize field in that table entry. For this table to
	 * work properly BPB_RsvdSecCnt must be 32, and BPB_NumFATs
	 * must be 2. Any of these values being different may require the first
	 * table entries DiskSize value to be changed otherwise the cluster count
	 * may be to low for FAT32.
	 */
	protected static final DiskSizeToSectorPerCluster[] diskTableFAT32 = new DiskSizeToSectorPerCluster[]
	{
		new DiskSizeToSectorPerCluster(66600L,		(byte)0),		//disks up to 32.5 MB, the 0 value for secPerClusVal trips an error
		new DiskSizeToSectorPerCluster(532480L,		(byte)1),		//disks up to 260 MB, 0.5k cluster
		new DiskSizeToSectorPerCluster(16777216L,	(byte)8),		//disks up to 8 GB, 4k cluster
		new DiskSizeToSectorPerCluster(33554432L,	(byte)16),		//disks up to 16 GB, 8k cluster
		new DiskSizeToSectorPerCluster(67108864L,	(byte)32),		//disks up to 32 GB, 16k cluster
		new DiskSizeToSectorPerCluster(0xFFFFFFFFL,	(byte)64)		//disks up to 32 GB, 32k cluster
	};
	
	
	/**
	 * Structure that contains two values: diskSize and secPerClusVal.
	 * The structure is used by getFATXXBPB methods for the initialisation
	 * of a FAT-file-system on disk.
	 */
	public static class DiskSizeToSectorPerCluster
	{
		/**The disk size*/
		public long diskSize;
		/**Sectors per cluster*/
		public byte secPerClusVal;
		
		/**Constructs a new DiskSizeToSectorPerCluster object
		 * 
		 * @param diskSize the disk size
		 * @param secPerClusVal the sectors per cluster value 
		 * */
		public DiskSizeToSectorPerCluster(long diskSize, byte secPerClusVal)
		{
			this.diskSize = diskSize;
			this.secPerClusVal = secPerClusVal;
		}	//end constructor
	}	//end inner class DiskSizeToSectorPerCluster

	
	/**
	 * Create an instance of this object. All variables are 
	 * initialized with the given bpbSector. Use this constructor
	 * by the BOOT process of the file system (device).
	 * @param device is a pointer to the device where the bpb is stored.
	 * @param bpbSector is the byte array representation of the BIOS Parameter Block.	 
	 * @throws InitializationException in case this object couldn't be initialized.
	 */
	public BPB(FATDevice device, byte[] bpbSector) throws InitializationException
	{		
		this.device = device;
		fatType = determineFatType(bpbSector);
		initializeBPB(bpbSector);
		
		if (FileSystem.debug)
		{
			System.out.println("\n\tBPB\n");
			Arrays.printHexArray(bpbSector,System.out);
		}
	}	//end constructor boot


	/**
	 * Creates an instance of this object. All variables (depending on the
	 * fat type) will be initialized. The constructor creates a fat type
	 * and diskSize dependend BIOS Parameter Block. Use this constructor
	 * by the FORMAT process.
	 * For a FAT12 file system diskSize must be smaller than 4084 blocks.
	 * For a FAT16 file system diskSize must be smaller than 4194304
	 * 512-byte-blocks and bigger than 32680 512-byte-blocks.
	 * For a FAT32 file system diskSize must be smaller than 0xFFFFFFFF bytes
	 * and bigger than 532480 512-byte-blocks
	 * 
	 * @param device the instance of FATDevice.
	 * @param diskSize is the size of the disk in number of 512 blocks.
	 * @param fatType is the type of fat that should be formatted.
	 * @throws WrongLength if the diskSize is to big for the given fatType.
	 * @throws InitializationException in case this object couldn't be initialized.
	 */
	public BPB(FATDevice device, long diskSize, byte fatType) throws WrongLength, InitializationException
	{
		this.device = device;
		this.fatType = fatType;

		//create a byte array that contains the fat type specific
		//information of the bpb
		byte[] bpbBlock = null;
		if (fatType == FAT.FAT12)
		{
			if (diskSize > 4084)
				throw new WrongLength("The maximum length for this FAT type is exceeded.\n Maximum length is 4084 512-byte-blocks", diskSize);
			bpbBlock = getFAT12BPB(diskSize);		//62 byte
		}
		else if (fatType == FAT.FAT16)
		{
			if (diskSize < 32680)
				throw new WrongLength("The minimum length for this FAT type is fell short of.\n Minimum length is 32680 512-byte-blocks", diskSize);
			if (diskSize > 4194304L)
				throw new WrongLength("The maximum length for this FAT type is exceeded\n. Maximum length is 4194304 512-byte-blocks", diskSize);
			bpbBlock = getFAT16BPB(diskSize);
		}
		else
		{
			if (diskSize < 532480)
				throw new WrongLength("The minimum length for this FAT type is fell short of.\n Minimum length is 532480 512-byte-blocks.", diskSize);
			bpbBlock = getFAT32BPB(diskSize);
		}

		//initialize the variables of this object with the parameters
		//of the byte array	
		try
		{
			initializeBPB(bpbBlock);
		}
		catch(Exception e)
		{
			throw new InitializationException("Couldn't initialze BPB, because of: "+e);
		}
		
		//it doesn't matter what kind of fat we have the bpb has always 512 Byte
		byte[] dataBlock = new byte[512];
		System.arraycopy(bpbBlock, 0, dataBlock, 0, bpbBlock.length);
		//last two bytes must have the following value
		dataBlock[510] = (byte)0x55;
		dataBlock[511] = (byte)0xAA;
		
		//write the BPB to disk
		device.writeSector(dataBlock, 0);
					
		//this is done to initialize countOfCluster value.
		determineFatType(bpbBlock);
	}	//end constructor format
		
	
	/**
	 * Returns a byte array of 62 entries representing the BPB of a FAT12.
	 * Initialize also the global variables with the same values used for the byte array.
	 * Use this method only if you create a file system.
	 *
	 * @param diskSize is the size of the disk in number of 512 blocks.
	 * This size sets the right SecPerClus value.
	 * @return a byte array of 62 entries representing the BPB of a FAT12.
	 */
	public byte[] getFAT12BPB(long diskSize)
	{
		byte[] bpb = new byte[62];
		
		//jmpBoot
		bpb[0] = (byte)0xEB;	//taken from documentation
		bpb[1] = (byte)0x3E;	//taken from the real FAT-System (MS-WIN 98 SE)
		bpb[2] = (byte)0x90;	//taken from documentation
		jmpBoot[0] = 0xEB;
		jmpBoot[1] = 0x3E;
		jmpBoot[2] = 0x90;
		
		//OEMName
		bpb[3] = (byte)0x4D;
		bpb[4] = (byte)0x53;
		bpb[5] = (byte)0x57;
		bpb[6] = (byte)0x49;
		bpb[7] = (byte)0x4E;
		bpb[8] = (byte)0x34;
		bpb[9] = (byte)0x2E;
		bpb[10] = (byte)0x31;
		OEMName = "MSWIN4.1";
		
		//BytsPerSec
		bpb[11] = (byte)0x00;	//0x0200 = 512
		bpb[12] = (byte)0x02;
		BytsPerSec = 512;
		
		//SecPerClus			
		bpb[13] = 1;	//taken from a real FAT-System
		SecPerClus = 1;
		
		//RsvdSecCnt
		bpb[14] = (byte)0x01;	//0x0001
		bpb[15] = (byte)0x00;
		RsvdSecCnt = 1;		//must be 1 for FAT12 and FAT16
		
		//NumFATs
		bpb[16] = (byte)0x02;
		NumFATs = 2;		//shoulb always be 2 for any FAT volume
		
		//RootEntCnt
		bpb[17] = (byte)0xE0;	//taken from real FAT-System
		bpb[18] = (byte)0x00;
		//should be 512 for FAT16 and 0 for FAT32, in general it must
		//specify a count when multiplied by 32 results in an even
		//multiple of BPB.BytsPerSec.
		RootEntCnt = 224;
		
		//TotSec16
		bpb[19] = (byte)(diskSize);
		bpb[20] = (byte)(diskSize >> 8);
		TotSec16 = (int)diskSize;	//2880;	//for a floppy
		
		//Media
		bpb[21] = (byte)0xF0;
		Media = 0xF0;		//0xF0 for removable media
			
		//FATSz16
		FATSz16 = (int)calculateNumberOfFATSectors(
			RootEntCnt,
			BytsPerSec,
			SecPerClus,
			diskSize,
			RsvdSecCnt,
			NumFATs,
			FAT.FAT12
		);
		bpb[22] = (byte)(FATSz16);	
		bpb[23] = (byte)(FATSz16 >> 8);
		FATSz32 = 0;
						
		//SecPerTrk
		bpb[24] = (byte)0x09;	//0x0009	////taken from a real FAT-System
		bpb[25] = (byte)0x00;
		SecPerTrk = 9;	
		
		//NumHeads
		bpb[26] = (byte)0x02;
		bpb[27] = (byte)0x00;
		NumHeads = 2;	//for a 1.44MB 3.5-inch floppy
		
		//HiddSec
		bpb[28] = (byte)0x00;
		bpb[29] = (byte)0x00;
		bpb[30] = (byte)0x00;
		bpb[31] = (byte)0x00;
		HiddSec = 0;	//zero for media that are not partitoned. Otherwise it is OS specific
		
		//TotSec32
		bpb[32] = (byte)0x00;
		bpb[33] = (byte)0x00;
		bpb[34] = (byte)0x00;
		bpb[35] = (byte)0x00;
		TotSec32 = 0;	//can be zero if TotSec16 is not zero. For FAT32 it must be non-zero
		
		//DrvNum
		bpb[36] = (byte)0x00;
		DrvNum = 0;		//zero for floppy and 0x80 for hard disk. Important: OS specific
		
		//Reserved1
		bpb[37] = (byte)0x00;
		Reserved1 = 0;	
		
		//BootSig
		bpb[38] = (byte)0x29;
		BootSig = 0x29;	//signature byte
		
		//VolID
		bpb[39] = (byte)0x00;
		bpb[40] = (byte)0x00;
		bpb[41] = (byte)0x00;
		bpb[42] = (byte)0x00;
		VolID = 0;		//not that important yet
		
		//VolLab
		bpb[43] = (byte)0x4E;
		bpb[44] = (byte)0x4F;
		bpb[45] = (byte)0x20;
		bpb[46] = (byte)0x4E;
		bpb[47] = (byte)0x41;
		bpb[48] = (byte)0x4D;
		bpb[49] = (byte)0x45;
		bpb[50] = (byte)0x20;
		bpb[51] = (byte)0x20;
		bpb[52] = (byte)0x20;
		bpb[53] = (byte)0x20;
		VolLab = "NO NAME    ";	//must be the same as the 11-byte volume label in root directory
		
		//FilSysType
		bpb[54] = (byte)0x46;
		bpb[55] = (byte)0x41;
		bpb[56] = (byte)0x54;
		bpb[57] = (byte)0x31;
		bpb[58] = (byte)0x32;
		bpb[59] = (byte)0x20;
		bpb[60] = (byte)0x20;
		bpb[61] = (byte)0x20;
		FilSysType = "FAT12   ";
				
		return bpb;
	}	//end getFAT12BPB
	

	/**
	 * Returns a byte array of 62 entries representing the BPB of a FAT16.
	 * Initialize also the global variables with the same values used for the byte array.
	 * Use this method only if you create a file system.
	 * @param diskSize the size of the disk in blocks (512 byte).
	 * @return byte array representation of the bpb.
	 */
	public byte[] getFAT16BPB(long diskSize)
	{
		byte[] bpb = getFAT12BPB(diskSize);
		
		//SecPerClus: get it from the static table
		for (int i=0; i < diskTableFAT16.length; i++)
			if (diskTableFAT16[i].diskSize >= diskSize)
			{
				//set sec per clus
				bpb[13] = diskTableFAT16[i].secPerClusVal;
				SecPerClus = diskTableFAT16[i].secPerClusVal;
				break;
			}
				
		bpb[17] = (byte)0x00;//0x0200 = 512
		bpb[18] = (byte)0x02;
		//should be 512 for FAT16 and 0 for FAT32, in general it must
		//specify a count when multiplied by 32 results in an even
		//multiple of BPB.BytsPerSec.
		RootEntCnt = 512;
		
		//if diskSize doesn't fits in TotSec16 it's stored in TotSec32
		if (diskSize >= 0x10000)
		{
			TotSec32 = diskSize;
			bpb[19] = bpb[20] = 0;
			
			bpb[32] = (byte)diskSize;
			bpb[33] = (byte)(diskSize >> 8);
			bpb[34] = (byte)(diskSize >> 16);
			bpb[35] = (byte)(diskSize >> 24);
			TotSec16 = 0;
		}	
		
		bpb[21] = (byte)0xF8;
		Media = 0xF8;		//0xF8 for non removable media
		
		bpb[58] = 0x36;	//change FilSysType to "FAT16   ";
		FilSysType = "FAT16   ";

		//FATSz16		
		FATSz16 = (int)calculateNumberOfFATSectors(
			RootEntCnt,
			BytsPerSec,
			SecPerClus,
			diskSize,
			RsvdSecCnt,
			NumFATs,
			FAT.FAT16
		);
		FATSz32 = 0;
		bpb[22] = (byte)(FATSz16 & 0x000000FF);
		bpb[23] = (byte)((FATSz16 & 0x0000FF00) >> 8);
		
		return bpb;
	}	//end getFAT16BPB
	
	
	/**
	 * Returns a byte array of 90 entries representing the BPB of a FAT32.
	 * Initializes also the global variables with the same values used for the byte array.
	 * Use this method only if you create a file system.
	 * 
	 * @param diskSize the number of sectors of the whole disk.
	 * @return a byte array of 90 entries representing the BPB of a FAT32.
	 */
	public byte[] getFAT32BPB(long diskSize)
	{
		byte[] tmp = getFAT16BPB(diskSize);
		byte[] bpb = new byte[90];
		System.arraycopy(tmp, 0, bpb, 0, tmp.length);
				
		//SecPerClus: get it from the static table
		for (int i=0; i < diskTableFAT32.length; i++)
			if (diskTableFAT32[i].diskSize >= diskSize)
			{
				//set sec per clus
				bpb[13] = diskTableFAT32[i].secPerClusVal;
				SecPerClus = diskTableFAT32[i].secPerClusVal;
				break;
			}
			
		bpb[14] = (byte)32;
		bpb[15] = (byte)0;
		RsvdSecCnt = 32;
				
		//should be 512 for FAT16 and 0 for FAT32, in general it must
		//specify a count when multiplied by 32 results in an even
		//multiple of BPB.BytsPerSec.
		RootEntCnt = 0;
		bpb[17] = (byte)RootEntCnt;
		bpb[18] = (byte)(RootEntCnt >> 8);
		
		TotSec16 = 0;
		bpb[19] = (byte)(TotSec16);
		bpb[20] = (byte)(TotSec16 >> 8);
						
		bpb[21] = (byte)0xF8;
		Media = 0xF8;		//0xF8 for non removable media
		
		FATSz16 = 0;
		bpb[22] = (byte)FATSz16;
		bpb[23] = (byte)(FATSz16 >> 8);
		
		TotSec32 = diskSize;	
		bpb[32] = (byte)diskSize;
		bpb[33] = (byte)(diskSize >> 8);
		bpb[34] = (byte)(diskSize >> 16);
		bpb[35] = (byte)(diskSize >> 24);
		
		//FATSz32
		FATSz32 = calculateNumberOfFATSectors(
			RootEntCnt,
			BytsPerSec,
			SecPerClus,
			diskSize,
			RsvdSecCnt,
			NumFATs,
			FAT.FAT32
		);
		bpb[36] = (byte)FATSz32;
		bpb[37] = (byte)(FATSz32 >> 8);
		bpb[38] = (byte)(FATSz32 >> 16);
		bpb[39] = (byte)(FATSz32 >> 24);
		
		ExtFlags = 0;
		ExtFlags |= 1;		//only FAT number 1 is active
		ExtFlags |= 0x80;	//only one FAT is active
		bpb[40] = (byte)ExtFlags;
		bpb[41] = (byte)(ExtFlags >> 8);
		
		FSVer = 0;	//set version to 0:0
		bpb[42] = 0;
		bpb[43] = 0;
		
		RootClus = 2;	//the first cluster number of the first cluster of the root directory. Usually 2
		bpb[44] = (byte)RootClus;
		bpb[45] = (byte)(RootClus >> 8);
		bpb[46] = (byte)(RootClus >> 16);
		bpb[47] = (byte)(RootClus >> 24);
		
		FSInfo = 1;	//sector number of the FSINFO structure in the reserved area of the volume
		bpb[48] = (byte)FSInfo;
		bpb[49] = (byte)(FSInfo >> 8);
		
		BkBootSec = 6;	//indicates the sector number in the reserved area of the volume of a copy of the boot record
						//No value other than 6 is recommended
		bpb[50] = (byte)BkBootSec;
		bpb[51] = (byte)(BkBootSec >> 8);
		
		Reserved[0] = 0;
		for (int i=52; i < 12; i++)
			bpb[i] = 0;
			
		//DrvNum is the same as for FAT12 or FAT16, only an other offset in BPB
		bpb[64] = (byte)DrvNum;
		
		//Reserved1 is the same as for FAT12 or FAT16, only an other offset in BPB
		bpb[65] = (byte)Reserved1;
		
		//BootSig is the same as for FAT12 or FAT16, only an other offset in BPB
		bpb[66] = (byte)BootSig;
		
		//VolID is the same as for FAT12 or FAT16, only an other offset in BPB
		bpb[67] = (byte)VolID;
		bpb[68] = (byte)(VolID >> 8);
		bpb[69] = (byte)(VolID >> 16);
		bpb[70] = (byte)(VolID >> 24);
		
		//VolLab is the same as for FAT12 or FAT16, only an other offset in BPB
		bpb[71] = (byte)0x4E;
		bpb[72] = (byte)0x4F;
		bpb[73] = (byte)0x20;
		bpb[74] = (byte)0x4E;
		bpb[75] = (byte)0x41;
		bpb[76] = (byte)0x4D;
		bpb[77] = (byte)0x45;
		bpb[78] = (byte)0x20;
		bpb[79] = (byte)0x20;
		bpb[80] = (byte)0x20;
		bpb[81] = (byte)0x20;
		
		//FilSysType		
		bpb[85] = 0x33;	//change FilSysType to "FAT32   ";
		bpb[86] = 0x32;	//change FilSysType to "FAT32   ";
		FilSysType = "FAT32   ";
				
		return bpb;
	}	//end getFAT32BPB
	
	
	/**
	 * Calculate the number of FAT sectors that are reserved for one FAT.
	 * @param rootEntCount the number of entries of the root directory.
	 * @param bytsPerSec the number of bytes for one sector.
	 * @param secPerClus the number of sectors for one cluster.
	 * @param diskSize the number of sectors of the whole disk.
	 * @param reservedSecCount the number of reserved sectors of the disk.
	 * @param numFats the number of FAT's.
	 * @param fatType the type of FAT.
	 * @return the number of FAT sectors for one FAT.
	 */
	private long calculateNumberOfFATSectors(
		int rootEntCount,
		int bytsPerSec,
		int secPerClus,
		long diskSize,
		int reservedSecCount,
		int numFats,
		byte fatType
	)
	{
		//for nore information about the following calculation see the 
		//Microsoft Paper.
		long rootDirSectors = ((rootEntCount * 32) + (bytsPerSec - 1)) / bytsPerSec;
		long tmpVal1 = diskSize - (reservedSecCount + rootDirSectors);
		
		//since there are no information about what to do for FAT12
		//I developed this part on my own.
		if (fatType == FAT.FAT12)
			return MyMath.roundUp(((tmpVal1 + 2.0) / (8.0 * bytsPerSec)) * 12.0);

		
		long tmpVal2 = (256 * secPerClus) + numFats;		
		if (fatType == FAT.FAT32)
			tmpVal2 = tmpVal2 / 2;
			
		return (tmpVal1 + (tmpVal2 - 1)) / tmpVal2;
	}	//end calcualteNumberOfFATSectors(...)
	
	
	/**
	 * Initialize the global variables. Depending on the length of the byte array bpb
	 * the variables will be initialized. If the length is 62 it is a BPB of FAT12 or 
	 * FAT16. If the length is 90 it is a BPB of FAT32. Only the variables of the
	 * specific BPB are correct all others are undefined.
	 * 
	 * @param bpb an array of bytes
	 * @throws InitializationException in case of an initialization error.
	 */
	protected void initializeBPB(byte[] bpb) throws InitializationException
	{
		//jmpBoot
		jmpBoot[0] = ByteArrayConversionsLittleEndian.convShort(bpb[0]);
		jmpBoot[1] = ByteArrayConversionsLittleEndian.convShort(bpb[1]);
		jmpBoot[2] = ByteArrayConversionsLittleEndian.convShort(bpb[2]);
		
		OEMName = ByteArrayConversionsLittleEndian.byteToString(bpb, 3, 3+8);
		BytsPerSec = ByteArrayConversionsLittleEndian.convInt(bpb[11], bpb[12]);
		SecPerClus = ByteArrayConversionsLittleEndian.convShort(bpb[13]);
		RsvdSecCnt = ByteArrayConversionsLittleEndian.convInt(bpb[14], bpb[15]);
		NumFATs = ByteArrayConversionsLittleEndian.convShort(bpb[16]);
		RootEntCnt = ByteArrayConversionsLittleEndian.convInt(bpb[17], bpb[18]);
		TotSec16 = ByteArrayConversionsLittleEndian.convInt(bpb[19], bpb[20]);
		Media = ByteArrayConversionsLittleEndian.convShort(bpb[21]);
		FATSz16 = ByteArrayConversionsLittleEndian.convInt(bpb[22], bpb[23]);
		SecPerTrk = ByteArrayConversionsLittleEndian.convInt(bpb[24], bpb[25]);
		NumHeads = ByteArrayConversionsLittleEndian.convInt(bpb[26], bpb[27]);
		HiddSec = ByteArrayConversionsLittleEndian.convLong(bpb[28], bpb[29], bpb[30], bpb[31]);
		TotSec32 = ByteArrayConversionsLittleEndian.convLong(bpb[32], bpb[33], bpb[34], bpb[35]);
		
		if (fatType == FAT.FAT12 || fatType == FAT.FAT16)
		{			
			DrvNum = ByteArrayConversionsLittleEndian.convShort(bpb[36]);
			Reserved1 = ByteArrayConversionsLittleEndian.convShort(bpb[37]);
			BootSig = ByteArrayConversionsLittleEndian.convShort(bpb[38]);
			VolID = ByteArrayConversionsLittleEndian.convLong(bpb[39], bpb[40], bpb[41], bpb[42]);
			VolLab = ByteArrayConversionsLittleEndian.byteToString(bpb, 43, 43+11);
			FilSysType = ByteArrayConversionsLittleEndian.byteToString(bpb, 54, 54+8);
		}
		else if (fatType == FAT.FAT32)
		{
			FATSz32 = ByteArrayConversionsLittleEndian.convLong(bpb[36], bpb[37], bpb[38], bpb[39]);
			ExtFlags = ByteArrayConversionsLittleEndian.convInt(bpb[40], bpb[41]);
			FSVer = ByteArrayConversionsLittleEndian.convInt(bpb[42], bpb[43]);
			RootClus = ByteArrayConversionsLittleEndian.convLong(bpb[44], bpb[45], bpb[46], bpb[47]);
			FSInfo = ByteArrayConversionsLittleEndian.convInt(bpb[48], bpb[49]);
			BkBootSec = ByteArrayConversionsLittleEndian.convInt(bpb[50], bpb[51]);
			
			for (int i=0; i < 12; i++)
				Reserved[i] = ByteArrayConversionsLittleEndian.convShort(bpb[i+52]);
						
			DrvNum = ByteArrayConversionsLittleEndian.convShort(bpb[64]);
			Reserved1 = ByteArrayConversionsLittleEndian.convShort(bpb[65]);
			BootSig = ByteArrayConversionsLittleEndian.convShort(bpb[66]);
			VolID = ByteArrayConversionsLittleEndian.convLong(bpb[67], bpb[68], bpb[69], bpb[70]);
			VolLab = ByteArrayConversionsLittleEndian.byteToString(bpb, 71, 71+11);
			FilSysType = ByteArrayConversionsLittleEndian.byteToString(bpb, 82, 82+8);
		}
		else
		{
			throw new InitializationException("Couldn't initialize BPB, because of: Wrong fat type in initializeBPB().");
		}
	}	//end initializeBPB(byte[] bpb)
	
	
	/**
	 * Return the sector number of the FSI structure. This value is
	 * only valid if the fat type is FAT32. An exception is thrown
	 * if this method is used by an other FAT type.
	 * @return the sector number of the FSInfo.
	 * @throws WrongFATType in case the FAT type is not FAT32.
	 */	
	public int getFSInfoSectorNumber() throws WrongFATType
	{
		if (fatType == FAT.FAT32)
			return FSInfo;
		else
			throw new WrongFATType(fatType, FAT.FAT32);
	}		//end getFSInfoSectorNumber()
	
	
	/**
	 * Calculate the number of sectors of the root directory.
	 * @return number of sectors of root directory
	 */
	public int getRootDirSectors()
	{
		return ((RootEntCnt * 32) + (BytsPerSec - 1)) / BytsPerSec;
	}	//end getRootDirSectors()
	

	/**
	 * Calculate the first sector number which contains the data of the files.
	 * @return the first data sector number
	 */
	public long getFirstDataSector()
	{
		long FATSz = 0;
		
		if (FATSz16 != 0)
			FATSz = FATSz16;
		else
			FATSz = FATSz32;
			
		return RsvdSecCnt + (NumFATs * FATSz) + getRootDirSectors();
	}	//end getFirstDataSector(int rootDirSectors)
	
	
	/**
	 * Return the first sector number of the root directory.
	 * @return the first root directory sector number.
	 */
	public long getFirstRootDirSecNum()
	{
		if (fatType == FAT.FAT12 || fatType == FAT.FAT16)
			return RsvdSecCnt + (NumFATs * FATSz16);
		else
			return RootClus;
	}	//end getFirstRootDirSecNum(byte fatType)
	
	
	/**
	 * Return the number of fat sectors.
	 * @return the number of fat sectors.
	 */
	public long getNumFatSectors()
	{
		if (FATSz16 != 0)
			return FATSz16;
		else
			return FATSz32;
	}	//end getNumFatSectors()
	
	
	/**
	 * Return the number of data sectors.
	 * @return number of data sectors.
	 */
	public long getNumDataSectors()
	{
		long totSec = 0;
		long FATSz = 0;

		if (FATSz16 != 0)
			FATSz = FATSz16;
		else
			FATSz = FATSz32;

		//determine FAT type. This is done by counting the clusters
		if (TotSec16 != 0)
			totSec = TotSec16;
		else
			totSec = TotSec32;

		return totSec - (RsvdSecCnt + (NumFATs * FATSz) + getRootDirSectors());
	}	//end getNumDataSectors()


	/**
	 * Return the last possible cluster number that is accessible by the
	 * file system. It might be that the real last cluster number is bigger
	 * than the value return from this method but it's not allowed to access
	 * this cluster. See Microsoft paper for further information.
	 * It' assumed that BPB is initialized.
	 * @return last accessible cluster number.
	 */
	public long getLastFatCluster()
	{
		return countOfClusters + 1;
	}	//end getLastFatCluster()


	/**
	 * Determine the fat type which depends on given bpb byte array.
	 * @param bpb the BIOS Parameter Block.
	 * @return the fat type.
	 */
	private byte determineFatType(byte[] bpb)
	{
		int BytsPerSec = ByteArrayConversionsLittleEndian.convInt(bpb[11], bpb[12]);
		short SecPerClus = ByteArrayConversionsLittleEndian.convShort(bpb[13]);
		int RsvdSecCnt = ByteArrayConversionsLittleEndian.convInt(bpb[14], bpb[15]);
		short NumFATs = ByteArrayConversionsLittleEndian.convShort(bpb[16]);
		int RootEntCnt = ByteArrayConversionsLittleEndian.convInt(bpb[17], bpb[18]);
		long TotSec16 = ByteArrayConversionsLittleEndian.convInt(bpb[19], bpb[20]);
		long TotSec32 = ByteArrayConversionsLittleEndian.convLong(bpb[32], bpb[33], bpb[34], bpb[35]);
		long FATSz16 = ByteArrayConversionsLittleEndian.convInt(bpb[22], bpb[23]);
		long FATSz32 = ByteArrayConversionsLittleEndian.convLong(bpb[36], bpb[37], bpb[38], bpb[39]);
		long rootDirSectors = ((RootEntCnt * 32) + (BytsPerSec - 1)) / BytsPerSec;
		long totSec = 0;
		long FATSz = 0;

		if (FATSz16 != 0)
			FATSz = FATSz16;
		else
			FATSz = FATSz32;

		//determine FAT type. This is done by counting the clusters
		if (TotSec16 != 0)
			totSec = TotSec16;
		else
			totSec = TotSec32;

		long dataSec = totSec - (RsvdSecCnt + (NumFATs * FATSz) + rootDirSectors);


		//the value of count of clusters is exactly the count of data clusters starting
		//at cluster 2.
		countOfClusters = dataSec / SecPerClus;	//computation rounds down
		byte fatType = 0;

		if (countOfClusters < 4085)
		{
			//Volume is FAT12
			fatType = FAT.FAT12;
		}
		else if (countOfClusters < 65525)
		{
			//Volume is FAT16
			fatType = FAT.FAT16;
		}
		else
		{
			//Volume is FAT32
			fatType = FAT.FAT32;
		}

		return fatType;
	}	//end determineFatType(byte[] bpb)


	/**
	 * Return the fat type.
	 * @return the fat type.
	 */
	public byte getFatType()
	{
		return fatType;
	}	//end getFatType()


	/**
	 * Return true if the FAT is mirrored at runtime. This method
	 * should be used only in case of a FAT32 file system.
	 * @return true if the FAT is mirrored at runtime.
	 * @throws WrongFATType if this method is used in case of FAT12 or FAT16.
	 */
	public boolean isFAT32Mirrored() throws WrongFATType
	{
		if (fatType != FAT.FAT32)
			throw new WrongFATType(fatType, FAT.FAT32);

		return (ExtFlags & 0x80) == 0;
	}	//end isFAT32Mirrored()


	/**
	 * Use this method only if mirroring of FAT32 is disabled and
	 * in case of a FAT32 file system.
	 * Return the zero-based number of active FAT.
	 * @return the zero-based number of active FAT.
	 * @throws WrongFATType if this method is used in case of FAT12 or FAT16.
	 */
	public int getActiveFAT32Number() throws WrongFATType
	{
		if (fatType != FAT.FAT32)
			throw new WrongFATType(fatType, FAT.FAT32);
		return ExtFlags & 0x0F;
	}	//end getActiveFAT32Number()


	/**
	 * Print the values of the BPB-Structure.
	 * 
	 * @return the values of the BPB-Structure as a string
	 */
	@Override
	public String toString()
	{
		String res = "";
		res += "FAT shared\n";
		res += "==========\n";
		res += "OEMName: "+OEMName+"\n";
		res += "BytsPerSec: "+BytsPerSec+"\n";
		res += "SecPerClus: "+SecPerClus+"\n";
		res += "RsvdSecCnt: "+RsvdSecCnt+"\n";
		res += "NumFATs: "+NumFATs+"\n";
		res += "RootEntCnt: "+RootEntCnt+"\n";
		res += "TotSec16: "+TotSec16+"\n";
		res += "Media: "+Media+"\n";
		res += "FatSz16: "+FATSz16+"\n";
		res += "SecPerTrk: "+SecPerTrk+"\n";
		res += "NumHeads: "+NumHeads+"\n";
		res += "HiddSec: "+HiddSec+"\n";
		res += "TotSec32: "+TotSec32+"\n";
		res += "\n";
		res += "FAT12 / FAT16 specific"+"\n";
		res += "======================"+"\n";
		res += "DrvNum: "+DrvNum+"\n";
		res += "Reserved1: "+Reserved1+"\n";
		res += "BootSig: "+BootSig+"\n";
		res += "VolID: "+VolID+"\n";
		res += "VolLab: "+VolLab+"\n";
		res += "FilSysType: "+FilSysType+"\n";
		res += "\n";
		res += "FAT32"+"\n";
		res += "====="+"\n";
		res += "FATSz32: "+FATSz32+"\n";
		res += "ExtFlags: "+ExtFlags+"\n";
		res += "FSVer: "+FSVer+"\n";
		res += "RootClus: "+RootClus+"\n";
		res += "FSInfo: "+FSInfo+"\n";
		res += "BkBootSec: "+BkBootSec+"\n";
		res += "Reserved: "+Reserved+"\n";
		res += "DrvNum: "+DrvNum+"\n";
		res += "Reserved1: "+Reserved1+"\n";
		res += "BootSig: "+BootSig+"\n";
		res += "VolID: "+VolID+"\n";
		res += "VolLab: "+VolLab+"\n";
		res += "FilSysType: "+FilSysType+"\n";
		res += "\n";
		res += "Some other worthy information:"+"\n";
		res += "==============================="+"\n";
		String fatType;
		if (getFatType() == FAT.FAT12)
			fatType = "FAT12";
		else if (getFatType() == FAT.FAT16)
			fatType = "FAT16";
		else if (getFatType() == FAT.FAT32)
			fatType = "FAT32";
		else
			fatType = "UNKNOWN";
		res += "fatType "+fatType+"\n";
		res += "last fat cluster "+getLastFatCluster()+"\n";
		res += "num data sectors "+getNumDataSectors()+"\n";
		res += "num fat sectors "+getNumFatSectors()+"\n";
		res += "first root directory sector number "+getFirstRootDirSecNum(/*getFatType()*/)+"\n";
		res += "first data sector "+getFirstDataSector()+"\n";
		res += "number of root directory sectors "+getRootDirSectors()+"\n";
		return res;
	}	//end toString()
}	//end class BPB
