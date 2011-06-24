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

package xxl.core.io.raw;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.XXLSystem;

/**
 * Contains some useful classes for practical tasks.
 */
public class RawAccessUtils {

	/**
	 * Creates a file with a user-defined length. This file
	 * can be used with RAFRawAccess.
	 *
	 * @param filename name of the file to be created
	 * @param sectors number of sectors
	 * @param sectorSize size of each sector in bytes
	 * @return true if the file has successfully been created.
	 */
	public static boolean createFileForRaw(String filename, long sectors, int sectorSize) {
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filename, "rw");
			raf.setLength(0);
			byte[] sector = new byte[sectorSize];
			for (int i = 0; i < sectors; i++)
				raf.write(sector);
			raf.close();
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	/**
	 * Creates a file with a user-defined length (512 bytes per sector). This file
	 * can be used with RAFRawAccess.
	 *
	 * @param filename name of the file to be created
	 * @param sectors number of sectors
	 * @return true if the file has successfully been created.
	 */
	public static boolean createFileForRaw(String filename, long sectors) {
		return createFileForRaw(filename,sectors,512);
	}

	/**
	 * Copies a RawAccess.
	 *
	 * @param from RawAccess that will be copied.
	 * @param to RawAccess that will be overwritten.
	 */
	public static void copyRawAccess(RawAccess from, RawAccess to) {
		long blocks = from.getNumSectors();
		int sectorSize = from.getSectorSize();
		
		if (blocks!=to.getNumSectors())
			throw new RuntimeException("copyRawAccess: Number of sectors not identical");
		if (sectorSize!=to.getSectorSize())
			throw new RuntimeException("copyRawAccess: size of sectors not identical");

		byte[] block = new byte[sectorSize];
		for (long i=0; i<blocks ; i++) {
			from.read(block, i);
			to.write(block, i);
		}
	}

	/**
	 * Fills a RawAccess with a special character.
	 *
	 * @param ra RawAccess that will be copied.
	 * @param numberOfSectors Number of sectors that will be overwritten.
	 * 	If this value is -1, the whole raw access will be overwritten.
	 * @param value byte value that is used for filling.
	 */
	public static void fillRawAccess(RawAccess ra, long numberOfSectors, int value) {
		byte[] block = new byte[ra.getSectorSize()];
		java.util.Arrays.fill(block, (byte) value);
		if (numberOfSectors==-1)
			numberOfSectors = ra.getNumSectors();
		for (long i=0; i<numberOfSectors ; i++) {
			ra.write(block, i);
		}
	}
	
	/**
	 * Checks the RawAccess if all bytes equals the specified byte value.
	 *
	 * @param ra the RawAccess to be checked
	 * @param numberOfSectors Number of sectors that will be checked.
	 * 	If this value is -1, the whole raw access will be checked.
	 * @param b the specified byte value
	 * @return true if the RawAccess only contains the specified byte value.
	 */
	public static boolean checkRawAccess(RawAccess ra, long numberOfSectors, byte b) {
		byte[] block = new byte[512];
		if (numberOfSectors==-1)
			numberOfSectors = ra.getNumSectors();
		
		for (int j=0; j<numberOfSectors; j++) {
			ra.read(block, j);
			for (int i=0; i<512; i++)
				if (block[i]!=b)
					return false;
		}
		return true;
	}
	
	/**
	 * Partitions a raw access into sizes.length+1 partitions.
	 * The last partition gets the rest of the free space of
	 * the raw access.
	 * 
	 * @param ra the given raw access
	 * @param sizes the sites of the partitions.
	 * @return an array with the partitions of the raw access.
	 */
	public static RawAccess[] rawAccessPartitioner(RawAccess ra, int sizes[]) {
		RawAccess ras[] = new RawAccess[sizes.length+1];
		int currOffset=0;
		
		for (int i=0; i<sizes.length; i++) {
			ras[i] = new PartitionRawAccess(ra,currOffset,sizes[i]);
			currOffset += sizes[i];
		}
		ras[sizes.length] = new PartitionRawAccess(ra,currOffset,ra.getNumSectors()-currOffset);
		
		return ras;
	}

	/**
	 * Creates a dummy file which is necessary for all classes which are
	 * inherited from java.io.RandomAccessFile.
	 * @param filename name of the dummy file.
	 * @return the associated file object.
	 */
	public static File createDummyFile(String filename) {
		File dummyFile = new File(filename);
		
		if (!dummyFile.exists()) {
			dummyFile.delete();
			try {
				dummyFile.createNewFile();
			}
			catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}
		}
		return dummyFile;
	}
	
	/**
	 * Creates a standard dummy file inside the outpath. This file is necessary for all 
	 * classes which are inherited from java.io.RandomAccessFile.
	 * @return the associated file object.
	 */
	public static File createStdDummyFile() {
		return createDummyFile(XXLSystem.getOutPath()+System.getProperty("file.separator")+"dummyFile");
	}
	
	/**
	 * Deletes a dummy file created with createDummyFile or createStdDummyFile.
	 * @param file the file object.
	 */
	public static void deleteDummyFile(File file) {
		file.delete();
	}
}
