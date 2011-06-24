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

/**
 * Implements RawAccess via usage of main memory.
 * It is fast and limited to 4GB.
 */
public class RAMRawAccess implements RawAccess {

	/**
	 * The array in main memory: [amount of sector][sectorSize]
	 */
	byte[][] array = null;
	/**
	 * Size of a sector
	 */
	private int sectorSize;

	/**
	 * Returns a new instance of a raw access that uses main memory.
	 *
	 * @param numblocks the amount of sectors
	 * @param sectorSize size of a sector
	 * @exception RawAccessException Description of the Exception
	 */
	public RAMRawAccess(long numblocks, int sectorSize) throws RawAccessException {
		this.sectorSize = sectorSize;
		open(numblocks);
	}

	/**
	 * Returns a new instance of a raw access that uses main memory
	 * (with sector size 512 bytes).
	 *
	 * @param numblocks the amount of sectors
	 * @exception RawAccessException Description of the Exception
	 */
	public RAMRawAccess(long numblocks) throws RawAccessException {
		this(numblocks,512);
	}

	/**
	 * Creates a standard raw access with 20480 blocks (1MB).
	 *
	 * @param filename normal devices have names, here it will be ignored (use NULL for example).
	 */
	public void open(String filename) {
		open(20480);
	}

	/**
	 * Creates a standard raw access with "numblocks" sectors.
	 *
	 * @param numblocks the amount of sectors
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void open(long numblocks) throws RawAccessException {
		int nb = (int) numblocks;
		// Casting alright?
		if (numblocks > nb)
			throw new RawAccessException("RAMRawAccess: wrong amount of blocks");
			
		try {
			array = new byte[(int) numblocks][sectorSize];
		}
		catch (OutOfMemoryError e) {
			throw new RawAccessException("RAMRawAccess: Out of memory");
		}
	}

	/**
	 * Closes the raw access.
	 *
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void close() throws RawAccessException {
		if (array == null)
			throw new RawAccessException("RAMRawAccess: close() failed");
	}

	/**
	 * Writes a block to the raw access.
	 *
	 * @param block array to be written
	 * @param sector number of the sector
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void write(byte[] block, long sector) throws RawAccessException {
		if (block.length != sectorSize)
			throw new RawAccessException("RAMRawAccess: write() wrong block length");
		if (array == null)
			throw new RawAccessException("RAMRawAccess: write() no device open");
		// array[(int) sector] = block;
		System.arraycopy(block, 0, array[(int) sector], 0, block.length);
	}

	/**
	 * Reads a block from the raw access.
	 *
	 * @param block byte array of sectorSize bytes for the sector
	 * @param sector number of the sector
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void read(byte[] block, long sector) throws RawAccessException {
		if (array == null)
			throw new RawAccessException("RAMRawAccess: read() no device open");
		
		// copy the block into the object we will return
		// Beware of returning references which can be overwritten!
		System.arraycopy(array[(int) sector], 0, block, 0, array[(int) sector].length);
	}

	/**
	 * The amount of blocks
	 *
	 * @return the amount of blocks. If anything fails the value will be -1
	 */
	public long getNumSectors() {
		// Do we have a "RAM" disk?
		if (array == null)
			return -1;
		return array.length;
	}

	/**
	 * Returns the size of a sector of the file/device.
	 *
	 * @return size of sectors
	 */
	public int getSectorSize() {
		return sectorSize;
	}

	/**
	 * Outputs a String representation of the raw device.
	 * @return A String representation.
	 */
	public String toString()  {
		return 
			"RAM raw access, sectors: "+getNumSectors()+
			", sectorSize: "+getSectorSize();
	}
}
