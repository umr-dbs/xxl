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
 * This is the implementation of RawAccess via JNI an C methods which implement the
 * unbuffered access to files and devices.
 */
public class NativeRawAccess implements RawAccess {

	/**
	 * HANDLE for the device
	 */
	protected long filep = 0;
	/**
	 * Amount of sectors
	 * The variable will be set by the native open method
	 */
	protected long sectors = 0;
	/**
	 * Size of a sector
	 */
	protected int sectorSize;
	/**
	 * Mode of access (bitwise encoded):
	 * <ul>
	 * <li>Bit 0: flush buffers after each write</li>
	 * <li>Bit 1: Try to set hard drive read cache?</li>
	 * <li>Bit 2: Mode of hard drive read cache</li>
	 * <li>Bit 3: Try to set hard drive write cache?</li>
	 * <li>Bit 4: Mode of hard drive write cache</li>
	 * </ul>
	 * Only used inside native code.
	 */
	public int mode=0;
	/**
	 * Last accessed sector. Only used inside native code.
	 */
	public long lastSector=0;

	/**
	 * Constructs a new raw access that uses JNI.
	 *
	 * @param filename of file or device (example: c: or /dev/hda4)
	 * @param sectorSize size of a sector
	 * @param mode mode of access (bitwise encoded). Bit 0: flush buffers after each write
	 * @exception RawAccessException a spezialized RuntimeException
	 */
	public NativeRawAccess(String filename, int sectorSize, int mode) throws RawAccessException {
		this.sectorSize = sectorSize;
		this.filep = 0;
		this.mode = mode;
		try {
			this.open(filename);
		}
		catch (RawAccessException e) {
			this.sectors = 0;
			throw new RawAccessException(e.toString());
		}
	}

	/**
	 * Constructs a new raw access that uses JNI.
	 *
	 * @param filename of file or device (example: c: or /dev/hda4)
	 * @param sectorSize size of a sector
	 * @exception RawAccessException a spezialized RuntimeException
	 */
	public NativeRawAccess(String filename, int sectorSize) throws RawAccessException {
		this (filename,sectorSize,0);
	}

	/**
	 * Constructs a new raw access that uses JNI
	 * (with sector size 512 bytes).
	 *
	 * @param filename of file or device (example: c: or /dev/hda4)
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public NativeRawAccess(String filename) throws RawAccessException {
		this(filename,512);
	}

	/**
	 * Wrapped native open method.
	 *
	 * @param filename of file or device (example: c: or /dev/hda4)
	 * @throws RawAccessException
	 */
	public native void open(String filename) throws RawAccessException;

	/**
	 * Wrapped native close method
	 *
	 * @throws RawAccessException
	 */
	public native void close() throws RawAccessException;

	/**
	 * Wrapped native write method
	 *
	 * @param block array to be written
	 * @param sector number of the sector
	 * @throws RawAccessException
	 */
	public native void write(byte[] block, long sector) throws RawAccessException;

	/**
	 * Wrapped native read method
	 *
	 * @param block array to be written
	 * @param sector number of the sector
	 * @throws RawAccessException
	 */
	public native void read(byte[] block, long sector) throws RawAccessException;

	/**
	 * Returns the amount of blocks of the device
	 *
	 * @return amount of blocks
	 */
	public long getNumSectors() {
		return sectors;
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
	 * Native method which sets the state of the hard drive cache
	 * according to the value. It might be impossible to set the
	 * state on every hard drive model. To check this, use
	 * getHardDriveCacheMode.
	 * @param cacheState The cache state which is wanted: Bit 0: read cache, Bit 1: write cache.
	 * @throws RawAccessException
	 */
	public native void setHardDriveCacheMode(int cacheState) throws RawAccessException;

	/**
	 * Gets the state of the hard drive cache.
	 * @return The cache state which could be set: Bit 0: read cache, Bit 1: write cache.
	 * @throws RawAccessException
	 */
	public int getHardDriveCacheMode() {
		return ((mode&4)>>2) | ((mode&16)>>3);
	}

	/**
	 * Outputs a String representation of the raw device.
	 * @return A String representation.
	 */
	public String toString()  {
		return 
			"Native raw access, sectors: "+getNumSectors()+
			", sectorSize: "+getSectorSize()+
			", cacheMode: "+getHardDriveCacheMode();
	}

	/*
	 *  JNI methods call native methods of a library which has to be loaded.
	 *  This will be done once by the first use of this class
	 */
	static {
		System.loadLibrary("RawAccess");
	}
}
