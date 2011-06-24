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
 * Tests a RawAccess by writing the data to a second
 * control raw access. After each read operation, the
 * data is compared between the original raw access and
 * the control raw access (in main memory).
 */
public class TesterRawAccess extends DecoratorRawAccess {

	/** 
	 * Internally used control raw access to store the data
	 * a second time.
	 */ 
	protected RAMRawAccess testerRA;

	/**
	 * Constructs a new tester for a RawAccess.
	 * @param ra The RawAccess to be tested.
	 * @param size Number of sectors which are reserved for
	 * 	the storage of the control raw access. -1 means the
	 * 	same size than the original raw access.
	 */
	public TesterRawAccess(RawAccess ra, long size) {
		super(ra);
		if (size==-1)
			size = ra.getNumSectors();
		
		byte value=0;
		System.out.println("Writing a value");
		RawAccessUtils.fillRawAccess(ra, size, value);
		System.out.println("Checking raw access");
		RawAccessUtils.checkRawAccess(ra, size, value);
		
		testerRA = new RAMRawAccess(size, ra.getSectorSize());
		
		byte block[] = new byte[ra.getSectorSize()];
		for (int i=0; i<size; i++) {
			ra.read(block, i);
			testerRA.write(block, i);
			System.out.print(i+" ");
		}
	}

	/**
	 * Writes a sector of a characteristic length to the file/device.
	 *
	 * @param block byte array which will be written to the sector
	 * @param sector number of the sector in the file/device where the block will be written
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void write(byte[] block, long sector) throws RawAccessException {
		super.write(block, sector);
		testerRA.write(block, sector);
	}

	/**
	 * Reads a sector of characteristic Bytes length from the file/device.
	 *
	 * @param block byte array of which will be written to the sector
	 * @param sector number of the sector in the file/device from where the block will be read
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void read(byte[] block, long sector) throws RawAccessException {
		super.read(block, sector);
		byte[] b2 = new byte[ra.getSectorSize()];
		testerRA.read(b2, sector);
		
		if (block.length!=b2.length)
			throw new RuntimeException("Not the same size read, sector: "+sector);
		
		for (int i=0; i<block.length; i++)
			if (block[i]!=b2[i])
				throw new RuntimeException("Not the same content, sector: "+sector+", byte: "+i+", data: "+block[i]+", instead: "+b2[i]);
	}

	/**
	 * Outputs a String representation of the raw device.
	 * @return A String representation.
	 */
	public String toString()  {
		return "Tester raw access of: "+ra;
	}
}
