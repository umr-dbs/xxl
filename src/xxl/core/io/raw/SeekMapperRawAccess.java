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
 * If you have a big hard drive and only relatively small data,
 * then possibly all data is on the same physical cylinder.
 * So, no seeks will be performed.
 * To test the algorithm under the influence of seeks, you can
 * use this class which to map an existing RawAccess. When moving 
 * to the next sector, a seek of 2^sequentialSeekDistanceExponent
 * sectors is performed.
 * <p>
 * Only those sectors are mapped which have a number below
 * the largest x with 2^x<=numberOfSectors.
 * <p>
 * In the example in the main method, a raw access of 18 sectors
 * is mapped with different seek distance exponents (2 and 3).
 */
public class SeekMapperRawAccess extends DecoratorRawAccess {

	/**
	 * The exponent to the base 2 of the seek distance. The seek
	 * from sector n to sector n+1 will be mapped to a seek distance
	 * of 2^sequentialSeekDistanceExponent.
	 */
	protected int sequentialSeekDistanceExponent;
	
	int maxMappedSector;
	int restExponent;
	
	/**
	 * Constructs a new RawAccess which maps the sector numbers to
	 * new ones (Permutation).
	 * @param ra RawAccess which is mapped.
	 * @param sequentialSeekDistanceExponent The exponent 
	 * 	to the base 2 of the seek distance. The seek from sector 
	 * 	n to sector n+1 will be mapped to a seek distance
	 *	of 2^sequentialSeekDistanceExponent.
	 */
	public SeekMapperRawAccess(RawAccess ra, int sequentialSeekDistanceExponent) {
		super(ra);
		this.sequentialSeekDistanceExponent = sequentialSeekDistanceExponent;
		
		int sizeExponent = (int) (Math.log(getNumSectors()) / Math.log(2));
		maxMappedSector = (1<<sizeExponent) - 1;
		
		restExponent = sizeExponent-sequentialSeekDistanceExponent;
		if (restExponent<0)
			throw new RuntimeException("sequentialSeekDistanceExponent is too big.");
	}

	/**
	 * Mapping function for the sectors. 
	 * @param sector Original sector number.
	 * @return New sector number.
	 */
	protected long mapFunction(long sector) {
		if (sector>=maxMappedSector)
			return sector;
		else
			return 
				((sector & ((1<< restExponent)-1)) << sequentialSeekDistanceExponent) + 
				(sector >> restExponent);
	}

	/**
	 * Writes a sector of a characteristic length to the file/device.
	 *
	 * @param block byte array which will be written to the sector
	 * @param sector number of the sector in the file/device where the block will be written
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void write(byte[] block, long sector) throws RawAccessException {
		ra.write(block, mapFunction(sector));
	}

	/**
	 * Reads a sector of characteristic Bytes length from the file/device.
	 *
	 * @param block byte array of which will be written to the sector
	 * @param sector number of the sector in the file/device from where the block will be read
	 * @exception RawAccessException a specialized RuntimeException
	 */
	public void read(byte[] block, long sector) throws RawAccessException {
		ra.read(block, mapFunction(sector));
	}

	/**
	 * Outputs a String representation of the raw device.
	 * @return A String representation.
	 */
	public String toString()  {
		return 
			"Seek mapper raw access of: "+ra;
	}
}
