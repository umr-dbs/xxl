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

package xxl.core.io;

import java.io.File;

import xxl.core.io.raw.NativeRawAccess;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RAMRawAccess;
import xxl.core.io.raw.RawAccess;
import xxl.core.io.raw.RawAccessArrayFilesystemOperations;
import xxl.core.io.raw.RawAccessUtils;
import xxl.core.io.raw.SeekMapperRawAccess;
import xxl.core.io.raw.StatisticsRawAccess;
import xxl.core.util.XXLSystem;

/**
 * Creates a simple raw device filesystem or connects to 
 * an existing filesystem.
 */
public class XXLFilesystem {
	/**
	 * Object which allows modifications on files on the
	 * file system.
	 */
	protected FilesystemOperations fso;
	/** RawAccess object */
	protected RawAccess ra = null;
	/** Dummy file which is needed. */
	protected File df = null;
	/** Name of the raw device (or path) */
	protected String rawDeviceName;
	/** Is it a native raw device? */
	protected boolean nativeRawDevice;

	public static RawAccess createRawAccess(boolean nativeRawDevice, int blockSize, int sizeOfRawDevice, String rawDeviceName, int seekExponent, boolean doStatistics) {
		RawAccess ra = null;
		
		if (rawDeviceName.equals(""))
			ra = new RAMRawAccess(sizeOfRawDevice, blockSize);
		else if (nativeRawDevice) {
			NativeRawAccess nra = new NativeRawAccess(rawDeviceName, blockSize, 0);
			nra.setHardDriveCacheMode(0);
			ra = nra;
			
			//try {
			//	ra = new LoggerRawAccess(new FileOutputStream("C:/raw.log"), new TesterRawAccess(nra, 1000)
			//	);
			//}
			//catch (IOException e) {}
		}
		else {
			String rawFileName = XXLSystem.getOutPath()+File.separator+rawDeviceName;
			System.out.println(rawFileName);
			RawAccessUtils.createFileForRaw(
				rawFileName,
				blockSize, 
				sizeOfRawDevice
			);
			ra = new RAFRawAccess(rawFileName, false, blockSize);
		}
		
		if (doStatistics)
			ra = new StatisticsRawAccess(ra);
		
		if (seekExponent>0)
			ra = new SeekMapperRawAccess(ra, seekExponent);
		
		return ra;
	}

	/** 
	 * Creates a filesystem based on a raw device. 
	 * @param nativeRawDevice Is it a native raw device?
	 * @param blockSize The block size
	 * @param sizeOfRawDevice The size of the raw device (if it is not a native raw device)
	 * @param rawDeviceName Name of the raw device (or path)
	 * @param partitions Size of each file on the static filesystem.
	 * @param fileNames Name of the files on the static filesystem.
	 * @param seekExponent Performs a sector mapping so that a seek of 
	 * 	size 2^seekExponent is performed, when going from one sector to 
	 * 	the next sector. If 0, then no mapping is performed.
	 * @param doStatistics Gather statistical informations on the main raw access?
	 * @param createFiles Determines if files are already available (false) or 
	 * 	new files should overwrite existing data inside the sectors.
	 * @param initBlocksAtEOF Determines iff new space at the end a RandomAccessFile should 
	 * 	be initialized will 0-values or not. <code>true</code> is the standard, because it
	 * 	is fully compliant with java.io.RandomAccessFile.
	 */
	public XXLFilesystem(boolean nativeRawDevice, int blockSize, int sizeOfRawDevice, String rawDeviceName, int partitions[], String fileNames[], int seekExponent, boolean doStatistics, boolean createFiles, boolean initBlocksAtEOF) {
		this.rawDeviceName = rawDeviceName;
		this.nativeRawDevice = nativeRawDevice;
		
		if (rawDeviceName.equals(""))
			ra = new RAMRawAccess(sizeOfRawDevice, blockSize);
		else if (nativeRawDevice) {
			NativeRawAccess nra = new NativeRawAccess(rawDeviceName, blockSize, 0);
			nra.setHardDriveCacheMode(0);
			ra = nra;
			
			//try {
			//	ra = new LoggerRawAccess(new FileOutputStream("C:/raw.log"), new TesterRawAccess(nra, 1000)
			//	);
			//}
			//catch (IOException e) {}
		}
		else {
			String rawFileName = XXLSystem.getOutPath()+File.separator+rawDeviceName;
			System.out.println(rawFileName);
			RawAccessUtils.createFileForRaw(
				rawFileName,
				blockSize, 
				sizeOfRawDevice
			);
			ra = new RAFRawAccess(rawFileName, false, blockSize);
		}
		
		if (doStatistics)
			ra = new StatisticsRawAccess(ra);
		
		if (seekExponent>0)
			ra = new SeekMapperRawAccess(ra, seekExponent);
		
		// produce dummy file
		df = RawAccessUtils.createStdDummyFile();
		
		RawAccess ras[];
		ras = RawAccessUtils.rawAccessPartitioner(ra, partitions);
		fso = new RawAccessArrayFilesystemOperations(ras, fileNames, createFiles, df, initBlocksAtEOF);
	}

	/**
	 * Connects to the normal filesystem using buffered write or not.
	 * @param unbufferedWrite use unbuffered write (needs java 1.4).
	 */
	public XXLFilesystem(boolean unbufferedWrite) {
		if (unbufferedWrite && (XXLSystem.getJavaVersion()<1.4))
			System.out.println("WARNING: using unbuffered write may cause an exeption with sdk<1.4");
		
		if (unbufferedWrite)
			fso = JavaFilesystemOperations.UNBUFFERED_DEFAULT_INSTANCE;
		else
			fso = JavaFilesystemOperations.DEFAULT_INSTANCE;
	}

	/**
	 * Returns a FilesystemOperations object which is able
	 * to manipulate files.
	 * @return a FilesystemOperations object
	 */
	public FilesystemOperations getFilesystemOperations() {
		return fso;
	}

	/**
	 * Closes the connection to a filesystem and removes
	 * files which were created inside the constructor.
	 */
	public void close() {
		if (ra!=null && ra instanceof RAFRawAccess) {
			ra.close();
			new File(XXLSystem.getOutPath()+File.separator+rawDeviceName).delete();
			RawAccessUtils.deleteDummyFile(df);
		}
	}

	/**
	 * Retrieves the raw access which is used.
	 * @return The raw access.
	 */
	public RawAccess getRawAccess() {
		return ra;
	}

	/**
	 * Outputs the statistical information which was gathered.
	 * @return Statistical information.
	 */
	public String toString() {
		if (ra!=null)
			return ra.toString();
		else
			return "Java Filesystem";
	}
}
