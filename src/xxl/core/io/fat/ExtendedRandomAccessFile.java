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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import xxl.core.io.fat.errors.DirectoryException;
import xxl.core.io.fat.errors.FileDoesntExist;
import xxl.core.io.fat.errors.InitializationException;
import xxl.core.io.fat.util.ByteArrayConversionsLittleEndian;
import xxl.core.io.fat.util.MyMath;


/**
 * This class has all operations that RandomAccessFile has, but uses our
 * device for this operations. If you order an RandomAccessFile from the class
 * FATDevice you get this class back and you can use it as a RandomAccessFile.
 * Some information about how the file works: If you create a new file that 
 * doesn't exist the file size will be zero and no cluster is allocated
 * to store data. Only if you use a write method or the method setLength(newLength)
 * clusters are allocated to store data. The constructor checks if there is already
 * a file with the given fileName this causes some IO-Operations. If you change the 
 * file length either by a write or setLength(newLength) method the file length
 * stored at the directory entry in the directory will not change. The new file
 * length will be stored at this directory entry only if you close this file
 * by the close method. This is done because otherwise there are to much IO-
 * Operations to actualize the file length. To minimize the IO-Operations
 * further this class manage a filePointer, which points to the actual position
 * (all read/write operations are done at this position), a cluster number, and 
 * a sector number they point to the actual cluster and sector. From time to time
 * the first and the last cluster number of the file is needed therefore it manages
 * this numbers also.
 * If an instance of this object is created the first call has to be the
 * super constructor this needs a fileName and will open that file. Therefore we
 * use a dummy file 'dummyFile' with read only access that will be closed after
 * the super constructor has been called.
 */
public class ExtendedRandomAccessFile extends RandomAccessFile
{

	/**
	 * The whole functionality of the methods is directed to device.
	 */
	private FATDevice device = null;
	
	/**
	 * The name of the file.
	 */
	private String fileName;
	
	/**
	 * The mode of the file it is either "r" which means read access only or
	 * it is "rw" which means read and write access.
	 */
	private String mode;
	
	/**
	 * The file pointer is an index to the actual position within the file.
	 * All read or write accesses occur at the file pointer position.
	 */
	private long filePointer;
	
	/**
	 * The number of the actual cluster where the filePointer points to.
	 */
	private long clusterNumber;
	
	/**
	 * The start cluster number of the file. As long as it is zero it's not valid.
	 */
	private long startClusterNumber = 0;
	
	/**
	 * The last cluster number of the file.
	 */
	private long lastClusterNumber = 0;

	/**
	 * The number of the actual sector number where the filePointer points to.
	 */
	private long sectorNumber;
	
	/**
	 * Counter that determines how many sectors within a cluster has been read/written.
	 */
	private long sectorCounter;
	
	/**
	 * Flag that indicates if the clusterNumber points to the EOC_MARK.
	 */
	private boolean isLastCluster;
	
	/**
	 * The actual length of the file.
	 */
	private long fileLength;
	
	/**
	 * Contains the length of the file before a file length changing operation.
	 */	
	private long oldFileLength;
	
	/**
	 * The number of sectors per cluster.
	 */
	private int secPerClus;
	
	/**
	 * The number of bytes per sector.
	 */
	private int bytsPerSec;
	
	/**
	 * Buffered sector
	 */
	private byte bufferedSector[];
	
	/**
	 * Indicates if the buffered sector has changed
	 */
	private boolean bufferedSectorChanged;

	/**
	 * Buffered sector.
	 */
	private long sectorInBuffer;
	
	/**
	 * Creates a random access file stream to read from, and optionally
	 * to write to, the file specified by the File argument. A new
	 * FileDescriptor object is created to represent this file connection. 
	 *
	 * The mode argument must either be equal to "r" or "rw", indicating
	 * that the file is to be opened for input only or for both input and
	 * output, respectively. The write methods on this object will always
	 * throw an IOException if the file is opened with a mode of "r". If
	 * the mode is "rw" and the file does not exist, then an attempt is
	 * made to create it. An IOException is thrown if the file argument
	 * refers to a directory. 
	 *
	 * If there is a security manager, its checkRead method is called with
	 * the pathname of the file argument as its argument to see if read
	 * access to the file is allowed. If the mode is "rw", the security
	 * manager's checkWrite method is also called with the path argument
	 * to see if write access to the file is allowed.
	 * 
	 * @param device object of FATDevice.
	 * @param file the file object.
	 * @param mode the access mode.
	 * @param dummyFile The implementation of the base class needs a file
	 *	that exists inside the file system. The file is never used,
	 *	just opened as read only and immediately closed again. This parameter
	 *	only exists because of the inflexible implementation of RandomAccessFile.
	 * @throws IllegalArgumentException if the mode argument is not equal
	 * to "r" or to "rw".
	 * @throws FileNotFoundException if the file exists but is a directory
	 * rather than a regular file, or cannot be opened or created for any other reason.
	 * @throws DirectoryException in case of a directory error.
	 */
	protected ExtendedRandomAccessFile(FATDevice device, ExtendedFile file, String mode, File dummyFile) throws FileNotFoundException, DirectoryException
	{
		this(device, file.getAbsolutePath(), mode, dummyFile);
	}	//end constructor
		
		
	/**
	 * Construct an instance of this object.
	 * @param device an object of the device where this file works on.
	 * @param fileName the name of the file.
	 * @param mode the mode of the file is either "r" which means read access only or
	 * it is "rw" which means read and write access.
	 * @param dummyFile The implementation of the base class needs a file
	 *	that exists inside the file system. The file is never used,
	 *	just opened as read only and immediately closed again. This parameter
	 *	only exists because of the inflexible implementation of RandomAccessFile.
	 * @throws FileNotFoundException in case the file is read-only but doesn't exist.
	 * @throws IllegalArgumentException in case one or more arguments are illegal.
	 * @throws DirectoryException in case of a directory error.
	 */
	protected ExtendedRandomAccessFile(FATDevice device, String fileName, String mode, File dummyFile) throws FileNotFoundException, DirectoryException {
		//if we use RandomAccessFile we have to create a file
		//therefore we use a dummy file with read access only
		//but we will never use this file, all file activities
		//will be directed to our device with the correct 
		//file given by fileName and the correct mode
		//In later versions we can use a ram-disk for the file, so
		//that no file operation on disk is used.
		super(dummyFile, "r");
		try
		{	
			//we never use this file -> close it		
			super.close();
		}
		catch(Exception e)
		{
			throw new InitializationException("Couldn't initialize ExtendedRandomAccessFile, because of: "+e);
		}
		
		if (!mode.equals(FATDevice.FILE_MODE_READ) &&
			!mode.equals(FATDevice.FILE_MODE_READ_WRITE))
			throw new IllegalArgumentException("The mode has to be either FATDevice.FILE_MODE_READ or FATDevice.FILE_MODE_READ_WRITE!");
		if (device.isDirectory(fileName))
			throw new FileNotFoundException("A directory is not accessible as an ExtendedRandomAccessFile!");
			
		this.device = device;
		this.fileName = fileName;
		this.mode = mode;
		
		try {
			if (device.fileExists(fileName)) {
				//get the cluster number of the directory entry indicated by fileName
				clusterNumber = device.getStartClusterNumber(fileName);
				startClusterNumber = clusterNumber;
				//calculate the start sector number from the clusterNumber
				sectorNumber = device.getFirstSectorNumberOfCluster(clusterNumber);
				isLastCluster = device.isLastCluster(clusterNumber);
				fileLength = device.length(fileName);
			}
			else if (mode.equals(FATDevice.FILE_MODE_READ_WRITE)) {
				clusterNumber = device.createFile(fileName);
				//at this moment the clusterNumber points to an incorrect value
				
				isLastCluster = true;	//there is no reserved cluster at this moment
				fileLength = 0;
			}
			else
				throw new FileNotFoundException("You can't read a file that doesn't exist! "+fileName);
			
			lastClusterNumber = getLastClusterNumber();
			
			bytsPerSec = device.getBytsPerSec();
			secPerClus = device.getSecPerClus();			
		}
		catch (FileDoesntExist fde)	//transform the FileDoesntExist exception to FileNotFoundException
		{
			throw new FileNotFoundException("Couldn't find the file: "+fileName);
		}

		filePointer = -1;
		sectorCounter = 0;
		oldFileLength = fileLength;
		
		sectorInBuffer = -1;
		bufferedSectorChanged = false;
		bufferedSector = new byte[bytsPerSec];
	}	//end constructor


	/**
	 * Return the name of this file.
	 * @return the name of this file
	 */
	public String getName() {
		return fileName;
	}	//end getName()
	
	
	/**
	 * Close this extended random access file stream and release any system
	 * resources associated with the stream.
	 * @throws IOException in case of an I/O error.
	 */
	public void close() throws IOException {
		commit();
		if (oldFileLength != fileLength)
		{
			device.writeLength(fileName, fileLength, true);
		}
				
		device.close(fileName);
	}	//end close

	/**
	 * Committing changes of the buffered sector to the device.
	 */
	private void commit() {
		if (bufferedSectorChanged) {
			device.writeSector(bufferedSector, sectorInBuffer);
			bufferedSectorChanged = false;
		}
	}
	
	/**
	 * Read a sector from the device into the buffer. If necessary 
	 * first write back changes.
	 * 
	 * @param sectorNumber the number of the sector to be read into the buffer.
	 */
	private void readSectorIntoBuffer(long sectorNumber) {
		if (sectorInBuffer==-1) {
			device.readSector(bufferedSector, sectorNumber);
			sectorInBuffer = sectorNumber;
		}
		
		if (sectorInBuffer!=sectorNumber) {
			commit();
			device.readSector(bufferedSector, sectorNumber);
			sectorInBuffer = sectorNumber;
		}
	}
	
	/**
	 * Reads a byte of data from this file. The byte is returned as
	 * an integer in the range 0 to 255 (0x00-0x0ff).
	 * @return the next byte of data, or -1 if the end of the
	 * file has been reached.
	 * @throws IOException if the end of file is exceed.
	 */
	public int read() throws IOException {
		filePointer++;		//set filePointer to the actual position			
		
		if (filePointer >= fileLength)
			return -1;
			// throw new IOException("End of file "+fileName+" exceed.");
		
		if (filePointer != 0 && filePointer % (bytsPerSec) == 0)
		{
			sectorNumber++;
			sectorCounter++;
			if (sectorCounter == secPerClus)
			{
				sectorCounter = 0;
				
				if (isLastCluster)
					throw new IOException("End of file "+fileName+" exceed.");
					
				clusterNumber = device.getFatContent(clusterNumber);	//update clusterNumber
				sectorNumber = device.getFirstSectorNumberOfCluster(clusterNumber);	//update sectorNumber
				isLastCluster = device.isLastCluster(clusterNumber);
			}
		}
		
		readSectorIntoBuffer(sectorNumber);
		return  ByteArrayConversionsLittleEndian.convShort(bufferedSector[(int) (filePointer%bytsPerSec)]);
		// old: device.readByte(filePointer, sectorNumber);
	}	//end read()
	
	
	/**
	 * Reads up to length bytes of data from this file into an array of bytes.
	 * @param buffer the buffer into which the data is read.
	 * @param offset the start offset of the data.
	 * @param length the maximum number of bytes read.
	 * @return the total number of bytes read into the buffer, or -1 if
	 * there is no more data because the end of the file has been reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read(byte[] buffer, int offset, int length) throws IOException {
		if (filePointer + 1 >= fileLength)
			return -1;
				
		for (int i=0; i < length; i++)
		{
			if (filePointer + 1 < fileLength)
				buffer[offset + i] = (byte)read();
			else
				return i;
		}
		
		return length;
	}	//end read(byte[], int, int)
	
	
	/**
	 * Reads up to b.length bytes of data from this file into an array of bytes.
	 * @param buffer the buffer into which the data is read.
	 * @return the total number of bytes read into the buffer, or -1 if there is
	 * no more data because the end of this file has been reached.
	 * @throws IOException - if an I/O error occurs.
	 */
	public int read(byte[] buffer) throws IOException {
		if (filePointer + 1 >= fileLength)
			return -1;
				
		for (int i=0; i < buffer.length; i++)
		{
			if (filePointer + 1 < fileLength)
				buffer[i] = (byte)read();
			else
				return i;
		}
		
		return buffer.length;
	}	//end read(byte[])


	/**
	 * Attempts to skip over n bytes of input discarding the skipped bytes. 
	 * This method may skip over some smaller number of bytes, possibly zero.
	 * This may result from any of a number of conditions; reaching end of
	 * file before n bytes have been skipped is only one possibility. This
	 * method never throws an EOFException. The actual number of bytes 
	 * skipped is returned. If n is negative, no bytes are skipped.	
	 * @param n the number of bytes to be skipped.
	 * @return the actual number of bytes skipped.
	 * @throws IOException - if an I/O error occurs.
	 */
	public int skipBytes(int n) throws IOException {
		if (n <= 0)
			return -1;
		
		long pos = getFilePointer();
		long newPos = pos + n;
		
		if (newPos > fileLength)
			newPos = fileLength;
			
		seek(newPos);
		
		return (int)(newPos - pos);
	}	//end skipBytes(int)
		
	
	/**
	 * Writes the specified byte to this file. The write starts at
	 * the current file pointer.
	 * @param value the byte to be written.
	 * @throws IOException - if an I/O error occurs.
	 */
	public void write(int value) throws IOException {
		if (mode.equals(FATDevice.FILE_MODE_READ))
			throw new IOException("You have no write access to the file: "+fileName);
		
		//the user seed beyond the end of the file, now
		//we need to extend the file size
		if (filePointer+1 >= fileLength)
			setLength(filePointer + 2);

		filePointer++;	

		if (startClusterNumber == 0) {
			clusterNumber = lastClusterNumber;
			startClusterNumber = clusterNumber;
			sectorNumber = device.getFirstSectorNumberOfCluster(clusterNumber);
		}
			
		if (filePointer != 0 && filePointer % (bytsPerSec) == 0) {
			// System.out.println("sectorNumber= "+sectorNumber+", sectorCounter="+sectorCounter);
			sectorNumber++;
			sectorCounter++;
			// System.out.println("sectorNumber= "+sectorNumber+", sectorCounter="+sectorCounter);
			if (sectorCounter == secPerClus) {
				// System.out.println("and further "+clusterNumber);
				sectorCounter = 0;
				clusterNumber = device.getFatContent(clusterNumber);
				sectorNumber = device.getFirstSectorNumberOfCluster(clusterNumber);
				isLastCluster = device.isLastCluster(clusterNumber);
			}
		}
		// System.out.print("write("+filePointer+","+value+","+sectorNumber+");");
		readSectorIntoBuffer(sectorNumber);
		bufferedSector[(int) (filePointer%bytsPerSec)] = (byte) value;
		bufferedSectorChanged = true;
		
		// old: device.writeByte(fileName, value, filePointer, sectorNumber);
	}	//end writeByte(int)


	/**
	 * Writes b.length bytes from the specified byte array to this
	 * file, starting at the current file pointer.
	 * @param b the data.
	 * @throws IOException - if an I/O error occurs.
	 */
	public void write(byte[] b) throws IOException {
		for (int i=0; i < b.length; i++)
			writeByte(b[i]);
	}	//end write(byte[])


	/**
	 * Writes length bytes to this file from the specified byte array starting at
	 * offset. The user has to be sure that the length
	 * of the array will not be exceed.
	 * @param b the data.
	 * @param offset the start offset in the data.
	 * @param length the number of bytes to write.
	 * @throws IOException - if an I/O error occurs.
	 */
	public void write(byte[] b, int offset, int length) throws IOException {
		for (int i=0; i < length; i++)
			writeByte(b[offset + i]);
	}	//end write(byte[], int, int)
	

	/**
	 * Sets the file-pointer offset, measured from the beginning
	 * of this file, at which the next read or write occurs. The
	 * offset may be set beyond the end of the file. Setting the
	 * offset beyond the end of the file does not change the file
	 * length. The file length will change only by writing after
	 * the offset has been set beyond the end of the file.
	 * @param pos - the offset position, measured in bytes from
	 * the beginning of the file, at which to set the file pointer.
	 * @throws IOException if an I/O error occurs.
	 */
	public void seek(long pos) throws IOException {	
		if (pos < 0)
			return;
		pos--;
		if (pos < 0)
		{
			filePointer = pos;
			clusterNumber = startClusterNumber;
			sectorCounter = 0;
			sectorNumber = device.getFirstSectorNumberOfCluster(startClusterNumber);
			return;
		}
		
		long clustersToSkip = pos / (bytsPerSec*secPerClus);
		long sectorsToSkip = (pos - clustersToSkip*bytsPerSec*secPerClus) / bytsPerSec;
				
		//get the cluster number of the directory entry indicated by fileName
		if (startClusterNumber > 0)
			clusterNumber = startClusterNumber;
		else
		{
			clusterNumber = device.getStartClusterNumber(fileName);
			startClusterNumber = clusterNumber;
		}
		
		if (clusterNumber != 0)
		{
			for (int i=0; i < clustersToSkip; i++)
			{
				//The filePointer is maybe set beyond the end of the file.
				//If the user use read an exception will be thrown but if
				//the user use write the file length will be extended.
				long fatContent = device.getFatContent(clusterNumber);
				if (device.isLastCluster(fatContent))
				{
					lastClusterNumber = clusterNumber;
					break;
				}
				clusterNumber = fatContent;
			}
			
			sectorNumber = device.getFirstSectorNumberOfCluster(clusterNumber) + sectorsToSkip;
			sectorCounter = sectorsToSkip;
		}
		
		filePointer = pos;
	}	//end seek(pos)
	
	
	/**
	 * Returns the current offset in this file.
	 * @return the offset from the beginning of the file, in bytes,
	 * at which the next read or write occurs.
	 * @throws IOException if an I/O error occurs.
	 */
	public long getFilePointer() throws IOException {
		return filePointer + 1;
	}	//end getFilePointer()
	
	
	/**
	 * Returns the length of this file.
	 * @return the length of this file, measured in bytes.
	 * @throws IOException if an I/O error occurs.
	 */
	public long length() throws IOException {
		return fileLength;
	}	//end length()
	
	
	/**
	 * Sets the length of this file. 
	 * If the present length of the file as returned by the length
	 * method is greater than the newLength argument then the file
	 * will be truncated. In this case, if the file offset as returned
	 * by the getFilePointer method is greater then newLength then
	 * after this method returns the offset will be equal to newLength. 
	 * If the present length of the file as returned by the length
	 * method is smaller than the newLength argument then the file will
	 * be extended. In this case, the contents of the extended portion
	 * of the file are not defined.
	 * @param newLength - The desired length of the file.
	 * @throws IOException if an I/O error occurs.
	 */
	public void setLength(long newLength) throws IOException {
		long numOfClustersFileLength = MyMath.roundUp((float)fileLength / (float)(secPerClus*bytsPerSec));
		long numOfClustersNewLength = MyMath.roundUp((float)newLength / (float)(secPerClus*bytsPerSec));
		long diff = numOfClustersFileLength - numOfClustersNewLength;
		// System.out.println("diff= "+diff);

		//truncate the file if the new length is smaller than the actual file length
		if (diff>0) {
			//free clusters
			//calculate the new last cluster number, that is the cluster number where newLength points to
			lastClusterNumber = startClusterNumber;
			for (long i=1; i < numOfClustersNewLength; i++)
				lastClusterNumber = device.getFatContent(lastClusterNumber);
			
			device.addFreeClustersMarkFirstAsEOC(lastClusterNumber);
			
			seek(newLength);
			fileLength = newLength;
		}
		else if (diff<0) {
			//extend the file if the fileLength is smaller than the wished length

			List freeClusters;
			//order new space
			// System.out.println("lcn="+lastClusterNumber+", noc="+numOfClusters);
			if (lastClusterNumber == 0) {
				freeClusters = device.extendFileSize(fileName, -diff);
				clusterNumber = device.getStartClusterNumber(fileName);
				startClusterNumber = clusterNumber;
				sectorNumber = device.getFirstSectorNumberOfCluster(clusterNumber);	//update sectorNumber
			}
			else
				freeClusters = device.extendFileSize(-diff, lastClusterNumber);

			//set the new last cluster number
			lastClusterNumber = ((Long)freeClusters.get(freeClusters.size() - 1)).longValue();
			isLastCluster = device.isLastCluster(clusterNumber);
		}

		if (fileLength<newLength) {
			long saveFileLength = fileLength;
			long saveFP = getFilePointer();

			fileLength = newLength;
			seek(saveFileLength);
			
			for (long i = saveFileLength; i<fileLength; i++)
				write(0);
			seek(saveFP);
		}
		else 
			fileLength = newLength;

		if (fileLength<filePointer+1)
			seek(fileLength);
	}	//end setLength(newLength)
	
	
	/**
	 * Return the last cluster number that is not the EOC_MARK.
	 * In case there is no cluster allocated to the file zero is 
	 * returned.
	 * @return the last cluster number that is not EOC_MARK or zero
	 * in case no cluster is allocated to the file.
	 */
	private long getLastClusterNumber() {		
		if (fileLength == 0 || clusterNumber == 0)
			return 0;

		long clusterNumberTmp = clusterNumber;
		long fatContent = device.getFatContent(clusterNumberTmp);
		while (!device.isLastCluster(fatContent)) {
			clusterNumberTmp = fatContent;
			fatContent = device.getFatContent(clusterNumberTmp);
		}
			
		return clusterNumberTmp;
	}	//end getLastClusterNumber()
	
	
	/**
	 * Return the mode of the file, that is either "r" (FATDevice.FILE_MODE_READ) or 
	 * "rw" (FATDevice.FILE_MODE_READ_WRITE).
	 * @return the mode of the file "r" or "rw".
	 */	
	public String getMode() {
		return mode;
	}	//end getMode()
	
	
	/**
	 * Set the file name to the given name.
	 * @param newName the new file name.
	 */
	protected void setName(String newName) {
		fileName = newName;
	}	//end setName(String newName)
}	//end class ExtendedRandomAccessFile
