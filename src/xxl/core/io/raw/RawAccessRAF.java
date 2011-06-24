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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import xxl.core.io.ByteArrayConversions;

/**
 * This class has all operations of RandomAccessFile, but uses
 * a raw access below. The maximum file size is the length of the 
 * raw access (in bytes) minus the size of one sector. Inside this
 * sector, some data (file length) is stored. It is always a full
 * sector needed, because block access should be possible.
 * One sector (the current) is buffered.
 */
public class RawAccessRAF extends RandomAccessFile {
	/** 
	 * Internally used raw access.
	 */
	protected RawAccess ra;

	/**
	 * The file pointer is an index to the actual position within the file.
	 * All read or write accesses occur at the file pointer position.
	 */
	protected long filePointer;

	/**
	 * Determines iff new space at the end of the file should be initialized
	 * will 0-values or not. <code>true</code> is the standard, because it
	 * is fully compliant with java.io.RandomAccessFile.
	 */
	protected boolean initBlocksAtEOF;

	/**
	 * The sector to which the file pointer points (has to be recalculated
	 * if filePointer changes)
	 */
	protected long filePointerSector;

	/**
	 * The offset position inside the filePointerSector to which the file pointer 
	 * points (has to be recalculated if filePointer changes)
	 */
	protected int filePointerOffset;

	/**
	 * The actual length of the file. The length is saved inside the
	 * first 8 bytes of the first sector. The maximum length is the 
	 * length of the RawAccess.
	 */
	protected long fileLength;

	/**
	 * Old length of the file.
	 */
	protected long oldFileLength;

	/**
	 * The number of bytes per sector.
	 */
	protected int bytesPerSec;

	/**
	 * Current sector in the buffer.
	 */
	protected long currentSectorInBuffer;

	/**
	 * Buffer for one sector.
	 */
	protected byte[] buffer;

	/**
	 * Indicates if the buffer did change (if it is dirty).
	 */
	protected boolean bufferChanged;

	/**
	 * Construct an instance of this object.
	 * @param ra raw access where this file works on.
	 * @param create if true, the file will be created inside the raw access,
	 *	else there has to be a file inside the raw access.
	 * @param dummyFile the implementation of the base class needs a file
	 *	that exists inside the file system. The file is never used,
	 *	just opened as read only and immediately closed again. This parameter
	 *	only exists because of the inflexible implementation of RandomAccessFile.
	 * @param initBlocksAtEOF Determines iff new space at the end of the file 
	 * 	should be initialized will 0-values or not. <code>true</code> is the
	 * 	standard, because it is fully compliant with java.io.RandomAccessFile.
	 * @throws FileNotFoundException
	 */
	public RawAccessRAF(RawAccess ra, boolean create, File dummyFile, boolean initBlocksAtEOF) throws FileNotFoundException {
		//if we use RandomAccessFile we have to create a file
		//therefore we use a dummy file with read access only
		//but we will never use this file, all file activities
		//will be directed to our device with the correct 
		//file given by fileName and the correct mode
		//In later versions we can use a ram-disk for the file, so
		//that no file operation on disk is used.
		super(dummyFile, "r");
		this.initBlocksAtEOF = initBlocksAtEOF;
		
		try {	
			//we never use this file -> close it		
			super.close();
		}
		catch(Exception e) {
			throw new RuntimeException("Couldn't initialize SimpleFilesystemRAF, because of: "+e);
		}
		
		this.ra = ra;
		
		bytesPerSec = ra.getSectorSize();
		this.buffer = new byte[bytesPerSec];
		
		if (!create) {
			ra.read(buffer,0);
			currentSectorInBuffer = 0;
			fileLength = ByteArrayConversions.convLongLE(buffer);
		}
		else {
			ra.write(buffer,0); // initialize the length (0) of the RAF inside the raw access
			currentSectorInBuffer = 0;
			fileLength = 0;
		}
		
		oldFileLength = fileLength;
		bufferChanged = false;
		filePointer = 0;
		
		calcFilePointer();
	}

	/**
	 * This method synchronizes filePointerSector and filePointerOffset after
	 * filePointer has been changed.
	 */
	private void calcFilePointer() {
		// old: not first sector, but only first 8 bytes.
		// filePointerSector = (filePointer+8)/bytesPerSec;
		// filePointerOffset = (int) ((filePointer+8)%bytesPerSec);
		filePointerSector = filePointer/bytesPerSec + 1;
		filePointerOffset = (int) (filePointer%bytesPerSec);
	}

	/**
	 * Increments the filePointer and also updates filePointerSector and filePointerOffset.
	 */
	private void incFilePointer() {
		filePointer++;
		if (filePointerOffset<bytesPerSec-1)
			filePointerOffset++;
		else {
			filePointerOffset = 0;
			filePointerSector++;
		}
	}

	/**
	 * Writes back the current memory resident sector back to disk if necessary.
	 */
	private void commit() {
		if (bufferChanged && currentSectorInBuffer>=0) {
			ra.write(buffer,currentSectorInBuffer);
			// currentSectorInBuffer is not set because sector is still in memory!
			bufferChanged = false;
		}
	}

	/**
	 * Reads the current sector if necessary. Makes the file
	 * longer, if the file pointer stands behind the end of
	 * the file.
	 */
	private void readSector() {
		if (currentSectorInBuffer!=filePointerSector) {
			commit();
			
			if (filePointer>fileLength) {
				// initialize the end of the file
				Arrays.fill(buffer,(byte) 0);
				// size in sectors so far
				long numberOfSectors = (fileLength+bytesPerSec-1)/bytesPerSec + 1;
				
				while (numberOfSectors<filePointerSector) {
					ra.write(buffer,numberOfSectors);
					currentSectorInBuffer = numberOfSectors;
					numberOfSectors++;
				}
				
				fileLength = filePointer;
			}
			else
				ra.read(buffer,filePointerSector);
			
			currentSectorInBuffer = filePointerSector;
		}
	}

	/**
	 * Close this extended random access file stream and release any system
	 * resources associated with the stream.
	 * @throws IOException in case of an I/O error.
	 */
	public void close() throws IOException {
		if (ra==null)
			throw new IOException("File not open");
		
		commit();
		if (oldFileLength != fileLength) {
			if (currentSectorInBuffer!=0)
				ra.read(buffer,0);
			ByteArrayConversions.convLongToByteArrayLE(fileLength,buffer);
			ra.write(buffer,0);
		}
		ra.close();
		ra = null;
	}

	/**
	 * Reads a byte of data from this file. The byte is returned as
	 * an integer in the range 0 to 255 (0x00-0x0ff).
	 * @return the next byte of data, or -1 if the end of the
	 * file has been reached.
	 * @throws IOException if the end of file is exceeded.
	 */
	public int read() throws IOException {
		if (ra==null)
			throw new IOException("File not open");
		
		if (filePointer >= fileLength)
			return -1;
		
		readSector();
		
		int value;
		if (buffer[filePointerOffset]<0)
			value = 256+buffer[filePointerOffset];
		else
			value = buffer[filePointerOffset];
		
		incFilePointer();
		
		return value;
	}

	/**
	 * Reads up to length bytes of data from this file into an array of bytes.
	 * @param array the buffer into which the data is read.
	 * @param offset the start offset of the data.
	 * @param length the maximum number of bytes read.
	 * @return the total number of bytes read into the buffer, or -1 if
	 * there is no more data because the end of the file has been reached.
	 * @throws IOException if an I/O error occurs.
	 */
	public int read(byte[] array, int offset, int length) throws IOException {
		if (ra==null)
			throw new IOException("File not open");
		
		if (filePointer >= fileLength)
			return -1;
		
		if (filePointer + length > fileLength)
			length = (int) (fileLength - filePointer);
		
		int bytesToRead = length;
		int currentLength;
		
		while (bytesToRead>0) {
			readSector();
			currentLength = bytesPerSec-filePointerOffset;
			if (currentLength>bytesToRead)
				currentLength = bytesToRead;
			
			System.arraycopy(buffer, filePointerOffset, array, offset, currentLength);
			
			bytesToRead -= currentLength;
			offset += currentLength;
			
			filePointer += currentLength;
			calcFilePointer();
		}
		return length;
	}

	/**
	 * Reads up to b.length bytes of data from this file into an array of bytes.
	 * @param buffer the buffer into which the data is read.
	 * @return the total number of bytes read into the buffer, or -1 if there is
	 * no more data because the end of this file has been reached.
	 * @throws IOException - if an I/O error occurs.
	 */
	public int read(byte[] buffer) throws IOException {
		return read(buffer, 0, buffer.length);
	}

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
		if (ra==null)
			throw new IOException("File not open");

		if (n <= 0)
			return -1;
		
		long oldFilePointer = filePointer;
		filePointer += n;
		if (filePointer>fileLength)
			filePointer=fileLength;
		calcFilePointer();
		
		return (int)(filePointer-oldFilePointer);
	}

	/**
	 * Writes the specified byte to this file. The write starts at
	 * the current file pointer.
	 * @param value the byte to be written.
	 * @throws IOException - if an I/O error occurs.
	 */
	public void write(int value) throws IOException {
		if (ra==null)
			throw new IOException("File not open");

		if ((value<-128) || (value>+255))
			throw new IOException("byte value outside range");
			
		readSector(); // read sector into buffer if necessary
		buffer[filePointerOffset] = (byte) value;
		bufferChanged = true;
		incFilePointer();
		
		// append done?
		if (filePointer>fileLength)
			fileLength = filePointer;
	}

	/**
	 * Writes b.length bytes from the specified byte array to this
	 * file, starting at the current file pointer.
	 * @param b the data.
	 * @throws IOException - if an I/O error occurs.
	 */
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	/**
	 * Writes length bytes from the specified byte array starting at
	 * offset off to this file. The user has to be sure that the length
	 * of the array will not exceed, other
	 * @param b the data.
	 * @param offset the start offset in the data.
	 * @param length the number of bytes to write.
	 * @throws IOException - if an I/O error occurs.
	 */
	public void write(byte[] b, int offset, int length) throws IOException {
		if (ra==null)
			throw new IOException("File not open");
		
		int bytesToWrite = length;
		int currentLength;
		
		while (bytesToWrite>0) {
			readSector(); // read sector into buffer if necessary
			
			currentLength = bytesPerSec-filePointerOffset;
			if (currentLength>bytesToWrite)
				currentLength = bytesToWrite;
			
			System.arraycopy(b, offset, buffer, filePointerOffset, currentLength);
			
			bufferChanged = true;
			bytesToWrite -= currentLength;
			offset += currentLength;
			
			filePointer += currentLength;
			calcFilePointer();
		}
		
		// append done?
		if (filePointer>fileLength)
			fileLength = filePointer;
	}

	/**
	 * Sets the file pointer offset, measured from the beginning
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
		if (ra==null)
			throw new IOException("File not open");

		filePointer = pos;
		calcFilePointer();
	}	

	/**
	 * Returns the current offset in this file.
	 * @return the offset from the beginning of the file, in bytes,
	 * at which the next read or write occurs.
	 * @throws IOException if an I/O error occurs.
	 */
	public long getFilePointer() throws IOException {
		if (ra==null)
			throw new IOException("File not open");

		return filePointer;
	}

	/**
	 * Returns the length of this file.
	 * @return the length of this file, measured in bytes.
	 * @throws IOException if an I/O error occurs.
	 */
	public long length() throws IOException {
		if (ra==null)
			throw new IOException("File not open");

		return fileLength;
	}

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
		if (ra==null)
			throw new IOException("File not open");
		
		if (newLength <= fileLength) {
			// truncate the file if the new length is smaller than the actual file length
			fileLength = newLength;
			// a sector behind the new end of the file could
			// be inside the buffer.
			if (currentSectorInBuffer>newLength/bytesPerSec+1)
				currentSectorInBuffer = -1;
		}
		else {
			if (initBlocksAtEOF) {
				// extend the file if the fileLength is smaller than the wished length
				// save filepointer
				long oldFilePointer = filePointer;
				
				filePointer = newLength;
				calcFilePointer();
				
				readSector();
				
				// restore filePointer
				filePointer = oldFilePointer;
				calcFilePointer();
			}
			else
				fileLength = newLength;
		}
		
		if (newLength<filePointer) {
			filePointer = newLength;
			calcFilePointer();
		}
	}
}
