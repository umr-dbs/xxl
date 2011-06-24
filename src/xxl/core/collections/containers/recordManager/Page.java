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

package xxl.core.collections.containers.recordManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import xxl.core.io.Block;
import xxl.core.io.Convertable;
import xxl.core.io.converters.BooleanArrayConverter;
import xxl.core.util.Arrays;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class represents a page that can contain multiple records.
 * It implements the convertable interface so that Page objects can
 * be serialized using the write and read methods. The enclosed Records
 * objects are simple wrappings of byte-arrays and can be used user-dependent.
 * <p>
 * This class is closly coupled with the RecordManager. Remember that
 * reserves does not not have to be forwarded to the page. So, the page
 * Usually cannot determine if an identifyer is really free. This is
 * the task of the RecordManager.
 * <p>
 * Some comments:
 * <ul>
 * <li>In each page there can be at most Short.MAX_VALUE Records. The number
 * 	of each record is inside the interval [0,Short.MAX_VALUE].</li>
 * <li>If the size of the page is smaller or equal than Short.MAX_VALUE, then 
 *	the serialization uses Short-Values for positions inside the Page.</li>
 * </ul>
 */
public class Page implements Convertable {

	/** 
	 * Constant which determines how many elements more than 
	 * currently necessary are reserved when an array resize operation
	 * is performed (>=0, Default value: 1).
	 */
	private static int RESERVE_MORE_ELEMENTS=1;

	/** the  maximum page-size */
	private int pageSize;

	/** the content (the serialized records) */
	private byte[] content;

	/** Number of records currently in the Page */
	private short numberOfRecords;

	/** Offsets of the record data inside the page */
	private int[] recordOffset;
	
	/** Identifyer of the records */
	private short[] recordNr;

	/** Determines if there is no record, but a TId-link to another record */
	private boolean[] isLink;

	/**
	 * Creates an empty page.
	 * @param pageSize the maximum size of the page
	 */
	public Page(int pageSize) {
		this.pageSize = pageSize;
		
		content = new byte[pageSize];
		numberOfRecords = 0;
		recordOffset = new int[1];
		recordNr = null;
		isLink = null;
		recordOffset[0]=0;
	}

	/**
	 * Calculates and returns the size (# of bytes) of the used space inside the Page.
	 * To determine, if a new record will fit into the Page, call 
	 * getSize(pageSize, numberOfRecords+1, oldSize+record.size) and compare the result
	 * with the pageSize.
	 *
	 * @param pageSize the size of the page in bytes.
	 * @param numberOfRecords the number of Records which are/should be inside a Page.
	 * @param numberOfBytesUsedByRecords the number of bytes used by the records itself
	 * 	(the data part).
	 * @return number of bytes.
	 */
	public static int getSize(int pageSize, int numberOfRecords, int numberOfBytesUsedByRecords) {
		if (pageSize<=Short.MAX_VALUE)
			return 
				2+numberOfRecords*2*2+2+
				BooleanArrayConverter.getSizeForArray(false,numberOfRecords)+
				numberOfBytesUsedByRecords;
		else
			return 
				2+numberOfRecords*4+4+numberOfRecords*2+
				BooleanArrayConverter.getSizeForArray(false,numberOfRecords)+
				numberOfBytesUsedByRecords;
	}

	/**
	 * Calculates the size of the biggest record that could be stored inside the
	 * Page.
	 *
	 * @param pageSize the size of the page in bytes.
	 * @return size of the biggest record in bytes.
	 */
	public static int getMaxRecordSize(int pageSize) {
		return pageSize - getSize(pageSize,1,0);
	}

	/**
	 * Returns the number of all records: normal records and link records. 
	 * @return the number of records.
	 */
	public short getNumberOfRecords() {
		return numberOfRecords;
	}
	
	/**
	 * Returns the number of bytes which is used for the data of the records.
	 * @return the number of bytes used by the Records.
	 */
	public int getNumberOfBytesUsedByRecords() {
		return recordOffset[numberOfRecords];
	}
	
	/**
	 * Returns the number of link records (the return value is inside the range [0,getNumberOfRecords()]). 
	 * @return the number of link records within the page.
	 */
	public short getNumberOfLinkRecords() {
		short count=0;
		for (int i=0; i<numberOfRecords; i++)
			if (isLink[i])
				count++;
		return count;
	}

	/**
	 * Calculates a new id for a new Record (seldomly/never used!).
	 * O(n log n)
	 * @return returns an unused record id.
	 */
	protected short getFreeRecordNumber() {
		TreeSet ts = new TreeSet();
		short minimum = Short.MAX_VALUE;
		short maximum = -1;
		for (int k=0; k<numberOfRecords; k++) {
			if (recordNr[k]<minimum)
				minimum = recordNr[k];
			if (recordNr[k]>maximum)
				maximum = recordNr[k];
			
			ts.add(new Short(recordNr[k]));
		}

		if (minimum!=0)
			return (short) (minimum-1);
		if (maximum<Short.MAX_VALUE)
			return (short) (maximum+1);
		
		short s=0;
		while (true)  {
			if (!ts.contains(new Short(s)))
				return s;
			s++;
		}
	}

	/**
	 * Calculates the current maxId (seldomly used!).
	 * Complexity: O(n)
	 * @return the maximum record id: -1 iff the Page is empty, else a value >= 0.
	 */
	protected short getMaxRecordId() {
		short maximum = -1;
		for (int k=1; k<numberOfRecords; k++)
			if (recordNr[k]>maximum)
				maximum = recordNr[k];
		return maximum;
	}

	/**
	 * Reads just the header-informations.
	 * The remaining bytes (the serialized records)) of the stream can be
	 * reconstructed by using the readTail(dataInput) method. That strategy
	 * enables, that we can see how much free space this page has or if it
	 * contains a searched record, before we reconstruct the whole page.
	 * That mechanism is more efficient than reading always the whole page.
	 * @param dataInput DataInput from which the header informations are read.
	 */
	public void readHeader(DataInput dataInput) {
		try {
			// read the number of records
			numberOfRecords = dataInput.readShort();

			resizeArrays(numberOfRecords+RESERVE_MORE_ELEMENTS+1);

			BooleanArrayConverter booleanArrayConverter = new BooleanArrayConverter(numberOfRecords);
			booleanArrayConverter.read(dataInput,isLink);
			
			if (pageSize<=Short.MAX_VALUE) {
				for (int i=0;i<numberOfRecords;i++) {
					recordOffset[i] = dataInput.readShort();
					recordNr[i] = dataInput.readShort();
				}
				// read the offset of the first free byte
				recordOffset[numberOfRecords] = dataInput.readShort();
			}
			else {
				for (int i=0;i<numberOfRecords;i++) {
					recordOffset[i] = dataInput.readInt();
					recordNr[i] = dataInput.readShort();
				}
				// read the offset of the first free byte
				recordOffset[numberOfRecords] = dataInput.readInt();
			}
		}
		catch(IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Reads the serialized Records.
	 * This method expects, that the stream contains no header informations and that these
	 * informations are already present in this page Object i.e. by using the readHeader() Method.
	 * This arises from the following reasond: We dont have to read the whole page if we
	 * only want to know the amount of free space in this page. If the page has enough free space,
	 * we can reconstruct this page by using this readTail method. That mechanisms is more
	 * efficient than reading always the whole page.
	 * @param dataInput DataInput from which the tail information is read.
	 */
	public void readTail(DataInput dataInput) {
		try {
			dataInput.readFully(content,0,recordOffset[numberOfRecords]);
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
		catch (Exception e) {
			throw new RuntimeException("readTail: "+recordOffset[numberOfRecords]+" / "+content.length);
		}
	}

	/**
	 * Reads the whole stream. At the beginning of the stream this method
	 * expects header informations.
	 * @param dataInput the dataInput stream holding the serialized page (header and data)
	 */
	public void read(DataInput dataInput) {
		readHeader(dataInput);
		readTail(dataInput);
	}

	/**
	 * Serializes the Page into the dataOutputStream.
	 * @param dataOutput DataOutput to which the Page is written.
	 */
	public void write(DataOutput dataOutput) {
		try{
			dataOutput.writeShort(numberOfRecords);
			
			BooleanArrayConverter booleanArrayConverter = new BooleanArrayConverter(numberOfRecords);
			booleanArrayConverter.write(dataOutput,isLink);
			
			if (pageSize<=Short.MAX_VALUE) {
				for (int i=0;i<numberOfRecords;i++) {
					dataOutput.writeShort((short) recordOffset[i]);	//writes the recordOffset
					dataOutput.writeShort(recordNr[i]);	//writes the recordNr
				}
				dataOutput.writeShort((short) recordOffset[numberOfRecords]); //the offset of the free space
			}
			else {	
				for (int i=0;i<numberOfRecords;i++) {
					dataOutput.writeInt(recordOffset[i]);	//writes the recordOffset
					dataOutput.writeShort(recordNr[i]);	//writes the recordNr
				}
				dataOutput.writeInt(recordOffset[numberOfRecords]); //the offset of the free space
			}
			
			// and now the data
			dataOutput.write(content, 0, recordOffset[numberOfRecords]);
		}
		catch(IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Copies the necessary information of the arrays to new (bigger or smaller) arrays.
	 * The resulting recordNr array has size newSize or recordNr==null iff newSize==null.
	 * @param newSize Desired size of the arrays.
	 */
	private void resizeArrays(int newSize) {
		int oldSize=0;
		if (recordNr!=null)
			oldSize = recordNr.length;
		
		int ro[] = new int[newSize+1];
		if (newSize>0) {
			short rn[] = new short[newSize];
			boolean il[] = new boolean[newSize];
			
			int copyCount = (oldSize>newSize)?newSize:oldSize; // minumum!
			if (copyCount>0) {
				System.arraycopy(recordOffset, 0, ro, 0, copyCount+1);
				System.arraycopy(recordNr, 0, rn, 0, copyCount);
				System.arraycopy(isLink, 0, il, 0, copyCount);
			}
			
			recordOffset = ro;
			recordNr = rn;
			isLink = il;
		}
		else { // no arrays
			recordOffset[0] = 0;
			recordNr = null;
			isLink = null;
		}
	}

	/**
	 * Inserts a Record into this Page.
	 * @param record the record that should be inserted
	 * @param newRecordNr the wished recordNr, if not available a new one will be calculated
	 * @param isLinkRecord determines if the record is a normal record (with data) or a link 
	 * 		record (true).
	 */
	public void insertRecord(Block record, short newRecordNr, boolean isLinkRecord) {
		if (isUsed(newRecordNr))
			throw new RuntimeException("Record is already inside the Page");

		System.arraycopy(record.array, 0, content, recordOffset[numberOfRecords], record.size);

		if (recordNr==null || recordNr.length<numberOfRecords)
			resizeArrays(numberOfRecords+RESERVE_MORE_ELEMENTS+1);
		
		//update the header-informations
		recordNr[numberOfRecords] = newRecordNr;
		recordOffset[numberOfRecords+1] = recordOffset[numberOfRecords]+record.size;
		isLink[numberOfRecords] = isLinkRecord;
		numberOfRecords++;
	}

	/**
	 * Inserts an empty Record into this Page.
	 * @param newRecordNr the wished recordNr, if not available a new one will be calculated
	 * @param size the size of the record that should be inserted.
	 */
	public void insertEmptyRecord(short newRecordNr, int size) {
		if (isUsed(newRecordNr))
			throw new RuntimeException("Record is already inside the Page");

		if (recordNr==null || recordNr.length<numberOfRecords)
			resizeArrays(numberOfRecords+RESERVE_MORE_ELEMENTS+1);
		
		//update the header-informations
		recordNr[numberOfRecords] = newRecordNr;
		recordOffset[numberOfRecords+1] = recordOffset[numberOfRecords]+size;
		isLink[numberOfRecords] = false; // it can be a link, but the link is inserted when an update occurs.
		numberOfRecords++;
	}

	/**
	 * Overwrites the Record with id recordNr with the given record.
	 * Note: It's the problem of the record-manager to check whether there's
	 * enough free space in this page, if the size of the new record is biggger
	 * than the size of the old record.
	 * @param record The new record.
	 * @param recordNr The number of the record inside the page. 
	 * @param isLinkRecord determines if the record is a normal record (with data) or a link 
	 * 		record (true).
	 */
	public void update(Block record, short recordNr, boolean isLinkRecord) {
		remove(recordNr);
		insertRecord(record, recordNr, isLinkRecord);
	}

	/**
	 * Removes the Record with recordNr from this Page.
	 * @param recordNr the recordNr of the Record that should be removed
	 * @throws NoSuchElementException - if a Record with the recordNr is not in this Page.
	 */
	public void remove(short recordNr) {
		int pos = getRecordPosition(recordNr);
		int offset = recordOffset[pos];
		int size = recordOffset[pos+1]-offset;

		System.arraycopy(content, offset+size, content, offset, content.length-(offset+size));

		// decrement number of records
		numberOfRecords--;

		// update the header
		// move the content of the array
		System.arraycopy(this.recordNr, pos+1, this.recordNr, pos, numberOfRecords+1-(pos+1));
		System.arraycopy(isLink, pos+1, isLink, pos, numberOfRecords+1-(pos+1));

		for (int i=pos;i<numberOfRecords;i++)
			recordOffset[i] = recordOffset[i+1]-size;

		recordOffset[numberOfRecords] =  recordOffset[numberOfRecords+1]-size;
	}

	/**
	 * Returns the Record with the given recordNr.
	 * Throws a Exeption is there's no Record with that id.
	 * @param recordNr Number of the Record inside the Page
	 * @param isLinkRecord returns as its first element, if the record is
	 * 		a link record or not (Java has no easy and performant way
	 * 		to return two values from a method).
	 * @return the desired Record
	 */
	public Block getRecord(short recordNr, boolean isLinkRecord[]) {
		int pos = getRecordPosition(recordNr);
		int offset=recordOffset[pos];
		int size = recordOffset[pos+1]-offset;
		isLinkRecord[0] = isLink[pos];
		return new Block(Arrays.copy(content, offset, offset+size), 0, size);
	}

	/**
	 * Checks whether there's a Record with the given recordNr in this page.
	 * @param recordNr The number of the record.
	 * @return true if there's a Record with the given recordNr in this page, else false.
	 */
	public boolean isUsed(short recordNr) {
		try {
			getRecordPosition(recordNr);
			return true;
		}
		catch (NoSuchElementException e) {
			return false;
		}
	}

	/**
	 * Calculates the position of the record, with the given id, as
	 * it occurs in the serialized page.
	 * 
	 * @param recordNr the id of the record whose position should be
	 *        calculated.
	 * @return the position of the record, with the given id, as
	 *         it occurs in the serialized page.
	 */
	private int getRecordPosition(short recordNr) {
		for (short i=0;i<numberOfRecords;i++)
			if (recordNr==this.recordNr[i])
				return i;

		throw new NoSuchElementException("Record not found: "+recordNr);
	}

	/**
	 * Returns the size of the Record with the number recordNr.
	 * Throws a NoSuchElementException if there's no Record with
	 * the specified recordNr.
	 * @param recordNr the nr of the Record in this Page.
	 * @return the size of the Record with the given recordNr
	 * @throws NoSuchElementException if there's no Record with the specified recordNr.
	 */
	public int getRecordSize(short recordNr) {
		int pos=getRecordPosition(recordNr);
		return (recordOffset[pos+1]-recordOffset[pos]);
	}

	/**
	 * Returns an Iterator over the record-ids of this page.
	 * @return an Iterator over the record-ids of this page.
	 */
	public Iterator idsWithoutLinkRecords() {
		return new Iterator() {
			int number=0;
			public boolean hasNext() {
				while (number<numberOfRecords && isLink[number])
					number++;
				return number<numberOfRecords;
			}
			public Object next() {
				if (number>=numberOfRecords)
					throw new RuntimeException("Next was called too often");
				return new Short(recordNr[number++]);
			}
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/**
	 * Outputs the data of the Page.
	 * @return String representation of important facts of this Object.
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer("#rec: "+numberOfRecords+"\tpageSize: "+pageSize+"\n");
		for (int i=0; i<numberOfRecords; i++) {
			sb.append("offset: "+recordOffset[i]+"\tid: "+recordNr[i]+"\tlink? "+isLink[i]+"\n");
		}
		sb.append("end: "+recordOffset[numberOfRecords]);
		return sb.toString();
	}
}
