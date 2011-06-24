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
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.AbstractContainer;
import xxl.core.collections.containers.Container;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.cursors.wrappers.AutomaticCloseCursor;
import xxl.core.cursors.wrappers.IteratorCursor;
import xxl.core.cursors.wrappers.PostponeUpdatesCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.Block;
import xxl.core.io.Convertable;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.MapConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class is an extension of Container which implements a record
 * manager on top of a given Container.<p>
 *
 * Note, that this Container is only able to deal with Objects 
 * which are Blocks. Although the insert and update methods
 * expect/return Objects, these Objects will be castet to Block.
 * The intenal identifyer are TIds (tuple identifier) or identifyer
 * supported by the TIDManager used.
 */
public class RecordManager extends AbstractContainer implements Convertable {

	/**
	 * Converter for a PageInformation entry.
	 */
	public Converter PAGEINFORMATION_CONVERTER = new Converter() {
		public Object read(DataInput dataInput, Object o) throws IOException {
			PageInformation pi;
			if (o==null)
				pi = new PageInformation();
			else
				pi = (PageInformation) o;
			
			pi.numberOfRecords = dataInput.readShort();
			pi.numberOfLinkRecords = dataInput.readShort();
			pi.numberOfBytesUsedByRecords = dataInput.readInt();
			pi.minRecordNumber = dataInput.readShort();
			pi.maxRecordNumber = dataInput.readShort();
			
			short len = dataInput.readShort();
			if (len==0) {
				pi.reservedRecordNumbers = null;
				pi.reservedRecordLengths = null;
			}
			else {
				pi.reservedRecordNumbers = new short[len];
				pi.reservedRecordLengths = new int[len];
				
				for (int i=0; i<pi.reservedRecordNumbers.length; i++) {
					pi.reservedRecordNumbers[i] = dataInput.readShort();
					pi.reservedRecordLengths[i] = dataInput.readInt();
				}
			}
			
			return pi;
		}

		/**
		 * Store the state information inside a DataOutput. This call can be and should
		 * be performed after the close call.
		 * @param dataOutput the dataOutput stream storing the serialized map
		 * @throws IOException
		 */
		public void write(DataOutput dataOutput, Object o) throws IOException {
			PageInformation pi = (PageInformation) o;
			
			dataOutput.writeShort(pi.numberOfRecords);
			dataOutput.writeShort(pi.numberOfLinkRecords);
			dataOutput.writeInt(pi.numberOfBytesUsedByRecords);
			dataOutput.writeShort(pi.minRecordNumber);
			dataOutput.writeShort(pi.maxRecordNumber);
			
			if (pi.reservedRecordNumbers==null)
				dataOutput.writeShort(0);
			else {
				dataOutput.writeShort(pi.reservedRecordNumbers.length);
				for (int i=0; i<pi.reservedRecordNumbers.length; i++) {
					dataOutput.writeShort(pi.reservedRecordNumbers[i]);
					dataOutput.writeInt(pi.reservedRecordLengths[i]);
				}
			}
		}
	};

	/**
	 * Does not make I/Os! Only utility methods for the RecordManager.
	 * Size: 14 bytes plus references of two arrays.
	 */
	public class PageInformation implements Serializable {
		/** number of Records inside the Page (without link records!) */
		public short numberOfRecords;
		/** number of link records inside the Page */
		public short numberOfLinkRecords;
		/** bytes used inside the page */
		public int numberOfBytesUsedByRecords;
		/** min recordNumber inside the Page */
		public short minRecordNumber;
		/** max recordNumber inside the Page */
		public short maxRecordNumber;
		/** reserved recordNumbers, -1 means: not reserved (and the rest is also not reserved) */
		public short reservedRecordNumbers[];
		/** the lengths of the reserved Records */
		public int reservedRecordLengths[];

		/**
		 * Constructs a new PageInformation object.
		 */
		public PageInformation() {
			// no reservations for arrays
			minRecordNumber = -1; // minmax not set
		}

		/**
		 * Checks the consistency between the Page and the PageInformation objects.
		 * @param pageId Identifyer of the page which is tested for consistency.
		 * @param p Page object to be checked against "this" object.
		 * @param fullCheck determines if a full check with the number of link records
		 *	is done. This is rather time consuming.
		 */
		public void checkConsistency(Object pageId, Page p, boolean fullCheck) {
			// determine if the Page and the PageInformation does not contradict
			if (p.getNumberOfRecords()!=numberOfRecords+numberOfLinkRecords)
				throw new RuntimeException("Number of records is not consistent");
			if (p.getNumberOfBytesUsedByRecords()!=numberOfBytesUsedByRecords)
				throw new RuntimeException("Number of bytes used by records is not consistent");
			
			if (fullCheck) {
				int linksCount = p.getNumberOfLinkRecords();
				if (linksCount!=numberOfLinkRecords)
					throw new RuntimeException("Number of link records is not consistent PageId="+pageId+" ("+linksCount+"<>"+numberOfLinkRecords+")");
			}
		}

		/**
		 * Writes the already reserved record numbers into the specified page (should
		 * be the page which belongs to this object). The caller is responsible
		 * for storing the page. All the reserved record numbers inside this object
		 * are deleted.
		 * @param p Page into which the corresponding reservations are written.
		 */
		public void writeReservedRecordNumbersIntoPage(Page p) {
			// insert all records
			if (reservedRecordNumbers!=null) {
				int i=0;
				
				while ( (i<numberOfDirectReserves) && (reservedRecordNumbers[i]!=-1) ) {
					p.insertEmptyRecord (reservedRecordNumbers[i],  reservedRecordLengths[i]);
					i++;
				}
			
				// delete information inside the PageInformation
				reservedRecordNumbers[0] = -1;
				reservedRecordNumbers[numberOfDirectReserves-1] = -1;
				
				// do not delete the reserved arrays --> reuse them
			}
		}

		/**
		 * Determines if there is space for a new reservation. 
		 * @return true iff a reservation is possible.
		 */
		public boolean isReservationPossible() {
			if (numberOfDirectReserves==0)
				return false;
			else
				return reservedRecordNumbers[numberOfDirectReserves-1]==-1;
		}

		/**
		 * Determines the exact number of reservations inside the object.
		 * 
		 * @return the exact number of reservations inside the object.
		 */
		private short getNumberOfReservations() {
			if (reservedRecordNumbers==null)
				return 0;
			else {
				short i=0;
				while( (i<numberOfDirectReserves) && (reservedRecordNumbers[i]!=-1) )
					i++;
				return i;
			}
		}

		/**
		 * Updates the memory structures for a reservation.
		 * 
		 * @param recordNr the id of the record to be reserved.
		 * @param recordLength the length of the record to be reserved.
		 */
		private void addReservation(short recordNr, int recordLength) {
			if (reservedRecordNumbers==null) {
				reservedRecordNumbers = new short[numberOfDirectReserves];
				reservedRecordLengths = new int[numberOfDirectReserves];
			}
			int pos = getNumberOfReservations();
			if (pos>=numberOfDirectReserves)
				throw new RuntimeException("No more reservations inside PageInfo possible - this Exception should never occur!");
			else {
				reservedRecordNumbers[pos] = recordNr;
				reservedRecordLengths[pos] = recordLength;
			}
		}

		/**
		 * If a reservation/insert/update is made directly on the Page object, then
		 * this reservation has to be send to the PageInformation object to update
		 * its internal structures (number of records, length of all records,
		 * min/max record numbers). Also, the strategy is informed about the
		 * update.
		 * @param tid record which is updated.
		 * @param numberOfRecordsAdded has to be -1, 0 or +1.
		 * @param numberOfBytesUsedByRecordsAdded can be positive, 0 or negative.
		 * @param numberOfLinkRecordsAdded has to be -1, 0 or +1.
		 */
		public void updateReserveInformation(TId tid, short numberOfRecordsAdded, int numberOfBytesUsedByRecordsAdded, short numberOfLinkRecordsAdded) {
			short recordNr = tid.getRecordNr();
			
			if (recordNr!=-1) {
				if (minRecordNumber==-1) {
					minRecordNumber = recordNr;
					maxRecordNumber = recordNr;
				}
				else {
					if (recordNr>maxRecordNumber)
						maxRecordNumber = recordNr;
					if (recordNr<minRecordNumber)
						minRecordNumber = recordNr;
				}
			}
			if (numberOfRecords==Short.MAX_VALUE)
				throw new RuntimeException("Only Short.MAX_VALUE records can be put into one Page");
			numberOfRecords += numberOfRecordsAdded;
			numberOfBytesUsedByRecords += numberOfBytesUsedByRecordsAdded;
			numberOfLinkRecords += numberOfLinkRecordsAdded;
			
			RecordManager.this.numberOfRecords += numberOfRecordsAdded;
			RecordManager.this.numberOfLinkRecords += numberOfLinkRecordsAdded;
			
			if (numberOfRecords<0)
				throw new RuntimeException("Illegal update information: numberOfRecords<0");
			else if (numberOfLinkRecords<0)
				throw new RuntimeException("Illegal update information: numberOfLinkRecords<0");
			else if (numberOfBytesUsedByRecords<0)
				throw new RuntimeException("Illegal update information: size of records less than 0");
			else if (numberOfBytesUsedByRecords>pageSize)
				throw new RuntimeException("Illegal update information: records larger than page size");
			
			if (Page.getSize(pageSize,numberOfRecords, numberOfBytesUsedByRecords)>pageSize)
				throw new RuntimeException("Record does not fit into the Page");
			
			strategy.recordUpdated(tid.getId(),this,recordNr,numberOfRecordsAdded,
				numberOfBytesUsedByRecordsAdded, numberOfLinkRecordsAdded);
		}

		/**
		 * Returns the number of bytes that would be free inside the current Page after
		 * a reservation of a certain number of bytes.
		 * @param numberOfBytes number of bytes for the reservation.
		 * @return the hypothetic number of bytes free after such a reservation. If <0 the
		 *	reservation is not possible.
		 */
		public int bytesFreeAfterPossibleReservation(int numberOfBytes) {
			return pageSize - Page.getSize(pageSize,numberOfRecords+numberOfLinkRecords+1, numberOfBytesUsedByRecords+numberOfBytes);
		}

		/**
		 * Returns the number of bytes that would be free inside the current Page after
		 * a reservation of a certain number of bytes.
		 * @param numOfRecords Number of records before the reservation.
		 * @param numOfLinkRecords Number of link records before the reservation.
		 * @param numOfBytesUsedByRecords Number of bytes used inside the record 
		 * 	before the reservation.
		 * @param numOfBytes number of bytes for the reservation.
		 * @return the hypothetic number of bytes free after such a reservation. If <0 the
		 *	reservation is not possible.
		 */
		public int bytesFreeAfterPossibleReservation(int numOfRecords, int numOfLinkRecords, int numOfBytesUsedByRecords, int numOfBytes) {
			return pageSize - Page.getSize(pageSize,numOfRecords+numOfLinkRecords+1, numOfBytesUsedByRecords+numOfBytes);
		}

		/**
		 * Tries to get a new recordNr without reading the page.
		 * If successful (return value>=0). It does not update the structures inside the
		 * PageInformation object which are modified using the updateReserveInformation. So, you
		 * have to call it yourself.
		 *
		 * @param record Record for which a number is wanted.
		 * @return -1 if no id can be given directly. Then you have to call 
		 *	writeReservedIdsIntoPage and use the methods of the Page itself.
		 */
		public short getNewReservedRecordNr (Block record) {
			if (Page.getSize(pageSize,numberOfRecords+1, numberOfBytesUsedByRecords+record.size)>pageSize)
				throw new RuntimeException("Record does not fit into Page");
			
			if (minRecordNumber>0) {
				minRecordNumber--;
				addReservation(minRecordNumber, record.size);
				return minRecordNumber;
			}
			else if (maxRecordNumber<Short.MAX_VALUE) {
				maxRecordNumber++;
				addReservation(maxRecordNumber, record.size);
				return maxRecordNumber;
			}
			else
				return -1;
		}
		
		/**
		 * Outputs some statistical data for the PageInformation.
		 * @return String representation of important facts of this Object.
		 */
		public String toString() {
			return "#rec: "+numberOfRecords+"\t#links: "+numberOfLinkRecords+"\t#bytes: "+numberOfBytesUsedByRecords;
		}
	}

	/**
	 * Underlying container used to store pages.
	 */
	private Container container;

	/**
	 * Blocksize of the pages.
	 */
	private int pageSize;

	/**
	 * The strategy used for insertion.
	 */
	private Strategy strategy;

	/**
	 * Manager which handles the tid-mapping
	 */
	private TIdManager tidManager;

	/**
	 * maps pages to a PageInformation object.
	 */
	private SortedMap pages;

	/**
	 * Length of the entries for reserved Ids in the Map Entry.
	 */
	private int numberOfDirectReserves;

	/**
	 * Internal page used to buffer one page.
	 */
	private Page internalPage;

	/**
	 * Internal page used to buffer one page.
	 */
	private Page internalPage2;

	/**
	 * Internal page used to buffer one page.
	 */
	private Page internalPage3;

	/**
	 * The size of the TIds (used only, if TId links
	 * are used by the TIdManager).
	 */
	private int tidSize;

	/**
	 * The converter which converts the TIds (used only, if TId links
	 * are used by the TIdManager).
	 */
	private FixedSizeConverter tidConverter;

	/**
	 * Determines iff insert was called from the update method or
	 * from the outside.
	 */
	private boolean internalInsertion;

	/**
	 * Number of records which are stored inside the RecordManager.
	 */
	protected int numberOfRecords;

	/**
	 * Number of link records which are currently needed inside the 
	 * RecordManager.
	 */
	protected int numberOfLinkRecords;

	/**
	 * Converter for the pages map. 
	 */
	protected MapConverter mapConverter;

	/**
	 * Creates a new record-manager which uses the given strategy and
	 * TIdManager.
	 * @param container the container in which the RecordManager stores its
	 * 		Blocks.
	 * @param pageSize size of each page inside the base container.
	 * @param strategy The strategy used for placement of records.
	 * @param tidManager A manager which is able to handle identifyers.
	 * @param numberOfDirectReserves number of slots for reserve operations 
	 * 		inside each page.
	 */
	public RecordManager(Container container, int pageSize, Strategy strategy, TIdManager tidManager, int numberOfDirectReserves) {
		
		this.container = container;
		this.pageSize = pageSize;
		this.strategy = strategy;
		this.tidManager = tidManager;
		this.numberOfDirectReserves = numberOfDirectReserves;

		if (tidManager.useLinks()) {
			this.tidConverter = TId.getConverter(container.objectIdConverter());
			tidSize = tidConverter.getSerializedSize();
		}

		internalPage = new Page(pageSize); // create a new page for short term internal usage
		internalPage2 = new Page(pageSize);
		internalPage3 = new Page(pageSize);
		pages = new TreeMap();
		
		// The strategy needs to be initialized.
		strategy.init(pages, pageSize, getMaxObjectSize());
		
		numberOfRecords = 0;
		numberOfLinkRecords = 0;
		internalInsertion = false;
		
		mapConverter = new MapConverter(
			container.objectIdConverter(),
			PAGEINFORMATION_CONVERTER,
			MapConverter.TREE_MAP_FACTORY_METHOD
		);
	}

	/**
	 * Removes all records from the RecordManager.
	 */
	public void clear() {
		numberOfRecords = 0;
		numberOfLinkRecords = 0;
		
		Iterator it = pages.keySet().iterator();
		container.removeAll(it);
		
		pages.clear();
		tidManager.removeAll();
		strategy.init(pages, pageSize, getMaxObjectSize());
	}

	/**
	 * Calculates the size of the biggest record that could be stored inside the
	 * RecordManager.
	 *
	 * @return size of the biggest record in bytes.
	 */
	public int getMaxObjectSize() {
		return Page.getMaxRecordSize(pageSize);
	}

	/**
	 * Closes the record manager. After a call to close, the state information is still
	 * available and can be stored using the write method.
	 */
	public void close() {
		super.close();
		strategy.close();
		tidManager.close();
	}

	/**
	 * Retrieve the state information.
	 * @param dataInput the dataInput stream holding the serialized map
	 * @throws IOException
	 */
	public void read(DataInput dataInput) throws IOException {
		pages.clear();
		
		numberOfRecords = dataInput.readInt();
		numberOfLinkRecords = dataInput.readInt();
		pages = (TreeMap) mapConverter.read(dataInput);
		
		strategy.init(pages, pageSize, getMaxObjectSize());
	}

	/**
	 * Store the state information inside a DataOutput. This call can be and should
	 * be performed after the close call.
	 * @param dataOutput the dataOutput stream storing the serialized map
	 * @throws IOException
	 */
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(numberOfRecords);
		dataOutput.writeInt(numberOfLinkRecords);
		mapConverter.write(dataOutput, pages);
	}

	/** Return value of the last getRecordFollowingTIdLink call */
	private TId currentTId;
	/** Return value of the last getRecordFollowingTIdLink call */
	private boolean linkFollowed;
	/** Return value of the last getRecordFollowingTIdLink call */
	private Page currentPage;

	/**
	 * Reads the wanted record and follows links if necessary.
	 * 
	 * @param tid the TId of the wanted record.
	 * @return the wanted record (if necessary, links are followed).
	 */
	private Block getRecordFollowingTIdLink(TId tid) {
		Block r;
		boolean isLinkRecord[] = new boolean[1];
		
		// Read the block from the container
		// Transform the block to a Page
		internalPage.read(((Block) container.get(tid.getId())).dataInputStream());
		
		// Extract the record from the page
		try {
			r = internalPage.getRecord(tid.getRecordNr(), isLinkRecord);
		}
		catch (NoSuchElementException e) {
			throw new NoSuchElementException("TId: "+tid.toString()+", isLinkRecord: "+isLinkRecord[0]);
		}
		linkFollowed = isLinkRecord[0];
		if (!linkFollowed) {
			currentTId = tid;
			currentPage = internalPage;
		}
		else {
			// Link to a record found
			try {
				currentTId = (TId) tidConverter.read(r.dataInputStream(),null);
			}
			catch (IOException e) {
				throw new WrappingRuntimeException(e);
			}
			currentPage = internalPage2;
			
			// Read the block from the container
			// Transform the block to a Page
			internalPage2.read(((Block) container.get(currentTId.getId())).dataInputStream());

			// Extract the record from the page
			r = internalPage2.getRecord(currentTId.getRecordNr(), isLinkRecord);
			if (isLinkRecord[0])
				throw new RuntimeException("Linked Record cannot be a link itself");
		}
		// Record found
		return r;
	}

	/**
	 * Returns a Record that fits to the given tid.
	 * @param id An TId-Object that indicates where to find the record.
	 * @param unfix Not yet implemented
	 * @return a Record which matches to the given tid.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get(Object id, boolean unfix) throws NoSuchElementException {
		TId tid = tidManager.query(id);

		if (tid!=null)
			return getRecordFollowingTIdLink(tid);
		else
			throw new NoSuchElementException("RecordManager: Record not found");
	}

	/**
	 * Reserves an id for subsequent use. In order to reserve some space,
	 * the size of the Object must be known and since we need the real Object
	 * to get the needed size, we insert the Object yet, instead of inserting
	 * only some dummy-bytes. The given Function must return the Object.
	 * Return a TId-Object that indicates the Page-ID and the Position of
	 * the inserted Object in this Page.
	 * @param getObject A parameterless function providing the object for that an id should be reserved.
	 * @return the reserved id (a TId-Object)
	 * @throws NoSuchElementException - if an object with an identifier id is not in the container.
	 */
	public Object reserve(Function getObject) {
		PageInformation pi;
		short recordNr;
		
		Block r = (Block) getObject.invoke();
		// return insert(r);
		
		if (r.size>getMaxObjectSize())
			throw new RuntimeException("Record too big ("+r.size+") bytes");
		
		Object pageId = strategy.getPageForRecord(r.size);
		
		if (pageId==null) {
			// Page must be allocated and written (or at least reserved)
			Page newPage = new Page(pageSize);
			recordNr = 0;
			newPage.insertRecord(r, (short) 0, false);
			
			Block b = new Block(pageSize);
			newPage.write(b.dataOutputStream());
			pageId = container.insert(b);
			
			// no fitting Page. So, create a new one.
			pi = new PageInformation();
			pages.put(pageId, pi);
			strategy.pageInserted(pageId, pi);
		}
		else {
			pi = (PageInformation) pages.get(pageId);
			recordNr=-1;
			int bf = pi.bytesFreeAfterPossibleReservation(r.size);
			if (bf<0)
				throw new RuntimeException("Strategy proposed a wrong page");
			
			// try in-memory recordnumber reservation
			if (pi.isReservationPossible())
				recordNr = pi.getNewReservedRecordNr(r);
			
			// external memory recordnumber reservation
			if (recordNr==-1) {
				// Transform the Block to a Page
				internalPage.read(((Block) container.get(pageId)).dataInputStream());
				
				// insert this kind of information if available
				pi.writeReservedRecordNumbersIntoPage(internalPage); 
				recordNr = internalPage.getFreeRecordNumber();
				internalPage.insertRecord(r, recordNr, false);
				
				Block b = new Block(pageSize);
				internalPage.write(b.dataOutputStream());
				container.update(pageId, b);
				
				// It must fit, if the Strategy works correctly
			}
		}
		
		TId tid = new TId(pageId,recordNr);
		pi.updateReserveInformation(tid, (short) 1, r.size, (short) 0);
		
		return tidManager.insert(tid);
	}

	/**
	 * This method updates the given object inside the
	 * RecordManager. If the new size of the record 
	 * requires that the record has to be moved to a 
	 * different physical location, the identifyer always 
	 * stays the same.
	 *
	 * @param id identifier of the Object.
	 * @param object the new object that should be associated to id.
	 * @param unfix signals whether the object can be removed from the underlying buffer.
	 * @throws NoSuchElementException - if an object with an identifier id does not exist in the container.
	 */
	public void update (Object id, Object object, boolean unfix) throws NoSuchElementException {

		TId tid = tidManager.query(id);

		if (tid!=null) {
			Block newRecord = (Block) object;
			Block oldRecord = getRecordFollowingTIdLink(tid);  // loads the page(s)
			
			PageInformation pi = (PageInformation) pages.get(currentTId.getId());
			PageInformation piLinkPage = null;
			
			currentPage.remove(currentTId.getRecordNr());
			
			// does the record still fit into the current page?
			if (pageSize >= Page.getSize(pageSize, pi.numberOfRecords + pi.numberOfLinkRecords, pi.numberOfBytesUsedByRecords + newRecord.size - oldRecord.size)) {
				// fits into the page
				currentPage.insertRecord(newRecord,currentTId.getRecordNr(), false);

				pi.writeReservedRecordNumbersIntoPage(currentPage); // insert this kind of information if available
				
				Block b = new Block(pageSize);
				currentPage.write(b.dataOutputStream());
				container.update(currentTId.getId(),b);
				
				// store currentPage with currentTId
				pi.updateReserveInformation(currentTId, (short) 0,newRecord.size - oldRecord.size,(short) 0);
			}
			else {
				// remove the record from the current page and store the page
				pi.writeReservedRecordNumbersIntoPage(currentPage); // insert this kind of information if available
				
				Block b = new Block(pageSize);
				currentPage.write(b.dataOutputStream());
				container.update(currentTId.getId(),b);

				pi.updateReserveInformation(currentTId, (short) -1,-oldRecord.size,(short) 0);

				boolean stored = false;
				
				if (linkFollowed) {
					// try to insert the record into the original page (internalPage, tid) if space is availlable
					piLinkPage = (PageInformation) pages.get(tid.getId());
					if (pageSize >= Page.getSize(pageSize, piLinkPage.numberOfRecords + piLinkPage.numberOfLinkRecords, piLinkPage.numberOfBytesUsedByRecords + newRecord.size - tidSize )) {
						internalPage.remove(tid.getRecordNr());
						internalPage.insertRecord(newRecord, tid.getRecordNr(), false);
						// store
						
						piLinkPage.writeReservedRecordNumbersIntoPage(internalPage); // insert this kind of information if available
						
						Block bl = new Block(pageSize);
						internalPage.write(bl.dataOutputStream());
						container.update(tid.getId(),bl);

						// store currentPage with currentTId
						piLinkPage.updateReserveInformation(tid, (short) 1,newRecord.size - tidSize,(short) -1);
						
						stored = true;
					}
				}
				
				if (!stored) {
					// insert the record into a different or new page
					// PageInformation is up to date! (important)
					internalInsertion = true;
					insert(newRecord,unfix);
					internalInsertion = false;
					// lastTId is the TId of the inserted Record.
					
					if (tidManager.useLinks()) {
						// update link to record from the original page (internalPage, tid)
						// or update the record inside the original page to a link

						Block linkRecord = new Block(tidSize);
						try {
							tidConverter.write(linkRecord.dataOutputStream(),lastTId);
						}
						catch (IOException e) {
							throw new WrappingRuntimeException(e);
						}
						
						if (linkFollowed) {
							// piLinkPage is still valid from above
							// a link has been exchanged with a link
							// no update of the pageInformation needed (same size, same number of records, same IDs)
							internalPage.update(linkRecord,tid.getRecordNr(),true);
						}
						else {
							internalPage.insertRecord(linkRecord,tid.getRecordNr(),true);
							// There has been only one page so far.
							// The page get the link entry (the record became removed already).
							piLinkPage = (PageInformation) pages.get(tid.getId());
							// insert link into internalPage
							piLinkPage.updateReserveInformation(tid, (short) 0, tidSize,(short) +1);
						}
						
						// store
						piLinkPage.writeReservedRecordNumbersIntoPage(internalPage); // insert this kind of information if available
						
						Block bl = new Block(pageSize);
						internalPage.write(bl.dataOutputStream());
						container.update(tid.getId(),bl);
					}
					else
						tidManager.update(id,lastTId);
				}
			}
		}
		else
			throw new NoSuchElementException("RecordManager: Record not found");
	}

	/** 
	 * A side effect of insert. This contains the last TId which was inserted by insert. 
	 * This is useful for the update operation.
	 */
	private TId lastTId;
	
	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * The identifier can be reused again when the object is deleted from
	 * the container. If unfixed, the object can be removed from the
	 * buffer. Otherwise, it has to be kept in the buffer until an
	 * <tt>unfix()</tt> is called.<br>
	 * After an insertion all the iterators operating on the container can
	 * be in an invalid state.<br>
	 * This method also allows an insertion of a null object. In the
	 * application would really like to have such objects in the
	 * container, some methods have to be modified.
	 *
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		// do not use internalPage because of the update method which uses insert.
		
		short recordNumber = 0;
		PageInformation pi;
		Page storagePage;
		Block newRecord = (Block) object;
		
		// insert the record into a different Page
		// ask strategy
		Object pageId = strategy.getPageForRecord(newRecord.size);
		
		if (pageId == null) {
			storagePage = new Page(pageSize);
			pi = new PageInformation();
			// recordNumber stays 0
		}
		else {
			storagePage = internalPage3;
			// load page into internalPage3
			storagePage.read(((Block) container.get(pageId)).dataInputStream());
			
			pi = (PageInformation) pages.get(pageId);
			
			// here, in memory recordNr reservation is not needed, 
			// because the page is definitively written below.
			pi.writeReservedRecordNumbersIntoPage(storagePage); // insert this kind of information if available
			recordNumber = storagePage.getFreeRecordNumber();
		}
		
		// insert
		storagePage.insertRecord(newRecord, recordNumber, false);
		// store
		Block b = new Block(pageSize);
		storagePage.write(b.dataOutputStream());
		
		if (pageId == null) {
			pageId = container.insert(b);
			pages.put(pageId, pi);
			strategy.pageInserted(pageId, pi);
		}
		else
			container.update(pageId,b);
		
		// remember last Tupel Identifier for linking inside update
		lastTId = new TId(pageId,recordNumber);
		pi.updateReserveInformation(lastTId, (short) 1,newRecord.size,(short) 0);
		
		if (internalInsertion)
			return null;
		else
			return tidManager.insert(lastTId);
	}

	/**
	 * Removes a record from a Page. If the Page is empty afterwards, the page is
	 * deleted from the container.
	 * 
	 * @param p the page to be updated.
	 * @param tid the TId of the record to be removed.
	 * @param pi some informations about the page to be updated.
	 * @param recordSize the size of the record to be removed.
	 * @param isLink a boolean flag determining whether the given TId is a link
	 *        or not.
	 */
	private void removeOrUpdatePage(Page p, TId tid, PageInformation pi, int recordSize, boolean isLink) {
		p.remove(tid.getRecordNr());
		
		if (p.getNumberOfRecords()==0) {
			// First make an update (because some strategies need that).
			pi.updateReserveInformation(tid,(short) (isLink?0:-1),-recordSize,(short) (isLink?-1:0));
			
			// Then remove the page
			container.remove(tid.getId());
			strategy.pageRemoved(tid.getId(),pi);
			if (pages.remove(tid.getId())!=pi)
				throw new RuntimeException("Page could not be removed from pages map");
		}
		else {
			Block b = new Block(pageSize);
			p.write(b.dataOutputStream());
			container.update(tid.getId(),b);
			
			pi.updateReserveInformation(tid,(short) (isLink?0:-1),-recordSize,(short) (isLink?-1:0));
		}
	}

	/**
	 * Removes the object with identifier id.
	 * An exception is thrown when an object with an identifier id is not in the container.
	 * @param id the identifyer of the object which will be removed.
	 */
	public void remove(Object id)  {
		TId tid = tidManager.query(id);

		if (tid!=null) {
			Block oldRecord = getRecordFollowingTIdLink(tid);  // loads the page(s)
			PageInformation pi = (PageInformation) pages.get(currentTId.getId());

			removeOrUpdatePage(currentPage, currentTId, pi, oldRecord.size, false);
			
			// Removes a link if it exists
			if (linkFollowed) {
				// internalPage, tid
				PageInformation piMain = (PageInformation) pages.get(tid.getId());

				removeOrUpdatePage(internalPage, tid, piMain, tidSize, true);
			}
			tidManager.remove(id);
		}
		else
			throw new NoSuchElementException("RecordManager: Record not found");
	}

	/**
	 * Returns the number of the records in this record-manager.
	 * @return the size (==number of records) of the record-manager
	 */
	public int size() {
		return numberOfRecords;
	}

	/**
	 * Returns the size of all stored records (in bytes) including link
	 * records.
	 * @return the size in bytes.
	 */
	public int sizeOfAllStoredRecords() {
		int bytecount = 0;
		Iterator it = pages.values().iterator();
		
		while (it.hasNext()) {
			PageInformation pi = (PageInformation) it.next();
			bytecount += pi.numberOfBytesUsedByRecords;
		}
		
		return bytecount;
	}

	/**
	 * Returns the number of Pages which are uses for the Records of the RecordManager.
	 * Pages with meta data are not counted.
	 * @return number of pages.
	 */
	public int numberOfPages() {
		return pages.size();
	}

	/**
	 * Returns the number link records.
	 * @return The number link records.
	 */
	public int numberOfLinkRecords() {
		return numberOfLinkRecords;
	}

	/**
	 * Returns the percentage of the pages which are full. The optimal value is
	 * nearby 1 but never reaches 1 because of the extra data that has to be
	 * stored for each record.
	 * @return the space usage percentage.
	 */
	public double getSpaceUsagePercentage() {
		return (double) sizeOfAllStoredRecords() / (pages.size()*pageSize);
	}
	
	/**
	 * Returns a converter for the ids generated by this container.
	 * @return a converter for serializing the identifiers of the container.
	 */
	public FixedSizeConverter objectIdConverter() {
		return tidManager.objectIdConverter();
	}

	/**
	 * Returns the size of the ids generated by this container in bytes.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return tidManager.getIdSize();
	}

	/**
	 * Checks whether there's exist a record with the specified id.
	 * @param id
	 * @return true iff a record with this id exists, false otherwise
	 */
	public boolean isUsed(Object id) {
		if (id==null)
			throw new RuntimeException("null is not allowed as an id");
		return tidManager.query(id)!=null;
	}

	private Iterator getIdIterator() {
		// If the TIdManager has its own id-type, then he is the only one that
		// can obtain a list of the ids.
		// If the TIdManager returns null, then we have TIds, which we only can
		// get from the pages
		Iterator it = tidManager.ids();
		if (it==null) {
			return new Sequentializer(new Mapper(
				new AbstractFunction() {
					public Object invoke(final Object pageId) {
						Block block = (Block) container.get(pageId); //get the block
						internalPage.readHeader(block.dataInputStream()); //reconstruct only the page-header
						return new Mapper(
							new AbstractFunction() {
								public Object invoke(Object internalId) {
									return new TId(pageId, ((Short) internalId).shortValue());
								}
							},
							internalPage.idsWithoutLinkRecords() //Iterator over the recordNr entries of the current page
						);
					}
				},
				pages.keySet().iterator()
			));
		}
		else
			return it;
	}

	/**
	 * Returns an iterator that delivers all the identifiers of the container that are in use.
	 * The Iterator supports remove(), but the remove Operation is performed after
	 * the end of traversing the ids (when hasNext returns false for the first time).
	 * @return an Iterator over the identifiers.
	 */
	public Iterator ids() {
		Iterator it = getIdIterator();
		// Wraps the given iterator and additionally implements remove
		return
			new AutomaticCloseCursor(
				new PostponeUpdatesCursor(
					new IteratorCursor(it),
					null, // no update in "Iterator"
					new AbstractPredicate() {
						public boolean invoke(Object o) { 
							RecordManager.this.remove(o);
							return true;
						}
					}
				)
			);
/*			return new Iterator() {
				Object currentId = null;
				LinkedList removeList = new LinkedList();
				public boolean hasNext() {
					if (it.hasNext())
						return true;
					else {
						// remove stored Ids
						while (!removeList.isEmpty()) {
							RecordManager.this.remove(removeList.getFirst());
							removeList.removeFirst();
						}
						return false;
					}
				}
				public Object next() {
					currentId = it.next();
					return currentId;
				}
				public void remove() {
					if (currentId!=null)
						// cannot delete here because of a ConcurrentModificationException.
						removeList.add(currentId);
					else
						throw new RuntimeException("Cannot delete. Call next() first.");
				}
			};
		}
		else {		
			final Iterator usedSetIt = pages.keySet().iterator();

			return new Iterator() {
				Iterator pit=null; // Iterator over the record-IDs of the current page
				Object pageId=null;
				TId currentTId=null;
				LinkedList removeList = new LinkedList();

				public boolean hasNext() {
					if (pit!=null && pit.hasNext())
						return true;
					else {
						while (usedSetIt.hasNext()) {  //check every page
							pageId = usedSetIt.next(); //select a new page

							Block block = (Block) container.get(pageId); //get the block
							internalPage.readHeader(block.dataInputStream()); //reconstruct only the page-header

							pit = internalPage.idsWithoutLinkRecords(); //Iterator over the recordNr entries of the current page
							if (pit.hasNext())
								return true; //the current page hosts some Records
						}
						// remove stored TIds
						while (!removeList.isEmpty()) {
							RecordManager.this.remove(removeList.getFirst());
							removeList.removeFirst();
						}
						return false;
					}
				}

				public Object next() {
					currentTId = new TId(pageId, ((Short) pit.next()).shortValue()); 
					return currentTId;
				}

				public void remove() {
					if (currentTId!=null)
						// cannot delete here because of a ConcurrentModificationException.
						removeList.add(currentTId);
					else
						throw new RuntimeException("Cannot delete. Call next() first.");
				}
			};
		}*/
	}

	public Cursor objects() {
		// return super.objects();
		/*Iterator itTemp = tidManager.ids();
		if (itTemp==null) {
			itTemp = new Sequentializer(new Mapper(
				pages.keySet().iterator(),
				new AbstractFunction() {
					public Object invoke(final Object pageId) {
						Block block = (Block) container.get(pageId); //get the block
						internalPage.readHeader(block.dataInputStream()); //reconstruct only the page-header
						return new Mapper(
							//internalPage.getRecord(rnr, linkRecord)
							internalPage.idsWithoutLinkRecords(), //Iterator over the recordNr entries of the current page
							new AbstractFunction() {
								public Object invoke(Object internalId) {
									return new TId(pageId, ((Short) internalId).shortValue());
								}
							}
						);
					}
				}
			));
		}*/
		final Iterator it = getIdIterator();
		
		// Wraps the given iterator and additionally implements remove
		return
			new AbstractCursor() {
				Cursor c=null;
				Object lastId=null;
				public void open() {
					super.open();
					c = new AutomaticCloseCursor(
						new PostponeUpdatesCursor(
							new IteratorCursor(it),
							new AbstractPredicate() {
								public boolean invoke(Object id, Object o) {
									// Object oa[] = (Object[])o;
									RecordManager.this.update(id, o);
									return true;
								}
							},
							new AbstractPredicate() {
								public boolean invoke(Object o) { 
									RecordManager.this.remove(o);
									return true;
								}
							}
						)
					);
				}
				public boolean hasNextObject() { 
					return c.hasNext();
				}
				public Object nextObject() {
					lastId = c.next();
					return RecordManager.this.get(lastId);
				}
				public void update(Object o) {
					c.update(o);
				}
				public boolean supportsUpdate() {
					return true;
				}
				public void remove() {
					c.remove();
				}
				public boolean supportsRemove() {
					return true;
				}
			};
	}

	/**
	 * Returns Map.Entry Objects containing the id as key and the
	 * Block as value.
	 * @return
	 */
	public Cursor entries() {
		final Iterator it = getIdIterator();
		
		// Wraps the given iterator and additionally implements remove
		return
			new AbstractCursor() {
				Cursor c=null;
				public void open() {
					super.open();
					c = new AutomaticCloseCursor(
						new PostponeUpdatesCursor(
							new IteratorCursor(it),
							new AbstractPredicate() {
								public boolean invoke(Object id, Object o) {
									// Object oa[] = (Object[])o;
									RecordManager.this.update(id, o);
									return true;
								}
							},
							new AbstractPredicate() {
								public boolean invoke(Object o) { 
									RecordManager.this.remove(o);
									return true;
								}
							}
						)
					);
				}
				public boolean hasNextObject() { 
					return c.hasNext();
				}
				public Object nextObject() {
					Object id = c.next();
					return new MapEntry(id, RecordManager.this.get(id));
				}
				public void update(Object o) {
					Map.Entry me = (Map.Entry) o;
					c.update(me.getValue());
				}
				public boolean supportsUpdate() {
					return true;
				}
				public void remove() {
					c.remove();
				}
				public boolean supportsRemove() {
					return true;
				}
			};
	}

	/**
	 * Checks the consistency of the internal structure with the Pages of the
	 * RecordManager. If there is a problem, then a RuntimeException is thrown.
	 */
	public void checkConsistency() {
		PageInformation pi;
		Object pageId;
		Map.Entry entry;
		Iterator it = pages.entrySet().iterator();
		
		while (it.hasNext()) {
			entry = (Map.Entry) it.next();
			pageId = entry.getKey();
			pi = (PageInformation) entry.getValue();
			
			Block block = (Block) container.get(pageId); // get the block
			Page p = new Page(pageSize);
			p.readHeader(block.dataInputStream()); // reconstruct only the page-header
			pi.checkConsistency(pageId, p, true);
			
			// test all links inside records? ...
		}
	}

	/**
	 * Outputs some statistical data for the RecordManager.
	 * @return String representation of important facts of this Object.
	 */
	public String toString() {
		return
			"Record Manager\n"+
			"Number of Records: "+size()+"\n"+
			"Number of bytes inside Records: "+sizeOfAllStoredRecords()+"\n"+
			"Number of link records: "+numberOfLinkRecords()+"\n"+
			"Number of Pages: "+numberOfPages()+"\n"+
			"Space usage percentage: "+getSpaceUsagePercentage()+"\n"+
			"Strategy: "+strategy;
	}

	/**
	 * Outputs statistical data for the RecordManager including information about
	 * each page.
	 * @return String representation of this Object.
	 */
	public String toStringWithPages() {
		StringBuffer sb = new StringBuffer(toString());
		sb.append("Pages:\n");
		Iterator it = pages.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			PageInformation pi = (PageInformation) entry.getValue();
			sb.append("Id: "+entry.getKey()+"\t"+pi);
			if (it.hasNext())
				sb.append("\n");
		}
		return sb.toString();
	}
	
	/**
	 * Returns the strategy which is used.
	 * @return Returns the strategy.
	 */
	public Strategy getStrategy() {
		return strategy;
	}
	
	/**
	 * Returns the tupel identifyer manager which is used.
	 * @return Returns the tidManager.
	 */
	public TIdManager getTIdManager() {
		return tidManager;
	}

}
