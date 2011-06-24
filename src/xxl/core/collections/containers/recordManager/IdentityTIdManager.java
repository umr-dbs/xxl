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
import java.util.Iterator;

import xxl.core.io.converters.FixedSizeConverter;

/**
 * This class implements the TIdManager interface and
 * always returns the same Object (identity).
 * This is useful when there is no translation between
 * logical and physical Ids.
 */
public class IdentityTIdManager implements TIdManager {
	/**
	 * Stores the converter for the TIds.
	 */
	private FixedSizeConverter tidConverter;
	
	/**
	 * Converter for the objects inside the TIds.
	 */
	protected FixedSizeConverter idConverter;

	/**
	 * Creates a new IdentityTIdManager.
	 * @param idConverter Converter used to convert the identifyers
	 * 		inside tuple identifyers.
	 */
	public IdentityTIdManager(FixedSizeConverter idConverter) {
		this.idConverter = idConverter;
		this.tidConverter = TId.getConverter(idConverter);
	}

	/**
	 * Returns a converter for the identifyer which 
	 * occur inside TIds.
	 * @return a converter for the identifyer which 
	 * 	occur inside TIds.
	 */
	public FixedSizeConverter getIdConverterInsideTIds() {
		return idConverter;
	}

	/**
	 * Translates an identifyer into a tuple identifyer
	 * (here, the id has to be a tuple identifyer and the
	 * reference to the same object is returned ==> identity).
	 * @param id identifyer
	 * @return the same tuple identifyer
	 */
	public TId query(Object id) {
		return (TId) id;
	}

	/**
	 * null is returned, because this is an identity mapping between
	 * Ids and TIds.
	 * @return null
	 */
	public Iterator ids() {
		return null;
	}

	/**
	 * Inserts a new TId and returns an identifyer.
	 * Here, the identity is returned.
	 * @param tid tuple identifyer
	 * @return the same object
	 */
	public Object insert(TId tid) {
		return tid;
	}

	/**
	 * Signals that an identifyer gets a new tuple identifyer.
	 * Here, there is nothing to do.
	 * @param id The identifyer used inside the application.
	 * @param newTId The new tuple identifyer inside the RecordManager. 
	 */
	public void update(Object id, TId newTId) {
	}

	/**
	 * Signals that the identifier is no longer needed.
	 * Here, there is nothing to do.
	 * @param id the identifyer which has been given out
	 *	is no longer needed by the RecordManager.
	 */
	public void remove(Object id) {
	}

	/**
	 * Signals that all identifiers are no longer needed.
	 */
	public void removeAll() {
	}

	/**
	 * Closes the TIdManager. There is nothing to do.
	 */
	public void close() {
	}

	/**
	 * Determines if the TIdManager uses TId-Links. This is
	 * only possible iff the Ids to be translated are TIds.
	 * @return true iff links are uses inside the RecordManager.
	 */
	public boolean useLinks() {
		return true;
	}

	/**
	 * Returns a converter for the ids which are translated by the 
	 * manager into TIds.
	 * @return a converter for serializing the identifiers, here tuple 
	 * 		identifyers.
	 */
	public FixedSizeConverter objectIdConverter() {
		return tidConverter;
	}

	/**
	 * Returns the size of the ids in bytes.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return tidConverter.getSerializedSize();
	}

	/**
	 * There is nothing to read (no state).
	 * @param dataInput DataInput used. 
	 */
	public void read(DataInput dataInput) {
	}

	/**
	 * There is nothing to write (no state).
	 * @param dataOutput DataOutput used.
	 */
	public void write(DataOutput dataOutput) {
	}
}
