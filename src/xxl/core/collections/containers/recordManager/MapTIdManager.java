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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import xxl.core.functions.Constant;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MapConverter;
import xxl.core.util.LongIdGenerator;

/**
 * This class implements a TIdManager which uses a main
 * memory HashMap to store the relationship between external
 * identifyers (of type Long) and tuple identifyers inside 
 * the record manager.
 * The identifyers are Long values which are produced by
 * a LongIdGenerator.
 */
public class MapTIdManager implements TIdManager {
	
	/**
	 * A main memory map to store the relationship between
	 * external identifiers (of type Long) and tuple
	 * identifiers inside the record manager. 
	 */
	protected Map map;
	
	/**
	 * An id generator which produces identifiers that are
	 * Long values.
	 */
	protected LongIdGenerator idGenerator;
	
	/**
	 * Converter for the map object.
	 */
	protected Converter mapConverter;

	/**
	 * Converter for the objects inside the TIds.
	 */
	protected FixedSizeConverter idConverter;

	/**
	 * Constructs a new MapTIdManager.
	 * @param idConverter Converter for the specific Ids which are used
	 * 	inside the TIds.
	 */
	public MapTIdManager (FixedSizeConverter idConverter) {
		this.idConverter = idConverter;
		
		map = new HashMap();
		idGenerator = new LongIdGenerator();
		
		mapConverter = new MapConverter(
			LongConverter.DEFAULT_INSTANCE,
			TId.getConverter(idConverter),
			MapConverter.HASH_MAP_FACTORY_METHOD
		);
	}

	/**
	 * Returns a converter for the identifyer which 
	 * occur inside TIds.
	 * @return A converter for the identifyer which 
	 * 	occur inside TIds.
	 */
	public FixedSizeConverter getIdConverterInsideTIds() {
		return idConverter;
	}

	/**
	 * Translates an identifyer into a tuple identifyer.
	 * If the id does not exist then the return value
	 * is unspecified.
	 * @param id identifyer
	 * @return the tuple identifyer for the given identifyer.
	 */
	public TId query(Object id) {
		return (TId) map.get(id);
	}

	/**
	 * Returns all currently stored ids in arbitrary order. If null
	 * is returned, then there is an identity mapping between
	 * Ids and TIds.
	 * @return Iterator with the ids or null.
	 */
	public Iterator ids() {
		return map.keySet().iterator();
	}

	/**
	 * Inserts a new TId and returns an identifyer.
	 * The RecordManager registers a tuple identifyer
	 * and gets back an identifyer for the object (which
	 * can also be the same tid). The TIdManager must
	 * be able to translate the returned Object back into
	 * the same TId.
	 * @param tid tuple identifyer
	 * @return the identifyer under which the Record
	 *	is reachable from outside the RecordManager.
	 */
	public Object insert(TId tid) {
		Object id = new Long(idGenerator.getIdentifyer(new Constant(map.keySet())));
		map.put(id,tid);
		return id;
	}

	/**
	 * Signals that an identifyer gets a new tuple identifyer.
	 * Here, the new TId is inserted into the map.
	 * @param id The identifyer used inside the application.
	 * @param newTId The new tuple identifyer inside the RecordManager. 
	 */
	public void update(Object id, TId newTId) {
		Object oldObj = map.put(id, newTId);
		if (oldObj==null)
			throw new RuntimeException("The given id is not valid");
	}

	/**
	 * Signals that the identifier is no longer needed. The 
	 * identifyer is taken out of the map.
	 * @param id the identifyer which has been given out
	 *	is no longer needed by the RecordManager.
	 */
	public void remove(Object id) {
		if (map.remove(id)==null)
			throw new RuntimeException("The given id is not valid");
		idGenerator.removeIdentifyer(((Long) id).longValue());
	}

	/**
	 * Signals that all identifiers are no longer needed.
	 */
	public void removeAll() {
		map.clear();
		idGenerator.reset();
	}

	/**
	 * Closes this TIDManager. The map must still be accessible 
	 * for the write method. So, nothing can be done here.
	 */
	public void close() {
		removeAll();
	}

	/**
	 * Determines if the TIdManager uses TId-Links. This is
	 * only possible iff the Ids to be translated are TIds.
	 * @return true iff links are uses inside the RecordManager.
	 */
	public boolean useLinks() {
		return false;
	}

	/**
	 * Returns a converter for the ids which are translated by the 
	 * manager into TIds.
	 * @return a converter for serializing the identifiers.
	 */
	public FixedSizeConverter objectIdConverter() {
		return LongConverter.DEFAULT_INSTANCE;
	}

	/**
	 * Returns the size of the ids in bytes.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize() {
		return LongConverter.DEFAULT_INSTANCE.getSerializedSize();
	}

	/**
	 * Read the map from a DataInput.
	 * @param dataInput DataInput used. 
	 * @throws IOException
	 */
	public void read(DataInput dataInput) throws IOException {
		// map = (Map) SerializableConverter.DEFAULT_INSTANCE.read(dataInput);
		map = (Map) mapConverter.read(dataInput);
		idGenerator.read(dataInput);
	}

	/**
	 * Writes the map to a DataOutput.
	 * @param dataOutput DataOutput used. 
	 * @throws IOException
	 */
	public void write(DataOutput dataOutput) throws IOException {
		// SerializableConverter.DEFAULT_INSTANCE.write(dataOutput, map);
		mapConverter.write(dataOutput, map);
		idGenerator.write(dataOutput);
	}
}
