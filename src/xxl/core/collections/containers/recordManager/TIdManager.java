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

import java.util.Iterator;

import xxl.core.io.Convertable;
import xxl.core.io.converters.FixedSizeConverter;

/**
 * A class that implements this interface allows to translate/map a
 * special kind of identifier into tupel identifyers. Inside a 
 * RecordManager there exist only tuple identifyer (physical identifyer) 
 * which determine the position of a record. But outside a RecordManager,
 * there can be different types of identifyer (logical identifyer, type: 
 * Object) which are translated back and forth by a TIdManager.
 * <p>
 * Remove operations signal that a certain Id is not
 * needed any longer.
 * Implementing classes usually store its internal
 * data structure inside a container. The state of the
 * TIdManager can be stored/retrieved by the methods of the
 * Convertable interface (read and write).
 */
public interface TIdManager extends Convertable {
	/**
	 * Translates an identifyer into a tuple identifyer.
	 * If the id does not exist then the return value
	 * is unspecified.
	 * @param id identifyer
	 * @return the tuple identifyer for the given identifyer.
	 */
	public TId query(Object id);
	
	/**
	 * Returns all currently stored ids in arbitrary order. If null
	 * is returned, then there is an identity mapping between
	 * Ids and TIds.
	 * @return Iterator with the ids or null.
	 */
	public Iterator ids();
	
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
	public Object insert(TId tid);

	/**
	 * Signals that an identifyer gets a new tuple identifyer.
	 * @param id The identifyer used inside the application.
	 * @param newTId The new tuple identifyer inside the RecordManager. 
	 */
	public void update(Object id, TId newTId);
	
	/**
	 * Signals that the identifier is no longer needed.
	 * @param id the identifyer which has been given out
	 *	is no longer needed by the RecordManager.
	 */
	public void remove(Object id);

	/**
	 * Signals that all identifiers are no longer needed.
	 */
	public void removeAll();

	/**
	 * Closes the TIdManager which might be needed to make
	 * the id mapping persistent. After a call to close, the
	 * behaviour of further method calls is unspecified (except
	 * read and write which have to work properly).
	 */
	public void close();	

	/**
	 * Determines if the TIdManager uses TId-Links. This is
	 * only possible iff the Ids to be translated are TIds.
	 * @return true iff links are uses inside the RecordManager.
	 */
	public boolean useLinks();

	/**
	 * Returns a converter for the ids which are translated by the 
	 * manager into TIds.
	 * @return a converter for serializing the identifiers.
	 */
	public FixedSizeConverter objectIdConverter();
	
	/**
	 * Returns the size of the ids in bytes.
	 * @return the size in bytes of each id.
	 */
	public int getIdSize();
}
