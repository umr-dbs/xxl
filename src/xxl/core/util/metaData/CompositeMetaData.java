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

package xxl.core.util.metaData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A composite metadata provides a generic metadata object that can be used to
 * merge different metadata object, so called <i>metadata fragments</i>, to a
 * single one that can be handled by the <code>getMetaData</code> method of the
 * interface <code>MetaDataProvider</code>. The different metadata fragments
 * describing an object can easily be plugged together to a composite metadata.
 * In order to retrieve the separate metadata fragments from the composite
 * metadata, they must be associated to unique identifiers, that are published
 * to composite metadata when a new metadata fragment is plugged in.
 * <code>MetaDataException</code> are used to indicate an inappropriate use of
 * identifiers, i.e., when adding a new metadata fragment associated with an
 * identifier that is already published to the composite metadata or removing a
 * metadata fragment associated with an identifier that is unknown.
 * 
 * <p><b>Note:</b> When trying to plug in a composite metadata to another
 * composite metadata using the <code>addAll</code> method, the metadata
 * fragments of the first composite metadata will be plugged in the second
 * composite metadata associated with the identifiers used in the first
 * composite metadata.</p>
 * 
 * @param <I> the type of the identifiers.
 * @param <M> the type of the metadata fragments.
 * @see xxl.core.util.metaData.MetaDataProvider#getMetaData()
 * @see xxl.core.util.metaData.MetaDataException
 */
public class CompositeMetaData<I, M> implements Iterable<Entry<I, M>>, Cloneable, Serializable {
	
	private static final long serialVersionUID = -5321284277751775898L;

	/**
	 * A concurrent hash map that is internally used to store the metadata
	 * fragments this composite metadata is built up of.
	 */
	protected ConcurrentHashMap<I, M> metaDataMap;
	
	/**
	 * The reentrant read/write lock is used to synchronize the access of the
	 * internally used concurrent hash map.
	 */
	protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	/**
	 * Creates a new composite metadata that does not contain any metadata
	 * fragments.
	 */
	public CompositeMetaData() {
		this.metaDataMap = new ConcurrentHashMap<I, M>();
	}
	
	/**
	 * Adds the given metadata fragment associated the specified identifier to
	 * this composite metadata. If the composite metadata already contains a
	 * metadata fragment for the specified identifier, an exception is thrown.
	 *
	 * @param identifier the identifier with which the specified metadata
	 *        fragment is to be associated.
	 * @param metaData the metadata fragment to be plugged to this composite
	 *        metadata associated with the specified identifier.
	 * @throws MetaDataException if the composite metadata already contains a
	 *         metadata fragment associated with the specified identifier.
	 */
	public void add(I identifier, M metaData) throws MetaDataException {
		lock.writeLock().lock();
		try {
			if (contains(identifier))
				throw new MetaDataException("composite metadata already contains specified metadata fragment \'" + identifier + "\'");
			metaDataMap.put(identifier, metaData);
		}
		finally {
			lock.writeLock().unlock();
		}
	}
	
	/**
	 * Adds the whole given composite metadata to this one by adding every
	 * metadata fragment of it. The metadata fragments of the given composite
	 * metadata are associated with their original identifiers. If the calling
	 * composite metadata already contains a metadata fragment for one of the
	 * given composite metadata's identifier, the remaining metadata fragments
	 * are added to it and afterwards an exception is thrown.
	 *
	 * @param <J> the type of the given composite metadata's identifiers.
	 * @param compositeMetaData the composite metadata whose metadata fragments
	 *        should be plugged to the calling composite metadata associated
	 *        with their original identifier.
	 * @throws MetaDataException if the composite metadata already contains a
	 *         metadata fragment associated with one of the given composite
	 *         matedata's identifiers.
	 */
	public <J extends I> void addAll(CompositeMetaData<J, ? extends M> compositeMetaData) throws MetaDataException {
		String message = "";
		for (Entry<J, ? extends M> entry : compositeMetaData) {
			try {
				add(entry.getKey(), entry.getValue());
			}
			catch (MetaDataException mde) {
				message += '\n' + mde.getMessage();
			}
		}
		if (message.length() > 0)
			throw new MetaDataException("some metadata fragments cannot be added to composite metadata:" + message);
	}
	
	/**
	 * Puts the given metadata fragment associated the specified identifier
	 * into this composite metadata and returns the metadata fragment
	 * previously associated with the specified identifier by the composite
	 * metadata or <code>null</code> if there is no such metadata fragment.
	 *
	 * @param identifier the identifier with which the specified metadata
	 *        fragment is to be associated.
	 * @param metaData the metadata fragment to be plugged to this composite
	 *        metadata associated with the specified identifier.
	 * @return the metadata fragment previously associated with the specified
	 *         identifier by this composite metadata or <code>null</code> if
	 *         there is no such metadata fragment.
	 */
	public M put(I identifier, M metaData) {
		lock.writeLock().lock();
		try {
			return metaDataMap.put(identifier, metaData);
		}
		finally {
			lock.writeLock().unlock();
		}
	}
	
	/**
	 * Puts the whole given composite metadata into this one by putting every
	 * metadata fragment of it. The metadata fragments of the given composite
	 * metadata are associated with their original identifiers. This method
	 * returns an iteration over the metadata fragments previously associated
	 * with the given composite metadata's identifiers by the calling composite
	 * metadata. If an identifier was not associated with a metadata fragment
	 * within the calling composite metadata the corresponding position of the
	 * returned iteration will be <code>null</code>.
	 *
	 * @param <J> the type of the given composite metadata's identifiers.
	 * @param compositeMetaData the composite metadata whose metadata fragments
	 *        should be plugged to the calling composite metadata associated
	 *        with their original identifier.
	 * @return an iteration over the metadata fragments previously associated
	 *         with the given composite metadata's identifiers by the calling
	 *         composite metadata. If an identifier was not associated with a
	 *         metadata fragment within the calling composite metadata the
	 *         corresponding position of the returned iteration will be
	 *         <code>null</code>.
	 */
	public <J extends I> Iterator<Entry<J, M>> putAll(CompositeMetaData<J, ? extends M> compositeMetaData) {
		HashMap<J, M> map = new HashMap<J, M>(compositeMetaData.size());
		String message = "";
		for (Entry<J, ? extends M> entry : compositeMetaData) {
			try {
				map.put(entry.getKey(), put(entry.getKey(), entry.getValue()));
			}
			catch (MetaDataException mde) {
				message += '\n' + mde.getMessage();
			}
		}
		if (message.length() > 0)
			throw new MetaDataException("some metadata fragments cannot be added to composite metadata:" + message);
		return map.entrySet().iterator();
	}
	
	/**
	 * Replaces the given metadata fragment associated the specified identifier
	 * in this composite metadata and returns the metadata fragment previously
	 * associated with the specified identifier by the composite metadata. If
	 * the composite metadata does not contain a metadata fragment for the
	 * specified identifier, an exception is thrown.
	 *
	 * @param identifier the identifier with which the specified metadata
	 *        fragment is to be associated.
	 * @param metaData the metadata fragment to be plugged to this composite
	 *        metadata associated with the specified identifier.
	 * @return the metadata fragment previously associated with the specified
	 *         identifier by this composite metadata.
	 * @throws MetaDataException if the composite metadata does not contain a
	 *         metadata fragment associated with the specified identifier.
	 */
	public M replace(I identifier, M metaData) throws MetaDataException {
		lock.writeLock().lock();
		try {
			if (!contains(identifier))
				throw new MetaDataException("composite meta data does not contain specified meta data \'" + identifier + "\'");
			return metaDataMap.put(identifier, metaData);
		}
		finally {
			lock.writeLock().unlock();
		}
	}
	
	/**
	 * Replaces the whole given composite metadata in this one by replacing
	 * every metadata fragment of it. The metadata fragments of the given
	 * composite metadata are associated with their original identifiers. This
	 * method returns an iteration over the metadata fragments previously
	 * associated with the given composite metadata's identifiers by the
	 * calling composite metadata. If the calling composite metadata does not
	 * contain a metadata fragment for every identifier of the given composite
	 * metadata, the remaining metadata fragments are replaced and afterwards
	 * an exception is thrown.
	 *
	 * @param <J> the type of the given composite metadata's identifiers.
	 * @param compositeMetaData the composite metadata whose metadata fragments
	 *        should be plugged to the calling composite metadata associated
	 *        with their original identifier.
	 * @return an iteration over the metadata fragments previously associated
	 *         with the given composite metadata's identifiers by the calling
	 *         composite metadata.
	 * @throws MetaDataException if the composite metadata does not contain a
	 *         metadata fragment for every identifier of the given composite
	 *         metadata.
	 */
	public <J extends I> Iterator<Entry<J, M>> replaceAll(CompositeMetaData<J, ? extends M> compositeMetaData) throws MetaDataException {
		HashMap<J, M> map = new HashMap<J, M>(compositeMetaData.size());
		String message = "";
		for (Entry<J, ? extends M> entry : compositeMetaData) {
			try {
				map.put(entry.getKey(), replace(entry.getKey(), entry.getValue()));
			}
			catch (MetaDataException mde) {
				message += '\n' + mde.getMessage();
			}
		}
		if (message.length() > 0)
			throw new MetaDataException("some metadata fragments cannot be added to composite metadata:" + message);
		return map.entrySet().iterator();
	}
	
	/**
	 * Returns <code>true</code> if this composite metadata contains a metadata
	 * fragment for the specified identifier.
	 *
	 * @param identifier the identifier whose presence in this composite
	 *        metadata is to be tested.
	 * @return <code>true</code> if this composite metadata contains a metadata
	 *         fragment for the specified identifier.
	 */
	public boolean contains(I identifier) {
		return metaDataMap.containsKey(identifier);
	}
	
	/**
	 * Returns the metadata fragment the specified identifier is associated
	 * with by this composite metadata. A return value of <code>null</code>
	 * indicates that <code>null</code> is explicitly associated with the given
	 * identifier by the composite metadata. If the composite metadata does not
	 * contain a metadata fragment for the specified identifier, an exception
	 * is thrown.
	 *
	 * @param identifier the identifier whose associated metadata fragment is
	 *        to be returned.
	 * @return the metadata fragment that is associated with the given
	 *         identifier by this composite metadata.
	 * @throws MetaDataException if the composite metadata does not contain a
	 *         metadata fragment associated with the specified identifier.
	 */
	public M get(I identifier) throws MetaDataException {
		lock.readLock().lock();
		try {
			if (!contains(identifier))
				throw new MetaDataException("composite meta data does not contain specified meta data \'" + identifier + "\'");
			return metaDataMap.get(identifier);
		}
		finally {
			lock.readLock().unlock();
		}
	}
	
	/**
	 * Removes the metadata fragment for this identifier from this composite
	 * metadata. If the composite metadata does not contain a metadata fragment
	 * for the specified identifier, an exception is thrown.
	 *
	 * @param identifier the identifier whose metadata fragment is to be
	 *        removed from the composite metadata.
	 * @return the previous metadata fragment associated with specified
	 *         identifier. A <code>null</code> return indicates that the
	 *         composite metadata previously associated <code>null</code> with
	 *         the specified identifier.
	 * @throws MetaDataException if the composite metadata does not contain a
	 *         metadata fragment associated with the specified identifier.
	 */
	public M remove(I identifier) throws MetaDataException {
		lock.writeLock().lock();
		try {
			if (!contains(identifier))
				throw new MetaDataException("composite meta data does not contain specified meta data \'" + identifier + "\'");
			return metaDataMap.remove(identifier);
		}
		finally {
			lock.writeLock().unlock();
		}
	}
	
	/**
	 * Removes all metadata fragment from this composite metadata.
	 */
	public void clear() {
		lock.writeLock().lock();
		try {
			metaDataMap.clear();
		}
		finally {
			lock.writeLock().unlock();
		}
	}
	
	/**
	 * Returns the number of metadata fragment this composite metadata is built
	 * up of.
	 *
	 * @return the number of metadata fragments this composite metadata is
	 *         built up of.
	 */
	public int size() {
		return metaDataMap.size();
	}
	
	/**
	 * Returns an iteration over the identifiers the metadata fragments this
	 * composite metadata is built up of are associated with. The iteration is
	 * backed by the composite metadata, so changes performed on the iteration
	 * are reflected in the composite metadata.
	 *
	 * @return an iteration over the identifiers the metadata fragments this
	 *         composite metadata is built up of are associated with.
	 */
	public Iterator<I> identifiers() {
		return metaDataMap.keySet().iterator();
	}
	
	/**
	 * Returns an iteration over the metadata fragments this composite metadata
	 * is built up of. The iteration is backed by the composite metadata, so
	 * changes performed on the iteration are reflected in the composite
	 * metadata.
	 *
	 * @return an iteration over the metadata fragments this composite metadata
	 *         is built up of.
	 */
	public Iterator<M> fragments() {
		return metaDataMap.values().iterator();
	}
	
	/**
	 * Returns an iteration over the identifier/metadata fragment pairs this
	 * composite metadata is built up of. The iteration is backed by the
	 * composite metadata, so changes performed on the iteration are reflected
	 * in the composite metadata.
	 *
	 * @return an iteration over the identifier/metadata fragment pairs this
	 *         composite metadata is built up of.
	 */
	public Iterator<Entry<I, M>> iterator() {
		return metaDataMap.entrySet().iterator();
	}
	
	/**
	 * Returns the hash code value for this composite metadata. The hash code
	 * of a composite metadata is defined to be the sum of the hash codes of
	 * each stored metadata fragment. This ensures that
	 * <code>t1.equals(t2)</code> implies that
	 * <code>t1.hashCode()==t2.hashCode()</code> for any two composite
	 * metadata objects <code>t1</code> and <code>t2</code>, as required by the
	 * general contract of {@link Object#hashCode() Object.hashCode}.
	 * 
	 * <p>This implementation simply returns the hash code of the internally
	 * used hash map.</p>
	 *
	 * @return the hash code value for this composite metadata.
	 * @see Object#hashCode()
	 * @see #equals(Object)
	 */
	@Override
	public int hashCode() {
		return metaDataMap.hashCode();
	}
	
	/**
	 * Indicates whether some other object is "equal to" this composite
	 * metadata.
	 * 
	 * <p>The <code>equals</code> method implements an equivalence relation: 
	 * <ul>
	 *     <li>
	 *         It is <i>reflexive</i>: for any reference value <code>x</code>,
	 *         <code>x.equals(x)</code> should return <code>true</code>.
	 *     </li>
	 *     <li>
	 *         It is <i>symmetric</i>: for any reference values <code>x</code>
	 *         and <code>y</code>, <code>x.equals(y)</code> should return
	 *         <code>true</code> if and only if <code>y.equals(x)</code>
	 *         returns <code>true</code>.
	 *     </li>
	 *     <li>
	 *         It is <i>transitive</i>: for any reference values
	 *         <code>x</code>, <code>y</code>, and <code>z</code>, if
	 *         <code>x.equals(y)</code> returns <code>true</code> and
	 *         <code>y.equals(z)</code> returns <code>true</code>, then
	 *         <code>x.equals(z)</code> should return <code>true</code>.
	 *     </li>
	 *     <li>
	 *         It is <i>consistent</i>: for any reference values <code>x</code>
	 *         and <code>y</code>, multiple invocations of
	 *         <code>x.equals(y)</code> consistently return <code>true</code>
	 *         or consistently return <code>false</code>, provided no
	 *         information used in <code>equals</code> comparisons on the
	 *         object is modified.
	 *     </li>
	 *     <li>
	 *         For any non-<code>null</code> reference value <code>x</code>,
	 *         <code>x.equals(null)</code> should return <code>false</code>.
	 *     </li>
	 * </ul></p>
	 * 
	 * <p>The current <code>equals</code> method returns true if and only if
	 * the given object:
	 * <ul>
	 *     <li>
	 *         is this composite metadata or
	 *     </li>
	 *     <li>
	 *         is an instance of the type <code>CompositeMetaData</code> and
	 *         the internally used hash maps of this and the specified object
	 *         are equal.
	 *     </li>
	 * </ul></p>
	 * 
	 * @param object the reference object with which to compare.
	 * @return <code>true</code> if this object is the same as the specified
	 *         object; <code>false</code> otherwise.
	 * @see #hashCode()
	 */
	@Override
	public boolean equals(Object object) {
		if (object == null)
			return false;
		if (this == object)
			return true;
		return object instanceof CompositeMetaData && metaDataMap.equals(((CompositeMetaData<?, ?>)object).metaDataMap);
	}
	
	/**
	 * Returns a shallow copy of this composite metadata instance: the
	 * identifiers and metadata fragments themselves are not cloned.
	 * 
	 * @return a shallow copy of this composite metadata.
	 * @throws CloneNotSupportedException if the instance cannot be cloned.
	 */
	@Override
	@SuppressWarnings("unchecked") // clone methods must create correct type
	public Object clone() throws CloneNotSupportedException {
		CompositeMetaData<I, M> clone = (CompositeMetaData<I, M>)super.clone();
		clone.metaDataMap.putAll(metaDataMap);
		return clone;
	}
	
	/**
	 * Returns a string representation of the composite metadata. In general,
	 * the <code>toString</code> method returns a string that "textually
	 * represents" this object. The result should be a concise but informative
	 * representation that is easy for a person to read.
	 * 
	 * <p>The <code>toString</code> method for this class simply returns the
	 * textually representation of its internally user hash map.</p>
	 * 
	 * @return  a string representation of the composite metadata.
	 */
	@Override
	public String toString() {
		return metaDataMap.toString();
	}
	
}
