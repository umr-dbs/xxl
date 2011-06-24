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

import java.util.Iterator;
import java.util.Map.Entry;

import xxl.core.cursors.Cursors;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.identities.UnmodifiableCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.cursors.unions.Sequentializer;
import xxl.core.functions.AbstractFunction;
import xxl.core.predicates.AbstractPredicate;

/**
 * A composite metadata object that wraps another one in a transparent way. The
 * metadata fragments of the wrapped composite metadata are fully visible from
 * the transparent composite metadata object, but changes to the transparent
 * composite metadata's fragment will only be done locally. I. e., new metadata
 * fragments will be added/put/replaced/removed in the transparent composite
 * metadata while the the wrapped composite metadata stays unchanged.
 * 
 * <p>Imagine the following situation. The transparent composite metadata
 * <tt>T</tt> has locally stored the following IDs and fragments
 * <tt>T&nbsp;{id<sub>1</sub>&nbsp;frag<sub>1</sub>,&nbsp;id<sub>2</sub>&nbsp;frag<sub>2</sub>}</tt>
 * and wraps a composite metadata <tt>C</tt> that stores the following IDs and
 * fragments
 * <tt>C&nbsp;{id<sub>2</sub>&nbsp;frag<sub>2</sub>,&nbsp;id<sub>3</sub>&nbsp;frag<sub>3</sub>}</tt>.
 * Then <tt>T</tt> will globally look like this
 * <tt>T&nbsp;{id<sub>1</sub>&nbsp;frag<sub>1</sub>,&nbsp;id<sub>2</sub>&nbsp;frag<sub>2</sub>,&nbsp;id<sub>3</sub>&nbsp;frag<sub>3</sub>}</tt>
 * and the following calls will produce the listed results.
 * <table frame="box">
 *   <tr>
 *     <th>call</th>
 *     <th>result</th>
 *     <th>reason/effect</th>
 *   </tr>
 *   <tr>
 *     <td><tt>T.add(id<sub>1</sub>, frag)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>T</tt> already contains <tt>id<sub>1</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.add(id<sub>2</sub>, frag)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>T</tt> already contains <tt>id<sub>2</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.add(id<sub>3</sub>, frag)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>C</tt> already contains <tt>id<sub>3</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.add(id<sub>4</sub>, frag)</tt></td>
 *     <td></td>
 *     <td><tt>id<sub>4</sub>&nbsp;frag</tt> is inserted into <tt>T</tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.put(id<sub>1</sub>, frag)</tt></td>
 *     <td><tt>frag<sub>1</sub></tt></td>
 *     <td>
 *       <tt>id<sub>1</sub>&nbsp;frag<sub>1</sub></tt> is replaced by
 *       <tt>id<sub>1</sub>&nbsp;frag</tt> in <tt>T</tt>
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.put(id<sub>2</sub>, frag)</tt></td>
 *     <td><tt>frag<sub>2</sub></tt></td>
 *     <td>
 *       <tt>id<sub>2</sub>&nbsp;frag<sub>2</sub></tt> is replaced by
 *       <tt>id<sub>2</sub>&nbsp;frag</tt> in <tt>T</tt>
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.put(id<sub>3</sub>, frag)</tt></td>
 *     <td><tt>frag<sub>3</sub></tt></td>
 *     <td>
 *       <tt>id<sub>3</sub>&nbsp;frag</tt> is inserted into <tt>T</tt> (and
 *       hides <tt>id<sub>3</sub>&nbsp;frag<sub>3</sub></tt> from <tt>C</tt>)
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.put(id<sub>4</sub>, frag)</tt></td>
 *     <td><tt>null</tt></td>
 *     <td><tt>id<sub>3</sub>&nbsp;frag</tt> is inserted into <tt>T</tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.replace(id<sub>1</sub>, frag)</tt></td>
 *     <td><tt>frag<sub>1</sub></tt></td>
 *     <td>
 *       <tt>id<sub>1</sub>&nbsp;frag<sub>1</sub></tt> is replaced by
 *       <tt>id<sub>1</sub>&nbsp;frag</tt> in <tt>T</tt>
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.replace(id<sub>2</sub>, frag)</tt></td>
 *     <td><tt>frag<sub>2</sub></tt></td>
 *     <td>
 *       <tt>id<sub>2</sub>&nbsp;frag<sub>2</sub></tt> is replaced by
 *       <tt>id<sub>2</sub>&nbsp;frag</tt> in <tt>T</tt>
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.replace(id<sub>3</sub>, frag)</tt></td>
 *     <td><tt>frag<sub>3</sub></tt></td>
 *     <td>
 *       <tt>id<sub>3</sub>&nbsp;frag</tt> is inserted into <tt>T</tt> (and
 *       hides <tt>id<sub>3</sub>&nbsp;frag<sub>3</sub></tt> from <tt>C</tt>)
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.replace(id<sub>4</sub>, frag)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>T</tt> and <tt>C</tt> do not contain <tt>id<sub>4</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.contains(id<sub>1</sub>)</tt></td>
 *     <td><tt>true</tt></td>
 *     <td><tt>T</tt> contains <tt>id<sub>1</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.contains(id<sub>2</sub>)</tt></td>
 *     <td><tt>true</tt></td>
 *     <td><tt>T</tt> contains <tt>id<sub>2</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.contains(id<sub>3</sub>)</tt></td>
 *     <td><tt>true</tt></td>
 *     <td><tt>C</tt> contains <tt>id<sub>3</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.contains(id<sub>4</sub>)</tt></td>
 *     <td><tt>false</tt></td>
 *     <td><tt>T</tt> and <tt>C</tt> do not contain <tt>id<sub>4</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.get(id<sub>1</sub>)</tt></td>
 *     <td><tt>frag<sub>1</sub></tt></td>
 *     <td><tt>T</tt> contains <tt>id<sub>1</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.get(id<sub>2</sub>)</tt></td>
 *     <td><tt>frag<sub>2</sub></tt></td>
 *     <td><tt>T</tt> contains <tt>id<sub>2</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.get(id<sub>3</sub>)</tt></td>
 *     <td><tt>frag<sub>3</sub></tt></td>
 *     <td><tt>C</tt> contains <tt>id<sub>3</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.get(id<sub>4</sub>)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>T</tt> and <tt>C</tt> do not contain <tt>id<sub>4</sub></tt></td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.remove(id<sub>1</sub>)</tt></td>
 *     <td><tt>frag<sub>1</sub></tt></td>
 *     <td>
 *       <tt>id<sub>1</sub>&nbsp;frag<sub>1</sub></tt> is removed from
 *       <tt>T</tt>
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.remove(id<sub>2</sub>)</tt></td>
 *     <td><tt>frag<sub>2</sub></tt></td>
 *     <td>
 *       <tt>id<sub>2</sub>&nbsp;frag<sub>2</sub></tt> is removed from
 *       <tt>T</tt> (but still visible from <tt>C</tt>)
 *     </td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.remove(id<sub>3</sub>)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>C</tt> cannot be changed</td>
 *   </tr>
 *   <tr>
 *     <td><tt>T.remove(id<sub>4</sub>)</tt></td>
 *     <td><tt>MetaDataException</tt></td>
 *     <td><tt>T</tt> and <tt>C</tt> do not contain <tt>id<sub>4</sub></tt></td>
 *   </tr>
 * </table></p>
 * 
 * <p><b>Note</b>, the above-mentioned behaviour shows differences between
 * composite metadata and transparent composite metadata. First, when the
 * transparent composite metadata as well as the wrapped composite metadata
 * contains the same ID then the removal of it will return the removed fragment
 * but the ID stays in the transparent composite metadata (because it is also
 * part of the wrapped composite metadata). Second, when an ID is only part of
 * the wrapped composite metadata but not of the surrounding transparent
 * composite metadata then the removal of it will be impossible.</p>
 * 
 * @param <I> the type of the identifiers.
 * @param <M> the type of the metadata fragments.
 * @see xxl.core.util.metaData.MetaDataProvider#getMetaData()
 * @see xxl.core.util.metaData.MetaDataException
 */
public class TransparentCompositeMetaData<I, M> extends CompositeMetaData<I, M> {
	
	/**
	 * The wrapped composite metadata whose metadata fragments are made visible
	 * from the surrounding transparent composite metadata, but cannot be
	 * changed.
	 */
	protected CompositeMetaData<I, M> compositeMetaData;
	
	/**
	 * Creates a new transparent composite metadata that wraps the given
	 * composite metadata. The wrapped composite metadata's fragments are made
	 * visible from the surrounding transparent composite metadata, but cannot
	 * be changed.
	 * 
	 * @param compositeMetaData the composite metadata to be wrapped.
	 */
	public TransparentCompositeMetaData(CompositeMetaData<I, M> compositeMetaData) {
		this.compositeMetaData = compositeMetaData;
	}
	
	/**
	 * Creates a new transparent composite metadata that wraps an empty
	 * composite metadata.
	 */
	public TransparentCompositeMetaData() {
		this(new CompositeMetaData<I, M>());
	}
	
	/**
	 * Returns the wrapped composite metadata.
	 * 
	 * @return the wrapped composite metadata.
	 */
	public CompositeMetaData<I, M> getCompositeMetaData() {
		return compositeMetaData;
	}
	
	/**
	 * Sets the wrapped composite metadata to the given one.
	 * 
	 * @param compositeMetaData the composite metadata that should be wrapped
	 *        by this transparent composite metadata.
	 */
	public void setCompositeMetaData(CompositeMetaData<I, M> compositeMetaData) {
		lock.writeLock().lock();
		try {
			this.compositeMetaData = compositeMetaData;
		}
		finally {
			lock.writeLock().unlock();
		}
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
	@Override
	public M put(I identifier, M metaData) {
		lock.writeLock().lock();
		try {
			if (metaDataMap.containsKey(identifier) || !compositeMetaData.contains(identifier))
				return metaDataMap.put(identifier, metaData);
			metaDataMap.put(identifier, metaData);
			return compositeMetaData.get(identifier);
		}
		finally {
			lock.writeLock().unlock();
		}
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
	@Override
	public M replace(I identifier, M metaData) throws MetaDataException {
		lock.writeLock().lock();
		try {
			if (metaDataMap.containsKey(identifier))
				return metaDataMap.put(identifier, metaData);
			if (compositeMetaData.contains(identifier)) {
				metaDataMap.put(identifier, metaData);
				return compositeMetaData.get(identifier);
			}
			throw new MetaDataException("composite meta data does not contain specified meta data \'" + identifier + "\'");
		}
		finally {
			lock.writeLock().unlock();
		}
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
	@Override
	public boolean contains(I identifier) {
		lock.readLock().lock();
		try {
			return metaDataMap.containsKey(identifier) || compositeMetaData.contains(identifier);
		}
		finally {
			lock.readLock().unlock();
		}
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
	@Override
	public M get(I identifier) throws MetaDataException {
		lock.readLock().lock();
		try {
			if (metaDataMap.containsKey(identifier))
				return metaDataMap.get(identifier);
			if (compositeMetaData.contains(identifier))
				return compositeMetaData.get(identifier);
			throw new MetaDataException("composite meta data does not contain specified meta data \'" + identifier + "\'");
		}
		finally {
			lock.readLock().unlock();
		}
	}
	
	/**
	 * Removes the metadata fragment for this identifer from this composite
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
	@Override
	public M remove(I identifier) throws MetaDataException {
		lock.writeLock().lock();
		try {
			if (metaDataMap.containsKey(identifier))
				return metaDataMap.remove(identifier);
			if (compositeMetaData.contains(identifier))
				throw new MetaDataException("transparent composite meta data does not contain specified meta data \'" + identifier + "\' as a direct fragment, hence it cannot be removed");
			throw new MetaDataException("composite meta data does not contain specified meta data \'" + identifier + "\'");
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
	@Override
	public int size() {
		lock.readLock().lock();
		try {
			return Cursors.count(iterator());
		}
		finally {
			lock.readLock().unlock();
		}
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
	@Override
	public Iterator<I> identifiers() {
		return new Mapper<Entry<I, M>, I>(
			new AbstractFunction<Entry<I, M>, I>() {
				@Override
				public I invoke(Entry<I, M> entry) {
					return entry.getKey();
				}
			},
			iterator()
		);
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
	@Override
	public Iterator<M> fragments() {
		return new Mapper<Entry<I, M>, M>(
			new AbstractFunction<Entry<I, M>, M>() {
				@Override
				public M invoke(Entry<I, M> entry) {
					return entry.getValue();
				}
			},
			iterator()
		);
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
	@Override
	public Iterator<Entry<I, M>> iterator() {
		return new Sequentializer<Entry<I, M>>(
			metaDataMap.entrySet().iterator(),
			new Filter<Entry<I, M>>(
				new UnmodifiableCursor<Entry<I, M>>(
					compositeMetaData.iterator()
				),
				new AbstractPredicate<Entry<I, M>>() {
					@Override
					public boolean invoke(Entry<I, M> entry) {
						return !metaDataMap.containsKey(entry.getKey());
					}
				}
			)
		);
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
		int hashCode = 0;
		for (Entry<I, M> entry : this)
			hashCode += entry.hashCode();
		return hashCode;
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
		return object instanceof TransparentCompositeMetaData && metaDataMap.equals(((TransparentCompositeMetaData<?, ?>)object).metaDataMap) && compositeMetaData.equals(((TransparentCompositeMetaData<?, ?>)object).compositeMetaData);
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
		TransparentCompositeMetaData<I, M> clone = (TransparentCompositeMetaData<I, M>)super.clone();
		clone.metaDataMap.putAll(metaDataMap);
		clone.setCompositeMetaData(compositeMetaData);
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
		StringBuffer string = new StringBuffer().append('{');
		
		I identifier;
		M metaData;
		for (Entry<I, M> entry : metaDataMap.entrySet()) {
			if (string.length() > 1)
				string.append(", ");
			string.append((identifier = entry.getKey()) == this ? "(this Map)" : identifier).append('=').append((metaData = entry.getValue()) == this ? "(this Map)" : metaData);
		}
		
		return string.append('}').toString();
	}
	
}
