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

package xxl.core.collections;

import java.util.Map.Entry;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class provides a straightforward implementation of the
 * <code>Map.Entry</code> interface from <code>java.util</code>. A MapEntry is
 * a key-value pair. The <code>Map.entrySet</code> method returns a
 * collection-view of the map, whose elements are of this class. One way to
 * obtain a reference to a map entry is from the iterator of this
 * collection-view. These <code>Map.Entry</code> objects are valid only for the
 * duration of the iteration; more formally, the behavior of a map entry is
 * undefined if the backing map has been modified after the entry was returned
 * by the iterator, except through the iterator's own remove operation, or
 * through the <code>setValue</code> operation on a map entry returned by the
 * iterator.
 * 
 * <p>Usage example (1).
 * <code><pre>
 *     // create two MapEntry
 *
 *     MapEntry&lt;Integer, String&gt; entry1 = new MapEntry&lt;Integer, String&gt;(5, "five"),
 *                               entry2 = new MapEntry&lt;Integer, String&gt;(2, "two");
 *
 *     // check if both entries are equal
 *
 *     if (entry1.equals(entry2))
 *         System.out.println("the entries are equal");
 *     else
 *         System.out.println("the entries are unqual");
 *
 *     // change the second entry
 *
 *     entry2.setKey(5);
 *
 *     // check if both entries are equal
 *
 *     if (entry1.equals(entry2))
 *         System.out.println("the entries are equal");
 *     else
 *         System.out.println("the entries are unqual");
 *
 *     // change the second entry
 *
 *     entry2.setKey(2);
 *     entry2.setValue("five");
 *
 *     // check if both entries are equal
 *
 *     if (entry1.equals(entry2))
 *         System.out.println("the entries are equal");
 *     else
 *         System.out.println("the entries are unqual");
 *
 *     // change the second entry
 *
 *     entry2.setKey(5);
 *
 *     // check if both entries are equal
 *
 *     if (entry1.equals(entry2))
 *         System.out.println("the entries are equal");
 *     else
 *         System.out.println("the entries are unqual");
 * </pre></code></p>
 *
 * @param <K> the type of the map entry's key.
 * @param <V> the type of the map entry's value.
 * @see java.util.Map.Entry
 */
public class MapEntry<K, V> implements Entry<K, V> {
	
	/** 
	 * Factory method. To get an Object of the class MapEntry, use the invoke
	 * method with two arguments: key and value.
	 */
	public static Function<Object, MapEntry<Object, Object>> FACTORY_METHOD = new AbstractFunction<Object, MapEntry<Object, Object>>() {
		@Override
		public MapEntry<Object, Object> invoke(Object key, Object value) {
			return new MapEntry<Object, Object>(key, value);
		}
	};

	/**
	 * Converts the MapEntry to an Object array.
	 */
	public static Function<Entry<?, ?>, Object[]> TO_OBJECT_ARRAY_FUNCTION = new AbstractFunction<Entry<?, ?>, Object[]>() {
		@Override
		public Object[] invoke(Entry<?, ?> mapEntry) {
			return new Object[] { 
				mapEntry.getKey(),
				mapEntry.getValue()
			};
		}
	};

	/**
	 * The key object of this MapEntry.
	 */
	protected K key;

	/**
	 * The value object of this MapEntry.
	 */
	protected V value;
	
	/**
	 * Returns a new function that returns the key of the entry submitted on
	 * invocation.
	 * 
	 * @param <K> the type of the entry's key.
	 * @param <V> the type of the entry's value.
	 * @return a new function that returns the key of the entry submitted on
	 *         invocation.
	 */
	public static <K, V> Function<Entry<K, V>, K> getKeyFunction() {
		return new AbstractFunction<Entry<K, V>, K>() {
			@Override
			public K invoke(Entry<K, V> entry) {
				return entry.getKey();
			}
		};
	}

	/**
	 * Returns a new function that returns the value of the entry submitted on
	 * invocation.
	 * 
	 * @param <K> the type of the entry's key.
	 * @param <V> the type of the entry's value.
	 * @return a new function that returns the value of the entry submitted on
	 *         invocation.
	 */
	public static <K, V> Function<Entry<K, V>, V> getValueFunction() {
		return new AbstractFunction<Entry<K, V>, V>() {
			@Override
			public V invoke(Entry<K, V> entry) {
				return entry.getValue();
			}
		};
	}

	/**
	 * Constructs a new MapEntry with the specified key and value object.
	 *
	 * @param key the key of the new MapEntry.
	 * @param value the value of the new MapEntry.
	 */
	public MapEntry(K key, V value) {
		this.key = key;
		this.value = value;
	}

	/**
	 * Compares the specified object with this entry for equality. Returns
	 * <code>true</code> if the given object is also a map entry and the two
	 * entries represent the same mapping. More formally, two entries
	 * <code>e1</code> and <code>e2</code> represent the same mapping if
	 * <code><pre>
	 * (e1.getKey()==null   ? e2.getKey()==null   : e1.getKey().equals(e2.getKey()))  &&
	 * (e1.getValue()==null ? e2.getValue()==null : e1.getValue().equals(e2.getValue()))
	 * </pre></code>
	 *
	 * @param o object to be compared for equality with this map entry.
	 * @return <code>true</code> if the specified object is equal to this map
	 *         entry.
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Entry))
			return false;
		Entry<?, ?> mapEntry = (Entry<?, ?>)o;
		return (
			getKey() == null ?
				mapEntry.getKey() == null :
				getKey().equals(mapEntry.getKey())
		) && (
			getValue() == null ?
				mapEntry.getValue() == null :
				getValue().equals(mapEntry.getValue())
		);
	}

	/**
	 * Returns the key corresponding to this entry.
	 *
	 * @return the key corresponding to this entry.
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Returns the value corresponding to this entry.
	 *
	 * @return the value corresponding to this entry.
	 */
	public V getValue() {
		return value;
	}

	/**
	 * Returns the hash code value for this map entry. The hash code of a
	 * map entry <tt>e</tt> is defined to be:
	 * <code><pre>
	 * (e.getKey()==null   ? 0 : e.getKey().hashCode()) ^
	 * (e.getValue()==null ? 0 : e.getValue().hashCode())
	 * </pre></code>
	 * This ensures that <code>e1.equals(e2)</code> implies that
	 * <code>e1.hashCode()==e2.hashCode()</code> for any two Entries
	 * <code>e1</code> and <code>e2</code>, as required by the general contract
	 * of Object.hashCode.
	 *
	 * @return the hash code value for this map entry.
	 * @see Object#hashCode()
	 * @see Object#equals(Object)
	 * @see #equals(Object)
	 */
	@Override
	public int hashCode() {
		return (getKey() == null ? 0: getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
	}

	/**
	 * Returns the value corresponding to this entry. If the mapping has
	 * been removed from the backing map (by the iterator's remove
	 * operation), the results of this call are undefined.
	 *
	 * @param key the new key of the map entry.
	 * @return the value corresponding to this entry.
	 */
	public K setKey(K key) {
		K oldKey = this.key;
		this.key = key;
		return oldKey;
	}

	/**
	 * Replaces the value corresponding to this entry with the specified
	 * value. The behavior of this call is undefined if the mapping has
	 * already been removed from the map (by the iterator's remove
	 * operation).
	 *
	 * @param value new value to be stored in this entry.
	 * @return old value corresponding to the entry.
	 * @throws UnsupportedOperationException if the <tt>put</tt> operation
	 *         is not supported by the backing map.
	 * @throws ClassCastException if the class of the specified value
	 *         prevents it from being stored in the backing map.
	 * @throws IllegalArgumentException if some aspect of this value
	 *         prevents it from being stored in the backing map.
	 * @throws NullPointerException the backing map does not permit null
	 *         values, and the specified value is null.
	 */
	public V setValue(V value) throws ClassCastException, IllegalArgumentException, NullPointerException, UnsupportedOperationException {
		V oldValue = this.value;
		this.value = value;
		return oldValue;
	}

	/**
	 * Converts the MapEntry to a String.
	 * @return a String representation of the key value pair. 
	 */
	@Override
	public String toString() {
		return "key: " + key + ", value: " + value;
	}
}
