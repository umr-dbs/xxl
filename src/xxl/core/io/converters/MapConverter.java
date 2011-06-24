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

package xxl.core.io.converters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;

/**
 * This class represents a converter for objects of type {@link java.util.Map}.
 * 
 * @param <K> the type of keys used by the maps to be converted.
 * @param <V> the type of values used by the maps to be converted.
 */
public class MapConverter<K, V> extends Converter<Map<K, V>> {

	/**
	 * A factory method which returns a new hash-map.
	 */
	public static Function<Object, HashMap<Object, Object>> HASH_MAP_FACTORY_METHOD = new AbstractFunction<Object, HashMap<Object, Object>>() {
		@Override
		public HashMap<Object, Object> invoke() {
			return new HashMap<Object, Object>();
		}
	};

	/**
	 * A factory method which returns a new tree-map.
	 */
	public static Function<Object, TreeMap<Object, Object>> TREE_MAP_FACTORY_METHOD = new AbstractFunction<Object, TreeMap<Object, Object>>() {
		@Override
		public TreeMap<Object, Object> invoke() {
			return new TreeMap<Object, Object>();
		}
	};

	/**
	 * A multi converter used internally.
	 */
	protected MultiConverter<Map.Entry<?, ?>> mapEntryConverter;

	/**
	 * A factory method which produces a new map. This function can be one of
	 * the above defined functions.
	 */
	protected Function<Object, ? extends Map<K, V>> mapCreator;

	/**
	 * Constructs a new converter for map structures.
	 *
	 * @param keyConverter the converter for the keys of the map entries.
	 * @param valueConverter the converter for the values of the map entries.
	 * @param mapCreator a factory method which produces a new map. This
	 *        function can be one of the above defined functions.
	 */
	public MapConverter(Converter<K> keyConverter, Converter<V> valueConverter, Function<Object, ? extends Map<K, V>> mapCreator) {
		this.mapCreator = mapCreator;
		this.mapEntryConverter = new MultiConverter<Map.Entry<?, ?>>(
			xxl.core.collections.MapEntry.FACTORY_METHOD,
			xxl.core.collections.MapEntry.TO_OBJECT_ARRAY_FUNCTION,
			keyConverter,
			valueConverter
		);
	}

	/**
	 * Reads the state (the attributes) for the specified object from the
	 * specified data input and returns the restored object.
	 *
	 * @param dataInput the stream to read data from in order to restore the
	 *        object.
	 * @param object the map to be restored. If the object is null it is
	 *        initialized by invoking the factory method.
	 * @return the restored object.
	 * @throws IOException if I/O errors occur.
	 */
	@Override
	@SuppressWarnings("unchecked") // due to the given functions, the used multi-converter can only generate the required map entries
	public Map<K, V> read(DataInput dataInput, Map<K, V> object) throws IOException {
		if (object == null)
			object = mapCreator.invoke();
		
		int size = dataInput.readInt();
		while (size-- > 0) {
			Map.Entry<K, V> me = (Map.Entry<K, V>)mapEntryConverter.read(dataInput);
			object.put(me.getKey(), me.getValue());
		}
		
		return object;
	}

	/**
	 * Writes the state (the attributes) of the specified object to the
	 * specified data output.
	 *
	 * @param dataOutput the stream to write the state (the attributes) of the
	 *        object to.
	 * @param object The map whose state should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Map<K, V> object) throws IOException {
		dataOutput.writeInt(object.size());
		for (Map.Entry<K, V> me : object.entrySet())
			mapEntryConverter.write(dataOutput, me);
	}
}
