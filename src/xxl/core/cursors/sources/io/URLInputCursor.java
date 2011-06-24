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

package xxl.core.cursors.sources.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URL;

import xxl.core.io.converters.Converter;

/**
 * This class provides a cursor that depends on a given URL, i.e., the data
 * stored in a ressource specified by a given URL. It iterates over the objects
 * that are read out of the URL's input stream. A converter is used in order to
 * read out the serialized objects. This class implements only a few
 * constructors. These constructers create an new
 * {@link xxl.core.cursors.sources.io.InputStreamCursor input stream cursor}
 * that depends on an input stream for reading from the connection to the given
 * URL.
 * 
 * <p><b>Example usage (1):</b>
 * <code><pre>
 *   // create a new URLInputIterator with ...
 *   
 *   URLInputCursor&lt;KPE&gt; cursor = new URLInputCursor&lt;KPE&gt;(
 *   
 *       // a converter for convertable objects
 *       
 *       new ConvertableConverter&lt;KPE&gt;(
 *       
 *           // a factory method that created KPEs with 2 dimensions
 *           
 *           new Function&lt;Object, KPE&gt;() {
 *               public KPE invoke() {
 *                   return new KPE(2);
 *               }
 *           }
 *       ),
 *       
 *       // a given URL
 *       
 *       new URL("http://www.xxl-library.de/rr_small.bin")
 *   );
 *   
 *   // open the cursor
 *   
 *   cursor.open();
 *   
 *   // print all elements of the cursor
 *   
 *   while(cursor.hasNext())
 *       System.out.println(cursor.next());
 *       
 *   // close the cursor
 *   
 *   cursor.close();
 * </pre></code></p>
 *
 * @param <E> the type of the elements read from the URL.
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.sources.io.InputStreamCursor
 */
public class URLInputCursor<E> extends InputStreamCursor<E> {

	/**
	 * Constructs a new URL-input cursor that depends on the specified URL and
	 * uses the specified converter in order to read out the serialized
	 * objects. An internal buffer of size <code>bufferSize</code> is used for
	 * the URL-input.
	 *
	 * @param converter the converter that is used for reading out the
	 *        serialized objects of this iteration.
	 * @param url the URL that specifies a ressource containing the serialized
	 *        objects of this iteration.
	 * @param bufferSize the size of the buffer that is used for the URL-input.
	 * @throws IOException if an I/O exception occurs.
	 */
	public URLInputCursor(Converter<? extends E> converter, URL url, int bufferSize) throws IOException {
		super(
			new DataInputStream(
				new BufferedInputStream(
					url.openStream(),
					bufferSize
				)
			),
			converter
		);
	}

	/**
	 * Constructs a new URL-input cusor that depends on the specified URL and
	 * uses the specified converter in order to read out the serialized
	 * objects. This constructor is equivalent to the call of
	 * <pre>
	 *   new URLInputIterator(converter, url, 4096)
	 * </pre>.
	 *
	 * @param converter the converter that is used for reading out the
	 *        serialized objects of this iteration.
	 * @param url the URL that contains the serialized objects of this
	 *        iteration.
	 * @throws IOException if an I/O exception occurs.
	 */
	public URLInputCursor(Converter<? extends E> converter, URL url) throws IOException {
		this(converter, url, 4096);
	}
}
