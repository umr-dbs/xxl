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

package xxl.core.spatial.cursors;

import java.io.File;

import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.spatial.KPE;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 *	The KPEInputCursor constructs an Iterator of KPEs
 *	for a given flat File containing KPE-Objects.
 *
 *  @see xxl.core.spatial.KPE
 */
public class KPEInputCursor extends FileInputCursor<KPE> {

	/** Creates a new KPEInputCursor.
	 *
	 *	@param file the flat file containing the input-data
	 *	@param bufferSize the buffer-size to be allocated for reading the data
	 *	@param dim the dimensionality of the input-KPEs
	 */
	public KPEInputCursor(File file, int bufferSize, final int dim) {
		super(
			new ConvertableConverter<KPE>(
				new AbstractFunction<Object, KPE>() {
					public KPE invoke() {
						return new KPE(new DoublePointRectangle(dim));
					}
				}
			),
			file,
			bufferSize
		);
	}

	/** Creates a new KPEInputCursor.
	 *
	 *  The bufferSize is set to 4096 bytes.
	 *
	 *	@param file the file containing the input-data
	 *	@param dim the dimensionality of the input-KPEs
	 */
	public KPEInputCursor(File file, final int dim){
		this(file, 4096, dim);
	}
}
