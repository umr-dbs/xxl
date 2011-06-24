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

package xxl.core.spatial.converters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.StringTokenizer;

import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.spatial.KPE;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 *	An AsciiKPEConverter can be used to convert KPE-Objects that are stored in
 *	ASCII-format to a binary representation.
 *
 *  @see xxl.core.spatial.KPE
 *  @see xxl.core.spatial.rectangles.Rectangle
 *
 */
public class AsciiKPEConverter extends Converter{

	/** dimension of the data
	 */
	protected int dim;

	/** start-ID to be assigned to the first element
	 */
	protected int no = 0;

	/** Creates a new AsciiKPEConverter.
	 *	@param dim dimension of the data
	 */
	public AsciiKPEConverter(int dim){
		this.dim = dim;
	}

	/** Creates a new AsciiKPEConverter.
	 *
	 *	@param dim dimension of the data
	 *	@param no start-ID to be assigned to the first element
	 */
	public AsciiKPEConverter(int dim, int no){
		this(dim);
		this.no = no;
	}

	/** Reads an Object from the given DataInput. This implementation parses the input and assumes
	 *  that the data is provided in lines containing string representations of 2*dim points.
	 *  The points are separated by whitespaces. The first dim points are used to create
	 *  the "lower-left" corner of a rectangle. The second dim points are used to create
	 *  the "upper-right" corner of a rectangle.
	 *
	 * @param dataInput
	 * @param object the Object for which attributes are to be
	 *		read. If object==null this method should create a
	 *		new Object;
	 *  @return the Object that was 'filled' with data from the given DataInput is
	 *		returned. This implementation returns a so called key-pointer-element (KPE)
	 *      containing a Rectangle based on DoublePoints.
	 * @throws IOException in case of I/O Error
	 */
	public Object read (DataInput dataInput, Object object) throws IOException{
		double[] ll = new double[dim];
		double[] ur = new double[dim];
		String s;
		do{
			s=dataInput.readLine();
			if(s==null)
				throw new EOFException();
		}while( s.startsWith("#") );
		
		StringTokenizer st = new StringTokenizer(s);
		for(int i=0; i<dim; i++){
			ll[i] = Double.parseDouble(st.nextToken());
		}

		for(int i=0; i<dim; i++){
			ur[i] = Double.parseDouble(st.nextToken());
		}
		DoublePoint llp = new DoublePoint(ll);
		DoublePoint urp = new DoublePoint(ur);
		return new KPE(new DoublePointRectangle(llp,urp), new Integer(no++), IntegerConverter.DEFAULT_INSTANCE);
	}

	/** Writes an Object to the given DataOutput (unsupported operation).
 	 * @param dataOutput
	 * @param object
	 * @throws IOException in case of I/O Error
	 */
	public void write (DataOutput dataOutput, Object object) throws IOException{
		throw new UnsupportedOperationException();
	}
}
