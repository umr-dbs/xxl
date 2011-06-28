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
package xxl.core.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.Convertable;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.math.Maths;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.TupleConverter;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 *	A KPE (key-pointer-element) stores a data Object and an ID. This is the key data-type for spatial data. 
 *  The data stored by objects of this class typically contains some sort of approximation like e.g. a
 *  Minimum Bounding Rectangle (MBR) or a point.
 *
 *	The ID can be an Integer or a reference pointing to the exact representation of the data Object.
 *	It is based on an {@link xxl.core.relational.tuples.ArrayTuple array-tuple}.
 *
 *  The user has to provide a converter for the data object. If no Converter is provided the data object
 *  is assumed to be of type Convertable.
 *  
 *  @see xxl.core.relational.tuples.ArrayTuple
 *  @see xxl.core.spatial.KPEzCode
 *
 */
public class KPE extends ArrayTuple implements Convertable {

	protected TupleConverter converter;
	
	/** Creates a new KPE.
	 *
	 *	@param objects the objects to be stored by this KPE
	 *	@param dataConverter the converters to use for the objects
	*/
	public KPE(Object[] objects, Converter<Object>[] dataConverter){
		super(objects);		
		converter = new TupleConverter(false, dataConverter);
	}

	/** Creates a new KPE.
	 *
	 *	@param data The data to be stored by this KPE (assumed to implement the Convertable interface).
	 *	@param ID The ID-Object wrapped by this KPE.
	 *	@param IDConverter The Converter for serializing the ID-Object wrapped by this KPE.
	*/
	public KPE(Object data, Object ID, Converter IDConverter){
		this(new Object[]{ data, ID }, new Converter[]{ ConvertableConverter.DEFAULT_INSTANCE, IDConverter} );
	}

	/** Creates a new KPE. The ID is set to {@link xxl.core.math.Maths#zero}. The IDConverter is set to <tt>IntegerConverter.DEFAULT_INSTANCE</tt>.
	 *
	 *	@param data The data to be stored by this KPE.
	 */
	public KPE(Object data){
		this(data, Maths.ZERO, IntegerConverter.DEFAULT_INSTANCE);	
	}

	/** Returns a physical copy of the given KPE.
	 *
	 * @param k the KPE to use as a template for the new instance
	 */
	public KPE(KPE k){
		super(k.tuple);
		this.converter = k.converter;
	}

	/** Creates a new KPE of dimension dim.
	 *
	 * @param dim the dimension of the rectangle to create
	 */
	public KPE(int dim){
		this(new DoublePointRectangle(dim));
	}

	/**
	 * Sets the specified column to the given object.
	 * 
	 * @param index the index of the column to be set.
	 * @param object the object the specified column should be set to.
	 * @return the object the specified column is set to.
	 */
	public Object setObject(int index, Object object) {
		return tuple[index-1] = object;
	}
	
	/**	Get method for backward-compatibility:
	 *
	 * @return the data stored by this KPE is returned
	 */
	public Object getData(){
		return getObject(1);	
	}

	/**	Set method for backward-compatibility:
	 *
	 * @param data sets the data stored by this KPE
	 * @return returns the object given
	 * 
	 */
	public Object setData(Object data){
		return setObject(1, data);	
	}
	
	/**	Get method for backward-compatibility:
	 *
	 * @return the ID stored by this KPE is returned
	 */
	public Object getID(){
		return getObject(2);	
	}	

	/**	Set method for backward-compatibility:
	 *
	 * @param ID sets the ID stored by this KPE
	 */
	public void setID(Object ID){
		setObject(2, ID);
	}
	/** Returns a physical copy of this KPE.
	 * 
	 * @return returns a physical copy of this KPE 
	 */
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	/** Returns a string representation of the object.
	 * @return a string representation of the object.
	 */
	public String toString(){
		return getData() + "ID: \t" + getID() + "\n";
	}
	
	/**
	 * Reads the state (the attributes) for an object of this class from
	 * the specified data input and restores the calling object. The state
	 * of the object before calling <tt>read</tt> will be lost.<br>
	 * The <tt>read</tt> method must read the values in the same sequence
	 * and with the same types as were written by <tt>write</tt>.
	 *
	 * @param dataInput the stream to read data from in order to restore
	 *        the object.
	 * @throws IOException if I/O errors occur.
	 */
	public void read(DataInput dataInput) throws IOException {
		Object[] newTuple = converter.read(dataInput, this).toArray();
		// XXX: CHANGE (20.11.2005 by Daniel Schaefer)
		System.arraycopy(newTuple,0, this.tuple, 0, tuple.length);
		// END OF CHANGE
	}

	/**
	 * Writes the state (the attributes) of the calling object to the
	 * specified data output. This method should serialize the state of
	 * this object without calling another <tt>write</tt> method in order
	 * to prevent recursions.
	 *
	 * @param dataOutput the stream to write the state (the attributes) of
	 *        the object to.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	public void write(DataOutput dataOutput) throws IOException {
		converter.write(dataOutput, this);
	}
}
