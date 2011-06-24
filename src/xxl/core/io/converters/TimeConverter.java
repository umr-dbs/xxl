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
import java.sql.Time;

/**
 * A Converter for object of type {@link java.sql.Time}.
 */
public class TimeConverter extends FixedSizeConverter<Time> {

	/**
	 * This instance can be used for getting a default instance of a time
	 * converter. It is similar to the <i>Singleton Design Pattern</i> (for
	 * further details see Creational Patterns, Prototype in <i>Design
	 * Patterns: Elements of Reusable Object-Oriented Software</i> by Erich
	 * Gamma, Richard Helm, Ralph Johnson, and John Vlissides) except that
	 * there are no mechanisms to avoid the creation of other instances of a
	 * time converter.
	 */
    public static final TimeConverter DEFAULT_INSTANCE = new TimeConverter();

	/**
	 * This field contains the number of bytes needed to serialize the
	 * <code>long</code> value of a <code>Time</code> object. Because this size
	 * is predefined it must not be measured each time.
	 */
	public static final int SIZE = 8;
	
	/**
	 * Sole constructor. (For invocation by subclass constructors, typically
	 * implicit.)
	 */
	public TimeConverter() {
		super(SIZE);
	}
	
	/** 
	 * Reads the <code>long</code> value for the specified (<code>Time</code>)
	 * object from the specified data input and returns the restored object.
	 * 
	 * @param dataInput the stream to read the <code>long</code> value from in
	 *        order to return an <code>Time</code> object.
	 * @param object the (<code>Time</code>) object to be restored.
	 * @return the read <code>Time</code> object.
	 * @throws IOException if I/O errors occur.
	 */
    @Override
	public Time read(DataInput dataInput, Time object) throws IOException {
		long time = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
		if (object == null)
			object = new Time(time);
		else
			object.setTime(time);
		return object;
    }

	/**
	 * Writes the <code>long</code> value of the specified <code>Time</code>
	 * object to the specified data output.  
	 *
	 * @param dataOutput the stream to write the <code>long</code> value of the
	 *        specified <code>Time</code> object to.
	 * @param object the <code>Time</code> object that <code>long</code> value
	 *        should be written to the data output.
	 * @throws IOException includes any I/O exceptions that may occur.
	 */
	@Override
	public void write(DataOutput dataOutput, Time object) throws IOException {
	    LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, object.getTime());
	}
}
