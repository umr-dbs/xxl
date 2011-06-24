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

public class NullSensitiveConverter<T> extends Converter<T> {
	
	protected Converter<T> converter;
	
	public NullSensitiveConverter(Converter<T> converter) {
		this.converter = converter;
	}

	@Override
	public T read(DataInput dataInput, T object) throws IOException {
		return dataInput.readBoolean() ? converter.read(dataInput, object) : null;
	}

	@Override
	public void write(DataOutput dataOutput, T object) throws IOException {
		if (object != null) {
			dataOutput.writeBoolean(true);
			converter.write(dataOutput, object);
		}
		else
			dataOutput.writeBoolean(false);
	}

}
