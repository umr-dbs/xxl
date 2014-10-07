/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
 * of Mathematics and Computer Science University of Marburg Germany
 * 
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * If not, see <http://www.gnu.org/licenses/>.
 * 
 * http://code.google.com/p/xxl/
 */

package xxl.core.io.converters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.StringConverter;

/**
 * Special converter to handle strings and their length.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class FixedSizeStringConverter extends FixedSizeConverter<String> {

  public static int calculateSize(int columnCharacterCount) {
    // TODO: For now, just use UTF8 (2 Byte) for each character.
    // This waste storage so do it better and check if a given character
    // needs one or two bytes.
    return columnCharacterCount * 2;
  }

  /**
   * @see FixedSizeConverter
   */
  public FixedSizeStringConverter(int size) {
    // TODO: Currently each character uses 2 Bytes (see calculateSize(int))
    // Improve this by check how many bytes are actually needed
    super(calculateSize(size));
  }

  /**
   * @see FixedSizeConverter#read(DataInput, Object)
   */
  @Override
  public String read(DataInput dataInput, String object) throws IOException {
    return StringConverter.DEFAULT_INSTANCE.read(dataInput);
  }

  /**
   * @see FixedSizeConverter#write(DataOutput, Object)
   */
  @Override
  public void write(DataOutput dataOutput, String object) throws IOException {
    StringConverter.DEFAULT_INSTANCE.write(dataOutput, object);
  }

}
