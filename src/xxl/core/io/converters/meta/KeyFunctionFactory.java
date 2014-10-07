/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2014 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
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

package xxl.core.io.converters.meta;

import xxl.core.functions.Function;
import xxl.core.indexStructures.builder.BPlusTree.BPlusTreeBuilder;
import xxl.core.io.converters.MeasuredConverter;

/**
 * Abstract base class for type specific function collection used in {@link BPlusTreeBuilder} and
 * other index structures builders.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public abstract class KeyFunctionFactory {

  /**
   * @return The MeasuredConverter built up with the given information
   */
  public abstract MeasuredConverter getKeyConverter();

  /**
   * @return The key function built up with the given information
   */
  public abstract Function getKeyFunction();

  /**
   * @return The KeyRange function built up with the given information
   */
  public abstract Function getKeyRangeFunction(int compoundKeyColumnIndex);

  /**
   * @return The KeyValueSeparator function built up with the given information
   */
  public abstract Function getKeyValueSeparatorFunction();
}
