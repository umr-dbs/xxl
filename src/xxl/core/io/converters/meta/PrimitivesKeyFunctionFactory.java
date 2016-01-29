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
import xxl.core.functions.concrete.PrimitivesGetKeyFunction;
import xxl.core.indexStructures.keyRanges.BooleanKeyRange;
import xxl.core.indexStructures.keyRanges.ByteKeyRange;
import xxl.core.indexStructures.keyRanges.DateKeyRange;
import xxl.core.indexStructures.keyRanges.DoubleKeyRange;
import xxl.core.indexStructures.keyRanges.FloatKeyRange;
import xxl.core.indexStructures.keyRanges.IntegerKeyRange;
import xxl.core.indexStructures.keyRanges.LongKeyRange;
import xxl.core.indexStructures.keyRanges.ShortKeyRange;
import xxl.core.indexStructures.keyRanges.TimeKeyRange;
import xxl.core.indexStructures.keyRanges.TimestampKeyRange;
import xxl.core.indexStructures.separators.BooleanSeparator;
import xxl.core.indexStructures.separators.ByteSeparator;
import xxl.core.indexStructures.separators.DateSeparator;
import xxl.core.indexStructures.separators.DoubleSeparator;
import xxl.core.indexStructures.separators.FloatSeparator;
import xxl.core.indexStructures.separators.IntegerSeparator;
import xxl.core.indexStructures.separators.LongSeparator;
import xxl.core.indexStructures.separators.ShortSeparator;
import xxl.core.indexStructures.separators.TimeSeparator;
import xxl.core.indexStructures.separators.TimestampSeparator;
import xxl.core.indexStructures.builder.BPlusTree.TupleType;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.MeasuredPrimitiveConverter;
import xxl.core.relational.JavaType;

/**
 * Provides a bundle of functions and a converter to work with java primitive types.<br/>
 * <br/>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * @see JavaType
 * 
 */
public class PrimitivesKeyFunctionFactory extends KeyFunctionFactory {

  /*
   * The types which should be managed
   */
  private JavaType mJavaType;

  /**
   * Constructs a new factory for the given data type
   * 
   * @param type Java data type
   */
  public PrimitivesKeyFunctionFactory(JavaType type) {
    mJavaType = type;
  }

  @Override
  public MeasuredConverter getKeyConverter() {
    return new MeasuredPrimitiveConverter(mJavaType);
  }

  @Override
  public Function getKeyFunction() {
    return new PrimitivesGetKeyFunction(mJavaType);
  }

  /**
   * <b>Note</b> An instance of <code>String</code> is not supported. Please use
   * {@link TupleKeyFunctionFactory} for {@link TupleType} handling.
   */
  @Override
  public Function getKeyRangeFunction(int i) {
    switch (mJavaType) {
      case BOOLEAN:
        return BooleanKeyRange.FACTORY_FUNCTION;
      case BYTE:
        return ByteKeyRange.FACTORY_FUNCTION;
      case DATE:
        return DateKeyRange.FACTORY_FUNCTION;
      case DOUBLE:
        return DoubleKeyRange.FACTORY_FUNCTION;
      case FLOAT:
        return FloatKeyRange.FACTORY_FUNCTION;
      case INT:
        return IntegerKeyRange.FACTORY_FUNCTION;
      case LONG:
        return LongKeyRange.FACTORY_FUNCTION;
      case SHORT:
        return ShortKeyRange.FACTORY_FUNCTION;
        // case STRING:
        // typeSize = FixedSizeStringConverter.calculateSize(mTupleMetaData
        // .getContentLength(columnIndex + 1));
        // break;
      case TIME:
        return TimeKeyRange.FACTORY_FUNCTION;
      case TIMESTAMP:
        return TimestampKeyRange.FACTORY_FUNCTION;
      default:
        throw new UnsupportedOperationException("Not implemented yet for \""
            + mJavaType + "\"");
    }
  }

  /**
   * <b>Note</b> An instance of <code>String</code> is not supported. Please use
   * {@link TupleKeyFunctionFactory} for {@link TupleType} handling.
   */
  @Override
  public Function getKeyValueSeparatorFunction() {
    switch (mJavaType) {
      case BOOLEAN:
        return BooleanSeparator.FACTORY_FUNCTION;
      case BYTE:
        return ByteSeparator.FACTORY_FUNCTION;
      case DATE:
        return DateSeparator.FACTORY_FUNCTION;
      case DOUBLE:
        return DoubleSeparator.FACTORY_FUNCTION;
      case FLOAT:
        return FloatSeparator.FACTORY_FUNCTION;
      case INT:
        return IntegerSeparator.FACTORY_FUNCTION;
      case LONG:
        return LongSeparator.FACTORY_FUNCTION;
      case SHORT:
        return ShortSeparator.FACTORY_FUNCTION;
        // case STRING:
        // typeSize = FixedSizeStringConverter.calculateSize(mTupleMetaData
        // .getContentLength(columnIndex + 1));
        // break;
      case TIME:
        return TimeSeparator.FACTORY_FUNCTION;
      case TIMESTAMP:
        return TimestampSeparator.FACTORY_FUNCTION;
      default:
        throw new UnsupportedOperationException("Not implemented yet for \""
            + mJavaType + "\"");
    }
  }

}
