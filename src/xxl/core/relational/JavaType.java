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

package xxl.core.relational;

/**
 * Models available Java types. Needed for auto-matching with
 * {@link xxl.core.relational.RelationalType RelationalType}
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see xxl.core.relational.RelationalType
 */
public enum JavaType {
  /**
   * Represents an <code>array</code>
   */
  ARRAY,

  /**
   * Represents {@link java.math.BigDecimal BigDecimal}
   */
  BIG_DECIMAL,

  /**
   * Represents <code>blob</code> type, see {@link java.sql.Blob Blob}
   */
  BLOB,

  /**
   * Represents {@link java.lang.Boolean Boolean}
   */
  BOOLEAN,

  /**
   * Represents {@link java.lang.Byte Byte}
   */
  BYTE,

  /**
   * Represents array of {@link java.lang.Byte Byte}
   */
  BYTE_ARRAY,

  /**
   * Represents <code>clob</code> type, see {@link java.sql.Clob Clob}
   */
  CLOB,

  /**
   * Represents {@link java.sql.Date Date}
   */
  DATE,

  /**
   * Represents <code>distinct</code> type (mapping of underlying type)
   */
  DISTINCT,

  /**
   * Represents {@link java.lang.Double Double}
   */
  DOUBLE,

  /**
   * Represents {@link java.lang.Float Float}
   */
  FLOAT,

  /**
   * Represents {@link java.lang.Integer Integer}
   */
  INT,

  /**
   * Represents {@link java.lang.Long Long}
   */
  LONG,

  /**
   * Represents <code>NCLOB</code> type
   */
  NCLOB,

  /**
   * Represents <code>null</code> because {@link java.sql.Types} requires it
   */
  NULL,

  /**
   * Represents a {@link java.lang.Object Object} (underlying Java class)
   */
  OBJECT,

  /**
   * Represents <code>other</code> type
   */
  OTHER,

  /**
   * Represents <code>ref</code> type
   */
  REF,

  /**
   * Represents {@link java.sql.RowId RowId}
   */
  ROWID,

  /**
   * Represents {@link java.lang.Short Short}
   */
  SHORT,

  /**
   * Represents {@link java.sql.SQLXML SQLXml}
   */
  SQLXML,

  /**
   * Represents {@link java.lang.String String}
   */
  STRING,

  /**
   * Represents a <i>struct</i> type
   */
  STRUCT,

  /**
   * Represents {@link java.sql.Time Time}
   */
  TIME,

  /**
   * Represents {@link java.sql.Timestamp TimeStamp}
   */
  TIMESTAMP,

  /**
   * Represents an <i>unkown</i> type
   */
  UNKNOWN,

  /**
   * Represents {@link java.net.URL URL}
   */
  URL
}
