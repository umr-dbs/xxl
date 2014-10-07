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
 * Models available relation SQL types instead of integer constants. See {@link java.sql.Types}.
 * Needed for auto-matching with {@link xxl.core.relational.JavaType JavaType}
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see xxl.core.relational.JavaType
 * 
 */
public enum RelationalType {
  /**
   * Represents {@link java.sql.Types} Array
   */
  ARRAY,

  /**
   * Represents {@link java.sql.Types} BigInt
   */
  BIGINT,

  /**
   * Represents {@link java.sql.Types} Binary
   */
  BINARY,

  /**
   * Represents {@link java.sql.Types} Bit
   */
  BIT,

  /**
   * Represents {@link java.sql.Types} Blob
   */
  BLOB,

  /**
   * Represents {@link java.sql.Types} Boolean
   */
  BOOLEAN,

  /**
   * Represents {@link java.sql.Types} Char
   */
  CHAR,

  /**
   * Represents {@link java.sql.Types} Clob
   */
  CLOB,

  /**
   * Represents {@link java.sql.Types} Datalink
   */
  DATALINK,

  /**
   * Represents {@link java.sql.Types} Date
   */
  DATE,

  /**
   * Represents {@link java.sql.Types} Decimal
   */
  DECIMAL,

  /**
   * Represents {@link java.sql.Types} Distinct
   */
  DISTINCT,

  /**
   * Represents {@link java.sql.Types} Double
   */
  DOUBLE,

  /**
   * Represents {@link java.sql.Types} Float
   */
  FLOAT,

  /**
   * Represents {@link java.sql.Types} Integer
   */
  INTEGER,

  /**
   * Represents {@link java.sql.Types} JavaObject
   */
  JAVA_OBJECT,

  /**
   * Represents {@link java.sql.Types} LONGNVARCHAR
   */
  LONGNVARCHAR,

  /**
   * Represents {@link java.sql.Types} LONGVARBINARY
   */
  LONGVARBINARY,

  /**
   * Represents {@link java.sql.Types} LONGVARCHAR
   */
  LONGVARCHAR,

  /**
   * Represents {@link java.sql.Types} NCHAR
   */
  NCHAR,

  /**
   * Represents {@link java.sql.Types} NCLOB
   */
  NCLOB,

  /**
   * Represents {@link java.sql.Types} NULL
   */
  NULL,

  /**
   * Represents {@link java.sql.Types} NUMERIC
   */
  NUMERIC,

  /**
   * Represents {@link java.sql.Types} NVARCHAR
   */
  NVARCHAR,

  /**
   * Represents {@link java.sql.Types} OTHER
   */
  OTHER,

  /**
   * Represents {@link java.sql.Types} REAL
   */
  REAL,

  /**
   * Represents {@link java.sql.Types} REF
   */
  REF,

  /**
   * Represents {@link java.sql.Types} ROWID
   */
  ROWID,

  /**
   * Represents {@link java.sql.Types} SMALLINT
   */
  SMALLINT,

  /**
   * Represents {@link java.sql.Types} SQLXML
   */
  SQLXML,

  /**
   * Represents {@link java.sql.Types} STRUCT
   */
  STRUCT,

  /**
   * Represents {@link java.sql.Types} TIME
   */
  TIME,

  /**
   * Represents {@link java.sql.Types} TIMESTAMP
   */
  TIMESTAMP,

  /**
   * Represents {@link java.sql.Types} TINYINT
   */
  TINYINT,

  /**
   * Represents {@link java.sql.Types} VARBINARY
   */
  VARBINARY,

  /**
   * Represents {@link java.sql.Types} VARCHAR
   */
  VARCHAR
}
