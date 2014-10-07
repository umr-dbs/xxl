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

package xxl.core.util;

import java.util.ArrayList;
import java.util.List;

import xxl.core.indexStructures.Entry;
import xxl.core.indexStructures.builder.IndexBuilder;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration;
import xxl.core.relational.JavaType;
import xxl.core.relational.RelationalType;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;
import xxl.core.relational.tuples.Tuple;

/**
 * Provides several static methods to convert one type to another.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class ConvertUtils {

  /**
   * BPlusTree is able to manage comparable objects or tuples. If tuples are used (wrapped by Entry
   * class) the Object <b>o</b> is a tuple (non-comparable) or a tuple subset which is the key (and
   * have to be comparable). In order to manage this casting (for delegation to BPlusTree) use this
   * method which <i>auto</i>matically cast <b>o</b> to fit the requirements.
   * 
   * @param o Object, e.g. primitive or tuple (or tuple subset)
   * @return The matching object
   */
  public static Object autoCast(Object o) {
    return (o instanceof Entry)
        ? ((Entry) o).asTuple()
        : ((o instanceof Entry.WithKey) ? ((Entry.WithKey) o)
            .asComparableArray() : o);
  }

  public static Comparable autoComparable(Object object, IndexBuilder creator) {
    if (object instanceof Tuple) {
      // If this is an instance of Tuple it is not possible
      // to cast it into Comparable as once because only the
      // key subset of this instance is comparable. In order
      // to cast it into Comparable project it to the
      // comparable subset.
      // TODO:
      Object[] tuple = ((Tuple) object).toArray();
      int[] keyIndices =
          ((BPlusConfiguration) creator.getIndexConfiguration())
              .getManagedType().getCompoundKeyIndices();
      Object[] keySubset = new Object[keyIndices.length];

      for (int index = 0; index < keyIndices.length; ++index) {
        keySubset[index] = (Comparable) tuple[keyIndices[index] - 1];
      }

      return new ColumnComparableArrayTuple(keySubset);

      // throw new IllegalArgumentException();
    } else
      return (object instanceof Comparable[]) ? new ColumnComparableArrayTuple(
          (Comparable[]) object) : (Comparable) object;
  }

  /**
   * Converts a object of {@link xxl.core.relational.RelationalType RelationalType} to
   * {@link java.sql.Types SQL Type} which is an integer. <br/>
   * <br/>
   * <b>Example</b> <code><pre>
   * 		int sqlType = Convert.toInt(RelationalType.ARRAY); // sqlTyp is now equal to java.sql.Types.ARRAY (= 2003)
   * </pre></code>
   * 
   * @param relationalType An object of type {@link xxl.core.relational.RelationalType
   *        RelationalType} which covers SQL types as an enum
   * @return The {@link java.sql.Types SQL Type} value of <i>relationalType</i> or
   *         <b>java.sql.Types.OTHER</b> if it fails
   * 
   * @see xxl.core.relational.RelationalType
   * @see java.sql.Types
   */
  public static int toInt(final RelationalType relationalType) {
    int retval = -1;

    switch (relationalType) {
      case ARRAY:
        retval = java.sql.Types.ARRAY;
        break;
      case BIGINT:
        retval = java.sql.Types.BIGINT;
        break;
      case BINARY:
        retval = java.sql.Types.BINARY;
        break;
      case BIT:
        retval = java.sql.Types.BIT;
        break;
      case BLOB:
        retval = java.sql.Types.BLOB;
        break;
      case BOOLEAN:
        retval = java.sql.Types.BOOLEAN;
        break;
      case CHAR:
        retval = java.sql.Types.CHAR;
        break;
      case CLOB:
        retval = java.sql.Types.CLOB;
        break;
      case DATALINK:
        retval = java.sql.Types.DATALINK;
        break;
      case DATE:
        retval = java.sql.Types.DATE;
        break;
      case DECIMAL:
        retval = java.sql.Types.DECIMAL;
        break;
      case DISTINCT:
        retval = java.sql.Types.DISTINCT;
        break;
      case DOUBLE:
        retval = java.sql.Types.DOUBLE;
        break;
      case FLOAT:
        retval = java.sql.Types.FLOAT;
        break;
      case INTEGER:
        retval = java.sql.Types.INTEGER;
        break;
      case JAVA_OBJECT:
        retval = java.sql.Types.JAVA_OBJECT;
        break;
      case LONGNVARCHAR:
        retval = java.sql.Types.LONGNVARCHAR;
        break;
      case LONGVARBINARY:
        retval = java.sql.Types.LONGVARBINARY;
        break;
      case LONGVARCHAR:
        retval = java.sql.Types.LONGVARCHAR;
        break;
      case NCHAR:
        retval = java.sql.Types.NCHAR;
        break;
      case NCLOB:
        retval = java.sql.Types.NCLOB;
        break;
      case NULL:
        retval = java.sql.Types.NULL;
        break;
      case NUMERIC:
        retval = java.sql.Types.NUMERIC;
        break;
      case NVARCHAR:
        retval = java.sql.Types.NVARCHAR;
        break;
      case OTHER:
        retval = java.sql.Types.OTHER;
        break;
      case REAL:
        retval = java.sql.Types.REAL;
        break;
      case REF:
        retval = java.sql.Types.REF;
        break;
      case ROWID:
        retval = java.sql.Types.ROWID;
        break;
      case SMALLINT:
        retval = java.sql.Types.SMALLINT;
        break;
      case SQLXML:
        retval = java.sql.Types.SQLXML;
        break;
      case STRUCT:
        retval = java.sql.Types.STRUCT;
        break;
      case TIME:
        retval = java.sql.Types.TIME;
        break;
      case TIMESTAMP:
        retval = java.sql.Types.TIMESTAMP;
        break;
      case TINYINT:
        retval = java.sql.Types.TINYINT;
        break;
      case VARBINARY:
        retval = java.sql.Types.VARBINARY;
        break;
      case VARCHAR:
        retval = java.sql.Types.VARCHAR;
        break;
      default:
        retval = java.sql.Types.OTHER;
        break;
    }
    return retval;
  }

  /**
   * Converts {@link xxl.core.relational.RelationalType RelationalType} object into
   * {@link xxl.core.relational.JavaType JavaType} type. <br/>
   * For type converting, see <a href="http://db.apache.org/ojb/docu/guides/jdbc-types.html">JDBC
   * Types</a> and <a href=
   * "http://publib.boulder.ibm.com/infocenter/idshelp/v111/index.jsp?topic=/com.ibm.jccids.doc/com.ibm.db2.luw.apdv.java.doc/doc/rjvjdata.htm"
   * >IBM: Data types that map to database data types in Java applications</a>
   * 
   * @param relationalType Your object of type <i>RelationalType</i> which should be converted
   * @return The assigned java type or <code>JavaType.UNKNOWN</code> if it fails.
   * @see xxl.core.relational.RelationalType
   * @see xxl.core.relational.JavaType
   */
  public static JavaType toJavaType(final RelationalType relationalType) {
    JavaType retval = JavaType.UNKNOWN;

    switch (relationalType) {
      case ARRAY:
        retval = JavaType.ARRAY;
        break;
      case BIGINT:
        retval = JavaType.LONG;
        break;
      case BINARY:
        retval = JavaType.BYTE_ARRAY;
        break;
      case BIT:
        retval = JavaType.BOOLEAN;
        break;
      case BLOB:
        retval = JavaType.BLOB;
        break;
      case BOOLEAN:
        retval = JavaType.BOOLEAN;
        break;
      case CHAR:
        retval = JavaType.STRING;
        break;
      case CLOB:
        retval = JavaType.CLOB;
        break;
      case DATALINK:
        retval = JavaType.URL;
        break;
      case DATE:
        retval = JavaType.DATE;
        break;
      case DECIMAL:
        retval = JavaType.BIG_DECIMAL;
        break;
      case DISTINCT:
        retval = JavaType.DISTINCT;
        break;
      case DOUBLE:
        retval = JavaType.DOUBLE;
        break;
      case FLOAT:
        retval = JavaType.FLOAT;
        break;
      case INTEGER:
        retval = JavaType.INT;
        break;
      case JAVA_OBJECT:
        retval = JavaType.OBJECT;
        break;
      case LONGNVARCHAR:
        retval = JavaType.STRING;
        break;
      case LONGVARBINARY:
        retval = JavaType.BYTE_ARRAY;
        break;
      case LONGVARCHAR:
        retval = JavaType.STRING;
        break;
      case NCHAR:
        retval = JavaType.STRING;
        break;
      case NCLOB:
        retval = JavaType.NCLOB;
        break;
      case NULL:
        retval = JavaType.NULL;
        break;
      case NUMERIC:
        retval = JavaType.BIG_DECIMAL;
        break;
      case NVARCHAR:
        retval = JavaType.STRING;
        break;
      case OTHER:
        retval = JavaType.OTHER;
        break;
      case REAL:
        retval = JavaType.FLOAT;
        break;
      case REF:
        retval = JavaType.REF;
        break;
      case ROWID:
        retval = JavaType.ROWID;
        break;
      case SMALLINT:
        retval = JavaType.SHORT;
        break;
      case SQLXML:
        retval = JavaType.SQLXML;
        break;
      case STRUCT:
        retval = JavaType.STRUCT;
        break;
      case TIME:
        retval = JavaType.TIME;
        break;
      case TIMESTAMP:
        retval = JavaType.TIMESTAMP;
        break;
      case TINYINT:
        retval = JavaType.BYTE;
        break;
      case VARBINARY:
        retval = JavaType.BYTE_ARRAY;
        break;
      case VARCHAR:
        retval = JavaType.STRING;
        break;
      default:
        retval = JavaType.UNKNOWN;
        break;
    }
    return retval;
  }

  /**
   * Converts a String representation of {@link xxl.core.relational.JavaType JavaType} type into
   * {@link xxl.core.relational.JavaType JavaType} enumeration type. <br/>
   * 
   * @param javaType Your string which represents <i>JavaType</i> member
   * @return The assigned java type or <code>JavaType.UNKNOWN</code> if it fails.
   * @see xxl.core.relational.JavaType
   */
  public static JavaType toJavaType(final String javaType) {
    if (javaType.compareToIgnoreCase(JavaType.ARRAY.toString()) == 0)
      return JavaType.ARRAY;
    else if (javaType.compareToIgnoreCase(JavaType.BIG_DECIMAL.toString()) == 0)
      return JavaType.BIG_DECIMAL;
    else if (javaType.compareToIgnoreCase(JavaType.BLOB.toString()) == 0)
      return JavaType.BLOB;
    else if (javaType.compareToIgnoreCase(JavaType.BOOLEAN.toString()) == 0)
      return JavaType.BOOLEAN;
    else if (javaType.compareToIgnoreCase(JavaType.BYTE.toString()) == 0)
      return JavaType.BYTE;
    else if (javaType.compareToIgnoreCase(JavaType.BYTE_ARRAY.toString()) == 0)
      return JavaType.BYTE_ARRAY;
    else if (javaType.compareToIgnoreCase(JavaType.CLOB.toString()) == 0)
      return JavaType.CLOB;
    else if (javaType.compareToIgnoreCase(JavaType.DATE.toString()) == 0)
      return JavaType.DATE;
    else if (javaType.compareToIgnoreCase(JavaType.DISTINCT.toString()) == 0)
      return JavaType.DISTINCT;
    else if (javaType.compareToIgnoreCase(JavaType.DOUBLE.toString()) == 0)
      return JavaType.DOUBLE;
    else if (javaType.compareToIgnoreCase(JavaType.FLOAT.toString()) == 0)
      return JavaType.FLOAT;
    else if (javaType.compareToIgnoreCase(JavaType.INT.toString()) == 0)
      return JavaType.INT;
    else if (javaType.compareToIgnoreCase(JavaType.LONG.toString()) == 0)
      return JavaType.LONG;
    else if (javaType.compareToIgnoreCase(JavaType.NCLOB.toString()) == 0)
      return JavaType.NCLOB;
    else if (javaType.compareToIgnoreCase(JavaType.NULL.toString()) == 0)
      return JavaType.NULL;
    else if (javaType.compareToIgnoreCase(JavaType.OBJECT.toString()) == 0)
      return JavaType.OBJECT;
    else if (javaType.compareToIgnoreCase(JavaType.OTHER.toString()) == 0)
      return JavaType.OTHER;
    else if (javaType.compareToIgnoreCase(JavaType.REF.toString()) == 0)
      return JavaType.REF;
    else if (javaType.compareToIgnoreCase(JavaType.ROWID.toString()) == 0)
      return JavaType.ROWID;
    else if (javaType.compareToIgnoreCase(JavaType.SHORT.toString()) == 0)
      return JavaType.SHORT;
    else if (javaType.compareToIgnoreCase(JavaType.SQLXML.toString()) == 0)
      return JavaType.SQLXML;
    else if (javaType.compareToIgnoreCase(JavaType.STRING.toString()) == 0)
      return JavaType.STRING;
    else if (javaType.compareToIgnoreCase(JavaType.STRUCT.toString()) == 0)
      return JavaType.STRUCT;
    else if (javaType.compareToIgnoreCase(JavaType.TIMESTAMP.toString()) == 0)
      return JavaType.TIMESTAMP;
    else if (javaType.compareToIgnoreCase(JavaType.TIME.toString()) == 0)
      return JavaType.TIME;
    else
      return JavaType.OTHER;
  }

  /**
   * Converts a object of {@link java.sql.Types SQL Type} which is an integer to
   * {@link xxl.core.relational.RelationalType RelationalType}. <br/>
   * <br/>
   * <b>Example</b> <code><pre>
   * 		RelationalType relType = Convert.toRelationalType(java.sql.ARRAY); // relType is now equal to RelationalType.ARRAY
   * </pre></code>
   * 
   * @param sqlTypConstant An value of {@link java.sql.Types SQL Type}
   * @return The RelationalType value of <i>sqlTypConstant</i> or <b>RelationalType.OTHER</b> if it
   *         fails
   * 
   * @see xxl.core.relational.RelationalType
   * @see xxl.core.relational.JavaType
   */
  public static RelationalType toRelationalType(final int sqlTypConstant) {
    RelationalType retval = RelationalType.OTHER;

    switch (sqlTypConstant) {
      case java.sql.Types.ARRAY:
        retval = RelationalType.ARRAY;
        break;
      case java.sql.Types.BIGINT:
        retval = RelationalType.BIGINT;
        break;
      case java.sql.Types.BINARY:
        retval = RelationalType.BINARY;
        break;
      case java.sql.Types.BIT:
        retval = RelationalType.BIT;
        break;
      case java.sql.Types.BLOB:
        retval = RelationalType.BLOB;
        break;
      case java.sql.Types.BOOLEAN:
        retval = RelationalType.BOOLEAN;
        break;
      case java.sql.Types.CHAR:
        retval = RelationalType.CHAR;
        break;
      case java.sql.Types.CLOB:
        retval = RelationalType.CLOB;
        break;
      case java.sql.Types.DATALINK:
        retval = RelationalType.DATALINK;
        break;
      case java.sql.Types.DATE:
        retval = RelationalType.DATE;
        break;
      case java.sql.Types.DECIMAL:
        retval = RelationalType.DECIMAL;
        break;
      case java.sql.Types.DISTINCT:
        retval = RelationalType.DISTINCT;
        break;
      case java.sql.Types.DOUBLE:
        retval = RelationalType.DOUBLE;
        break;
      case java.sql.Types.FLOAT:
        retval = RelationalType.FLOAT;
        break;
      case java.sql.Types.INTEGER:
        retval = RelationalType.INTEGER;
        break;
      case java.sql.Types.JAVA_OBJECT:
        retval = RelationalType.JAVA_OBJECT;
        break;
      case java.sql.Types.LONGNVARCHAR:
        retval = RelationalType.LONGNVARCHAR;
        break;
      case java.sql.Types.LONGVARBINARY:
        retval = RelationalType.LONGVARBINARY;
        break;
      case java.sql.Types.LONGVARCHAR:
        retval = RelationalType.LONGVARCHAR;
        break;
      case java.sql.Types.NCHAR:
        retval = RelationalType.NCHAR;
        break;
      case java.sql.Types.NCLOB:
        retval = RelationalType.NCLOB;
        break;
      case java.sql.Types.NULL:
        retval = RelationalType.NULL;
        break;
      case java.sql.Types.NUMERIC:
        retval = RelationalType.NUMERIC;
        break;
      case java.sql.Types.NVARCHAR:
        retval = RelationalType.NVARCHAR;
        break;
      case java.sql.Types.OTHER:
        retval = RelationalType.OTHER;
        break;
      case java.sql.Types.REAL:
        retval = RelationalType.REAL;
        break;
      case java.sql.Types.REF:
        retval = RelationalType.REF;
        break;
      case java.sql.Types.ROWID:
        retval = RelationalType.ROWID;
        break;
      case java.sql.Types.SMALLINT:
        retval = RelationalType.SMALLINT;
        break;
      case java.sql.Types.SQLXML:
        retval = RelationalType.SQLXML;
        break;
      case java.sql.Types.STRUCT:
        retval = RelationalType.STRUCT;
        break;
      case java.sql.Types.TIME:
        retval = RelationalType.TIME;
        break;
      case java.sql.Types.TIMESTAMP:
        retval = RelationalType.TIMESTAMP;
        break;
      case java.sql.Types.TINYINT:
        retval = RelationalType.TINYINT;
        break;
      case java.sql.Types.VARBINARY:
        retval = RelationalType.VARBINARY;
        break;
      case java.sql.Types.VARCHAR:
        retval = RelationalType.VARCHAR;
        break;
      default:
        retval = RelationalType.OTHER;
        break;
    }

    return retval;
  }

  /**
   * Returns the fiber of {@link #toRelationalType(int)} at <code>e</code>, that means the inverse
   * image (preimage) of the singleton <code>e</code>. In other words this method returns all
   * Integers <code>i</code> for which #toRelationalType(<code>i</code>) equals <code>e</code>
   * holds.
   * 
   * @param e a relational type
   * @return the preimage of <code>e</code>
   */
  public Integer[] getFiber(RelationalType e) {
    /*
     * Please note, that java.sql.Types is not iterable and update this Integers if necessary
     */
    int domain[] =
        new int[] {-7, -6, 5, 4, -5, 6, 7, 8, 2, 3, 1, 12, -1, 91, 92, 93, -2,
            -3, -4, 0, 1111, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 70, 16,
            -8, -15, -9, -16, 2011, 2009};

    List<Integer> result = new ArrayList<>();
    for (int i : domain)
      if (toRelationalType(i) == e) result.add(i);
    return result.toArray(new Integer[result.size()]);
  }
}
