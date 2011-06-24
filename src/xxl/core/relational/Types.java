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

package xxl.core.relational;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.StoredColumnMetaData;
import xxl.core.util.metaData.MetaDataException;

/**
 * This class provides a set of type codes that can be used to identify Java
 * types. It also provides methods for converting type codes into type names
 * and back again and methods to resolve adequate Java types for SQL types and
 * vice versa. The present type mapping follows the conventions made in the
 * chapter <i>Mapping SQL and Java Types</i> available 
 * <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jdbc/getstart/mapping.html#996857">here</a>.
 * 
 * @see java.sql.Types
 */
public final class Types {

	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.lang.Object</code>, i.e., the concrete type of the object is
	 * unknown.
	 */
	public static final int UNKNOWN = 0;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.lang.Boolean</code>.
	 */
	public static final int BOOLEAN = 1;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.lang.Byte</code>.
	 */
	public static final int BYTE = 2;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.lang.Short</code>.
	 */
	public static final int SHORT = 3;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.lang.Integer</code>.
	 */
	public static final int INTEGER = 4;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.lang.Long</code>.
	 */
	public static final int LONG = 5;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.lang.Float</code>.
	 */
	public static final int FLOAT = 6;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.lang.Double</code>.
	 */
	public static final int DOUBLE = 7;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.math.BigDecimal</code>.
	 */
	public static final int BIG_DECIMAL = 8;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.lang.Character</code>.
	 */
	public static final int CHARACTER = 9;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.lang.String</code>.
	 */
	public static final int STRING = 10;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.sql.Date</code>.
	 */
	public static final int DATE = 11;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>java.sql.Time</code>.
	 */
	public static final int TIME = 12;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.sql.Timestamp</code>.
	 */
	public static final int TIMESTAMP = 13;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.sql.Timestamp</code>.
	 */
	public static final int CLOB = 14;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.sql.Timestamp</code>.
	 */
	public static final int BLOB = 15;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.sql.Timestamp</code>.
	 */
	public static final int ARRAY = 16;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.sql.Timestamp</code>.
	 */
	public static final int STRUCT = 17;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type
	 * <code>java.sql.Timestamp</code>.
	 */
	public static final int REF = 18;
	
	/**
	 * The constant in the Java programming language, sometimes referred to as
	 * a type code, that identifies the Java type <code>byte[]</code>.
	 */
	public static final int BYTE_ARRAY = 19;

	/**
	 * A map storing the Java types and the associated SQL types. The type
	 * codes of the Java types are mapped to their names and the SQL type codes
	 * they are associated with.
	 */
	private static final Map<Integer, Object[]> javaTypeMap;
	
	/**
	 * A map storing the SQL types and the associated Java types. The type
	 * codes of the SQL types are mapped to their names and the Java type codes
	 * they are associated with.
	 */
	private static final Map<Integer, Object[]> sqlTypeMap;
	
	static {
		javaTypeMap = new HashMap<Integer, Object[]>();
		putTypeMapping(javaTypeMap, STRING,      "java.lang.String",     java.sql.Types.LONGVARCHAR);
		putTypeMapping(javaTypeMap, BIG_DECIMAL, "java.math.BigDecimal", java.sql.Types.NUMERIC);
		putTypeMapping(javaTypeMap, BOOLEAN,     "java.lang.Boolean",    java.sql.Types.BIT);
		putTypeMapping(javaTypeMap, BYTE,        "java.lang.Byte",       java.sql.Types.TINYINT);
		putTypeMapping(javaTypeMap, SHORT,       "java.lang.Short",      java.sql.Types.SMALLINT);
		putTypeMapping(javaTypeMap, INTEGER,     "java.lang.Integer",    java.sql.Types.INTEGER);
		putTypeMapping(javaTypeMap, LONG,        "java.lang.Long",       java.sql.Types.BIGINT);
		putTypeMapping(javaTypeMap, FLOAT,       "java.lang.Float",      java.sql.Types.REAL);
		putTypeMapping(javaTypeMap, DOUBLE,      "java.lang.Double",     java.sql.Types.DOUBLE);
		putTypeMapping(javaTypeMap, BYTE_ARRAY,  "byte[]",               java.sql.Types.LONGVARBINARY);
		putTypeMapping(javaTypeMap, DATE,        "java.sql.Date",        java.sql.Types.DATE);
		putTypeMapping(javaTypeMap, TIME,        "java.sql.Time",        java.sql.Types.TIME);
		putTypeMapping(javaTypeMap, TIMESTAMP,   "java.sql.Timestamp",   java.sql.Types.TIMESTAMP);
		putTypeMapping(javaTypeMap, CLOB,        "java.sql.Clob",        java.sql.Types.CLOB);
		putTypeMapping(javaTypeMap, BLOB,        "java.sql.Blob",        java.sql.Types.BLOB);
		putTypeMapping(javaTypeMap, ARRAY,       "java.sql.Array",       java.sql.Types.ARRAY);
		putTypeMapping(javaTypeMap, STRUCT,      "java.sql.Struct",      java.sql.Types.STRUCT);
		putTypeMapping(javaTypeMap, REF,         "java.sql.Ref",         java.sql.Types.REF);
		putTypeMapping(javaTypeMap, UNKNOWN,     "java.lang.Object",     java.sql.Types.JAVA_OBJECT);
		// additional types specified in java.sql.Types that are not mentioned
		// in the above-mentioned paper 
		putTypeMapping(javaTypeMap, CHARACTER,   "java.lang.Character",  java.sql.Types.LONGVARCHAR);

		sqlTypeMap = new HashMap<Integer, Object[]>();
		putTypeMapping(sqlTypeMap, java.sql.Types.CHAR,          "CHAR",          STRING);
		putTypeMapping(sqlTypeMap, java.sql.Types.VARCHAR,       "VARCHAR",       STRING);
		putTypeMapping(sqlTypeMap, java.sql.Types.LONGVARCHAR,   "LONGVARCHAR",   STRING);
		putTypeMapping(sqlTypeMap, java.sql.Types.NUMERIC,       "NUMERIC",       BIG_DECIMAL);
		putTypeMapping(sqlTypeMap, java.sql.Types.DECIMAL,       "DECIMAL",       BIG_DECIMAL);
		putTypeMapping(sqlTypeMap, java.sql.Types.BIT,           "BIT",           BOOLEAN);
		putTypeMapping(sqlTypeMap, java.sql.Types.TINYINT,       "TINYINT",       SHORT);
		putTypeMapping(sqlTypeMap, java.sql.Types.SMALLINT,      "SMALLINT",      SHORT);
		putTypeMapping(sqlTypeMap, java.sql.Types.INTEGER,       "INTEGER",       INTEGER);
		putTypeMapping(sqlTypeMap, java.sql.Types.BIGINT,        "BIGINT",        LONG);
		putTypeMapping(sqlTypeMap, java.sql.Types.REAL,          "REAL",          FLOAT);
		putTypeMapping(sqlTypeMap, java.sql.Types.FLOAT,         "FLOAT",         DOUBLE);
		putTypeMapping(sqlTypeMap, java.sql.Types.DOUBLE,        "DOUBLE",        DOUBLE);
		putTypeMapping(sqlTypeMap, java.sql.Types.BINARY,        "BINARY",        BYTE_ARRAY);
		putTypeMapping(sqlTypeMap, java.sql.Types.VARBINARY,     "VARBINARY",     BYTE_ARRAY);
		putTypeMapping(sqlTypeMap, java.sql.Types.LONGVARBINARY, "LONGVARBINARY", BYTE_ARRAY);
		putTypeMapping(sqlTypeMap, java.sql.Types.DATE,          "DATE",          DATE);
		putTypeMapping(sqlTypeMap, java.sql.Types.TIME,          "TIME",          TIME);
		putTypeMapping(sqlTypeMap, java.sql.Types.TIMESTAMP,     "TIMESTAMP",     TIMESTAMP);
		putTypeMapping(sqlTypeMap, java.sql.Types.DISTINCT,      "DISTINCT",      UNKNOWN);
		putTypeMapping(sqlTypeMap, java.sql.Types.CLOB,          "CLOB",          CLOB);
		putTypeMapping(sqlTypeMap, java.sql.Types.BLOB,          "BLOB",          BLOB);
		putTypeMapping(sqlTypeMap, java.sql.Types.ARRAY,         "ARRAY",         ARRAY);
		putTypeMapping(sqlTypeMap, java.sql.Types.STRUCT,        "STRUCT",        STRUCT);
		putTypeMapping(sqlTypeMap, java.sql.Types.REF,           "REF",           REF);
		putTypeMapping(sqlTypeMap, java.sql.Types.JAVA_OBJECT,   "JAVA_OBJECT",   UNKNOWN);
		// additional types specified in java.sql.Types that are not mentioned
		// in the above-mentioned paper 
		putTypeMapping(sqlTypeMap, java.sql.Types.BOOLEAN,       "BOOLEAN",       BOOLEAN);
		putTypeMapping(sqlTypeMap, java.sql.Types.NULL,          "NULL",          UNKNOWN);
		putTypeMapping(sqlTypeMap, java.sql.Types.OTHER,         "OTHER",         UNKNOWN);
		putTypeMapping(sqlTypeMap, java.sql.Types.DATALINK,      "DATALINK",      UNKNOWN);
	}
	
	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Types() {
		// private access in order to ensure non-instantiability
	}
	
	/**
	 * Puts the type specifed by its type code and its name into the given map
	 * and associates it with the type specified by the second type code.
	 * 
	 * @param typeMap the map the specified type should be put into.
	 * @param typeCode the type code of the type to be put into the map.
	 * @param typeName the name of the type to be put into the map.
	 * @param associatedTypeCode the type code the specified type should be
	 *        associated with.
	 */
	private static void putTypeMapping(Map<Integer, Object[]> typeMap, int typeCode, String typeName, int associatedTypeCode) {
		typeMap.put(
			typeCode,
			new Object[] {
				typeName,
				associatedTypeCode
			}
		);
	}
	
	/**
	 * Determines a Java type that is able to store an object of the specified
	 * SQL type.
	 * 
	 * @param sqlType the SQL type code for which an adequate Java type should
	 *        be resolved.
	 * @return a Java type that is able to store an object of the specified SQL
	 *         type.
	 * @throws IllegalArgumentException if the SQL type code is unknown.
	 */
	public static int getJavaType(int sqlType) throws IllegalArgumentException {
		Object[] value = sqlTypeMap.get(sqlType);
		if (value == null)
			throw new IllegalArgumentException("unknown SQL type code " + sqlType);
		return (Integer)value[1];
	}

	/**
	 * Resolve the type code of the specified Java type name.
	 * 
	 * @param javaTypeName the name of the Java type whose type code should be
	 *        resolved.
	 * @return the type code of the specified Java type.
	 * @throws IllegalArgumentException if the Java type name is unknown.
	 */
	public static int getJavaTypeCode(String javaTypeName) throws IllegalArgumentException {
		for (Map.Entry<Integer, Object[]> entry : javaTypeMap.entrySet())
			if (((String)entry.getValue()[0]).equalsIgnoreCase(javaTypeName))
				return entry.getKey();
		throw new IllegalArgumentException("unknown Java type name " + javaTypeName);
	}
	
	/**
	 * Resolve the name of the specified Java type code.
	 * 
	 * @param javaType the Java type code which name should be resolved.
	 * @return the name of the specified Java type code.
	 * @throws IllegalArgumentException if the Java type code is unknown.
	 */
	public static String getJavaTypeName(int javaType) throws IllegalArgumentException {
		Object[] value = javaTypeMap.get(javaType);
		if (value == null)
			throw new IllegalArgumentException("unknown Java type code " + javaType);
		return (String)value[0];
	}

	/**
	 * Determines the most general SQL type code that is able to store an
	 * object of the specified Java type.
	 * 
	 * @param javaType the Java type code for which an adequate SQL type should
	 *        be resolved.
	 * @return the most general SQL type that is able to store an object of the
	 *         specified Java type.
	 * @throws IllegalArgumentException if the Java type code is unknown.
	 */
	public static int getSqlType(int javaType) throws IllegalArgumentException {
		Object[] value = javaTypeMap.get(javaType);
		if (value == null)
			throw new IllegalArgumentException("unknown Java type code " + javaType);
		return (Integer)value[1];
	}

	/**
	 * Resolve the type code of the specified SQL type name.
	 * 
	 * @param sqlTypeName the name of the SQL type whose type code should be
	 *        resolved.
	 * @return the type code of the specified SQL type.
	 * @throws IllegalArgumentException if the SQL type name is unknown.
	 */
	public static int getSqlTypeCode(String sqlTypeName) throws IllegalArgumentException {
		for (Map.Entry<Integer, Object[]> entry : sqlTypeMap.entrySet())
			if (((String)entry.getValue()[0]).equalsIgnoreCase(sqlTypeName))
				return entry.getKey();
		throw new IllegalArgumentException("unknown SQL type name " + sqlTypeName);
	}
	
	/**
	 * Resolve the name of the specified SQL type code.
	 * 
	 * @param sqlType the SQL type code which name should be resolved.
	 * @return the name of the specified SQL type code.
	 * @throws IllegalArgumentException if the SQL type code is unknown.
	 */
	public static String getSqlTypeName(int sqlType) throws IllegalArgumentException {
		Object[] value = sqlTypeMap.get(sqlType);
		if (value == null)
			throw new IllegalArgumentException("unknown SQL type code " + sqlType);
		return (String)value[0];
	}
	
	/**
	 * Creates a column metadata describing a column holding objects of the
	 * specified Java type.
	 * 
	 * @param javaType the Java type the column must be able to store.
	 * @return a column metadata describing a column holding objects of the
	 *         specified Java type.
	 */
	public static ColumnMetaData getColumnMetaData(int javaType) {
		switch (javaType) {
			case STRING:
			case BIG_DECIMAL:
			case FLOAT:
			case DOUBLE:
			case BYTE_ARRAY:
				throw new IllegalArgumentException("because of different precision/scale handling this method does not provide column metadata for object of type " + getJavaTypeName(javaType) + ". Use Types.getColumnMetaData(int,java.lang.Object) instead.");
			default:
				return getColumnMetaData(javaType, null);
		}
	}

	/**
	 * Creates a column metadata describing a column holding the given object
	 * of the specified Java type. The object is used to determine the columns
	 * precision and scale (for <code>String</code>, <code>BigDecimal</code>,
	 * <code>Float</code>, <code>Double</code> and <code>byte[]</code>
	 * objects), otherwise it is ignored.
	 * 
	 * @param javaType the Java type the column must be able to store.
	 * @param javaObject the Java object used for determining the columns
	 *        precision and scale.
	 * @return a column metadata describing a column holding the given object
	 *         of the specified Java type.
	 */
	public static ColumnMetaData getColumnMetaData(int javaType, Object javaObject) {
		String javaTypeName = getJavaTypeName(javaType);
		int sqlType = getSqlType(javaType);
		String sqlTypeName = getSqlTypeName(sqlType);
		switch (javaType) {
			case CHARACTER:
				return new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 1, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case BOOLEAN:
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 1, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case BYTE:
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 3, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case SHORT:
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 5, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case INTEGER:
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 10, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case LONG:
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 19, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case DATE:
			case TIME:
			case TIMESTAMP:
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 0, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case CLOB:
			case BLOB:
			case ARRAY:
			case STRUCT:
			case REF:
			case UNKNOWN:
				return new StoredColumnMetaData(false, false, false, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", 0, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case STRING:
				String string = (String)javaObject;
				return new StoredColumnMetaData(false, true, true, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", string.length(), 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case BIG_DECIMAL:
				BigDecimal bigDecimal = (BigDecimal)javaObject;
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", bigDecimal.precision(), bigDecimal.scale(), "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case FLOAT:
				bigDecimal = new BigDecimal((Float)javaObject, new MathContext(8, RoundingMode.HALF_EVEN));
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", bigDecimal.precision(), bigDecimal.scale(), "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case DOUBLE:
				bigDecimal = new BigDecimal((Double)javaObject, new MathContext(16, RoundingMode.HALF_EVEN));
				return new StoredColumnMetaData(false, false, true, false, ResultSetMetaData.columnNoNulls, true, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", bigDecimal.precision(), bigDecimal.scale(), "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			case BYTE_ARRAY:
				byte[] bytes = (byte[])javaObject;
				return new StoredColumnMetaData(false, false, false, false, ResultSetMetaData.columnNoNulls, false, sqlTypeName.length() + 7, sqlTypeName + " column", sqlTypeName + "_COLUMN", "", bytes.length, 0, "", "", sqlType, sqlTypeName, true, false, false, javaTypeName);
			default:
				throw new IllegalArgumentException("unknown SQL type code " + sqlType);
		}
	}
	
	/**
	 * Determines the object size of an object of the given SQL type and
	 * precision.
	 * 
	 * @param sqlType the SQL type of the object whose objects size should be
	 *        determined.
	 * @param precision the precision of the object whose objects size should
	 *        be determined.
	 * @return the object size of an object of the given SQL type and
	 *         precision.
	 */
	public static int getObjectSize(int sqlType, int precision) {
		switch (getJavaType(sqlType)) {
			case BOOLEAN:
			case BYTE:
				return 1;
			case CHARACTER:
			case SHORT:
				return 2;
			case INTEGER:
			case FLOAT:
				return 4;
			case LONG:
			case DOUBLE:
			case DATE:
			case TIME:
			case TIMESTAMP:
				return 8;
			case BYTE_ARRAY:
				return precision;
			case BIG_DECIMAL:
			case STRING:
				return precision*2;
			case CLOB:
			case BLOB:
			case ARRAY:
			case STRUCT:
			case REF:
			case UNKNOWN:
				return 8;
			default:
				throw new IllegalArgumentException("unknown SQL type code " + sqlType);
		}
	}
	
	/**
	 * Determines the object size of an object stored in a relational column
	 * specified by the given column metadata.
	 * 
	 * @param columnMetaData the column metadata describing the column whose
	 *        object size should be determined.
	 * @return the object size of an object stored in a relational column
	 *         specified by the given column metadata.
	 */
	public static int getObjectSize(ColumnMetaData columnMetaData) {
		try {
			return getObjectSize(columnMetaData.getColumnType(), columnMetaData.getPrecision());
		}
		catch (SQLException sqle) {
			throw new MetaDataException("the metadata cannot be accessed properly because of the following SQL exception: " + sqle.getMessage());
		}
	}
	
	/**
	 * Determines the object size of an object stored in the
	 * <code>column</code>th column of a table specified by the given
	 * relational metadata.
	 * 
	 * @param resultSetMetaData the relational metadata describing the table
	 *        storing the desired colum.
	 * @param column the index of the desired column inside the table specified
	 *        by the given relational metadata.
	 * @return the object size of an object stored in the <code>column</code>th
	 *         column of a table specified by the given relational metadata.
	 */
	public static int getObjectSize(ResultSetMetaData resultSetMetaData, int column) {
		try {
			return getObjectSize(resultSetMetaData.getColumnType(column), resultSetMetaData.getPrecision(column));
		}
		catch (SQLException sqle) {
			throw new MetaDataException("the metadata cannot be accessed properly because of the following SQL exception: " + sqle.getMessage());
		}
	}
	
	/**
	 * Determines the object size of an object stored in a relational table
	 * specified by the given relational metadata.
	 * 
	 * @param resultSetMetaData the relational metadata describing the table
	 *        whose object size should be determined.
	 * @return the object size of an object stored in a relational table
	 *         specified by the given relational metadata.
	 */
	public static int getObjectSize(ResultSetMetaData resultSetMetaData) {
		try {
			int objectSize = 0;
			for (int column = 1; column <= resultSetMetaData.getColumnCount(); objectSize += getObjectSize(resultSetMetaData, column++));
			return objectSize;
		}
		catch (SQLException sqle) {
			throw new MetaDataException("the metadata cannot be accessed properly because of the following SQL exception: " + sqle.getMessage());
		}
	}
	
	/**
	 * Determines whether the given Java types are comparable.
	 * 
	 * @param type1 the first Java type to be checked.
	 * @param type2 the second Java type to be checked.
	 * @return <code>true</code> if the given Java types are comparable,
	 *         otherwise <code>false</code>.
	 */
	public static boolean areComparable(int type1, int type2) {
		switch (type1) {
			case BYTE:
			case SHORT:
			case INTEGER:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case BIG_DECIMAL:
				return type2 == BYTE || type2 == SHORT || type2 == INTEGER || type2 == LONG || type2 == FLOAT || type2 == DOUBLE || type2 == BIG_DECIMAL;
			case CHARACTER:
			case STRING:
				return type2 == CHARACTER || type2 == STRING;
			default:
				return type1 == type2;
		}
	}
	
}
