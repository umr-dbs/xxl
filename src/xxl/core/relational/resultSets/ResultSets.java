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

package xxl.core.relational.resultSets;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.mappers.Mapper;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * This class contains various useful <tt>static</tt> methods for managing
 * columns of result sets.
 *
 * <p>Most of these methods are used internally by the join operation of this
 * package.</p>
 *
 * <p>This class cannot become instantiated.</p>
 */
public class ResultSets {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private ResultSets() {
		// private access in order to ensure non-instantiability
	}

	/**
	 * Transfers an array of column names into an array of indices. For every
	 * column, the method findColumn from the class {@link java.sql.ResultSet}
	 * is called exactly once.
	 *
	 * <p>This method is identical to
	 * <code>ResultSetMetaDatas.getColumnIndices(resultSet.getMetaData(), columnNames)</code>
	 * (except exception handling if <code>getMetaData()</code> itself throws
	 * an exception).</p>
	 *
	 * @param resultSet the result set that is used.
	 * @param columnNames an array of strings that contains the names of some
	 *        of the result set's columns.
	 * @return an array of int values containing the indices of the given
	 *         columns.
	 * @throws SQLException if a database access error occurs.
	 */
	public static int[] getColumnIndices(ResultSet resultSet, String... columnNames) throws SQLException {
		int[] columnIndices = new int[columnNames.length];
		for (int i = 0; i < columnIndices.length; i++)
			columnIndices[i] = resultSet.findColumn(columnNames[i]);
		return columnIndices;
	}

	/**
	 * Transfers an array of indices into an array of column names. To get the
	 * column names, this method uses the metadata of the result set.
	 *
	 * @param resultSet the result set that is used.
	 * @param columnIndices an array of int values that contains the indices of
	 *        some of the result set's columns.
	 * @return an array of string objects containing the names of the given
	 *         columns.
	 * @throws SQLException if a database access error occurs.
	 */
	public static String[] getColumnNames(ResultSet resultSet, int... columnIndices) throws SQLException {
		return ResultSetMetaDatas.getColumnNames(resultSet.getMetaData(), columnIndices);
	}

	/**
	 * Creates a SQL query string for the creation of a table with the given
	 * name and the schema given as a result set's metadata object.
	 * 
	 * @param tableName the name of the table to be created.
	 * @param rsmd the schema of the new table.
	 * @param typeMap a map which maps the type names to type names which are
	 *        understood by a specific database system.
	 * @return the query string. 
	 * @throws SQLException if a database access error occurs.
	 */
	public static String getCreateTableQuery(String tableName, ResultSetMetaData rsmd, Map<String, String> typeMap) throws SQLException {
		StringBuffer sb = new StringBuffer("create table ");
		sb.append(tableName);
		sb.append("(\n");
		
		String typeName, realTypeName;
		
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			if (i > 1)
				sb.append(",\n");
			sb.append("\t");
			sb.append(rsmd.getColumnName(i));
			sb.append(" ");
			
			typeName = rsmd.getColumnTypeName(i);
			realTypeName = typeMap.get(typeName);
			if (realTypeName == null)
				realTypeName = typeName;
			
			sb.append(realTypeName);
		}
		sb.append("\n)");
		
		return sb.toString();
	}

	/**
	 * Creates a SQL query string for the creation of a table with the given
	 * name and the schema given as a result set's metadata object. The
	 * standard type names are used.
	 * 
	 * @param tableName the name of the table to be created.
	 * @param rsmd the schema of the new table.
	 * @return the query string. 
	 * @throws SQLException if a database access error occurs.
	 */
	public static String getCreateTableQuery(String tableName, ResultSetMetaData rsmd) throws SQLException {
		return getCreateTableQuery(tableName, rsmd, new HashMap<String, String>());
	}

	/**
	 * Returns a <i>prepared statement</i> which is used to insert data of a
	 * given schema into a database. 
	 * 
	 * @param con the connection to the database.
	 * @param tableName the name of the table where the data is inserted.
	 * @param rsmd the relational schema of the table of the database.
	 * @return the <i>prepared statement</i> which can be used for insertion.
	 * @throws SQLException if a database access error occurs.
	 */
	public static PreparedStatement getPreparedInsertStatement(Connection con, String tableName, ResultSetMetaData rsmd) throws SQLException {
		if (rsmd.getColumnCount() == 0)
			return null;
		
		StringBuffer sb = new StringBuffer("insert into ");
		sb.append(tableName);
		sb.append("values(");
		
		for (int i = 1; i < rsmd.getColumnCount(); i++)
			sb.append("?,");
		sb.append("?)");
		
		return con.prepareStatement(sb.toString());
	}

	/**
	 * Inserts a complete MetaDataCursor (which provides at least relational
	 * metadata) into a table inside a database. Inside this method, a
	 * <i>prepared statement</i> is used.
	 * 
	 * @param mdc the input data which will be written into the database.
	 * @param con the connection to the database.
	 * @param tableName the name of the table where the data is inserted.
	 * @throws SQLException if a database access error occurs.
	 */
	public static void insertIntoTable(MetaDataCursor<Tuple, CompositeMetaData<? super String, ? super ResultSetMetaData>> mdc, Connection con, String tableName) throws SQLException {
		ResultSetMetaData rsmd = ResultSetMetaDatas.getResultSetMetaData(mdc);
		PreparedStatement ps = getPreparedInsertStatement(con, tableName, rsmd);
		while (mdc.hasNext()) {
			Tuple t = mdc.next();
			
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				ps.setObject(i, t.getObject(i));
			
			ps.executeUpdate();
		}
	}

	/**
	 * Returns a function which transforms a tuple (the only parameter with
	 * which the function has to be called) into a SQL insert query string.
	 * 
	 * @param tableName the name of the table into which the tuple should be
	 *        inserted.
	 * @return the conversion function.
	 */
	public static Function<Tuple, String> getMapFunctionTupleToInsertQuery(final String tableName) {
		return new AbstractFunction<Tuple, String>() {
			@Override
			public String invoke(Tuple tuple) {
				if (tuple.getColumnCount() == 0)
					return null;
				
				StringBuffer sb = new StringBuffer("insert into ");
				sb.append(tableName);
				sb.append(" values (");
				
				for (int i = 1; i <= tuple.getColumnCount(); i++) {
					if (i > 1)
						sb.append(",");
					Object currentObject = tuple.getObject(i);
					if (currentObject instanceof Boolean)
						sb.append(((Boolean)currentObject).booleanValue() ? 1 : 0);
					else {
						if (currentObject instanceof String || currentObject instanceof Character)
							sb.append("'");
						if (currentObject instanceof Date)
							sb.append("#");
						sb.append(currentObject);
						if (currentObject instanceof String  || currentObject instanceof Character)
							sb.append("'");
						if (currentObject instanceof Date)
							sb.append("#");
					}
				}
				sb.append(")");
				
				return sb.toString();
			}
		};
	}

	/**
	 * Creates a table in a database which is compatible with the given schema.
	 * 
	 * @param tableName the name of the table to be created.
	 * @param rsmd the schema of the new table.
	 * @param con the connection to the database.
	 * @param sqlLog a print stream to which SQL query strings are written.
	 * @param typeMap a map which maps the type names to type names which are
	 *        understood by a specific database system.
	 * @return <code>true</code> iff the table was created successfully.
	 */
	public static boolean createTable(String tableName, ResultSetMetaData rsmd, Connection con, PrintStream sqlLog, Map<String, String> typeMap) {
		Statement stmt = null;
		try {
			String createTableQuery = getCreateTableQuery(tableName, rsmd, typeMap);
			sqlLog.println(createTableQuery);
			
			stmt = con.createStatement();
			stmt.execute(createTableQuery);
			stmt.close();
			return true;
		}
		catch (SQLException sqle) {
			if (stmt != null) {
				try {
					stmt.close();
				}
				catch (SQLException e) {
					// just ignore SQL exceptions when trying to close the statement
				}
			}
			return false;
		}
	}

	/**
	 * Creates a table in a database which is compatible with the given schema.
	 * The standard type names are used.
	 * 
	 * @param tableName the name of the table to be created.
	 * @param rsmd the schema of the new table.
	 * @param con the connection to the database.
	 * @param sqlLog a print stream to which SQL query strings are written.
	 * @return <code>true</code> iff the table was created successfully.
	 */
	public static boolean createTable(String tableName, ResultSetMetaData rsmd, Connection con, PrintStream sqlLog) {
		return createTable(tableName, rsmd, con, sqlLog, new HashMap<String, String>());
	}

	/**
	 * Inserts the tuples of a given metadata cursor into a table of a
	 * database. For each tuple, a new insert query is created. This method
	 * does not use <i>prepared statements</i>.
	 * 
	 * @param tableName the name of the table where the tuples are inserted.
	 * @param mdc the metadata cursor which contains the tuples to be inserted.
	 * @param con the connection to the database.
	 * @param sqlLog a print stream to which SQL query strings are written.
	 * @return the number of tuples inserted into the database.
	 * @throws SQLException if a database access error occurs.
	 */
	public static int insertIntoTable(String tableName, MetaDataCursor<? extends Tuple, ?> mdc, Connection con, PrintStream sqlLog) throws SQLException {
		Statement stmt = null;
		
		int count=0;
		try {
			Cursor<String> sqlStrings = new Mapper<Tuple, String>(
				getMapFunctionTupleToInsertQuery(tableName),
				mdc
			);
			// Cursors.println(sqlStrings);
			stmt = con.createStatement();

			while (sqlStrings.hasNext()) {
				String query = sqlStrings.next();
				sqlLog.println(query);
				stmt.execute(query);
				count++;
			}
			stmt.close();
			
			return count;
		}
		catch (SQLException sqle) {
			if (stmt != null)
				stmt.close();
			throw sqle;
		}
	}

	/**
	 * Writes the metadata cursor to a print stream. Each tuple is separated
	 * with a given String (use "\t" for tab separation). To be compatible with
	 * <i>GnuPlot</i>, use for example:
	 * <code>
	 *   writeToPrintStream(mdc, new PrintStream(new FileOutputStream("test.plt")), false, "\t"); 
	 * </code>
	 * 
	 * @param mdc the metadata cursor which is processed.
	 * @param ps a print stream where the output is sent.
	 * @param writeHeadline when it is specified by <code>true</code> a line
	 *        with the column names will be written at first.
	 * @param separator the separator which separates a column of a tuple from
	 *        the next column (also used for the column names if
	 *        <code>writeHeadline</code> is true).
	 * @throws SQLException if a database access error occurs.
	 */
	public static void writeToPrintStream(MetaDataCursor<? extends Tuple, ? extends CompositeMetaData<? super String, ? super ResultSetMetaData>> mdc, PrintStream ps, boolean writeHeadline, String separator) throws SQLException {
		ResultSetMetaData rsmd = ResultSetMetaDatas.getResultSetMetaData(mdc);
		int columnCount = rsmd.getColumnCount();
		
		if (writeHeadline) {
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1)
					ps.print(separator);
				ps.print(rsmd.getColumnName(i));
			}
			ps.println();
		}
			
		Tuple tuple;
		while (mdc.hasNext()) {
			tuple = mdc.next();
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1)
					ps.print(separator);
				ps.print(tuple.getObject(i));
			}
			ps.println();
		}
	}
	
}
