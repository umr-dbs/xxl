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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;

import xxl.core.collections.bags.Bag;
import xxl.core.collections.bags.ListBag;
import xxl.core.collections.sweepAreas.ListSAImplementor;
import xxl.core.comparators.ComparableComparator;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;
import xxl.core.functions.AbstractFunction;
import xxl.core.math.functions.DistinctMetaDataAggregationFunction;
import xxl.core.math.functions.MetaDataAggregationFunction;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.relational.cursors.Aggregator;
import xxl.core.relational.cursors.GroupAggregator;
import xxl.core.relational.cursors.MergeSorter;
import xxl.core.relational.cursors.NestedLoopsDifference;
import xxl.core.relational.cursors.NestedLoopsDistinct;
import xxl.core.relational.cursors.NestedLoopsGrouper;
import xxl.core.relational.cursors.NestedLoopsIntersection;
import xxl.core.relational.cursors.NestedLoopsJoin;
import xxl.core.relational.cursors.Projection;
import xxl.core.relational.cursors.Renaming;
import xxl.core.relational.cursors.Selection;
import xxl.core.relational.cursors.SortBasedDifference;
import xxl.core.relational.cursors.SortBasedDistinct;
import xxl.core.relational.cursors.SortBasedDivision;
import xxl.core.relational.cursors.SortBasedGrouper;
import xxl.core.relational.cursors.SortBasedIntersection;
import xxl.core.relational.cursors.SortMergeJoin;
import xxl.core.relational.cursors.Union;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.resultSets.ResultSets;
import xxl.core.relational.tuples.ArrayTuple;
import xxl.core.relational.tuples.Tuple;
import xxl.core.relational.tuples.Tuples;
import xxl.core.util.WrappingRuntimeException;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * Class used for testing and demonstrating the functionality of
 * XXL's package {@link xxl.core.relational}. <br>
 *
 * For these use cases an installation of the DBMS Cloudscape is necessary and
 * the attribute <tt>databaseLocation</tt> has to be set to the directory
 * Cloudscape provides its demo database (toursDB).
 * So the programm has to be called as follows: <br>
 * <pre>
 * 	java TestRelational %CLOUDSCAPE_INSTALL%\\demo\\databases\\
 * </pre>
 */
public class TestRelational {

	// Cloudscape mode, protocol and driver
	/**The used framework, default value "net" ("embedded for embedded mode)*/
	public static String framework = "net";
	/**The used driver, default value "com.ibm.db2.jcc.DB2Driver" ("org.apache.derby.jdbc.EmbeddedDriver" for embedded mode)*/
	public static String driver = "com.ibm.db2.jcc.DB2Driver";
	/**The used protocol, default value "jdbc:derby:net://dbs2:1527/" ("jdbc:derby:" for embedded mode)*/
	public static String protocol = "jdbc:derby:net://dbs2:1527/";

	// database location and name
	// using %CLOUDSCAPE_INSTALL%\\demo\\databases\\toursDB
	
	/**The database location, default value null */
	public static String dataBaseLocation = null;
	/**The database name, default value "toursDB" */
	public static String dataBaseName = "toursDB";

	/**The first statement.*/
	protected static Statement s;
	/**The second statement.*/
	protected static Statement t;
	/**The first query as a string.*/
	protected static String sqlQuery1;
    /**The second query as a string.*/
	protected static String sqlQuery2;
	/**The first result set.*/
	protected static ResultSet rs1;
	/**The second result set.*/
	protected static ResultSet rs2;

	/**Returns a connection to the given database.
	 * @param dataBaseLocation the database location
	 * @param dataBaseName the database name
	 * @param create indicates if the database should be created.
	 * @param autoCommit if true then the autocommit will be activated.
	 * @return the connection.*/
	protected static Connection getConnection(String dataBaseLocation, String dataBaseName, boolean create, boolean autoCommit) {
		try {
			Class.forName(driver).newInstance();
			System.out.println("Loaded the appropriate driver.");
			Connection conn = create ?
				DriverManager.getConnection(protocol + dataBaseName + ";create=true", "APP", "APP") :
				DriverManager.getConnection(protocol + "\"" + dataBaseLocation + dataBaseName + "\"", "APP", "APP");
			System.out.println("Connected to database.");
			conn.setAutoCommit(autoCommit);
			System.out.println("Autocommit activated: " + autoCommit);
			return conn;
		}
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}
	/**Closes the connection
	 * @param conn the connection. */
	protected static void closeConnection(Connection conn) {
		try {
			conn.commit();
			conn.close();
			System.out.println("Committed transaction and closed connection.");
		}
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**Shuts down the Cloudscape database*/
	protected static void shutDownCloudscape() {
		boolean gotSQLExc = false;
		if (framework.equals("embedded")) {
			try {
				DriverManager.getConnection("jdbc:cloudscape:;shutdown=true");
			} catch (SQLException se) {
				gotSQLExc = true;
			}
			if (!gotSQLExc)
				System.out.println("Database did not shut down normally");
			else
				System.out.println("Database shut down normally");
		}
	}

	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void selection(Statement s) throws SQLException {
		System.out.println("\nSELECTION");

		sqlQuery1 = "SELECT * FROM FLIGHTAVAILABILITY";
		rs1 = s.executeQuery(sqlQuery1);
		ResultSetMetaData rsmd1 = rs1.getMetaData();
		final int column = ResultSetMetaDatas.getColumnIndex(rsmd1, "SEGMENT_NUMBER");

		// SELECT  *
		// FROM    FLIGHTAVAILABILITY
		// WHERE   SEGMENT_NUMBER = 2;

		Selection selection = new Selection(
			rs1,
			ArrayTuple.FACTORY_METHOD,
			new AbstractPredicate<Tuple>() {
				@Override
				public boolean invoke(Tuple tuple) {
					return tuple.getInt(column) == 2;
				}
			}
		);
		
		selection.open();
		printResultSetMetaData(selection);
		printTuples(selection);
		selection.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void projection(Statement s) throws SQLException {
		System.out.println("\nPROJECTION");

		sqlQuery1 = "SELECT * FROM FLIGHTAVAILABILITY";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT  FLIGHT_ID, ECONOMY_SEATS_TAKEN as SEATS
		// FROM    FLIGHTAVAILABILITY;

		Renaming renaming = new Renaming(
			new Projection(
				rs1,
				ArrayTuple.FACTORY_METHOD,
				ResultSets.getColumnIndices(
					rs1,
					"FLIGHT_ID",
					"ECONOMY_SEATS_TAKEN"
				)
			),
			new int[] {2},
			new String[] {"SEATS"}
		);

		renaming.open();
		printResultSetMetaData(renaming);
		printTuples(renaming);
		renaming.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void union(Statement s, Statement t) throws SQLException {
		System.out.println("\nUNION");

		sqlQuery1 = "SELECT * FROM FLIGHTAVAILABILITY WHERE SEGMENT_NUMBER = 2";
		rs1 = s.executeQuery(sqlQuery1);
		sqlQuery2 = "SELECT * FROM FLIGHTAVAILABILITY WHERE SEGMENT_NUMBER <> 2";
		rs2 = t.executeQuery(sqlQuery2);

		// SELECT  *
		// FROM    FLIGHTAVAILABILITY
		// UNION
		// SELECT  *
		// FROM    FLIGHTAVAILABILITY;

		Union union = new Union(rs1, rs2, ArrayTuple.FACTORY_METHOD);

		union.open();
		printResultSetMetaData(union);
		printTuples(union);
		union.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void groupSortBased(Statement s) throws SQLException {
		System.out.println("\nsort-based GROUP");

		sqlQuery1 = "SELECT * FROM FLIGHTS ORDER BY DEST_AIRPORT ASC";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT    *
		// FROM      FLIGHTS
		// GROUP BY  DEST_AIRPORT
		// ORDER BY  DEST_AIRPORT ASC;

		SortBasedGrouper grouper = new SortBasedGrouper(
			rs1,
			ArrayTuple.FACTORY_METHOD,
			ResultSets.getColumnIndices(rs1, new String[]{"DEST_AIRPORT"})
		);

		grouper.open();
		printResultSetMetaData(grouper);
		Cursor<Tuple> cursor;
		while(grouper.hasNext()) {
			System.out.print("new group: ");
			cursor = grouper.next();
			while (cursor.hasNext())
				System.out.print(cursor.next() + " ");
			System.out.println();
		}
		grouper.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void groupNestedLoops(Statement s) throws SQLException {
		System.out.println("\nnested-loops GROUP");

		sqlQuery1 = "SELECT * FROM FLIGHTS";
		rs1 = s.executeQuery(sqlQuery1);
		ResultSetMetaData rsmd1 = rs1.getMetaData();
		final int column = ResultSetMetaDatas.getColumnIndex(rsmd1, "DEST_AIRPORT");

		// SELECT    *
		// FROM      FLIGHTS
		// GROUP BY  DEST_AIRPORT;

		NestedLoopsGrouper grouper = new NestedLoopsGrouper(
			rs1,
			new AbstractFunction<Tuple, Object>() {
				@Override
				public Object invoke(Tuple tuple) {
					return tuple.getObject(column);
				}
			},
			new HashMap<Object, Bag<Tuple>>(),
			4096,
			32,
			8
		);

		grouper.open();
		printResultSetMetaData(grouper);
		Cursor<Tuple> cursor;
		while(grouper.hasNext()) {
			System.out.print("new group: ");
			cursor = grouper.next();
			while (cursor.hasNext())
				System.out.print(cursor.next() + " ");
			System.out.println();
		}
		grouper.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void sort(Statement s) throws SQLException {
		System.out.println("\nSORT");

		sqlQuery1 = "SELECT * FROM FLIGHTS";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT    *
		// FROM      FLIGHTS
		// ORDER BY  DEST_AIRPORT ASC, ARRIVE_TIME;

		MergeSorter sorter = new MergeSorter(rs1,
			ArrayTuple.FACTORY_METHOD,
			ResultSets.getColumnIndices(
				rs1,
				"DEST_AIRPORT",
				"ARRIVE_TIME"
			),
			new boolean[] {
				true,
				false
			}
		);

		sorter.open();
		printResultSetMetaData(sorter);
		printTuples(sorter);
		sorter.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void distinctSortBased(Statement s) throws SQLException {
		System.out.println("\nsort-based DISTINCT");

		sqlQuery1 = "SELECT AIRLINE FROM AIRLINES";
		rs1 = s.executeQuery(sqlQuery1);
		ResultSetMetaData rsmd1 = rs1.getMetaData();
		final int column = ResultSetMetaDatas.getColumnIndex(rsmd1, "AIRLINE");

		// SELECT DISTINCT  AIRLINE
		// FROM      		AIRLINES;

		SortBasedDistinct distinct = new SortBasedDistinct(
			new MergeSorter(
				rs1,
				ArrayTuple.FACTORY_METHOD,
				new int[] {
					column
				},
				new boolean[] {
					true
				}
			),
			new AbstractPredicate<Tuple>() {
				@Override
				public boolean invoke(Tuple last, Tuple next) {
					return last.getString(column).equalsIgnoreCase(next.getString(column));
				}
			}
		);

		distinct.open();
		printResultSetMetaData(distinct);
		printTuples(distinct);
		distinct.close();

		// reset rs1
		rs1 = s.executeQuery(sqlQuery1);

		// early duplicate elimination
		System.out.println("Early duplicate elimination: ");
		distinct = new SortBasedDistinct(
			rs1,
			Tuples.getTupleComparator(column),
			8,
			12*4096,
			4*4096
		);

		distinct.open();
		printResultSetMetaData(distinct);
		printTuples(distinct);
		distinct.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @throws SQLException*/
	public static void distinctNestedLoops(Statement s) throws SQLException {
		System.out.println("\nnested-loops DISTINCT");

		sqlQuery1 = "SELECT AIRLINE FROM AIRLINES";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT DISTINCT  AIRLINE
		// FROM      		AIRLINES;

		NestedLoopsDistinct distinct = new NestedLoopsDistinct(rs1, 12*4096, 24);

		distinct.open();
		printResultSetMetaData(distinct);
		printTuples(distinct);
		distinct.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void differenceSortBased(Statement s, Statement t) throws SQLException {
		System.out.println("\nsort-based DIFFERENCE");

		sqlQuery1 = "SELECT CITY_NAME FROM CITIES ORDER BY CITY_NAME";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery1);

		// SELECT    CITY_NAME
		// FROM      CITIES
		// ORDER BY  CITY_NAME
		// MINUS
		// SELECT    CITY_NAME
		// FROM      CITIES
		// ORDER BY  CITY_NAME;

		SortBasedDifference difference = new SortBasedDifference(
			rs1,
			rs2,
			Tuples.getTupleComparator(
				new int[] {
					1
				},
				new boolean[] {
					true
				}
			),
			false,
			true
		);

		difference.open();
		printResultSetMetaData(difference);
		printTuples(difference);
		difference.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void differenceNestedLoops(Statement s, Statement t) throws SQLException {
		System.out.println("\nnested-loops DIFFERENCE");

		sqlQuery1 = "SELECT CITY_NAME FROM CITIES ORDER BY CITY_NAME";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery1);

		// SELECT    CITY_NAME
		// FROM      CITIES
		// MINUS
		// SELECT    CITY_NAME
		// FROM      CITIES;

		NestedLoopsDifference difference = new NestedLoopsDifference (
			rs1,
			rs2,
			2048,
			8,
			new AbstractFunction<Object, ListBag<Tuple>>() {
				@Override
				public ListBag<Tuple> invoke() {
					return new ListBag<Tuple>();
				}
			},
			false
		);

		difference.open();
		printResultSetMetaData(difference);
		printTuples(difference);
		difference.close();
	}
	
	/**
	 * Implementation of the relational operators.
	 * 
	 * @param s the statement.
	 * @throws SQLException
	 */
	public static void aggregate(Statement s) throws SQLException {
		System.out.println("\nAGGREGATION");

		System.out.println("\nAggregation example 1:");
		sqlQuery1 = "SELECT * FROM FLIGHTS";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT  AIRCRAFT, SUM(MILES) AS TOTAL_MILES, COUNT(FLIGHT_ID) AS NO_OF_FLIGHTS
		// FROM    FLIGHTS;

		// Note: incremental computation of the final aggregation value

		MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> aggregate = new Aggregator(
			rs1,
			new MetaDataAggregationFunction[] {
				Aggregator.SUM,
				Aggregator.COUNT
			},
			ResultSetMetaDatas.getColumnIndices(
				rs1.getMetaData(),
				new String[] {
					"MILES",
					"FLIGHT_ID"
				}
			),
			new String[] {
				"TOTAL_MILES",
				"NO_OF_FLIGHTS"
			},
			ResultSetMetaDatas.getColumnIndices(
				rs1.getMetaData(),
				new String[] {
					"AIRCRAFT"
				}
			),
			ArrayTuple.FACTORY_METHOD
		);

		aggregate.open();
		printResultSetMetaData(aggregate);
		printTuples(aggregate);
		aggregate.close();
		
		System.out.println("\nAggregation example 2:");
		sqlQuery1 = "SELECT * FROM FLIGHTS";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT  COUNT(DISTINCT FLIGHT_ID), AIRCRAFT
		// FROM    FLIGHTS;

		aggregate = new Aggregator(
			rs1,
			new MetaDataAggregationFunction[] {
				new DistinctMetaDataAggregationFunction(Aggregator.COUNT)
			},
			ResultSetMetaDatas.getColumnIndices(
				rs1.getMetaData(),
				new String[] {
					"FLIGHT_ID"
				}
			),
			ResultSetMetaDatas.getColumnIndices(
				rs1.getMetaData(),
				new String[] {
					"AIRCRAFT"
				}
			),
			ArrayTuple.FACTORY_METHOD
		);

		aggregate.open();
		printResultSetMetaData(aggregate);
		printTuples(aggregate);
		aggregate.close();
		
		System.out.println("\nAggregation example 3:");
		sqlQuery1 = "SELECT * FROM FLIGHTS ORDER BY AIRCRAFT";
		rs1 = s.executeQuery(sqlQuery1);

		// SELECT    AIRCRAFT, SUM(MILES) AS MILES_OF_AIRCRAFT
		// FROM      FLIGHTS
		// GROUP BY  AIRCRAFT;

		aggregate = new GroupAggregator(
			new SortBasedGrouper(
				rs1,
				ArrayTuple.FACTORY_METHOD,
				ResultSetMetaDatas.getColumnIndices(
					rs1.getMetaData(),
					new String[] {
						"AIRCRAFT"
					}
				)
			),
			new MetaDataAggregationFunction[] {
				Aggregator.SUM
			},
			ResultSetMetaDatas.getColumnIndices(
				rs1.getMetaData(),
				new String[] {
					"MILES"
				}
			),
			new String[] {
				"MILES_OF_AIRCRAFT"
			},
			ResultSetMetaDatas.getColumnIndices(
				rs1.getMetaData(),
				new String[] {
					"AIRCRAFT"
				}
			),
			ArrayTuple.FACTORY_METHOD
		);

		aggregate.open();
		printResultSetMetaData(aggregate);
		printTuples(aggregate);
		aggregate.close();
	}
	
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void joinNestedLoops(Statement s, final Statement t) throws SQLException {
		System.out.println("\nnested-loops JOIN");

		sqlQuery1 = "SELECT * FROM COUNTRIES";
		sqlQuery2 = "SELECT * FROM CITIES";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery2);

		// SELECT  *
		// FROM    COUNTRIES
		// JOIN    CITIES
		// WHERE   COUNTRIES.COUNTRY_ISO_CODE = CITIES.COUNTRY_ISO_CODE

		NestedLoopsJoin join = new NestedLoopsJoin(
			rs1,
			rs2,
			new AbstractFunction<Object, ResultSet>() {
				@Override
				public ResultSet invoke() {
					try {
						return t.executeQuery(sqlQuery2);
					}
					catch (SQLException se) {
						throw new WrappingRuntimeException(se);
					}
				}
			},
			ArrayTuple.FACTORY_METHOD,
			NestedLoopsJoin.Type.NATURAL_JOIN
		);

		join.open();
		printResultSetMetaData(join);
		printTuples(join);
		join.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void joinSortBased(Statement s, Statement t) throws SQLException {
		System.out.println("\nsort-based JOIN");

		sqlQuery1 = "SELECT * FROM COUNTRIES ORDER BY COUNTRY_ISO_CODE";
		sqlQuery2 = "SELECT * FROM CITIES ORDER BY COUNTRY_ISO_CODE";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery2);
		ResultSetMetaData rsmd1 = rs1.getMetaData();
		final int column1 = ResultSetMetaDatas.getColumnIndex(rsmd1, "COUNTRY_ISO_CODE");
		ResultSetMetaData rsmd2 = rs2.getMetaData();
		final int column2 = ResultSetMetaDatas.getColumnIndex(rsmd2, "COUNTRY_ISO_CODE");

		// SELECT  *
		// FROM    COUNTRIES
		// JOIN    CITIES
		// WHERE   COUNTRIES.COUNTRY_ISO_CODE = CITIES.COUNTRY_ISO_CODE

		SortMergeJoin join = new SortMergeJoin(
			rs1,
			rs2,
			// short version: using PredicateBasedSweepArea
			new AbstractFunction<Object, ListBag<Tuple>>() {
				@Override
				public ListBag<Tuple> invoke() {
					return new ListBag<Tuple>();
				}
			},
			new Comparator<Tuple>() {
				Comparator<String> comp = ComparableComparator.STRING_COMPARATOR;
				
				public int compare(Tuple o1, Tuple o2) {
					return comp.compare(o1.getString(column1), o2.getString(column2)); // comparing join attributes
				}
			},
			ArrayTuple.FACTORY_METHOD,
			SortMergeJoin.Type.NATURAL_JOIN
		);

		join.open();
		printResultSetMetaData(join);
		printTuples(join);
		join.open();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void intersectionSortBased(Statement s, Statement t) throws SQLException {
		System.out.println("\nsort-based INTERSECTION");

		sqlQuery1 = "SELECT CITY_ID FROM CITIES ORDER BY CITY_ID";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery1);

		// SELECT     CITY_ID
		// FROM       CITIES
		// ORDER BY   CITY_ID
		// INTERSECT
		// SELECT     CITY_ID
		// FROM       CITIES
		// ORDER BY   CITY_ID;

		SortBasedIntersection intersection = new SortBasedIntersection(
			rs1,
			rs2,
			new ListSAImplementor<Tuple>(),
			new Comparator<Tuple>() {
				Comparator<Integer> comp = ComparableComparator.INTEGER_COMPARATOR;
				
				public int compare(Tuple o1, Tuple o2) {
					return comp.compare(o1.getInt(1), o2.getInt(1)); // comparing join attributes
				}
			}
		);

		intersection.open();
		printResultSetMetaData(intersection);
		printTuples(intersection);
		intersection.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void intersectionNestedLoops(Statement s, final Statement t) throws SQLException {
		System.out.println("\nnested-loops INTERSECTION");

		sqlQuery1 = "SELECT CITY_ID FROM CITIES";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery1);

		// SELECT     CITY_ID
		// FROM       CITIES
		// INTERSECT
		// SELECT     CITY_ID
		// FROM       CITIES;

		NestedLoopsIntersection intersection = new NestedLoopsIntersection(
			rs1,
			rs2,
			new AbstractFunction<Object, ResultSet>() {
				@Override
				public ResultSet invoke() {
					try {
						return t.executeQuery(sqlQuery1);
					}
					catch (SQLException se) {
						throw new WrappingRuntimeException(se);
					}
				}
			}
		);

		intersection.open();
		printResultSetMetaData(intersection);
		printTuples(intersection);
		intersection.close();
	}
	/**Implementation of the relational operators.
	  * 
	  * @param s the statement.
	  * @param t the statement.
	  * @throws SQLException*/
	public static void divisionSortBased (Statement s, Statement t) throws SQLException {
		System.out.println("\nsort-based DIVISION");

		// Division: Kemper/Eickler: Datenbanksysteme; page 83

		// create new tables H and L
		s.execute("CREATE TABLE H (M CHAR(2), V CHAR(2))");
		s.execute("CREATE TABLE L (V CHAR(2))");
		// inserting tuples
		s.execute("INSERT INTO H(M, V) VALUES ('m1', 'v1')");
		s.execute("INSERT INTO H(M, V) VALUES ('m2', 'v3')");
		s.execute("INSERT INTO H(M, V) VALUES ('m1', 'v2')");
		s.execute("INSERT INTO H(M, V) VALUES ('m1', 'v3')");
		s.execute("INSERT INTO H(M, V) VALUES ('m2', 'v2')");
		s.execute("INSERT INTO L(V) VALUES ('v1')");
		s.execute("INSERT INTO L(V) VALUES ('v2')");

		sqlQuery1 = "SELECT * FROM H ORDER BY M ASC, V ASC";
		sqlQuery2 = "SELECT * FROM L ORDER BY V ASC";
		rs1 = s.executeQuery(sqlQuery1);
		rs2 = t.executeQuery(sqlQuery2);


		SortBasedDivision division = new SortBasedDivision(
			rs1,
			rs2,
			ArrayTuple.FACTORY_METHOD
		);

		division.open();
		printResultSetMetaData(division);
		printTuples(division);
		division.close();
		
		// drop tables H and L
		s.execute("DROP TABLE H");
		s.execute("DROP TABLE L");
	}

	/**
	 * Prints the tuples of the cursor.
	 * 
	 * @param cursor the given cursor.
	 * @throws SQLException
	 */
	protected static void printTuples(MetaDataCursor<? extends Tuple, CompositeMetaData<Object, Object>> cursor) throws SQLException {
		ResultSetMetaData rsmd = ResultSetMetaDatas.getResultSetMetaData(cursor);
		System.out.println("tuples: ");
		while (cursor.hasNext()) {
			Tuple tuple = cursor.next();
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(tuple.getString(i) + "\t");
			System.out.println();
		}
		cursor.close();
		System.out.println();
	}
	
	/**
	 * Prints the metadata of the cursor.
	 * 
	 * @param cursor the given cursor.
	 * @throws SQLException
	 */
	protected static void printResultSetMetaData(MetaDataCursor<?, CompositeMetaData<Object, Object>> cursor) throws SQLException {
		System.out.println("meta data:");
		ResultSetMetaData rsmd = ResultSetMetaDatas.getResultSetMetaData(cursor);
		for (int i=1; i <= rsmd.getColumnCount(); i++) {
			System.out.println("column number: " + i);
			System.out.println("\tcolumn name:         " + rsmd.getColumnName(i));
			System.out.println("\tcolumn type:         " + rsmd.getColumnTypeName(i));
			System.out.println("\tcolumn display size: " + rsmd.getColumnDisplaySize(i));
			System.out.println("\tcolumn precision:    " + rsmd.getPrecision(i));
			System.out.println("\tcolumn scale:        " + rsmd.getScale(i));
			System.out.println("\tcolumn class name:   " + rsmd.getColumnClassName(i));
		}
		System.out.println();
	}

	/**Test the (@link xxxl.core.relational) package with the Cloudscape database.
	  * @param args the arguments*/
	public static void main (String[] args) {
		if (args.length < 1) {
			System.out.println("Please specify the directory Cloudscape provides the demo database 'ToursDB'. Set parameter to: %CLOUDSCAPE_INSTALL%\\demo\\databases\\");
			return;
		}
		dataBaseLocation = args[0];

		System.out.println("TestRelational starting in " + framework + " mode.");

		try {
			Connection conn = getConnection(dataBaseLocation, dataBaseName, false, false);

			s = conn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE
            );
			t = conn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE
            );

			// SELECTION
			selection(s);

			// PROJECTION
			projection(s);

			// UNION
			union(s, t);

			// SORTER
			sort(s);

			// GROUPER
			groupSortBased(s);
			groupNestedLoops(s);

			// DISTINCT
			distinctSortBased(s);
			distinctNestedLoops(s);

			// DIFFERENCE
			differenceSortBased(s, t);
			differenceNestedLoops(s, t);

			// AGGREGATE
			aggregate(s);

			// JOIN
			joinSortBased(s, t);
			joinNestedLoops(s, t);

			// INTERSECTION
			intersectionSortBased(s, t);
			intersectionNestedLoops(s, t);

			// DIVISION
			divisionSortBased(s, t);

			s.close();
			t.close();
			closeConnection(conn);

			shutDownCloudscape();
			System.out.println("TestRelational finished.");

		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
