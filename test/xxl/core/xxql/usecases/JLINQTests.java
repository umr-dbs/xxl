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

package xxl.core.xxql.usecases;


import static xxl.core.xxql.AdvPredicate.*;
import static xxl.core.xxql.AggregateUtils.*;
import static xxl.core.xxql.columns.ColumnUtils.*;
import static xxl.core.xxql.columns.Column.SubQueryType.*;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import xxl.core.xxql.AdvTupleCursor;
import xxl.core.xxql.AdvTupleCursor.CachingStrategy;
import xxl.core.xxql.AdvTupleCursor.JOIN_TYPE;

@SuppressWarnings("unused")
public class JLINQTests {
	
	private static Connection connection;
	private final static String DATABASE_PATH = "src/xxlinq/tests/DB";
	private final static String TABLE_VENDORS = "Vendors";
	private final static String TABLE_SALES = "Sales";
	private final static String TABLE_ITEMS = "Items";
	
	private static AdvTupleCursor getCursor1(){
		List<String> l1 = Arrays.asList("d", "c", "b", "a", "a");
		AdvTupleCursor atc1 = new AdvTupleCursor(l1, "liste1", null);
		return atc1;
	}
	private static AdvTupleCursor getCursor2(){
		List<String> l1 = Arrays.asList("c", "c", "d", "e", "e");
		AdvTupleCursor atc1 = new AdvTupleCursor(l1, "liste2", null);
		return atc1;
	}
	private static AdvTupleCursor getCursor3(){
		List<Integer> l1 = Arrays.asList(12, 4, 19, 18, 5);
		AdvTupleCursor atc1 = new AdvTupleCursor(l1, "liste3", null);
		return atc1;
	}
	private static AdvTupleCursor getCursor4(){
		List<Integer> l1 = Arrays.asList(42, 15, 7, 10, 9);
		AdvTupleCursor atc1 = new AdvTupleCursor(l1, "liste4", null);
		return atc1;
	}
	
	private static Connection getConnection(){
		if(connection == null){
			try {
				Class.forName("org.relique.jdbc.csv.CsvDriver");
				connection = DriverManager.getConnection("jdbc:relique:csv:" + DATABASE_PATH);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	      return connection;
	}
	private static AdvTupleCursor getVendors(String alias){
		Statement stmt;
		ResultSet results = null;
		try {
			stmt = getConnection().createStatement();
			results = stmt.executeQuery("SELECT * FROM " + TABLE_VENDORS);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		AdvTupleCursor atc1 = new AdvTupleCursor(results, TABLE_VENDORS, null);	
		atc1.setCachingStrategy(CachingStrategy.ONLY_FIRST, false);
		return atc1.select(alias, 
								colSTATICCALL("ID", Integer.class, "valueOf", col(1)),
								col(2),
								col(3));
	}
	
	private static AdvTupleCursor getItems(){
		Statement stmt;
		ResultSet results = null;
		try {
			stmt = getConnection().createStatement();
			results = stmt.executeQuery("SELECT * FROM " + TABLE_ITEMS);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		AdvTupleCursor atc1 = new AdvTupleCursor(results, TABLE_ITEMS, null);	
		atc1.setCachingStrategy(CachingStrategy.ONLY_FIRST, false);
		atc1.select(colCAST(col("ID","blub"), Integer.class), col(2), colCAST(col(3), JLINQTests.class));
		return atc1.select(colSTATICCALL("ID", Integer.class, "valueOf", col(1)), col(2), colSTATICCALL("Price", Double.class, "valueOf", col(3)));
	}
	
	private static AdvTupleCursor getSales(String alias){
		Statement stmt;
		ResultSet results = null;
		try {
			stmt = getConnection().createStatement();
			results = stmt.executeQuery("SELECT * FROM " + TABLE_SALES);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		AdvTupleCursor atc1 = new AdvTupleCursor(results, TABLE_SALES, null);	
		atc1.setCachingStrategy(CachingStrategy.ONLY_FIRST, false);
		return atc1.select(alias,
								colSTATICCALL("USER_ID", 	Integer.class, 	"valueOf", col(1)),
								colSTATICCALL("ITEM_ID", 	Integer.class, 	"valueOf", col(2)),
								colSTATICCALL("YEAR", 		Integer.class, 	"valueOf", col(3)),
								colSTATICCALL("MONTH", 		Integer.class, 	"valueOf", col(4)),
								colSTATICCALL("DAY", 		Integer.class, 	"valueOf", col(5)),
								colSTATICCALL("AMOUNT", 	Integer.class, 	"valueOf", col(6)),
								colSTATICCALL("SALE_PRICE", 		Double.class, 	"valueOf", col(7)));
	}
	private static void printCursor(AdvTupleCursor atc){
		System.out.println(atc.getResultSetMetaData().getAlias());
		System.out.println(atc.getResultSetMetaData());
		while (atc.hasNext()) {
			System.out.println((atc.next()));
		}
		System.out.println();
		atc.reset();
	}

	public static void main(String[] args){
//		//Operators
		testSimpleSelect();
		testSimpleWhere();
		testSimpleJoin();
		testSimpleGroupBy();
		testSimpleSort();
		testSimpleUion();
		testSimpleIntersect();
		testSimpleDifference();
		testSimpleTop();
		
//		//Columns
//		testSimpleIndexColumn();
//		testSimpleIndexRenameColumn();
//		testSimpleNameColumn();
//		testSimpleNameRenameColumn();
//		testSimpleValColumn();
//		testSimpleIfThenElse();
		
//		// Reflection Columns
//		testSimpleReflectionConstructor();
//		testSimpleReflectionStatic();
//		testSimpleReflectionObject();
//		testSimpleCast();
		
//		testSimpleExists();
//		testSimpleAll();
//		// ausgiebigere tests
////		test1();
//		test2();
////		test3();
		
		
	}
	
//	private static void test3() {
//		AdvTupleCursor atc1 = getCursor3();
//		AdvTupleCursor atc2 = atc1.groupBy(PROJ(colNEW(java.awt.Point.class, col(1),col(1))), AGGR(COUNT("anzahl")));
//		printCursor(atc2);
//	}
//	private static void test1() {
//		AdvTupleCursor vendors = getVendors("Vendors");
//		//printCursor(vendors);
//		AdvTupleCursor items = getItems();
//		//printCursor(items);
//		AdvTupleCursor sales = getSales("Sales");
//		//printCursor(sales);
//		
//		AdvTupleCursor result = 
//			sales.join(vendors, col("Sales.USER_ID").EQ(col("Vendors.ID")))
//					.join("allJoins", items,col("Sales.ITEM_ID").EQ(col("Items.ID")))
//					.groupBy(PROJ(col("NAME"),col("FORENAME"),col("YEAR"),col("MONTH")), 
//							AGGR(SUM(col("AMOUNT").MUL(col("SALE_PRICE")),"summe")))
//					.orderBy(col("NAME"),col("FORENAME"),col("YEAR"),col("MONTH"))
//					;
//		
//		printCursor(result);
//	}
//	private static void test2() {
//		//AdvTupleCursor vendors = getVendors();
//		//printCursor(vendors);
//		AdvTupleCursor items = getItems();
//		//printCursor(items);
//		//AdvTupleCursor sales = getSales();
//		//printCursor(sales);
//		
//		AdvTupleCursor result = 
//			getVendors("Vendors")
//			.join(
//					getSales("Monat1").where((col("YEAR").EQ(val(2010))).AND((col("MONTH")).EQ(val(1))))
//						.groupBy(PROJ(col("USER_ID")), AGGR(SUM(col("AMOUNT").MUL(col("SALE_PRICE")),"Umsatz_Januar"))), 
//					col("Vendors.ID").EQ(col("Monat1.USER_ID")),
//					JOIN_TYPE.LEFT_OUTER_JOIN)
//					
//			.join(
//					getSales("Monat2").where((col("YEAR").EQ(val(2010))).AND((col("MONTH")).EQ(val(2))))
//						.groupBy(PROJ(col("USER_ID")), AGGR(SUM(col("AMOUNT").MUL(col("SALE_PRICE")),"Umsatz_Februar"))), 
//					col("Vendors.ID").EQ(col("Monat2.USER_ID")),
//					JOIN_TYPE.LEFT_OUTER_JOIN)
//					
//			.join(
//					getSales("Monat3").where((col("YEAR").EQ(val(2010))).AND((col("MONTH")).EQ(val(3))))
//						.groupBy(PROJ(col("USER_ID")), AGGR(SUM(col("AMOUNT").MUL(col("SALE_PRICE")),"Umsatz_März"))), 
//					col("Vendors.ID").EQ(col("Monat3.USER_ID")),
//					JOIN_TYPE.LEFT_OUTER_JOIN)
//			
//			.select(col("NAME"),col("FORENAME"),col("Umsatz_Januar"),col("Umsatz_Februar"),col("Umsatz_März"))
//					
//			;
//		
//		printCursor(result);
//		
//	}
	private static void testSimpleCast() {
		System.out.println("\n\n*** Simpler Casting Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor3();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col("value"), colCAST(col("value", "Spalte2"), Object.class));
		System.out.println("XXLINQ: atc1.select(col(\"value\"), colCAST(col(\"value\", \"Spalte2\"), Object.class));");
		System.out.println("SQL*:  SELECT value, cast(Object, value) as Spalte2 FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleReflectionObject() {
		System.out.println("\n\n*** Simpler Referenzaufruf Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor3();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col("value"), colOBJCALL(col("value"), "equals", val(18)));
		System.out.println("XXLINQ: atc1.select(col(\"value\"), colOBJCALL(col(\"value\"), \"equals\", val(18)));");
		System.out.println("SQL*:  SELECT value, value.equals(18) FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleReflectionStatic() {
		System.out.println("\n\n*** Simpler statischer Aufruf Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor3();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col("value"), colSTATICCALL(java.lang.Integer.class, "toBinaryString", col("value")));
		System.out.println("XXLINQ: atc1.select(col(\"value\"), colSTATICCALL(java.lang.Integer.class, \"toBinaryString\", col(\"value\")));");
		System.out.println("SQL*:  SELECT value, Integer.toBinaryString(value) FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleReflectionConstructor() {
		System.out.println("\n\n*** Simpler Neues Objekt Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor3();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(colNEW(java.awt.Point.class, col(1), col(1)));
		System.out.println("XXLINQ: atc1.select(colNEW(java.awt.Point.class, col(1), col(1)));");
		System.out.println("SQL*:  SELECT new Point(COLUMN_1, COLUMN_1) FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleIfThenElse() {
		System.out.println("\n\n*** Simpler If-Then-Else Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor3();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col("value"), IfThenElse("Grösser_10_?", col("value").GEQ(val(10)), val("ja"), val("nein")));
		System.out.println("XXLINQ: atc1.select(col(\"value\"));");
		System.out.println("SQL:   SELECT value, IF value >= 10 THEN 'ja' ELSE 'nein' as Grösser_10_? FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleNameColumn() {
		System.out.println("\n\n*** Simpler NAME Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col("value"));
		System.out.println("XXLINQ: atc1.select(col(\"value\"));");
		System.out.println("SQL*:  SELECT value FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	
	
	private static void testSimpleNameRenameColumn() {
		System.out.println("\n\n*** Simpler NAME as NEUER_NAME Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col("value", "neuerSpaltenName"));
		System.out.println("XXLINQ: atc1.select(col(\"value\", \"neuerSpaltenName\"));");
		System.out.println("SQL*:  SELECT COLUMN_1 FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleIndexRenameColumn() {
		System.out.println("\n\n*** Simpler COLUMN_1 as NEUER_NAME Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col(1, "neuerSpaltenName"));
		System.out.println("XXLINQ: atc1.select(col(1, \"neuerSpaltenName\"));");
		System.out.println("SQL*:  SELECT COLUMN_1 as neuerSpaltenName FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleIndexColumn() {
		System.out.println("\n\n*** Simpler COLUMN_1 Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col(1));
		System.out.println("XXLINQ: atc1.select(col(1));");
		System.out.println("SQL*:  SELECT COLUMN_1 FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleValColumn() {
		System.out.println("\n\n*** Simpler value(Konstante) Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select(col(1), val("Hallo Welt!", "Spalte2"));
		System.out.println("XXLINQ: atc1.select(col(1), val(\"Hallo Welt!\", \"Spalte2\"));");
		System.out.println("SQL:   SELECT COLUMN_1, 'Hallo Welt! as Spalte1 FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleSelect() {
		System.out.println("\n\n*** Simpler SELECT Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.select("Tabelle1", col("value","Spalte1"));
		System.out.println("XXLINQ: atc1.select(\"Tabelle1\", col(\"value\",\"Spalte1\"));");
		System.out.println("SQL:   SELECT value as Spalte1 FROM atc1 Tabelle1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	
	private static void testSimpleWhere() {
		System.out.println("\n\n*** Simpler WHERE Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.where(col("value").EQ(val("a")));
		System.out.println("XXLINQ: atc1.where(col(\"value\").EQ(val(\"a\")));");
		System.out.println("SQL:   SELECT * FROM atc1 WHERE value='a'");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	
	private static void testSimpleJoin() {
		System.out.println("\n\n*** Simpler JOIN Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		AdvTupleCursor atc2 = getCursor2();
		printCursor(atc2);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.join("Tabelle1",atc2, col("liste1.value").EQ(col("liste2.value")));
		System.out.println("XXLINQ: atc1.join(atc2,col(\"value\").EQ(col(\"liste2.value\")), \"Tabelle1\");");
		System.out.println("SQL:   SELECT * FROM (\n" +
				           "            SELECT * FROM liste1 JOIN liste2 ON liste1.value = liste2.value\n" +
				           "       ) Tabelle1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	
	private static void testSimpleGroupBy() {
		System.out.println("\n\n*** Simpler GROUP BY Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.groupBy("Tabelle1", PROJ(col("liste1.value")),AGGR(COUNT("Anzahl")));
		System.out.println("XXLINQ: atc1.groupBy(\"Tabelle1\", PROJ(col(\"liste1.value\")),AGGR(COUNT(\"Anzahl\")));");
		System.out.println("SQL:   SELECT * FROM(\n" +
				           "            SELECT liste1.value, count() as Anzahl FROM atc1 liste1\n" +
				           "       ) Tabelle1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleSort() {
		System.out.println("\n\n*** Simpler SORT Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.orderBy(true, col("liste1.value"));
		System.out.println("XXLINQ: atc1.sort(true, col(\"liste1.value\"));");
		System.out.println("SQL:   SELECT * FROM atc1 liste1 ORDER BY liste1.value ASC");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleUion() {
		System.out.println("\n\n*** Simpler UNION Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		AdvTupleCursor atc2 = getCursor2();
		printCursor(atc1);
		printCursor(atc2);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.union(atc2);
		System.out.println("XXLINQ: atc1.union(atc2);");
		System.out.println("SQL:   SELECT * FROM atc1\n" +
						   "       UNION\n" +
						   "       SELECT * FROM atc2\n");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
		
	}
	private static void testSimpleIntersect() {
		System.out.println("\n\n*** Simpler INTERSECT Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		AdvTupleCursor atc2 = getCursor2();
		printCursor(atc1);
		printCursor(atc2);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.intersect(atc2);
		System.out.println("XXLINQ: atc1.intersect(atc2);");
		System.out.println("SQL:   SELECT * FROM atc1\n" +
						   "       INTERSECT\n" +
						   "       SELECT * FROM atc2\n");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleDifference() {
		System.out.println("\n\n*** Simpler DIFFERENCE Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		AdvTupleCursor atc2 = getCursor2();
		printCursor(atc1);
		printCursor(atc2);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.difference(atc2);
		System.out.println("XXLINQ: atc1.difference(atc2);");
		System.out.println("SQL:   SELECT * FROM atc1\n" +
						   "       DIFFERENCE\n" +
						   "       SELECT * FROM atc2\n");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	private static void testSimpleTop() {
		System.out.println("\n\n*** Simpler TOP Test:");
		System.out.println("EINGABE:");
		AdvTupleCursor atc1 = getCursor1();
		printCursor(atc1);
		System.out.println("-----------------------------------------------");
		AdvTupleCursor result = 
							  atc1.top(3);
		System.out.println("XXLINQ: atc1.top(3);");
		System.out.println("SQL:   SELECT TOP 3 * FROM atc1");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		printCursor(result);
	}
	
	private static void testSimpleExists(){
		AdvTupleCursor atc1 = getCursor1();
		AdvTupleCursor atc2 = getCursor2();
		System.out.println("\n\n*** Simpler EXISTS Test:");
		System.out.println("EINGABEN:");
		printCursor(atc1);
		printCursor(atc2);
		System.out.println("XXLINQ: atc1.where( EXISTS(atc2.where( col(\"liste1.value\").EQ(col(\"liste2.value\")) )) );");
		System.out.println("SQL:   SELECT * FROM atc1 WHERE EXISTS ");
		System.out.println("\t (SELECT * FROM atc2 WHERE atc1.value = atc2.value)");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		
		AdvTupleCursor result = atc1.where( EXISTS(atc2.where( col("liste1.value").EQ(col("liste2.value")) )) );
		printCursor(result);
	}
	
	private static void testSimpleAll(){
		AdvTupleCursor atc1 = getCursor3();
		AdvTupleCursor atc2 = getCursor4();
		System.out.println("\n\n*** Simpler ALL-subquery Test:");
		System.out.println("EINGABEN:");
		printCursor(atc1);
		printCursor(atc2);
		System.out.println("XXLINQ: atc1.where( col(\"liste3.value\").LT(ALL, atc2.select(col(\"value\")) ) );");
		System.out.println("SQL:   SELECT * FROM atc1 WHERE value < ALL (SELECT value FROM atc2");
		System.out.println("-----------------------------------------------");
		System.out.println("AUSGABE:");
		
		AdvTupleCursor result = atc1.where( col("liste3.value").LT(ALL, atc2.select(col("value"))) );
		printCursor(result);
	}
	
}
