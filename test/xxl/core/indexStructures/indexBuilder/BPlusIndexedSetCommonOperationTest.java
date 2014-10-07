package xxl.core.indexStructures.indexBuilder;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import xxl.core.indexStructures.Entry;
import xxl.core.indexStructures.IndexedSet;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;
import xxl.core.relational.tuples.ColumnComparableTuple;

public class BPlusIndexedSetCommonOperationTest {

  final int MAX_ITEMS_TO_INSERT1 = 48;
  final String MY_TABLE_PRIMITIVE_NAME = "TablePrimitives";
  final String MY_TABLE_RELATIONAL_COMPOUND_KEY_NAME = "TableRational2";

  final String MY_TABLE_RELATIONAL_SINGLE_KEY_NAME = "TableRational1";
  IndexedSet primitiveSet;
  IndexedSet relationalSet;
  IndexedSet relationalSetCompoundKey;

  @Test
  public void findObjectInPrimitiveTableDoesNotExists() {
    long find = Long.MAX_VALUE;
    boolean result = primitiveSet.contains(find);
    Assert.assertEquals(result, false);
  }

  @Test
  public void findObjectInPrimitiveTableExists() {
    long find = 10L;
    boolean result = primitiveSet.contains(find);
    Assert.assertEquals(result, true);
  }

  @Test
  public void findObjectInRelationalTableDoesNotExists() {
    Entry.WithKey toElement = new Entry.WithKey(4, 0);
    boolean result = relationalSet.contains(toElement);
    Assert.assertEquals(result, false);
  }

  @Test
  public void findObjectInRelationalTableExists() {
    Entry.WithKey toElement = new Entry.WithKey(4, 4);
    boolean result = relationalSet.contains(toElement);
    Assert.assertEquals(result, true);
  }

  @Test
  public void insertExistingDataIntoPrimitiveTable() {
    boolean result = primitiveSet.add(0L);
    /*
     * Entry already exists, so result should be false
     */
    Assert.assertEquals(result, false);
  }

  @Test
  public void insertExistingDataIntoRelationalTable() {
    boolean result = relationalSet.add(new Entry(0, "String"));
    /*
     * Key already exists, so result should be false
     */
    Assert.assertEquals(result, false);
  }

  @Test
  public void insertExistingDataIntoRelationalTableWhereTupleDimensionIsTooLarge() {
    boolean result =
        primitiveSet.add(new Entry(Integer.MAX_VALUE, 0, "String",
            Integer.MAX_VALUE));
    /*
     * Entry already exists, so result should be false
     */
    Assert.assertEquals(result, false);
  }

  @Test
  public void insertExistingDataIntoRelationalWithCompoundKeyTable() {
    boolean result =
        relationalSetCompoundKey.add(new Entry(0, 0, String.valueOf(System
            .currentTimeMillis())));
    /*
     * Key already exists, so result should be false
     */
    Assert.assertEquals(result, false);
  }

  @Test
  public void IsRelationalTableEmptyIsEmpty1() throws SQLException {
    IndexedSet set =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    Assert.assertEquals(set.isEmpty(), true);
    Assert.assertEquals(set.size(), 0);
    Assert.assertEquals(set.sizeBigInteger(), BigInteger.ZERO);
  }

  @Test
  public void IsRelationalTableEmptyIsEmpty2() throws SQLException {
    IndexedSet set =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    List<Entry> collection = new ArrayList<>();
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; ++i) {
      collection.add(new Entry(i, i));
    }
    set.addAll(collection);
    set.removeAll(collection);
    Assert.assertEquals(set.isEmpty(), true);
    Assert.assertEquals(set.size(), 0);
    Assert.assertEquals(set.sizeBigInteger(), BigInteger.ZERO);
  }

  @Test
  public void IsRelationalTableEmptyIsNotEmpty1() throws SQLException {
    IndexedSet set =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    set.add(new Entry(0, 0));
    System.err.println(set.isEmpty());
    Assert.assertEquals(set.isEmpty(), false);
//    Assert.assertNotEquals(set.size(), 0);
//    Assert.assertNotEquals(set.sizeBigInteger(), BigInteger.ZERO);
  }

  @Test
  public void IsRelationalTableEmptyIsNotEmpty2() throws SQLException {
    IndexedSet set =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    List<Entry> collection = new ArrayList<>();
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; ++i) {
      collection.add(new Entry(i, i));
    }
    set.addAll(collection);
    Assert.assertEquals(set.isEmpty(), false);
//    Assert.assertNotEquals(set.size(), 0);
//    Assert.assertNotEquals(set.sizeBigInteger(), BigInteger.ZERO);
  }

  @BeforeTest
  public void prepareSet() throws SQLException {
    primitiveSet =
        Builders.createBPlusTree.Long(MY_TABLE_PRIMITIVE_NAME).getBuilder()
            .create();
    relationalSet =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    relationalSetCompoundKey =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_COMPOUND_KEY_NAME)
                    .addInteger("FK1").addInteger("FK2").addVarChar("Info", 50))
            .setCompoundKey(2, 1).getBuilder().create();

    for (long i = 0; i < MAX_ITEMS_TO_INSERT1; ++i) {
      boolean result = primitiveSet.add(i);
      Assert.assertEquals(result, true);
    }

    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; ++i) {
      boolean result = relationalSet.add(new Entry(i, i));
      Assert.assertEquals(result, true);
    }

    for (int fk1 = 0; fk1 < MAX_ITEMS_TO_INSERT1 / 2; ++fk1)
      for (int fk2 = 0; fk2 < MAX_ITEMS_TO_INSERT1 / 2; ++fk2) {
        boolean result =
            relationalSetCompoundKey.add(new Entry(fk1, fk2, String
                .valueOf(System.currentTimeMillis())));
        Assert.assertEquals(result, true);
      }

  }

  @Test
  public void relationalSetAddAll() throws SQLException {
    IndexedSet set =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    List<ColumnComparableTuple> collection = new ArrayList<>();
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; ++i) {
      collection.add(new ColumnComparableArrayTuple(i, i));
    }
    set.addAll(collection);
    Object[] setcontent = set.toArray();
    for (int i = 0; i < collection.size(); ++i)
      Assert.assertEquals(collection.get(i).toArray(), setcontent[i]);
  }

  @Test
  public void relationalSetClear() throws SQLException {
    IndexedSet set =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                    .addInteger("ID").addInteger("INT")).getBuilder().create();
    List<Entry> collection = new ArrayList<>();
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; ++i) {
      collection.add(new Entry(i, i));
    }
    set.clear();
    Assert.assertEquals(set.isEmpty(), true);
    Assert.assertEquals(set.size(), 0);
    Assert.assertEquals(set.sizeBigInteger(), BigInteger.ZERO);
  }

  @Test
  public void relationalSetContains() {

  }

  @Test
  public void relationalSetContainsAll() {

  }

  // @Test
  // public void queryLeftOutSideRightOutSidePrimitiveTable() {
  // long fromElement = -10L;
  // long toElement = 2*MAX_ITEMS_TO_INSERT1;
  // Cursor cursor = primitiveSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Assert.assertEquals(cursor.next(), 0L);
  //
  // List<Long> items = new ArrayList<>();
  // items.add(0L); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add((Long) cursor.next());
  //
  // Assert.assertEquals((Long)(items.get(items.size()-1))-MAX_ITEMS_TO_INSERT1+1, 0);
  // }
  //
  // @Test
  // public void queryLeftInSideRightOutSidePrimitiveTable() {
  // long fromElement = 0;
  // long toElement = MAX_ITEMS_TO_INSERT1*2;
  // Cursor cursor = primitiveSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Assert.assertEquals(cursor.next(), 0L);
  //
  // List<Long> items = new ArrayList<>();
  // items.add(0L); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add((Long) cursor.next());
  //
  // Assert.assertEquals((Long)(items.get(items.size()-1))-MAX_ITEMS_TO_INSERT1+1, 0);
  // }
  //
  // @Test
  // public void queryLeftOutSideRightInSidePrimitiveTable() {
  // long fromElement = -10L;
  // long toElement = 10L;
  // Cursor cursor = primitiveSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Assert.assertEquals(cursor.next(), 0L);
  //
  // List<Long> items = new ArrayList<>();
  // items.add(0L); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add((Long) cursor.next());
  //
  // Assert.assertEquals((Long)(items.get(items.size()-1))-toElement, 0);
  // }
  //
  // @Test
  // public void queryLeftInSideRightInSidePrimitiveTable() {
  // long fromElement = 5L;
  // long toElement = MAX_ITEMS_TO_INSERT1-5;
  // Cursor cursor = primitiveSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Assert.assertEquals(cursor.next(), fromElement);
  //
  // List<Long> items = new ArrayList<>();
  // items.add(fromElement); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add((Long) cursor.next());
  //
  // Assert.assertEquals((Long)(items.get(items.size()-1))-toElement, 0);
  // }
  //
  /*
   * 
   */

  // @Test
  // public void queryLeftInSideRightInSideRelationalTable() {
  // Entry.WithKey fromElement = new Entry.WithKey(0);
  // Entry.WithKey toElement = new Entry.WithKey(MAX_ITEMS_TO_INSERT1-5);
  //
  // Cursor cursor = relationalSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Assert.assertEquals(cursor.next(), new ArrayTuple(0,0));
  //
  // List<Tuple> items = new ArrayList<>();
  // items.add(new ArrayTuple(0,0)); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add((Tuple) cursor.next());
  //
  // for(int rows = 0; rows < items.size(); ++rows) {
  // Object[] queryRow = items.get(rows).toArray();
  // Object[] originalRow = (Object[]) relationalSet.toArray()[rows];
  // for(int cols = 0; cols < originalRow.length; ++cols) {
  // Assert.assertEquals(queryRow[cols], originalRow[cols]);
  // }
  // }
  // }

  @Test
  public void relationalSetRemove() {

  }

  @Test
  public void relationalSetRemoveAll() {

  }


  // @Test
  // public void queryLeftOutSideRightOutSideRelationalTable() {
  // Entry.WithKey fromElement = new Entry.WithKey(-MAX_ITEMS_TO_INSERT1);
  // Entry.WithKey toElement = new Entry.WithKey(MAX_ITEMS_TO_INSERT1*2);
  // Cursor cursor = relationalSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Tuple t = (Tuple) cursor.next();
  // Assert.assertEquals(t.getInt(1), 0);
  // Assert.assertEquals(t.getInt(2), 0);
  //
  // List<Object> items = new ArrayList<>();
  // items.add(t.toArray()); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add(((Tuple)cursor.next()).toArray());
  //
  // Assert.assertEquals(items.toArray(), relationalSet.toArray());
  //
  // }
  //
  // @Test
  // public void queryLeftInSideRightOutSideRelationalTable() {
  // Entry.WithKey fromElement = new Entry.WithKey(0);
  // Entry.WithKey toElement = new Entry.WithKey(MAX_ITEMS_TO_INSERT1*2);
  // Cursor cursor = relationalSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Tuple t = (Tuple) cursor.next();
  // Assert.assertEquals(t.getInt(1), 0);
  // Assert.assertEquals(t.getInt(2), 0);
  //
  // List<Object> items = new ArrayList<>();
  // items.add(t.toArray()); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add(((Tuple)cursor.next()).toArray());
  //
  // Assert.assertEquals(items.toArray(), relationalSet.toArray());
  // }
  //
  // @Test
  // public void queryLeftOutSideRightInSideRelationalTable() {
  // Entry.WithKey fromElement = new Entry.WithKey(-MAX_ITEMS_TO_INSERT1);
  // Entry.WithKey toElement = new Entry.WithKey(MAX_ITEMS_TO_INSERT1-1);
  // Cursor cursor = relationalSet.findBetween(fromElement, toElement);
  // Assert.assertEquals(cursor.hasNext(), true);
  // Tuple t = (Tuple) cursor.next();
  // Assert.assertEquals(t.getInt(1), 0);
  // Assert.assertEquals(t.getInt(2), 0);
  //
  // List<Object> items = new ArrayList<>();
  // items.add(t.toArray()); // was removed for assert first element
  // while(cursor.hasNext())
  // items.add(((Tuple)cursor.next()).toArray());
  //
  // Assert.assertEquals(items.toArray(), relationalSet.toArray());
  // }

  @Test
  public void relationalSetRetainAll() {

  }

  @Test
  public void relationalSetSize() {

  }

  @Test
  public void removeDataFromPrimitiveTableFails() {
    long object = 20L;
    primitiveSet.remove(object);
    // Test if the removed object is not there anymore
    boolean result = primitiveSet.remove(object);
    primitiveSet.add(object);
    /*
     * Entry is inside dataset. The result should be true
     */
    Assert.assertEquals(result, false);
  }

  @Test
  public void removeDataFromPrimitiveTableOkay() {
    long object = 20L;
    boolean result = primitiveSet.remove(object);
    primitiveSet.add(object);
    /*
     * Entry is inside dataset. The result should be true
     */
    Assert.assertEquals(result, true);
  }


  @Test
  public void removeDataFromRelationalTableRemoveOverEntityFails() {
    Entry object = new Entry(0, 0);
    relationalSet.remove(object);
    // Test if the removed object is not there anymore
    boolean result = relationalSet.remove(object);
    relationalSet.add(object);
    /*
     * Entry is inside dataset. The result should be true
     */
    Assert.assertEquals(result, false);
  }

  @Test
  public void removeDataFromRelationalTableRemoveOverEntityOkay() {
    Entry object = new Entry(0, 0);
    boolean result = relationalSet.remove(object);
    relationalSet.add(object);
    /*
     * Entry is inside dataset. The result should be true
     */
    Assert.assertEquals(result, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void removeDataFromRelationalTableRemoveOverEntityWhichIsTooShort() {
    Entry object = new Entry(0);
    relationalSet.remove(object);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void removeDataFromRelationalTableRemoveOverKeyOkay() {
    Entry.WithKey object = new Entry.WithKey(0);
    relationalSet.remove(object);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCompoundKeyDouble() throws SQLException {
    Builders.createBPlusTree
        .Tuples(
            Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                .addInteger("ID").addInteger("INT")).setCompoundKey(2, 2)
        .getBuilder().create();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCompoundKeyNegative() throws SQLException {
    Builders.createBPlusTree
        .Tuples(
            Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                .addInteger("ID").addInteger("INT")).setCompoundKey(-3)
        .getBuilder().create();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCompoundKeyNull() throws SQLException {
    Builders.createBPlusTree
        .Tuples(
            Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                .addInteger("ID").addInteger("INT")).setCompoundKey()
        .getBuilder().create();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCompoundKeyOutOfRange() throws SQLException {
    Builders.createBPlusTree
        .Tuples(
            Schemas.createSchema(MY_TABLE_RELATIONAL_SINGLE_KEY_NAME)
                .addInteger("ID").addInteger("INT")).setCompoundKey(3)
        .getBuilder().create();
  }



  // TODO: "Reflection-Test"

  public String toString() {
    return "BPlusIndexedSet Common Operation Test";
  }
}
