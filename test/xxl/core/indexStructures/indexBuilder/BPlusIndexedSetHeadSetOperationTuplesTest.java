package xxl.core.indexStructures.indexBuilder;

import java.sql.SQLException;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusIndexedSetView;
import xxl.core.indexStructures.Entry;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.relational.schema.Schemas;
import xxl.core.relational.tuples.ColumnComparableArrayTuple;
import xxl.core.relational.tuples.Tuple;

public class BPlusIndexedSetHeadSetOperationTuplesTest {

  final int MAX_ITEMS_TO_INSERT1 = 100_000;
  /*
   * Set A
   */
  final String MY_TABLE_NAME1 = "MyTable1";
  BPlusIndexedSet mySet1;

  @BeforeMethod
  public void prepareSet() throws SQLException {
    mySet1 =
        Builders.createBPlusTree
            .Tuples(
                Schemas.createSchema(MY_TABLE_NAME1).addInteger("ID")
                    .addVarChar("NAME", 100)).getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; i++)
      mySet1.add(new Entry(i, "Doe"));
  }


  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void subSetHeadSetBoundGreaterMaxContentTest() {
    final int KEY_VALUE = MAX_ITEMS_TO_INSERT1 + 1;
    Entry.WithKey upperBound = new Entry.WithKey(KEY_VALUE);
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
  }

  @Test
  public void subSetHeadSetContentTest() {
    final int KEY_VALUE = MAX_ITEMS_TO_INSERT1 / 2;
    Entry.WithKey upperBound = new Entry.WithKey(KEY_VALUE);

    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView)  mySet1.headSet(upperBound);
    
    Object[] o = subSet.toArray();
    for (int i = 0; i < o.length; i++) {
		Object[] entry = (Object[]) o[i];
		Assert.assertEquals(entry, new Object[] {i, "Doe"});
	}
  }

  @Test
  public void subSetHeadSetIdentityTest() {
    Tuple upperBound = (Tuple) mySet1.last();
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);

    Object[] a = subSet.toArray();
    for (int i = 0; i < a.length; i++) {
		Object[] entry = (Object[]) a[i];
		Assert.assertEquals(entry, new Object[] {i, "Doe"});
	}
  }


  @Test(expectedExceptions = IllegalArgumentException.class)
  public void subSetHeadSetNegativeBoundTest() {
    final int KEY_VALUE = -1;
    Entry.WithKey upperBound = new Entry.WithKey(KEY_VALUE);
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
  }

  @Test
  public void subSetHeadSetValidBoundTest() {
    final int KEY_VALUE = 1000;
    Entry.WithKey upperBound = new Entry.WithKey(KEY_VALUE);
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
    Object[] o = subSet.toArray();

    Assert.assertEquals(((Object[]) (o[0]))[0], 0);
    Assert.assertEquals(((Object[]) (o[0]))[1], "Doe");

    Assert.assertEquals(((Object[]) (o[o.length - 1]))[0], KEY_VALUE - 1);
    Assert.assertEquals(((Object[]) (o[o.length - 1]))[1], "Doe");
  }

  // @Test
  // public void subSetHeadSetEmptyTest() {
  // Cursor fst = mySet1.findBetween((Comparable)mySet1.first(), (Comparable)mySet1.last());
  // fst.next();
  // Cursor snd = mySet1.findBetween(mySet1.autoComparable(fst.next()), new
  // Entry.WithKey(mySet1.autoComparable(mySet1.last())));
  // Tuple upperBound = (Tuple) snd.next();
  // BPlusIndexedSet subSet = (BPlusIndexedSet) mySet1.headSet(upperBound);
  // Object[] a = subSet.toArray();
  // Assert.assertEquals(a.length, 1);
  // }



  // TODO: "Reflection-Test"

  public String toString() {
    return "BPlusIndexedSet Operation Test for tuple types (headSet)";
  }
}
