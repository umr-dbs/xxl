package xxl.core.indexStructures.indexBuilder;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusIndexedSetView;
import xxl.core.indexStructures.builder.Builders;

public class BPlusIndexedSetHeadSetOperationPrimitiveTest {

  final int MAX_ITEMS_TO_INSERT1 = 25;
  /*
   * Set A
   */
  final String MY_TABLE_NAME1 = "MyTable1";
  BPlusIndexedSet mySet1;

  @BeforeMethod
  public void prepareSet() {
    mySet1 =
        Builders.createBPlusTree.Integer(MY_TABLE_NAME1).getBuilder().create();

    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; i++)
      mySet1.add(i);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void subSetHeadSetBoundGreaterMaxContentTest() {
    int upperBound = MAX_ITEMS_TO_INSERT1 + 100;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void subSetHeadSetBoundGreaterThanMaximumContentTest() {
    int upperBound = MAX_ITEMS_TO_INSERT1 + 100;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
    Assert.assertEquals(0, subSet.size());
  }

  @Test
  public void subSetHeadSetContentTest() {
    int upperBound = MAX_ITEMS_TO_INSERT1 / 2;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
    Object[] o = subSet.toArray();
    Object[] b = new Integer[MAX_ITEMS_TO_INSERT1 / 2];
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1 / 2; i++)
      b[i] = i;
    Assert.assertEquals(o, b);
  }

  @Test
  public void subSetHeadSetIdentityTest() {
    int upperBound = (int) mySet1.last();

    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
    Object[] a = subSet.toArray();
    Object[] b = new Object[MAX_ITEMS_TO_INSERT1 - 1];
    // items in subset are strictly less than upperBound
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1 - 1; i++)
      b[i] = i;
    Assert.assertEquals(a, b);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void subSetHeadSetNegativeBoundTest() {
    int upperBound = -1000;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
  }

  @Test
  public void subSetHeadSetValidBoundTest() {
    int upperBound = 10;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.headSet(upperBound);
    Object[] o = subSet.toArray();
    Assert.assertEquals(o[0], 0);
    Assert.assertEquals(o[o.length - 1], upperBound - 1);
  }

  // @Test
  // public void subSetHeadSetEmptyTest() {
  // Cursor fst = mySet1.findBetween((Comparable)mySet1.first(), (Comparable)mySet1.last());
  // fst.next();
  // Cursor snd = mySet1.findBetween((Comparable) fst.next(), (Comparable)mySet1.last());
  // int upperBound = (int) snd.next();
  // BPlusIndexedSet subSet = (BPlusIndexedSet) mySet1.headSet(upperBound);
  // Object[] a = subSet.toArray();
  // Assert.assertEquals(a.length, 1);
  // }

  // TODO: "Reflection-Test"

  public String toString() {
    return "BPlusIndexedSet Operation Test for primitive types (headSet)";
  }
}
