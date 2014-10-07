package xxl.core.indexStructures.indexBuilder;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import xxl.core.indexStructures.BPlusIndexedSet;
import xxl.core.indexStructures.BPlusIndexedSetView;
import xxl.core.indexStructures.builder.Builders;
import xxl.core.util.Arrays;

public class BPlusIndexedSetTailSetOperationPrimitiveTest {

  final int MAX_ITEMS_TO_INSERT1 = 1000;
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

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void subSetTailSetBoundGreaterThanMaximumContentTest() {
    int lowerBound = MAX_ITEMS_TO_INSERT1 + 100;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.tailSet(lowerBound);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void subSetTailSetBoundIsNullTest() {
    BPlusIndexedSetView subSet = (BPlusIndexedSetView) mySet1.tailSet(null);
  }

  @Test
  public void subSetTailSetContentTest() {
    int upperBound = MAX_ITEMS_TO_INSERT1 / 2;
    @SuppressWarnings("unchecked")
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.tailSet(upperBound);
    Object[] o = subSet.toArray();
    Object[] b = new Integer[MAX_ITEMS_TO_INSERT1 / 2];
    for (int i = MAX_ITEMS_TO_INSERT1 / 2; i < MAX_ITEMS_TO_INSERT1; i++)
      b[i - MAX_ITEMS_TO_INSERT1 / 2] = i;
    Arrays.println(o, System.out);
    Arrays.println(b, System.out);
    Assert.assertEquals(o, b);
  }

  @Test
  public void subSetTailSetEmptyTest() {
    int lowerBound = (int) mySet1.last();

    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.tailSet(lowerBound);
    Object[] a = subSet.toArray();
    Assert.assertEquals(a.length, 1);
  }

  @Test
  public void subSetTailSetIdentityTest() {
    int lowerBound = (int) mySet1.first();
    @SuppressWarnings("unchecked")
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.tailSet(lowerBound);
    Object[] a = subSet.toArray();
    Object[] b = new Object[MAX_ITEMS_TO_INSERT1];
    // items in subset are greater or equal than lowerBound
    for (int i = 0; i < MAX_ITEMS_TO_INSERT1; i++)
      b[i] = i;
    Assert.assertEquals(a, b);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void subSetTailSetNegativeBoundTest() {
    int lowerBound = -1000;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.tailSet(lowerBound);
  }

  @Test
  public void subSetTailSetValidBoundTest() {
    int lowerBound = 100;
    BPlusIndexedSetView subSet =
        (BPlusIndexedSetView) mySet1.tailSet(lowerBound);
    Object[] o = subSet.toArray();
    Assert.assertEquals(o[0], lowerBound);
    Assert.assertEquals(o[o.length - 1], MAX_ITEMS_TO_INSERT1 - 1);
  }

  // TODO: "Reflection-Test"

  public String toString() {
    return "BPlusIndexedSet Operation Test for primitive types (tailSet)";
  }
}
