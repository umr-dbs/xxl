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

package xxl.core.indexStructures;

import static xxl.core.util.ConvertUtils.autoCast;
import static xxl.core.util.ConvertUtils.autoComparable;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;

import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.KeyRange;
import xxl.core.indexStructures.keyRanges.BooleanKeyRange;
import xxl.core.indexStructures.keyRanges.DateKeyRange;
import xxl.core.indexStructures.keyRanges.DoubleKeyRange;
import xxl.core.indexStructures.keyRanges.FloatKeyRange;
import xxl.core.indexStructures.keyRanges.IntegerKeyRange;
import xxl.core.indexStructures.keyRanges.LongKeyRange;
import xxl.core.indexStructures.keyRanges.ShortKeyRange;
import xxl.core.indexStructures.keyRanges.TimeKeyRange;
import xxl.core.indexStructures.keyRanges.TimestampKeyRange;
import xxl.core.indexStructures.keyRanges.TupleKeyRangeFunction;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration;
import xxl.core.indexStructures.builder.BPlusTree.BPlusTreeBuilder;
import xxl.core.indexStructures.builder.BPlusTree.ManagedType;
import xxl.core.indexStructures.builder.BPlusTree.PrimitiveType;
import xxl.core.indexStructures.builder.BPlusTree.TupleType;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.DateConverter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.FloatConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.ShortConverter;
import xxl.core.io.converters.TimeConverter;
import xxl.core.io.converters.TimestampConverter;
import xxl.core.io.propertyList.Property;
import xxl.core.io.propertyList.PropertyList;
import xxl.core.io.propertyList.json.JSONPrinter;
import xxl.core.relational.tuples.Tuple;

/**
 * An implementation of the {@link IndexedSet} super class to use sets which are indexed by a
 * {@link BPlusTree}.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @param <E> The data type which should be managed
 */
public class BPlusIndexedSet<E> extends IndexedSet<BPlusTree, E> {

  public static final String BPLUS_TUPLE_MET_EXTENSION = ".met2";


  /*
   * File extension for meta data (table name, content type) and reloading BPlusTree meta data in
   * JSON-Format for tuple data (index entry, key range)
   */
  public static final String META_FILE_EXTENSION = ".meta";

  /*
   * The count of items inside the indexed set
   */
  private BigInteger mSize = BigInteger.ZERO;

  /**
   * Sets up a new instance of <code>BPlusIndexedSet</code> with an instance of
   * <code>BPlusTree</code> and it's builder object. The builder instance contains different
   * <i>meta</i> data for the index, e.g. the file path, the data type, containers and others.
   * 
   * @param tree An instance of an {@link BPlusTree}
   * @param creator The {@link BPlusTreeBuilder} which created <code>tree</code>
   */
  public BPlusIndexedSet(BPlusTree tree, BPlusTreeBuilder creator) {
    super(tree, creator);
  }

  @Override
  public boolean add(E e) {
    try {
      if (((BPlusConfiguration) mCreator.getIndexConfiguration())
          .getManagedType().getContentClass() == ManagedType.ContentClass.CONTENT_CLASS_COMPLEX) {
        Tuple tuple = (e instanceof Entry) ? ((Entry) e).asTuple() : (Tuple) e;
        int columnCount =
            ((BPlusConfiguration) mCreator.getIndexConfiguration())
                .getManagedType().getMetaData().getColumnCount();
        if (tuple.toArray().length != columnCount)
          throw new IllegalArgumentException(
              "Item to add does not match the tables schemas (" + e
                  + "). Expected " + columnCount + " columns but "
                  + tuple.toArray().length + " was given.");
      }

      if (contains(e)) return false;
      if (!mTree.contains((e instanceof Entry) ? ((Entry) e).asTuple() : e))
        mTree.insert((e instanceof Entry) ? ((Entry) e).asTuple() : e);
      mSize = mSize.add(BigInteger.ONE);
      return true;
    } catch (IllegalArgumentException | SQLException ex) {
      return false;
    }
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    boolean result = true;
    for (E item : c)
      result &= add(item);
    return result;
  }

  @Override
  public void clear() {
    mTree.clear();
  }

  @Override
  public Comparator<? super E> comparator() {
    // TODO: Should a comparator be available?
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object o) {
    return mTree.contains(autoComparable(autoCast(o), mCreator));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    boolean result = true;
    for (Object item : c) {
      result &= contains(item);
      if (!result) return false;
    }
    return result;
  }

  @Override
  public E first() {
    return (E) ((KeyRange) mTree.rootDescriptor()).minBound();
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    return subSetWithBoundOption(first(), toElement, true);
  }

  @Override
  public boolean isEmpty() {
    return mSize.equals(BigInteger.ZERO);
  }

  @Override
  public E last() {
    return (E) ((KeyRange) mTree.rootDescriptor()).maxBound();
  }

  private Comparable max(Comparable a, Comparable b) {
    return (a.compareTo(b) >= 0) ? a : b;
  }

  @Override
  public boolean remove(Object o) {
    Object tupleToRemove = o;
    if (o instanceof Tuple) {
      tupleToRemove = (Tuple) o;
    } else if (o instanceof Entry) {
      tupleToRemove = ((Entry) o).asTuple();
    } else if (o instanceof Entry.WithKey) {
      // Delete each entity which has this key
      throw new IllegalArgumentException(
          "Deleting entries only restriction by a given key (subset) is too dangerous.");
    }

    if (tupleToRemove instanceof Tuple || tupleToRemove instanceof Entry) {
      try {
        if (((tupleToRemove instanceof Tuple)
            ? (Tuple) tupleToRemove
            : ((Entry) tupleToRemove).asTuple()).toArray().length != ((BPlusConfiguration) mCreator
            .getIndexConfiguration()).getManagedType().getMetaData()
            .getColumnCount())
          throw new IllegalArgumentException("The column count of entry \"" + o
              + "\" does not match the tables column count.");
      } catch (SQLException error) {}
    }

    Object removedObject = mTree.remove(tupleToRemove);
    if (removedObject != null) {
      mSize = mSize.subtract(BigInteger.ONE);
      if (mSize.compareTo(BigInteger.ZERO) == -1)
        throw new IndexOutOfBoundsException("Set size is less than zero.");
      return true;
    } else
      return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean setChanged = false;
    for (Object o : c)
      setChanged &= remove(o);
    return setChanged;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean setChanged = false;
    Comparable minBound = ((KeyRange) mTree.rootDescriptor()).minBound();
    Comparable maxBound = ((KeyRange) mTree.rootDescriptor()).maxBound();

    Cursor cursor = mTree.rangeQuery(minBound, maxBound);
    while (cursor.hasNext()) {
      Object o = cursor.next();
      if(!c.contains(o)) {
        cursor.remove();
        setChanged = true;
      }
    }
    return setChanged;
  }

  /*
   * Stores the index structure meta data (index entry, key range etc.). This is an implementation
   * of the super class methods and handles the concrete saving process for the BPlusTree
   */
  @Override
  protected void saveIndexStructureMetaData() throws IOException {

    /*
     * BPlusTree required meta file (see BPlusTree documentation)
     */
    File filePath = new File(this.getFilePath() + META_FILE_EXTENSION);

    /*
     * Saving the BPlus root entry, level information, root entry ID independent of the managed typ
     * to the meta file
     */
    DataOutputStream out = new DataOutputStream(new FileOutputStream(filePath));
    IndexEntry rootEntry = (IndexEntry) ((BPlusTree) mTree).rootEntry();
    int level = rootEntry.parentLevel();
    long rootId = (Long) rootEntry.id();

    /*
     * Save level at first position
     */
    IntegerConverter.DEFAULT_INSTANCE.writeInt(out, level);


    BPlusConfiguration config =
        (BPlusConfiguration) mCreator.getIndexConfiguration();
    ManagedType managedType = config.getManagedType();

    /*
     * Depending on the data type which is managed by the tree, the data type for the minKey, maxKey
     * etc. is different. The following lines of code stores the information minKey, maxKey,
     * rootKey, rootId depending on the data type.
     */
    if (managedType instanceof PrimitiveType) {
      /*
       * Storing min and max bound of primitive types which are managed by the three
       */
      PrimitiveType primitiveManaged = (PrimitiveType) managedType;
      switch (primitiveManaged.getContentClassSubType()) {
        case BOOLEAN: {
          boolean minKey =
              (Boolean) ((BooleanKeyRange) mTree.rootDescriptor()).minBound();
          boolean maxKey =
              (Boolean) ((BooleanKeyRange) mTree.rootDescriptor()).maxBound();
          boolean rootKey = (Boolean) rootEntry.separator().sepValue();

          /*
           * Save rootKey at second position
           */
          BooleanConverter.DEFAULT_INSTANCE.writeBoolean(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          BooleanConverter.DEFAULT_INSTANCE.writeBoolean(out, minKey);
          BooleanConverter.DEFAULT_INSTANCE.writeBoolean(out, maxKey);
        }
          break;
        case DATE: {
          Date minKey =
              new Date(
                  (Long) ((DateKeyRange) mTree.rootDescriptor()).minBound());
          Date maxKey =
              new Date(
                  (Long) ((DateKeyRange) mTree.rootDescriptor()).maxBound());
          Date rootKey = new Date((Long) rootEntry.separator().sepValue());

          /*
           * Save rootKey at second position
           */
          DateConverter.DEFAULT_INSTANCE.write(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          DateConverter.DEFAULT_INSTANCE.write(out, minKey);
          DateConverter.DEFAULT_INSTANCE.write(out, maxKey);
        }

          break;
        case DOUBLE: {
          double minKey =
              (Double) ((DoubleKeyRange) mTree.rootDescriptor()).minBound();
          double maxKey =
              (Double) ((DoubleKeyRange) mTree.rootDescriptor()).maxBound();
          double rootKey = (Double) rootEntry.separator().sepValue();

          /*
           * Save rootKey at second position
           */
          DoubleConverter.DEFAULT_INSTANCE.writeDouble(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          DoubleConverter.DEFAULT_INSTANCE.writeDouble(out, minKey);
          DoubleConverter.DEFAULT_INSTANCE.writeDouble(out, maxKey);
        }
          break;
        case FLOAT: {
          float minKey =
              (Float) ((FloatKeyRange) mTree.rootDescriptor()).minBound();
          float maxKey =
              (Float) ((FloatKeyRange) mTree.rootDescriptor()).maxBound();
          float rootKey = (Float) rootEntry.separator().sepValue();

          /*
           * Save rootKey at second position
           */
          FloatConverter.DEFAULT_INSTANCE.write(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          FloatConverter.DEFAULT_INSTANCE.write(out, minKey);
          FloatConverter.DEFAULT_INSTANCE.write(out, maxKey);
        }
          break;
        case INT: {
          int minKey =
              (Integer) ((IntegerKeyRange) mTree.rootDescriptor()).minBound();
          int maxKey =
              (Integer) ((IntegerKeyRange) mTree.rootDescriptor()).maxBound();
          int rootKey = (Integer) rootEntry.separator().sepValue();

          /*
           * Save rootKey at second position
           */
          IntegerConverter.DEFAULT_INSTANCE.writeInt(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          IntegerConverter.DEFAULT_INSTANCE.writeInt(out, minKey);
          IntegerConverter.DEFAULT_INSTANCE.writeInt(out, maxKey);
        }
          break;
        case LONG: {
          long minKey =
              (Long) ((LongKeyRange) mTree.rootDescriptor()).minBound();
          long maxKey =
              (Long) ((LongKeyRange) mTree.rootDescriptor()).maxBound();
          long rootKey = (Long) rootEntry.separator().sepValue();

          /*
           * Save rootKey at second position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, minKey);
          LongConverter.DEFAULT_INSTANCE.writeLong(out, maxKey);
        }
          break;
        case SHORT: {
          short minKey =
              (Short) ((ShortKeyRange) mTree.rootDescriptor()).minBound();
          short maxKey =
              (Short) ((ShortKeyRange) mTree.rootDescriptor()).maxBound();
          short rootKey = (Short) rootEntry.separator().sepValue();

          /*
           * Save rootKey at second position
           */
          ShortConverter.DEFAULT_INSTANCE.writeShort(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          ShortConverter.DEFAULT_INSTANCE.writeShort(out, minKey);
          ShortConverter.DEFAULT_INSTANCE.writeShort(out, maxKey);
        }
          break;
        case TIMESTAMP: {
          Timestamp minKey =
              new Timestamp(
                  (Long) ((TimestampKeyRange) mTree.rootDescriptor())
                      .minBound());
          Timestamp maxKey =
              new Timestamp(
                  (Long) ((TimestampKeyRange) mTree.rootDescriptor())
                      .maxBound());
          Timestamp rootKey =
              new Timestamp((Long) rootEntry.separator().sepValue());

          /*
           * Save rootKey at second position
           */
          TimestampConverter.DEFAULT_INSTANCE.write(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          TimestampConverter.DEFAULT_INSTANCE.write(out, minKey);
          TimestampConverter.DEFAULT_INSTANCE.write(out, maxKey);
        }
          break;
        case TIME: {
          Time minKey =
              new Time(
                  (Long) ((TimeKeyRange) mTree.rootDescriptor()).minBound());
          Time maxKey =
              new Time(
                  (Long) ((TimeKeyRange) mTree.rootDescriptor()).maxBound());
          Time rootKey = new Time((Long) rootEntry.separator().sepValue());

          /*
           * Save rootKey at second position
           */
          TimeConverter.DEFAULT_INSTANCE.write(out, rootKey);
          /*
           * Save rootID at third position
           */
          LongConverter.DEFAULT_INSTANCE.writeLong(out, rootId);
          /*
           * Store min-/max bound at fourth position
           */
          TimeConverter.DEFAULT_INSTANCE.write(out, minKey);
          TimeConverter.DEFAULT_INSTANCE.write(out, maxKey);

        }
          break;
        default:
          throw new UnsupportedOperationException(
              "Storing data not implemented yet for type \""
                  + primitiveManaged.getContentClass() + "\"");

      }


    } else if (managedType instanceof TupleType) {
      /*
       * Storing min and max bound of tuple types which are managed by the three
       */
      PropertyList tupleTableDescription = new PropertyList();
      tupleTableDescription.add(new Property("RootKey", ((Tuple) rootEntry
          .separator().sepValue()).toArray()));
      tupleTableDescription.add(new Property("RootID", rootId));
      tupleTableDescription.add(new Property("MinKey",
          ((Tuple) ((TupleKeyRangeFunction) mTree.rootDescriptor()).minBound())
              .toArray()));
      tupleTableDescription.add(new Property("MaxKey",
          ((Tuple) ((TupleKeyRangeFunction) mTree.rootDescriptor()).maxBound())
              .toArray()));
      JSONPrinter printer = new JSONPrinter(tupleTableDescription);
      File output =
          new File(config.getFileSystemFilePath() + BPLUS_TUPLE_MET_EXTENSION);
      if (output.exists()) output.delete();
      FileOutputStream fso = new FileOutputStream(output);
      printer.print(fso);

    } else
      throw new UnsupportedOperationException(
          "Exception while saving the set content. Unknown managed type \""
              + managedType.getContentType() + "\".");
  }

  @Override
  public int size() {
    return mSize.intValue();
  }

  @Override
  public BigInteger sizeBigInteger() {
    return mSize;
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    return subSetWithBoundOption(fromElement, toElement, true);
  }

  /*
   * To avoid code duplicates the process of sub setting is done with this methods. Both types,
   * inclusive or exclusive the upper bound, are needed to satisfy the Java SortedSet specification.
   * The flag <code>excludeToElement</code> controls this.
   */
  private SortedSet<E> subSetWithBoundOption(E fromElement, E toElement,
      boolean excludeToElement) {
    if (fromElement == null || (toElement == null))
      throw new NullPointerException("Subset bound have to be non-null");

    if ((autoComparable(autoCast(fromElement), mCreator))
        .compareTo(autoComparable(autoCast(toElement), mCreator)) > 0)
      throw new IllegalArgumentException("Predicate from " + fromElement
          + " > " + " to " + toElement + " is illegal");

    Comparable naturalMinBound = ((KeyRange) mTree.rootDescriptor()).minBound();
    Comparable naturalMaxBound = ((KeyRange) mTree.rootDescriptor()).maxBound();

    if ((autoComparable(fromElement, mCreator)).compareTo(naturalMinBound) < 0)
      throw new IndexOutOfBoundsException(
          "fromElement is lesser than the content's minimum");
    if ((autoComparable(fromElement, mCreator)).compareTo(naturalMaxBound) > 0)
      throw new IndexOutOfBoundsException(
          "fromElement is greater than the content's maximum");
    if ((autoComparable(toElement, mCreator)).compareTo(naturalMinBound) < 0)
      throw new IndexOutOfBoundsException(
          "toElement is lesser than the content's minimum");
    if ((autoComparable(toElement, mCreator)).compareTo(naturalMaxBound) > 0)
      throw new IndexOutOfBoundsException(
          "toElement is greater than the content's maximum");

    System.err.println(fromElement);

    return new BPlusIndexedSetView(this, autoComparable(fromElement, mCreator),
        autoComparable(toElement, mCreator), excludeToElement);
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    return subSetWithBoundOption(fromElement, last(), false);
  }

  @Override
  public Object[] toArray() {
    Comparable minBound = ((KeyRange) mTree.rootDescriptor()).minBound();
    Comparable maxBound = ((KeyRange) mTree.rootDescriptor()).maxBound();

    Cursor allEntries = mTree.query();
    List<Object> result = new ArrayList<>();
    while (allEntries.hasNext()) {
      Object entry = allEntries.next();
      Comparable item = autoComparable(autoCast(entry), mCreator);// (Comparable) allEntries.next();
      if (item.compareTo(minBound) >= 0 && item.compareTo(maxBound) <= 0) {
        if (entry instanceof Tuple) {
          // In case of relational managing tree, convert it to an array of arrays.
          result.add(((Tuple) entry).toArray());
        } else {
          result.add((Object) item);
        }
      }
    }
    return result.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Comparable minBound = ((KeyRange) mTree.rootDescriptor()).minBound();
    Comparable maxBound = ((KeyRange) mTree.rootDescriptor()).maxBound();

    Cursor allEntries = mTree.query();
    List<T> result = new ArrayList<>();
    while (allEntries.hasNext()) {
      Comparable item = (Comparable) allEntries.next();
      if (item.compareTo(minBound) >= 0 && item.compareTo(maxBound) <= 0)
        result.add((T) item);
    }
    return result.toArray(a);
  }

  public String toString() {
    if (((BPlusConfiguration) mCreator.getIndexConfiguration())
        .getManagedType().getContentClass() == ManagedType.ContentClass.CONTENT_CLASS_PRIMITIVE) {
      return java.util.Arrays.toString(toArray());
    } else {
      StringBuilder result = new StringBuilder();
      Object[] rows = toArray();
      if (rows.length == 0) result.append("[]");

      for (int i = 0; i < rows.length; ++i) {
        result.append("[");
        Object[] columns = (Object[]) rows[i];
        for (int j = 0; j < columns.length; ++j) {
          result.append(columns[j]);
          if (j < columns.length - 1) result.append(", ");
        }

        result.append("]");
        if (i < rows.length - 1) result.append("\n");
      }
      return result.toString();
    }

  }

}
