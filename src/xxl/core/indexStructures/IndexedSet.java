/*
 * XXL: The eXtensible and fleXible Library for data processing
 * 
 * Copyright (C) 2000-2014 Prof. Dr. Bernhard Seeger Head of the Database Research Group Department
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

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.SortedSet;

import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.indexStructures.builder.IndexBuilder;

/**
 * The abstract base class <code>IndexedSet</code> is a Java <code>SortedSet</code> implementation
 * to wrap the abilities of various index structures in order to act like a normal Java
 * <code>SortedSet</code>. With this it is possible to use a set independent of the underlying
 * (tree) index type and without a deeper knowledge how to use the index structure exactly.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @param <IndexStructure> The underlying implementation of a tree index structure
 * @param <E> The data type which should be managed
 */
public abstract class IndexedSet<IndexStructure extends Tree, E>
    implements
      SortedSet<E> {

  /*
   * The XXL buffered container. This is created somewhere outside this class.
   */
  private BufferedContainer mBufferedContainer;

  /*
   * The configured index builder to create the index structure. This is created somewhere outside
   * this class.
   */
  protected IndexBuilder mCreator;

  /*
   * The path to a file system directory in which the data of the index structure will be sorted
   */
  private String mFilePath;

  /*
   * The reference to the index structure
   */
  protected IndexStructure mTree;

  /**
   * Sets up a new instance of <code>IndexedSet</code> with a given implementation of a tree index
   * and it's builder object. The builder instance contains different <i>meta</i> data for the
   * index, e.g. the file path, the data type, containers and others.
   * 
   * @param tree An instance of an index structure
   * @param creator The builder which created the index structure
   */
  public IndexedSet(IndexStructure tree, IndexBuilder creator) {
    mTree = tree;
    if (creator.getIndexConfiguration().storeAtFileSystem())
      mFilePath = creator.getIndexConfiguration().getFileSystemFilePath();
    mBufferedContainer = creator.getIndexConfiguration().getBufferedContainer();
    mCreator = creator;
  }

  /**
   * Depending on the index type some data will be stored to hard drive during run time or
   * persistently if the index supports the saving of it's content.
   * 
   * @return Directory in file system which will contain the data of the index structure
   */
  public String getFilePath() {
    return mFilePath;
  }

  /**
   * Returns the underlying index structure. Please note: Although it is more flexible to perform
   * queries directly with the underlying index structure it is not recommended to access it.
   * Because of performance reasons some methods e.g. {@link SortedSet#size()} may be calculated by
   * the IndexedSet sub class and not by querying the underlying tree. If you manipulate the content
   * of the index structure by hand some methods of the wrapping IndexedSet may produce unexpected
   * output.
   * 
   * @return A (editable) direct reference to the underlying index structure.
   */
  public IndexStructure getIndexStructure() {
    return mTree;
  }

  @Override
  public Iterator<E> iterator() {
    return mTree.query();
  }

  /**
   * Stores the content of this set to hard drive. <i>Please note</i>: Depending on the underlying
   * index structure this method only <i>flushes</i> buffers. It is possible that <i>unsaved</i>
   * changes after a <code>save()</code> call will also be available in future without another
   * <code>save()</code> call. But calling <code>save()</code> ensures that the <i>entire</i>
   * content of the set is stored and changes will <i>not</i> be missed if the set is dropped from
   * the computers main memory.
   * 
   * @throws IOException If something went wrong during IO
   */
  public void save() throws IOException {
    saveIndexStructureMetaData();

    mBufferedContainer.flush();
    mBufferedContainer.close();
  }

  /*
   * The index structure implementation depending save process
   */
  protected abstract void saveIndexStructureMetaData() throws IOException;

  /**
   * Returns the size of the current set as <code>BigInteger</code>. Dependent on the underlying
   * index structure the content amount is only limited by the <i>hard drive</i> capacity. It may
   * comes to an overflow for standard <code>Integer</code> data which contains the set size. This
   * methods guarantees the correct set size.
   * 
   * @return The set size
   */
  public abstract BigInteger sizeBigInteger();


  @Override
  public abstract Object[] toArray();

  @Override
  public abstract <T> T[] toArray(T[] a);

  public abstract String toString();


}
