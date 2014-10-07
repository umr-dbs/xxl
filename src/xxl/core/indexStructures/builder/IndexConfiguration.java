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

package xxl.core.indexStructures.builder;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.MapContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.indexStructures.builder.BPlusTree.BPlusConfiguration.Creator;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;


/**
 * An abstract class for setting up requirements for a given index structure needed for a concrete
 * {@link IndexBuilder}. Depending of the concrete index structure an
 * <code>IndexConfiguration</code> implementation could specify all the customizations of this index
 * structure, e.g. if it should be kept in memory or stored on the hard drive. <br/>
 * <br/>
 * <b>How to add own new builders</b> <br/>
 * If you want to write your own builder for a new index structure ensure first that this structure
 * inherits from abstract class <code>Tree</code>. After this create a concrete implementation
 * <i>this</i> abstract class <code>IndexConfiguration</code> in which you create getter and setter
 * and also <i>default</i> values for all the customizations for your index structure. When writing
 * your own <code>IndexConfiguration</code> implementation you have to implement the method
 * {@link #getBuilder()} at least. In this method you have to construct your concrete builder and
 * return it.
 * 
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see IndexBuilder Easily create or restore index structures
 * @see Creator Setting up the requirements for the BPlus tree
 */
public abstract class IndexConfiguration {

  /*
   * The location selection where the index should be kept
   */
  public enum Location {
    LOCATION_FILESYSTEM, LOCATION_MAIN_MEMORY
  }

  /*
   * Default block size
   */
  private static final int INDEX_REQUIREMENTS_DEFAULT_BLOCK_SIZE = 4096;

  /*
   * Default LRU Buffer size
   */
  private static final int INDEX_REQUIREMENTS_DEFAULT_LRU_BUFFER_SIZE = 20;

  /*
   * Buffer and it's block size. By default a LRU buffer is used
   */
  protected int mBlockSize = INDEX_REQUIREMENTS_DEFAULT_BLOCK_SIZE;

  /*
   * LRUBuffer, Container and BufferedContainer with default values
   */
  protected Buffer mBuffer = new LRUBuffer<>(
      INDEX_REQUIREMENTS_DEFAULT_LRU_BUFFER_SIZE);
  protected Container mConverterContainer = new MapContainer();
  private BufferedContainer mBufferedContainer = new BufferedContainer(
      mConverterContainer, mBuffer);
  
  /*
   * The path where all files were stored (e.g. meta information file)
   */
  protected String mFileSystemFilePath;

  /*
   * Indicator if the index should be in main memory or at hard drive
   */
  protected Location mLocation = Location.LOCATION_MAIN_MEMORY;

  /**
   * Return the current block size in Byte for the underlying storage medium used in BPlusTree
   * constructor.
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return Block size in Byte.
   * 
   */
  public int getBlockSize() {
    return mBlockSize;
  }


  /**
   * Returns the current buffer used for the BPlus tree. By default it's a single (unshared) LRU
   * buffer with a capacity of INDEX_REQUIREMENTS_DEFAULT_LRU_BUFFER_SIZE items.
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return the buffer
   * 
   * @see Buffer
   */
  public Buffer getBuffer() {
    return mBuffer;
  }

  /**
   * Returns the buffered container which should be used by the BPlus tree
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return the buffered container
   * 
   * @see Container
   */
  public BufferedContainer getBufferedContainer() {
    return mBufferedContainer;
  }

  /**
   * Returns a builder which can create an index structure that satisfy the given configuration.
   * 
   * @return The builder
   */
  public abstract IndexBuilder getBuilder();


  /**
   * Returns the converter container used by the BPlus tree.
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return the converter container
   * 
   * @see Container
   */
  Container getConverterContainer() {
    return mConverterContainer;
  }

  /**
   * Returns the path in which the BPlus tree stores it data in case of storing the BPlus tree
   * persistent at the hard drive. Please note that the file name is taken from the meta data table
   * name.
   * 
   * <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return The file path excluding the file extension.
   * 
   * @throws IllegalArgumentException If the BPlus tree should be stored at main memory because
   *         there is no file system path to return.
   */
  public String getFileSystemFilePath() throws IllegalArgumentException {
    if (mLocation.equals(Location.LOCATION_MAIN_MEMORY))
      throw new IllegalArgumentException(
          "The BPlus tree is stored at main memory. There is no file path available for persistent storage.");
    return mFileSystemFilePath;
  }

  /**
   * Returns the location where the BPlus tree should be stored. <br/>
   * <br/>
   * <b>Note</b>: Because this only used by the BPlus tree builder the visibility of this method is
   * restricted to package wide visibility.
   * 
   * @return the BPlus tree location
   * 
   * @see Location
   */
  public Location getLocation() {
    return mLocation;
  }

  /*
   * Sets the buffer container
   */
  protected void overrideBufferContainer(BufferedContainer bc) {
    mBufferedContainer = bc;
  }

  /**
   * Indicates if the current configuration will force an index to store itself at file system or on
   * main memory. By default it is stored in main memory.
   * 
   * @return <b>true</b> if the index ist stored at hard drive, otherwise <b>false</b>
   */
  public boolean storeAtFileSystem() {
    return mLocation == Location.LOCATION_FILESYSTEM;
  }

  /*
   * Updates the buffer container to match with the given converter container and the given buffer.
   */
  protected void updateBufferContainer() {
    mBufferedContainer = new BufferedContainer(mConverterContainer, mBuffer);
  }



}
