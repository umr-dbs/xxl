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

import xxl.core.indexStructures.IndexedSet;
import xxl.core.indexStructures.Tree;
import xxl.core.indexStructures.builder.BPlusTree.BPlusTreeBuilder;
import xxl.core.io.propertyList.IPropertyListSerializable;
import xxl.core.relational.metaData.TupleMetaData;

/**
 * With the help of a concrete implementation of the abstract <code>IndexBuilder</code> class it is
 * possible to easily generate index structures or restore them from a storage medium. The
 * construction of this index structure and decision which concrete structure to use depends on the
 * concrete implementation of <code>IndexBuilder</code> and a given concrete implementation of a
 * given <i>configuration</i>. Depending on the chosen configuration and builder a special structure
 * that inherits from the superclass <code>Tree</code> will be generated and returned ready to use. <br/>
 * <br/>
 * 
 * XXL provides a set of built-in builders which are accessible through concrete
 * <i>configuration</i> e.g. <code>BPlusTreeConfiguration</code>. By design each builder is inside
 * the concrete <i>configuration</i> and can be executed with the method
 * <code>.getBuilder().create()</code> of the given <code>IndexConfiguration</code> implementation. <br/>
 * <br/>
 * 
 * <b>How to add own new builders</b> <br/>
 * If you want to write your own builder for a new index structure ensure first that this structure
 * inherits from abstract class <code>Tree</code>. After this create a concrete implementation of
 * abstract class <code>IndexConfiguration</code> in which you create getter and setter and also
 * <i>default</i> values for all the customizations for your index structure. Then implement your
 * own <code>IndexBuilder</code> subclass in which you have to implement {@link #create()}. Inside
 * this method you have to construct and setup your index structure according to the given
 * configuration structure which you designed in the previous step and which is accessible through
 * the protected member {@link #mIndexConfiguration}.
 * 
 * @see TupleMetaData Setting up table meta data
 * @see BPlusTreeBuilder A builder for a BPlus tree
 * @see IndexConfiguration Setting up the requirements for a specific index structure
 * @see Tree The base class for the index structures
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public abstract class IndexBuilder<Self, ExternInformation>
    implements
      IPropertyListSerializable<Self, ExternInformation> {
  /*
   * The concrete configuration which setup the needs for the index structure like storing in memory
   * or a persistent storage medium.
   */
  protected IndexConfiguration mIndexConfiguration;

  /**
   * Constructs this builder object with a given configuration. This configuration contains all the
   * users needs for the index structure. A need could be if the structures should be stored in
   * memory or on a persistent storage medium. By calling {@link #create()} an instance of a
   * concrete <code>Tree</code> subclass is initialized depending of these needs and will be
   * returned.
   * 
   * @param configuration
   */
  public IndexBuilder(IndexConfiguration configuration) {
    mIndexConfiguration = configuration;
  }

  /**
   * Builds up a concrete subclass of <code>Tree</code> depending on the given <i>configuration</i>
   * and returns this as super class <code>Tree</code> type object.
   * 
   * @return The index structures built up according to <i>configuration</i> needs.
   */
  public abstract IndexedSet create();

  /**
   * Returns the current configuration
   * 
   * @return The index configuration
   */
  public IndexConfiguration getIndexConfiguration() {
    return mIndexConfiguration;
  }

}
