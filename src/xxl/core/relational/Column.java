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

package xxl.core.relational;

import xxl.core.util.ConvertUtils;

/**
 * An object which is a column object.<br/>
 * <br/>
 * <b>Note:</b> If you want to use a string typed column you have to use the
 * <tt>Column(RelationalType, int, String)</tt> constructor.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see xxl.core.indexStructures.BPlusTree
 */
public class Column {

  private String mName;
  /*
   * Column properties
   */
  private RelationalType mType;
  /*
   * If the column is a string typed one hold the maximum length in this member
   */
  private int mVarcharLength = 0;

  /**
   * Setup the column to be a <b>string</b> column.<br/>
   * <br/>
   * 
   * <b>Caution:</b> If you set <tt>type</tt> to a <b>non-string type</b> a
   * <tt>IllegalArgumentException</tt> will occur. If you want to use this column as a
   * <i>non</i>-string column use <tt>Column(RelationalType, String)</tt> constructor instead.
   * 
   * @param type The {@link RelationalType RelationalType} for this column
   * @param stringLength The <b>maximum</b> <i>character</i> length for an entry. The entry length
   *        have to be smaller or equal to this bound. Otherwise an
   *        <tt>IllegalArgumentException</tt> will occur.<br/>
   * <br/>
   * 
   *        e.g. <i>3</i> for "XXL" <br/>
   * <br/>
   * 
   *        Please not that a <b>Exception</b> will be thrown if the string value of an entry is
   *        greater than <tt>stringLength</tt>
   * @param name The column name
   */
  public Column(RelationalType type, int stringLength, String name) {
    this.mType = type;
    this.mName = name;

    checkColumnIsStringType(type, stringLength);

    mVarcharLength = stringLength;
  }

  /**
   * Setup the column to be a <b>non-string</b> column.<br/>
   * <br/>
   * 
   * <b>Caution:</b> If you set <tt>type</tt> to a <b>string type</b> a
   * <tt>IllegalArgumentException</tt> will occur. If you want to use this column as a string column
   * use <tt>Column(RelationalType, int, String)</tt> constructor instead which expects the string
   * length additionally.
   * 
   * @param type The {@link RelationalType RelationalType} for this column
   * @param name The column name
   */
  public Column(RelationalType type, String name) {
    if (ConvertUtils.toJavaType(type).equals(JavaType.STRING))
      throw new IllegalArgumentException(
          "Column type is set to \"string\" but there is no length specified. Use the other constructor instead.");

    this.mType = type;
    this.mName = name;
  }

  /*
   * Check if a given column is a string column and if the string length is valid if it is so.
   */
  private void checkColumnIsStringType(RelationalType type, int stringLength) {
    if (stringLength < 1
        || !ConvertUtils.toJavaType(type).equals(JavaType.STRING))
      throw new IllegalArgumentException(
          "Column type is not a string type or string length is less than 1 character.");
  }

  /**
   * Gets the maximum length for an entry if this column is marked as a <b>string</b> column. Please
   * ensure this otherwise an <tt>IllegalArgumentException</tt> will occur.
   * 
   * @return The maximum character length, e.g. <i>3</i> for "XXL"
   */
  public int getContentLength() {
    checkColumnIsStringType(mType, mVarcharLength);
    return mVarcharLength;
  }

  /**
   * Gets the name of this column.
   * 
   * @return the columns name
   */
  public String getName() {
    return mName;
  }

  /**
   * Gets the column type
   * 
   * @return The {@link RelationalType} column type.
   */
  public RelationalType getType() {
    return mType;
  }

  /**
   * Sets the name for this column
   * 
   * @param name the columns name
   */
  public void setName(String name) {
    this.mName = name;
  }

  /**
   * Sets the column type
   * 
   * @param type The {@link RelationalType} column type.
   */
  public void setType(RelationalType type) {
    this.mType = type;
  }

}
