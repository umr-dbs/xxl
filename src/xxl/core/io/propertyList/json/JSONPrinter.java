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

package xxl.core.io.propertyList.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import xxl.core.io.propertyList.Property;
import xxl.core.io.propertyList.PropertyList;
import xxl.core.io.propertyList.PropertyList.PropertyListItem;
import xxl.core.io.propertyList.PropertyListPrinter;

/**
 * The JSONPrinter is a class which takes a {@link PropertyList} instance and prints it to a given
 * {@link OutputStream} in a format of a JSON file. <br/>
 * <br/>
 * <b>Example</b> In the example a property list is creates which contains a (simple) table dump of
 * a student database table. At first the table name "Students" is set. After this two tuples of
 * students where added. Each has two attributes which are ID and the students name. Finally the
 * property list is printed with a <code>JSONPrinter</code> <br/>
 * <code><pre>
 * PropertyList myTable = new PropertyList();
 * myTable.add(new Property("Table", "Students"));
 * 
 * PropertyList myTuple1 = new PropertyList();
 * myTuple1.add(new Property("ID", 202031));
 * myTuple1.add(new Property("Name", "Mustermann"));
 * myTuple1.add(new Property("Subjects", new Object[] { "Maths", "Physics" }));
 * 
 * PropertyList myTuple2 = new PropertyList();
 * myTuple2.add(new Property("ID", 405066));
 * myTuple2.add(new Property("Name", "Musterfrau"));
 * myTuple2.add(new Property("Subjects", new Object[] { "English", "Geography" }));
 * 
 * 
 * myTable.add(new Property("Tuple1", myTuple1));
 * myTable.add(new Property("Tuple2", myTuple2));
 * 
 * JSONPrinter printer = new JSONPrinter(myTable);
 * printer.print(System.out);
 * </pre></code> This produces the following output on the console: <code><pre>
 * { 
 *  "Table": "Students"
 *  "Tuple1": { 
 *    "ID": 202031
 *    "Name": "Mustermann"
 *    "Subjects": [ "Maths", "Physics" ]
 *  }
 *  "Tuple2": { 
 *    "ID": 405066
 *    "Name": "Musterfrau"
 *    "Subjects": [ "English", "Geography" ]
 *  }
 * }
 * </pre></code> Another more elegant way to store multiple instances of an object, like the tuples
 * in the example above, is to add a property which contains a array of property lists. Those nested
 * property lists models the object. In the previous example replace <code><pre>
 * myTable.add(new Property("Tuple1", myTuple1));
 * myTable.add(new Property("Tuple2", myTuple2));</pre></code> with <code><pre>
 * Object[] propertyListArray = new Object[] { myTuple1, myTuple2 };
 * myTable.add(new Property("Tuples", propertyListArray));
 * </pre></code> The output result is <code><pre>
 * {
 *  "Table": "Students"
 *  "Tuples": [ {
 *      "ID": 202031
 *      "Name": "Mustermann"
 *      "Subjects": [ "Maths", "Physics" ]
 *    }, {
 *      "ID": 405066
 *      "Name": "Musterfrau"
 *      "Subjects": [ "English", "Geography" ]
 *    } ]
 * }
 * </pre></code>
 * 
 * By default the printer adds automatically white spaces, tabs and line breaks. If you want to
 * disable at least one of this features use {@link #print(OutputStream, int)}.
 * 
 * <b>Note:</b> If you want to set a value assignment to <code>null</code> just leave the value out
 * and use {@link Property#Property(String)} constructor, e.g.
 * <code>new Property("ThisIsNull")</code> which leads to the JSON output
 * <code>"ThisIsNull": null</code>. <br/>
 * <br/>
 * 
 * <b>Remark:</b> The printer supports the following primitive data types (or wrapper) for
 * <ul>
 * <li>String</li>
 * <li>java.sql.Date</li>
 * <li>java.sql.Time</li>
 * <li>java.sql.Timestamp</li>
 * <li>Integer</li>
 * <li>Boolean</li>
 * <li>Byte</li>
 * <li>Double</li>
 * <li>Float</li>
 * <li>Long</li>
 * <li>Short</li>
 * </ul>
 * or an array of <code>Object</code> which also contains the data types noted above. Also an empty
 * <code>Object[]</code> is possible just as nested ones.<br/>
 * <br/>
 * If you insert a value which does <i>not</i> match the types above, data type it will be discarded
 * and printed as <code>null</code>.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see PropertyList a collection of Properties
 * @see Property a single item for a PropertyList
 * @see PropertyListPrinter the base class for all PropertyListPrinters
 * 
 */
public class JSONPrinter extends PropertyListPrinter {

  /*
   * JSON specific special characters like delimiter, assignment etc. which maybe extended by a
   * additionally white space if the flag mFlagNoWhitespaces is not set.
   */
  public static final String ARRAY_DELIMITER_LEFT = "[";
  public static final String ARRAY_DELIMITER_RIGHT = "]";
  public static final String ASSIGMENT_CHARACTER = ":";
  public static final String LIST_DELIMITER = ",";
  /*
   * Control sequences
   */
  public static final String NEWLINE_CHARACTER = "\n";
  /*
   * Options for formatting JSON output
   */
  public final static int NO_LINEBREAKS = 0x01;

  public final static int NO_TABS = 0x02;
  public final static int NO_WHITESPACES = 0x04;
  /*
   * JSON specific null value
   */
  public static final String NULL_VALUE = "null";

  public static final String PROPERTY_LIST_DELIMITER_LEFT = "{";

  public static final String PROPERTY_LIST_DELIMITER_RIGHT = "}";
  public static final String STRING_LITERAL_QUOTE = "\"";
  public static final String TAB_CHARACTER = "\t";

  /*
   * Format flags which controls if additionally breaks, tabs and spaces are added or not to the
   * format output
   */
  private boolean mFlagNoLineBreaks = false;

  private boolean mFlagNoTabs = false;

  private boolean mFlagNoWhitespaces = false;

  /**
   * Constructs a new instance of <code>JSONPrinter</code> to a given instance of
   * {@link PropertyList}. Use {@link #print(OutputStream)} to print the content of the property
   * list to several output streams.
   * 
   * @param propertyList The property list which should be printed in JSON format
   */
  public JSONPrinter(PropertyList propertyList) {
    super(propertyList);
  }

  /*
   * To prevent invalid output some input string have to be escaped. Precisely replace backslash,
   * single and double quote, new line, carriage return and tab
   */
  private String escapeString(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
        .replace("\r", "\\r").replace("\t", "\\t").replace("'", "\\'");
  }

  /**
   * Prints the given {@link PropertyList} to the output.
   * 
   * @param output Output stream
   * @throws IOException If it fails to write into output stream
   * 
   * @see #print(OutputStream, int) for format options
   */
  public final void print(OutputStream output) throws IOException {
    print(output, 0);
    output.flush();
  }

  /**
   * Prints the given {@link PropertyList} to the output.
   * 
   * By default the printer adds automatically white spaces, tabs and line breaks. This method
   * expects restrictions to this behavior. These are
   * <ul>
   * <li>NO_LINEBREAKS</li>
   * <li>NO_TABS</li>
   * <li>NO_WHITESPACES</li>
   * </ul>
   * Use logical <code>or</code> to combine this features.<br/>
   * <br/>
   * <b>Example</b> This code disables any extensions that are only for the better human
   * readability. <code><pre>
   * import static JSONPrinter.*;
   * 
   * printer.print(System.out, NO_LINEBREAKS | NO_TABS | NO_WHITESPACES)
   * </pre></code>
   * 
   * @param output Output stream
   * @param printerOptions A combination of NO_LINEBREAKS, NO_TABS, NO_WHITESPACES or zero
   * @throws IOException If it fails to write into output stream
   */
  public final void print(OutputStream output, int printerOptions)
      throws IOException {
    mFlagNoLineBreaks = (printerOptions & NO_LINEBREAKS) == NO_LINEBREAKS;
    mFlagNoTabs = (printerOptions & NO_TABS) == NO_TABS;
    mFlagNoWhitespaces = (printerOptions & NO_WHITESPACES) == NO_WHITESPACES;

    print(mPropertyList, "", output);
  }

  /*
   * Prints the body of an objects, which contains of a list of names and value assigments or nested
   * objects.
   */
  private void print(PropertyList l, String prefix, OutputStream output)
      throws IOException {

    String bodyStart =
        PROPERTY_LIST_DELIMITER_LEFT
            + (mFlagNoLineBreaks ? "" : NEWLINE_CHARACTER);
    output.write(bodyStart.getBytes());

    for (PropertyListItem pl : l.listPropertyLists()) {
      PropertyList v = pl.getPropertyList();
      String assigment =
          prefix + (mFlagNoTabs ? "" : TAB_CHARACTER) + STRING_LITERAL_QUOTE
              + escapeString(pl.getName()) + STRING_LITERAL_QUOTE
              + ASSIGMENT_CHARACTER + (mFlagNoWhitespaces ? "" : " ");
      StringBuilder value = new StringBuilder();

      output.write(assigment.getBytes());

      print((PropertyList) v, prefix + (mFlagNoTabs ? "" : TAB_CHARACTER),
          output);
    }

    for (Property p : l) {
      Object v = (Object) p.getValue();
      String assigment =
          prefix + (mFlagNoTabs ? "" : TAB_CHARACTER) + STRING_LITERAL_QUOTE
              + escapeString(p.getName()) + STRING_LITERAL_QUOTE
              + ASSIGMENT_CHARACTER + (mFlagNoWhitespaces ? "" : " ");
      StringBuilder value = new StringBuilder();

      output.write(assigment.getBytes());

      if (v instanceof String)
        value.append(STRING_LITERAL_QUOTE + escapeString((String) v)
            + STRING_LITERAL_QUOTE);
      else if (v instanceof Date || v instanceof Time || v instanceof Timestamp)
        value.append(STRING_LITERAL_QUOTE + v + STRING_LITERAL_QUOTE);
      else if (v instanceof Integer || v instanceof Float
          || v instanceof Boolean || v instanceof Double || v instanceof Byte
          || v instanceof Long || v instanceof Short)
        value.append(v);
      else if (v instanceof Object[])
        value.append(printArray((Object[]) v));
      else if (v instanceof PropertyList) {
        print((PropertyList) v, prefix + (mFlagNoTabs ? "" : TAB_CHARACTER),
            output);
      } else
        value.append(NULL_VALUE);

      if (!mFlagNoLineBreaks) value.append(NEWLINE_CHARACTER);

      output.write(value.toString().getBytes());
    }

    String bodyEnd =
        prefix + PROPERTY_LIST_DELIMITER_RIGHT
            + ((mFlagNoLineBreaks) ? "" : NEWLINE_CHARACTER);
    output.write(bodyEnd.getBytes());
  }

  /*
   * If a component is an array of objects, just print it as enumeration of strings surround with [
   * and ]. If the value is not a number (0-9 and .) it will be embedded into double quotes. Nested
   * arrays are also possible which leads to a recursive call.
   */
  private String printArray(Object[] array) throws IOException {
    StringBuilder retval =
        new StringBuilder(ARRAY_DELIMITER_LEFT
            + (mFlagNoWhitespaces ? "" : " "));
    for (int i = 0; i < array.length; i++) {
      Object o = array[i];

      if (o instanceof Object[])
        retval.append(printArray((Object[]) o));
      else if (o instanceof PropertyList) {
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        print((PropertyList) o, (mFlagNoTabs ? "" : TAB_CHARACTER
            + TAB_CHARACTER), byteArrayOutput);
        retval.append(byteArrayOutput.toString());
      } else if (o instanceof String || o instanceof Date || o instanceof Time
          || o instanceof Timestamp)
        retval.append(STRING_LITERAL_QUOTE + o + STRING_LITERAL_QUOTE);
      else if (o instanceof Integer || o instanceof Boolean
          || o instanceof Byte || o instanceof Double || o instanceof Float
          || o instanceof Long || o instanceof Short)
        retval.append(o);
      else
        retval.append(NULL_VALUE);

      if (i < array.length - 1)
        retval.append(LIST_DELIMITER + (mFlagNoWhitespaces ? "" : " "));

    }
    retval.append((mFlagNoWhitespaces ? "" : " ") + ARRAY_DELIMITER_RIGHT);
    return retval.toString();
  }
}
