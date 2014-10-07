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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import xxl.core.io.propertyList.Property;
import xxl.core.io.propertyList.PropertyList;
import xxl.core.io.propertyList.PropertyListReader;

/**
 * Base class for Consumer objects. A consumer takes a document extracts some data and returns the
 * remaining document.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 * @see PropertyConsumer
 * @see PropertyListConsumer
 * @see ValueConsumer
 * 
 */
abstract class Consumer {

  /*
   * If something went wrong during extracting (e.g. syntax error) an exception will be thrown.
   * Inside the exception error message there is a snapshot of the current line where the error
   * occurs. The PARSE_ERROR_DOC_THUMBNAIL_SIZE member sets the maximum length if this snapshot in
   * characters.
   */
  private static final int PARSE_ERROR_DOC_THUMBNAIL_SIZE = 50;

  /**
   * Creates a new instance.
   * 
   * @param document The document source which should be consumed
   */
  public Consumer(String document) {
    document = document;
  }

  /**
   * This method takes the given document and extracts some information depending of the
   * <code>Consumer</code> implementation. The remaining document is then returned.
   * 
   * @param document The source document
   * @param objectArray (Optional) A list of read objects
   * @return The input document minus the extraction
   */
  public abstract String consume(String document, List<Object> objectArray);

  /**
   * If something went wrong during extracting (e.g. syntax error) an exception will be thrown.
   * Inside the exception error message there is a snapshot of the current line where the error
   * occurs. This method creates that snapshot. If <code>doc</code> is longer than the snapshot
   * length, <code>doc</code> will be cut and "..." will be added.
   * 
   * @param doc The document
   * @return The snapshot
   */
  protected String createThumbnail(String doc) {
    return "..."
        + doc.substring(0,
            Math.min(PARSE_ERROR_DOC_THUMBNAIL_SIZE, doc.length()))
        + ((doc.length() > PARSE_ERROR_DOC_THUMBNAIL_SIZE) ? "..." : "");
  }

  /**
   * Substrings <code>src</code> from <code>offset</code> position to <code>offset+windowSize</code>
   * .
   * 
   * @param src String
   * @param offset Start
   * @param windowSize Length
   * @return
   */
  protected String createWindow(String src, int offset, int windowSize) {
    return src.substring(offset, offset + min(windowSize, src.length() - 1));
  }

  /**
   * Checks if <code>consumeString</code> can be extracted from <code>document</code>. In order to
   * this white spaces of <code>document</code> are deleted and it's checked if
   * <code>document</code> starts with the consuming string.<br/>
   * <br/>
   * <b>Note:</b> If document is shorter than the string which should be consumed or the document
   * does not start with the consuming string an exception will be thrown.
   * 
   * @param document A string
   * @param consumeString A prefix of string
   * @return The length of <code>consumeString</code>
   */
  public int getPrefixLength(String document, String consumeString) {
    document = document.trim();
    if (document.length() < consumeString.length())
      throw new IndexOutOfBoundsException();
    if (!document.startsWith(consumeString))
      throw new IllegalArgumentException("Unexcepted token in \""
          + createThumbnail(document) + "\". Should be: " + consumeString);
    return consumeString.length();
  }

  /**
   * Skips (leading) whitespace character in <code>document</code>
   * 
   * @param document Input
   * @return Input without leading whitespaces
   */
  public String skipWhitespace(String document) {
    for (int i = 0; i < document.length(); i++) {
      char cursor = document.charAt(i);
      if ((cursor != ' ') && (cursor != '\n') && (cursor != '\t')
          && (cursor != '\r')) return document.substring(i, document.length());
    }
    return document.trim();
  }
}


/**
 * Parses a JSON styled printing of a {@link PropertyList}. Please note that this class is used to
 * load a {@link PropertyList} which was stored with {@link JSONPrinter}.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
class JSONParser {
  /*
   * The read PropertyList
   */
  PropertyList retval = new PropertyList();

  /**
   * Parses the content of <code>inputStream</code> in order to create a PropertyList with the
   * content inside <code>inputStream</code>
   * 
   * @param inputStream Input
   */
  public JSONParser(InputStream inputStream) {
    parse(inputStream);
  }

  /**
   * @return Returns the property list
   */
  public PropertyList getPropertyList() {
    return retval;
  }

  /*
   * Parses <code>in</code> with PropertyListConsumer and stored the resulting PropertyList into
   * retval member
   */
  private void parse(InputStream in) {
    String document = new Scanner(in).useDelimiter("\\A").next();
    PropertyListConsumer consumer = new PropertyListConsumer(document, retval);
    consumer.consume(document, new ArrayList<Object>());
  }
}


/**
 * Reads a JSON styled printing of a {@link PropertyList}. Please note that this class is used to
 * load a {@link PropertyList} which was stored with {@link JSONPrinter}. The file which should be
 * read have to end with <code>.json</code>.
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
public class JSONReader extends PropertyListReader {

  /*
   * JSON file extension
   */
  public static final String FILE_EXTENSION = "json";

  /**
   * Parses the input stream to an instance of PropertyList and returns it. If something went wrong
   * (e.g. Syntax error) an underlying worker object will throw an exception.
   */
  @Override
  public PropertyList read(InputStream inputStream) {
    JSONParser p = new JSONParser(inputStream);
    return p.getPropertyList();
  }
}


/**
 * This class is an implementation of the abstract base class {@link Consumer}. It will consume a
 * {@link Property} for a {@link PropertyList}. <br/>
 * <br/>
 * <br/>
 * The general algorithm to reload a PropertyList with Consumers is <br/>
 * <code><pre>
 *      Input: Source document src
 *      
 *      PropertyListConsumer extracts a PropertyList pl from src
 *          |
 *          |- calls PropertyConsumer for each property p in pl 
 *                      |
 *                      |- calls ValueConsumer for each value in p                   
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
class PropertyConsumer extends Consumer {

  /*
   * The resulting PropertyList which is a reference to a PropertyList outside this Consumer
   */
  PropertyList mRestoredPropertyList;

  /**
   * Takes the given <code>document</code> and <code>receiver</code> in order to consume it when
   * {@link #consume(String, List)} is called.
   * 
   * @param document The document
   * @param receiver The PropertyList which should take the information provided in document
   */
  public PropertyConsumer(String document, PropertyList receiver) {
    super(document);
    mRestoredPropertyList = receiver;
  }

  /**
   * Consumes <code>document</code> and extracts all values (and nested PropertyLists). The founded
   * objects are stored inside the PropertyList given by the constructor. Additionally there are
   * also stored in <code>objectArray</code> which is needed for internally recursive call. You only
   * interested in the resulting PropertyList-reference you provided in the constructor.
   */
  @Override
  public String consume(String document, List<Object> objectArray) {
    document = skipWhitespace(document);

    if (document.isEmpty()) return "";
    if (!document.startsWith(JSONPrinter.STRING_LITERAL_QUOTE))
      throw new IllegalArgumentException("Invalid string format ("
          + createThumbnail(document) + "). Excepted: "
          + JSONPrinter.STRING_LITERAL_QUOTE);

    document = document.substring(JSONPrinter.STRING_LITERAL_QUOTE.length());

    String nameEndToken = JSONPrinter.STRING_LITERAL_QUOTE;
    String assignmentToken = JSONPrinter.ASSIGMENT_CHARACTER;
    for (int i = 0; i < document.length(); i++) {

      String window = super.createWindow(document, i, nameEndToken.length());
      if (window.equals(nameEndToken)) {
        String propertyName = document.substring(0, i); // OKAY
        if (propertyName.startsWith(JSONPrinter.STRING_LITERAL_QUOTE)) {
          propertyName =
              propertyName.substring(JSONPrinter.STRING_LITERAL_QUOTE.length());
          document =
              document.substring(JSONPrinter.STRING_LITERAL_QUOTE.length());
        }
        document =
            document.substring(propertyName.length()
                + JSONPrinter.STRING_LITERAL_QUOTE.length());
        document = skipWhitespace(document);

        if (!document.startsWith(JSONPrinter.ASSIGMENT_CHARACTER))
          throw new IllegalArgumentException("Invalid string format ("
              + createThumbnail(document) + "). Excepted: "
              + JSONPrinter.ASSIGMENT_CHARACTER);

        document = document.substring(JSONPrinter.ASSIGMENT_CHARACTER.length());
        document = skipWhitespace(document);

        ValueConsumer consumer =
            new ValueConsumer(document, propertyName, mRestoredPropertyList);
        String documentRest = consumer.consume(document, objectArray);
        i = 0;
        document = skipWhitespace(documentRest);

        if (document.startsWith(JSONPrinter.PROPERTY_LIST_DELIMITER_RIGHT)) {
          document =
              document.substring(JSONPrinter.PROPERTY_LIST_DELIMITER_RIGHT
                  .length());
          return document;
        }
      }
    }
    throw new IllegalArgumentException("Invalid block format ("
        + createThumbnail(document) + "). Expected: "
        + JSONPrinter.PROPERTY_LIST_DELIMITER_RIGHT);
  }

}


/**
 * This class is an implementation of the abstract base class {@link Consumer}. It will create a
 * {@link PropertyList} by consuming all {@link Property Properties} inside the given document. <br/>
 * <br/>
 * <br/>
 * The general algorithm to reload a PropertyList with Consumers is <br/>
 * <code><pre>
 *      Input: Source document src
 *      
 *      PropertyListConsumer extracts a PropertyList pl from src
 *          |
 *          |- calls PropertyConsumer for each property p in pl 
 *                      |
 *                      |- calls ValueConsumer for each value in p                   
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
class PropertyListConsumer extends Consumer {

  /*
   * Count of consumed property lists and the return value (PropertyList reference outside this
   * class)
   */
  protected int mConsumedCount = 0;
  protected PropertyList mRestoredPropertyList = null;

  /**
   * Constructs a new consumer instance with a document to consume and a reference to PropertyList
   * which should be extended by all new parsed objects.
   * 
   * @param document The source document
   * @param retval The return value, have to be non-null
   */
  public PropertyListConsumer(String document, PropertyList retval) {
    super(document);
    this.mRestoredPropertyList = retval;
  }

  /**
   * Extracts the next property list in <code>document</code> and calls for each item a
   * {@link PropertyConsumer}.
   */
  @Override
  public String consume(String document, List<Object> objectArray) {
    document = skipWhitespace(document);

    if (!document.startsWith(JSONPrinter.PROPERTY_LIST_DELIMITER_LEFT))
      throw new IllegalArgumentException("Invalid string format ("
          + createThumbnail(document) + "). Excepted: "
          + JSONPrinter.PROPERTY_LIST_DELIMITER_LEFT);

    document =
        document.substring(JSONPrinter.PROPERTY_LIST_DELIMITER_LEFT.length());
    boolean eofFlag = false;
    while (!document.isEmpty() && document != null && !eofFlag) {
      if (document.startsWith(JSONPrinter.PROPERTY_LIST_DELIMITER_RIGHT)) {
        eofFlag = true;
        continue;
      }
      PropertyConsumer consumer =
          new PropertyConsumer(document, mRestoredPropertyList);
      document = consumer.consume(document, objectArray);
    }
    document = skipWhitespace(document);
    return document;
  }
}


/**
 * This class extracts a value for a Property from a given source document.
 * 
 * <br/>
 * <br/>
 * <br/>
 * The general algorithm to reload a PropertyList with Consumers is <br/>
 * <code><pre>
 *      Input: Source document src
 *      
 *      PropertyListConsumer extracts a PropertyList pl from src
 *          |
 *          |- calls PropertyConsumer for each property p in pl 
 *                      |
 *                      |- calls ValueConsumer for each value in p                   
 * </pre></code>
 * 
 * @author Marcus Pinnecke (pinnecke@mathematik.uni-marburg.de)
 * 
 */
class ValueConsumer extends Consumer {

  /*
   * The property name which will contain the read value
   */
  private String mPropertyName;

  /*
   * A property list reference outside this class which contains the property
   */
  private PropertyList mRestoredPropertyList;

  /**
   * Takes a <code>document</code> and a <code>property name</code>, extracts the next value given
   * in <code>document</code> and assigns this value to the <code>property name</code>. Finally by
   * calling {@link #consume(String, List)} the property is added to
   * <code>restoredPropertyList</code>.
   */
  public ValueConsumer(String document, String propertyName,
      PropertyList restoredPropertyList) {
    super(document);
    mPropertyName = propertyName;
    mRestoredPropertyList = restoredPropertyList;
  }

  /**
   * Consumes the document and parse it for the following values
   * <ul>
   * <li>arrays (calls {@link #consumeArray(String, List)})</li>
   * <li>null (calls {@link #consumeNull(String, List)})</li>
   * <li>boolean (calls {@link #consumeBoolean(String, List)})</li>
   * <li>strings (calls {@link #consumeString(String, List)})</li>
   * <li>numbers (calls {@link #consumeNumber(String, List)})</li>
   * <li>nested property lists (calls {@link PropertyListConsumer})</li>
   * </ul>
   * 
   */
  @Override
  public String consume(String document, List<Object> objectArray) {
    String type =
        document.substring(
            0,
            min(document.length(),
                max(JSONPrinter.ARRAY_DELIMITER_LEFT.length(),
                    max(JSONPrinter.NULL_VALUE.length(),
                        max(JSONPrinter.PROPERTY_LIST_DELIMITER_LEFT.length(),
                            JSONPrinter.STRING_LITERAL_QUOTE.length())))));

    if (document.startsWith(JSONPrinter.ARRAY_DELIMITER_LEFT))
      document = consumeArray(document, objectArray);
    else if (document.startsWith(JSONPrinter.NULL_VALUE))
      document = consumeNull(document, objectArray);
    else if (document.toLowerCase().startsWith("false")
        || document.toLowerCase().startsWith("true"))
      document = consumeBoolean(document, objectArray);
    else if (document.startsWith(JSONPrinter.PROPERTY_LIST_DELIMITER_LEFT))
      document = consumePropertyList(document, objectArray);
    else if (document.startsWith(JSONPrinter.STRING_LITERAL_QUOTE))
      document = consumeString(document, objectArray);
    else {
      char cursor = document.charAt(0);
      if ((cursor == '0' || cursor == '1' || cursor == '2' || cursor == '3'
          || cursor == '4' || cursor == '5' || cursor == '6' || cursor == '7'
          || cursor == '8' || cursor == '9' || cursor == '-' || cursor == '+' || cursor == '.'))
        document = consumeNumber(document, objectArray);
      else
        throw new UnsupportedOperationException("Unkown type in ("
            + createThumbnail(document) + ")");
    }

    return document;
  }


  /**
   * Consumes a JSON styled array value
   */
  private String consumeArray(String document, List<Object> objectArray) {
    objectArray = new ArrayList<Object>();
    document = document.substring(JSONPrinter.ARRAY_DELIMITER_LEFT.length());
    document = skipWhitespace(document);

    while (!document.startsWith(JSONPrinter.ARRAY_DELIMITER_RIGHT)) {
      if (!document.startsWith(JSONPrinter.LIST_DELIMITER)) {
        document = consume(document, objectArray);
        mRestoredPropertyList.add(new Property(mPropertyName, objectArray));
        document = skipWhitespace(document);
      } else {
        document = document.substring(JSONPrinter.LIST_DELIMITER.length());
        document = skipWhitespace(document);
      }
      document = skipWhitespace(document);
    }
    document = document.substring(JSONPrinter.ARRAY_DELIMITER_RIGHT.length());
    return document;
  }

  /**
   * Consumes a JSON styled boolean value
   */
  private String consumeBoolean(String document, List<Object> objectArray) {
    document = skipWhitespace(document);
    Boolean retval = false;

    if (document.toLowerCase().startsWith("true")) retval = true;

    mRestoredPropertyList.add(new Property(mPropertyName, retval));
    objectArray.add(retval);

    return document.substring(retval.toString().length());
  }

  /**
   * Consumes a JSON styled NULL value
   */
  private String consumeNull(String document, List<Object> objectArray) {
    document = skipWhitespace(document);
    mRestoredPropertyList.add(new Property(mPropertyName));
    objectArray.add(null);
    return document.substring(JSONPrinter.NULL_VALUE.length());
  }

  /**
   * Consumes a JSON styled number value
   */
  private String consumeNumber(String document, List<Object> objectArray) {

    boolean greaterZeroFlag = true;
    String numberValue = "";

    for (int i = 0; i < document.length(); i++) {

      char cursor = document.charAt(i);

      if ((cursor == '0' || cursor == '1' || cursor == '2' || cursor == '3'
          || cursor == '4' || cursor == '5' || cursor == '6' || cursor == '7'
          || cursor == '8' || cursor == '9')
          || (cursor == '.') || (cursor == '+') || (cursor == '-')) {

        if (numberValue.contains("."))
          throw new IllegalArgumentException("Invalid number format ("
              + createThumbnail(document) + ")");

        numberValue += cursor;
      } else {
        Double value = 0d;
        try {
          value = Double.valueOf(numberValue);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Unable to load value in ("
              + createThumbnail(document) + ") because: " + e.getMessage());
        }
        if (numberValue.contains(".") || numberValue.contains(",")) {
          if (value.compareTo(new Double(Float.MAX_VALUE)) < 0) {
            float retval = (float) value.doubleValue();
            mRestoredPropertyList.add(new Property(mPropertyName, retval));
            objectArray.add(retval);
          } else {
            double retval = value.doubleValue();
            mRestoredPropertyList.add(new Property(mPropertyName, value));
            objectArray.add(retval);
          }
        } else {
          Long lvalue = value.longValue();
          if (lvalue.compareTo(new Long(Integer.MAX_VALUE)) < 0) {
            int retval = lvalue.intValue();
            mRestoredPropertyList.add(new Property(mPropertyName, retval));
            objectArray.add(retval);
          } else {
            long retval = lvalue.longValue();
            mRestoredPropertyList.add(new Property(mPropertyName, retval));
            objectArray.add(retval);
          }
        }
        return document.substring(i);
      }
    }

    throw new IllegalArgumentException(
        "Unexcepted end of file while reading number.");
  }

  /**
   * Consumes a nested property list
   */
  private String consumePropertyList(String document, List<Object> objectArray) {

    document =
        document.substring(JSONPrinter.PROPERTY_LIST_DELIMITER_LEFT.length());
    PropertyList nestedList = new PropertyList();
    PropertyConsumer nestedProperyList =
        new PropertyConsumer(document, nestedList);
    String rest = nestedProperyList.consume(document, objectArray);
    mRestoredPropertyList.add(new Property(mPropertyName, nestedList));
    return rest;

  }

  /**
   * Consumes an JSON styled string value
   */
  private String consumeString(String document, List<Object> objectArray) {

    String stringQuote = JSONPrinter.STRING_LITERAL_QUOTE;
    document = document.substring(stringQuote.length());

    for (int i = 0; i < document.length(); i++) {
      String excapeChecker = document.substring(i, min(i, i + 2));
      if ((excapeChecker.equalsIgnoreCase("\\\\"))
          || (excapeChecker.equalsIgnoreCase("\\\""))
          || (excapeChecker.equalsIgnoreCase("\\n"))
          || (excapeChecker.equalsIgnoreCase("\\r"))
          || (excapeChecker.equalsIgnoreCase("\\t"))
          || (excapeChecker.equalsIgnoreCase("\\'"))) {
        i += 2;
        continue;
      }

      String window = createWindow(document, i, stringQuote.length());
      if (window.equalsIgnoreCase(stringQuote)) {
        String value = document.substring(0, i);
        mRestoredPropertyList.add(new Property(mPropertyName, value));
        objectArray.add(value);
        return document.substring(i + stringQuote.length() + 1);
      }
    }
    throw new IllegalArgumentException(
        "Unexcepted end of file while reading string.");
  }
}
