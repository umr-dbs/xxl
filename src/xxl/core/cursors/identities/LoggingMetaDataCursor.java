/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2011 Prof. Dr. Bernhard Seeger
                        Head of the Database Research Group
                        Department of Mathematics and Computer Science
                        University of Marburg
                        Germany

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library;  If not, see <http://www.gnu.org/licenses/>. 

    http://code.google.com/p/xxl/

*/

package xxl.core.cursors.identities;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.cursors.DecoratorCursor;
import xxl.core.cursors.MetaDataCursor;

/**
 * This class decorates a {@link xxl.core.cursors.Cursor cursor} or a
 * {@link xxl.core.cursors.MetaDataCursor metadata-cursor} passing by all its
 * method calls while writing logging information to a print-stream. It realizes
 * the design pattern named <i>Decorator</i>.
 * 
 * <p><b>Note:</b> When the given input iteration only implements the interface
 * {@link java.util.Iterator} it is wrapped to a cursor by a call to the static
 * method {@link xxl.core.cursors.Cursors#wrap(Iterator) wrap}.</p>
 * 
 * <p>This class is useful for debugging purposes. Such a logging-cursor can be
 * included easily into an operator tree in order to log method calls.</p>
 *
 * @see java.util.Iterator
 * @see xxl.core.cursors.Cursor
 * @see xxl.core.cursors.SecureDecoratorCursor
 */
public class LoggingMetaDataCursor extends DecoratorCursor implements MetaDataCursor {

	/**
	 * A print-stream to which the logging information will be sent.
	 */
	protected PrintStream printStream;

	/**
	 * A name that is used to identify this instance of a logging-cursor in
	 * bulked log file, etc.
	 */
	protected String name;

	/**
	 * A boolean flag deciding whether the logging information should be sent to
	 * <tt>printStream</tt> or not.
	 */
	protected boolean doLogging;

	/**
	 * A boolean flag deciding whether the objects flowing through this operator
	 * should also be logged.
	 */
	protected boolean printData;

	/**
	 * Creates a new logging-cursor.  If an iterator is given to this constructor
	 * it is wrapped to a cursor.
	 *
	 * @param iterator the iteration to be decorated and logged.
	 * @param printStream a print-stream to which the logging information will be
	 *        sent.
	 * @param name a name that is used to identify this instance of a
	 *        logging-cursor in bulked log file, etc.
	 * @param printData determines whether the objects flowing through this
	 *        operator should also be logged.
	 */
	public LoggingMetaDataCursor(Iterator iterator, PrintStream printStream, String name, boolean printData) {
		super(iterator);
		this.printStream = printStream;
		this.name = name;
		this.doLogging = true;
		this.printData = printData;
		logEntry("constructed", true);
	}

	/**
	 * This method writes the logging information concerning a method call to
	 * <tt>printStream</tt> if and only if <tt>doLogging</tt> is set to
	 * <tt>true</tt>.
	 * 
	 * @param method the name of the logged method.
	 * @param after determines whether the logging information is sent after the
	 *        execution of the logged method.
	 *        
	 */
	protected void logEntry(String method, boolean after) {
		if (doLogging) {
			printStream.print(name);
			printStream.print("\tCursor\t");
			printStream.print(method);
			printStream.print("\t");
			printStream.println(after ? "after" : "before");
		}
	}

	/**
	 * This method writes the logging information concerning the parameters of a
	 * method call to <tt>printStream</tt> if and only if <tt>doLogging</tt> and
	 * <tt>printData</tt> are set to <tt>true</tt>.
	 * 
	 * @param method the name of the logged method.
	 * @param data a string describing the parameters of the logged method call.
	 * @param in a boolean flag determining whether the logged parameter is an
	 *        input or an output parameter.
	 */
	protected void logData(String method, String data, boolean in) {
		if (doLogging && printData) {
			printStream.print(name);
			printStream.print("\tCursor\t");
			printStream.print(method);
			printStream.print("\t");
			printStream.println((in ? "input: " : "output: ") + data);
		}
	}

	/**
	 * Sets the logging mode.
	 *
	 * @param doLogging if <tt>true</tt> logging information is written to the
	 *        specified print-stream.
	 */
	public void setLoggingMode(boolean doLogging) {
		this.doLogging = doLogging;
	}

	/**
	 * Opens the cursor, i.e., signals the cursor to reserve resources, open
	 * files, etc. Before a cursor has been opened calls to methods like
	 * <tt>next</tt> or <tt>peek</tt> are not guaranteed to yield proper
	 * results. Therefore <tt>open</tt> must be called before a cursor's data
	 * can be processed. Multiple calls to <tt>open</tt> do not have any effect,
	 * i.e., if <tt>open</tt> was called the cursor remains in the state
	 * <i>opened</i> until its <tt>close</tt> method is called.
	 * 
	 * <p>Note, that a call to the <tt>open</tt> method of a closed cursor
	 * usually does not open it again because of the fact that its state
	 * generally cannot be restored when resources are released respectively
	 * files are closed.</p>
	 */
	public void open() {
		logEntry("open", false);
		super.open();
		logEntry("open", true);
	}

	/**
	 * Closes the cursor, i.e., signals the cursor to clean up resources, close
	 * files, etc. When a cursor has been closed calls to methods like
	 * <tt>next</tt> or <tt>peek</tt> are not guaranteed to yield proper
	 * results. Multiple calls to <tt>close</tt> do not have any effect, i.e.,
	 * if <tt>close</tt> was called the cursor remains in the state
	 * <i>closed</i>.
	 * 
	 * <p>Note, that a closed cursor usually cannot be opened again because of
	 * the fact that its state generally cannot be restored when resources are
	 * released respectively files are closed.</p>
	 */
	public void close() {
		logEntry("close", false);
		super.close();
		logEntry("close", true);
	}

	/**
	 * Returns <tt>true</tt> if the iteration has more elements. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> or <tt>peek</tt> would
	 * return an element rather than throwing an exception.)
	 * 
	 * <p>This operation is implemented idempotent, i.e., consequent calls to
	 * <tt>hasNext</tt> do not have any effect.</p>
	 *
	 * @return <tt>true</tt> if the cursor has more elements.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 */
	public boolean hasNext() throws IllegalStateException {
		logEntry("hasNext", false);
		boolean hasNext = super.hasNext();
		logData("hasNext", Boolean.toString(hasNext), false);
		logEntry("hasNext", true);
		return hasNext;
	}

	/**
	 * Returns the next element in the iteration. This element will be
	 * accessible by some of the cursor's methods, e.g., <tt>update</tt> or
	 * <tt>remove</tt>, until a call to <tt>next</tt> or <tt>peek</tt> occurs.
	 * This is calling <tt>next</tt> or <tt>peek</tt> proceeds the iteration and
	 * therefore its previous element will not be accessible any more.
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException if the iteration has no more elements.
	 */
	public Object next() throws IllegalStateException, NoSuchElementException {
		logEntry("next", false);
		Object next = super.next();
		logData("next", next.toString(), false);
		logEntry("next", true);
		return next;
	}

	/**
	 * Shows the next element in the iteration without proceeding the iteration
	 * (optional operation). After calling <tt>peek</tt> the returned element is
	 * still the cursor's next one such that a call to <tt>next</tt> would be
	 * the only way to proceed the iteration. But be aware that an
	 * implementation of this method uses a kind of buffer-strategy, therefore
	 * it is possible that the returned element will be removed from the
	 * <i>underlying</i> iteration, e.g., the caller can use an instance of a
	 * cursor depending on an iterator, so the next element returned by a call
	 * to <tt>peek</tt> will be removed from the underlying iterator which does
	 * not support the <tt>peek</tt> operation and therefore the iterator has to
	 * be wrapped and buffered.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors. After calling the <tt>peek</tt> method a call to <tt>next</tt>
	 * is strongly recommended.</p> 
	 *
	 * @return the next element in the iteration.
	 * @throws IllegalStateException if the cursor is already closed when this
	 *         method is called.
	 * @throws NoSuchElementException iteration has no more elements.
	 * @throws UnsupportedOperationException if the <tt>peek</tt> operation is
	 *         not supported by the cursor.
	 */
	public Object peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		logEntry("peek", false);
		Object peek = super.peek();
		logData("peek", peek.toString(), false);
		logEntry("peek", true);
		return peek;
	}

	/**
	 * Returns <tt>true</tt> if the <tt>peek</tt> operation is supported by the
	 * cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>peek</tt> operation is supported by the
	 *         cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsPeek() {
		logEntry("supportsPeek", false);
		boolean supportsPeek = super.supportsPeek();
		logData("supportsPeek", Boolean.toString(supportsPeek), false);
		logEntry("supportsPeek", true);
		return supportsPeek;
	}

	/**
	 * Removes from the underlying data structure the last element returned by
	 * the cursor (optional operation). This method can be called only once per
	 * call to <tt>next</tt> or <tt>peek</tt> and removes the element returned
	 * by this method. Note, that between a call to <tt>next</tt> and
	 * <tt>remove</tt> the invocation of <tt>peek</tt> or <tt>hasNext</tt> is
	 * forbidden. The behaviour of a cursor is unspecified if the underlying
	 * data structure is modified while the iteration is in progress in any way
	 * other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>remove</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>remove</tt> operation is
	 *         not supported by the cursor.
	 */
	public void remove() throws IllegalStateException, UnsupportedOperationException {
		logEntry("remove", false);
		super.remove();
		logEntry("remove", true);
	}

	/**
	 * Returns <tt>true</tt> if the <tt>remove</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>remove</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsRemove() {
		logEntry("supportsRemove", false);
		boolean supportsRemove = super.supportsRemove();
		logData("supportsRemove", Boolean.toString(supportsRemove), false);
		logEntry("supportsRemove", true);
		return supportsRemove;
	}

	/**
	 * Replaces the last element returned by the cursor in the underlying data
	 * structure (optional operation). This method can be called only once per
	 * call to <tt>next</tt> or <tt>peek</tt> and updates the element returned
	 * by this method. Note, that between a call to <tt>next</tt> and
	 * <tt>update</tt> the invocation of <tt>peek</tt> or <tt>hasNext</tt> is
	 * forbidden. The behaviour of a cursor is unspecified if the underlying
	 * data structure is modified while the iteration is in progress in any way
	 * other than by calling this method.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @param object the object that replaces the last element returned by the
	 *        cursor.
	 * @throws IllegalStateException if the <tt>next</tt> or <tt>peek</tt> method
	 *         has not yet been called, or the <tt>update</tt> method has already
	 *         been called after the last call to the <tt>next</tt> or
	 *         <tt>peek</tt> method.
	 * @throws UnsupportedOperationException if the <tt>update</tt> operation is
	 *         not supported by the cursor.
	 */
	public void update(Object object) throws IllegalStateException, UnsupportedOperationException {
		logEntry("update", false);
		logData("update", object.toString(), true);
		super.update(object);
		logEntry("update", true);
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>update</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>update</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsUpdate() {
		logEntry("supportsUpdate", false);
		boolean supportsUpdate = super.supportsRemove();
		logData("supportsUpdate", Boolean.toString(supportsUpdate), false);
		logEntry("supportsUpdate", true);
		return supportsUpdate;
	}

	/**
	 * Resets the cursor to its initial state such that the caller is able to
	 * traverse the underlying data structure again without constructing a new
	 * cursor (optional operation). The modifications, removes and updates
	 * concerning the underlying data structure, are still persistent.
	 * 
	 * <p>Note, that this operation is optional and might not work for all
	 * cursors.</p>
	 *
	 * @throws UnsupportedOperationException if the <tt>reset</tt> operation is
	 *         not supported by the cursor.
	 */
	public void reset() throws UnsupportedOperationException {
		logEntry("reset", false);
		super.reset();
		logEntry("reset", true);
	}
	
	/**
	 * Returns <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 * the cursor. Otherwise it returns <tt>false</tt>.
	 *
	 * @return <tt>true</tt> if the <tt>reset</tt> operation is supported by
	 *         the cursor, otherwise <tt>false</tt>.
	 */
	public boolean supportsReset() {
		logEntry("supportsReset", false);
		boolean supportsReset = super.supportsReset();
		logData("supportsReset", Boolean.toString(supportsReset), false);
		logEntry("supportsReset", true);
		return supportsReset;
	}

	/**
	 * Returns the metadata information for this metadata-cursor. The return
	 * value of this method can be an arbitrary kind of metadata information,
	 * e.g., relational metadata information, therefore it is of type
	 * {@link java.lang.Object}. When using a metadata-cursor, it has to be
	 * guaranteed, that all elements contained in this metadata-cursor refer to
	 * the same metadata information. That means, every time <tt>getMetaData</tt>
	 * is called on a metadata-cursor, it should return exactly the same metadata
	 * information.
	 *
	 * @return an object representing metadata information for this
	 *         metadata-cursor.
	 */
	public Object getMetaData() {
		logEntry("getMetaData", false);
		if (!(getDecoree() instanceof MetaDataCursor))
			throw new UnsupportedOperationException("no MetaDataCursor specified.");
		Object metaData = ((MetaDataCursor)getDecoree()).getMetaData();
		logData("getMetaData", metaData.toString(), false);
		logEntry("getMetaData", true);
		return metaData;
	}
}
