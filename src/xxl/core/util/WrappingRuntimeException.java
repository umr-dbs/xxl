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

package xxl.core.util;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * This Exception is useful for wrapping arbitrary Exceptions. <br>
 * A RuntimeException can be thrown at any place in the code
 * and does not have to be declared in the throws-clause. <br>
 * This means that by catching a non-RuntimeException and wrapping it,
 * this exception can be thrown at any place in the code even in
 * constructors.<br>
 * More detailled: This class is a subclass of the class <tt>RuntimeException</tt>,
 * therefore a method is not required to declare in its <code>throws</code>
 * clause any subclasses of <code>RuntimeException</code> that might
 * be thrown during the execution of the method but not caught. <p>
 *
 * <b>Note:</b> The <code>Throwable</code> class is the superclass of all errors
 * and exceptions in the Java language. Only objects that are
 * instances of this class (or of one of its subclasses) are thrown
 * by the Java Virtual Machine or can be thrown by the Java
 * <code>throw</code> statement. Similarly, only this class or one of
 * its subclasses can be the argument type in a <code>catch</code>
 * clause.
 * <p>
 * Instances of two subclasses, {@link java.lang.Error} and
 * {@link java.lang.Exception}, are conventionally used to indicate
 * that exceptional situations have occurred. Typically, these instances
 * are freshly created in the context of the exceptional situation so
 * as to include relevant information (such as stack trace data).
 * <p>
 * By convention, class <code>Throwable</code> and its subclasses have
 * two constructors, one that takes no arguments and one that takes a
 * <code>String</code> argument that can be used to produce an error
 * message.
 * <p>
 * A <code>Throwable</code> class contains a snapshot of the
 * execution stack of its thread at the time it was created. It can
 * also contain a message string that gives more information about
 * the error.<p>
 *
 * A WrappingRuntimeExcpetion passes by all method calls to it using the methods
 * of the exception to be wrapped.
 *
 * Example demonstrating the usage of a WrappingRuntimeException:
 * <br><br>
 * <code><pre>
 * 	class TestWrappingRuntimeException {
 *
 * 	// common handling of exceptions
 * 	public void handlingNoRuntimeExceptions (int i) throws IOException, NoSuchMethodException, IllegalAccessException {
 * 		switch(i) {
 * 				case 1: throw new IOException("IOException geworfen!");
 * 				case 2: throw new NoSuchMethodException("NoSuchMethodException geworfen!");
 * 				case 3: throw new IllegalAccessException("IllegalAccessException geworfen!");
 * 		}
 * 	}
 *
 * 	// using a WrappingRuntimeException instead
 * 	public void usingWrappingRuntimeException (int i) {
 * 		try {
 * 			switch(i) {
 * 				case 1: throw new IOException("IOException geworfen!");
 * 				case 2: throw new NoSuchMethodException("NoSuchMethodException geworfen!");
 * 				case 3: throw new IllegalAccessException("IllegalAccessException geworfen!");
 * 			}
 * 		}
 * 		catch (Exception e) {
 * 			throw new WrappingRuntimeException(e);
 * 		}
 * 	}
 *
 * 	public static void main (String[] args) {
 *
 * 		TestWrappingRuntimeException t = new TestWrappingRuntimeException();
 *
 * 		for(int i=1; i<=3; i++) {
 *
 * 			// common handling of exceptions
 * 			try {
 * 				t.handlingNoRuntimeExceptions(i);
 * 			}
 * 			catch (IOException ie) {
 * 				System.out.println(ie.toString());
 * 			}
 * 			catch (NoSuchMethodException nsme) {
 * 				System.out.println(nsme.toString());
 * 			}
 * 			catch (IllegalAccessException iae) {
 * 				System.out.println(iae.toString());
 * 			}
 *
 * 			// using a WrappingRuntimeException instead
 * 			try {
 * 				t.usingWrappingRuntimeException(i);
 * 			}
 * 			catch (WrappingRuntimeException e) {
 * 				System.out.println(e.toString());
 * 			}
 * 		}
 * 	}
 * }
 * </code></pre>
 * This is example shows the difference between the
 * handling with NonRuntimeException and using a WrappingRuntimeException
 * which wraps these NonRuntimeExceptions to a RuntimeException that
 * can be catched later with the intention to extract the original error
 * message.<p>
 * This action is especially useful if the user does not want (or even is
 * not able) to declare the possibly thrown Non-Runtime-Exceptions
 * in each method. For example a method may throw a Non-Runtime-Exception
 * and this method is used in an other method of an other class. Now the
 * user does not want to handle this Non-Runtime-Exception explicitely, so
 * he can use a WrappingRuntimeException instead and therefore he is able
 * throw an exception without declaring it in the throws-clause.
 * Later, even in an other class, a method can catch this WrappingRuntimeException
 * and extract the original error message calling the WrappingRuntimeException's
 * method <code>toString</code>.
 *
 * @see java.lang.Throwable
 * @see java.lang.Exception
 * @see java.lang.RuntimeException
 * @see xxl.core.io
 */
public class WrappingRuntimeException extends RuntimeException {

	/** The exception to be wrapped to a <tt>RuntimeException</tt>. */
	public final Throwable throwable;

	/**
	 * Creates a new WrappingRuntimeException.
	 *
	 * @param throwable the non-RuntimeException to be wrapped.
	 */
	public WrappingRuntimeException (Throwable throwable) {
		this.throwable = throwable;
	}

	/**
	 * Returns the message string of the wrapped exception (a throwable object). <br>
	 * The call is passed by, i.e. <code>throwable.getMessage()</code> is returned.
	 *
	 * @return the error message string of the wrapped exception
	 * 		if it was {@link java.lang.Throwable#Throwable(String) created} with an
	 * 		error message string; or <code>null</code> if it was
	 * 		{@link java.lang.Throwable#Throwable() created} with no error message.
	 */
	public String getMessage () {
		return throwable.getMessage();
	}

	/**
	 * Returns the localized description of the wrapped exception in order to produce a
	 * locale-specific message. <br>If the class of the wrapped exception does not override this
	 * method, the default implementation returns the same result as
	 * <code>getMessage()</code>.
	 * The call is passed by, i.e. <code>throwable.getLocalizedMessage()</code> is returned.
	 *
	 * @return the localized description of the wrapped exception.
	 */
	public String getLocalizedMessage () {
		return throwable.getLocalizedMessage();
	}

	/**
	 * Returns a short description of the wrapped exception. <br>
	 * If the wrapped exception was
	 * {@link java.lang.Throwable#Throwable(String) created} with an error message string,
	 * then the result is the concatenation of three strings:
	 * <ul>
	 * <li>The name of the actual class of this object
	 * <li>": " (a colon and a space)
	 * <li>The result of the {@link #getMessage()} method for this object
	 * </ul>
	 * If the wrapped exception was {@link java.lang.Throwable#Throwable() created}
	 * with no error message string, then the name of the actual class of
	 * this object is returned.
	 * The call is passed by, i.e. <code>throwable.toString()</code> is returned.
	 *
	 * @return a string representation of the wrapped exception.
	 */
	public String toString () {
		return throwable.toString();
	}

	/**
	 * Prints the wrapped exception and its backtrace to the
	 * standard error stream. <br>This method prints a stack trace for the
	 * wrapped exception object on the error output stream that is
	 * the value of the field <code>System.err</code>. The first line of
	 * output contains the result of the {@link #toString()} method for
	 * this object. Remaining lines represent data previously recorded by
	 * the method {@link #fillInStackTrace()}. <br>The format of this
	 * information depends on the implementation, but the following
	 * example may be regarded as typical:
	 * <blockquote><pre>
	 * java.lang.NullPointerException
	 * 		at MyClass.mash(MyClass.java:9)
	 * 		at MyClass.crunch(MyClass.java:6)
	 * 		at MyClass.main(MyClass.java:3)
	 * </pre></blockquote>
	 * This example was produced by running the program:
	 * <blockquote><pre>
	 *
	 * class MyClass {
	 *
	 * 		public static void main(String[] argv) {
	 * 			crunch(null);
	 * 		}
	 * 		static void crunch(int[] a) {
	 * 			mash(a);
	 * 		}
	 *
	 * 		static void mash(int[] b) {
	 * 		System.out.println(b[0]);
	 * 		}
	 * }
	 * </pre></blockquote>
	 * The call is passed by, i.e. <code>throwable.printStackTrace()</code> is returned.
	 *
	 * @see java.lang.System#err err
	 */
	public void printStackTrace () {
		throwable.printStackTrace();
	}

	/**
	 * Prints the wrapped excpetion and its backtrace to the
	 * specified print stream. <br>
	 * The call is passed by, i.e. <code>throwable.printStackTrace(s)</code> is returned.
	 * @param s output print stream 
	 */
	public void printStackTrace (PrintStream s) {
		throwable.printStackTrace(s);
	}

	/**
	 * Prints the wrapped exception and its backtrace to the specified
	 * print writer. <br>
	 * The call is passed by, i.e. <code>throwable.printStackTrace(s)</code> is returned.
	 * 	@param s output print writer 
	 */
	public void printStackTrace (PrintWriter s) {
		throwable.printStackTrace(s);
	}

	/**
	 * Fills in the execution stack trace. This method records within 
	 * this Throwable object information about the current state of 
	 * the stack frames for the current thread.
	 * @see java.lang.Throwable#fillInStackTrace()
	 */
	public synchronized Throwable fillInStackTrace() {
		if (throwable==null)
			return super.fillInStackTrace();
		else
			return throwable.fillInStackTrace();
	}

	/**
	 * Returns the cause of this throwable or null if the cause is 
	 * nonexistent or unknown. (The cause is the throwable that 
	 * caused this throwable to get thrown.)
	 * @see java.lang.Throwable#getCause()
	 */
	public Throwable getCause() {
		return throwable.getCause();
	}

	/**
	 * Provides programmatic access to the stack trace information 
	 * printed by printStackTrace(). Returns an array of stack trace 
	 * elements, each representing one stack frame. The zeroth element 
	 * of the array (assuming the array's length is non-zero) 
	 * represents the top of the stack, which is the last method 
	 * invocation in the sequence. Typically, this is the point 
	 * at which this throwable was created and thrown. The last 
	 * element of the array (assuming the array's length is non-zero) 
	 * represents the bottom of the stack, which is the first method 
	 * invocation in the sequence.
	 * @see java.lang.Throwable#getStackTrace()
	 */
	public StackTraceElement[] getStackTrace() {
		return throwable.getStackTrace();
	}

	/**
	 * Initializes the cause of this throwable to the specified 
	 * value. (The cause is the throwable that caused this 
	 * throwable to get thrown.)
	 * @see java.lang.Throwable#initCause(java.lang.Throwable)
	 */
	public synchronized Throwable initCause(Throwable cause) {
		return throwable.initCause(cause);
	}

	/**
	 * Sets the stack trace elements that will be returned by 
	 * getStackTrace() and printed by printStackTrace() and related 
	 * methods. This method, which is designed for use by RPC 
	 * frameworks and other advanced systems, allows the client 
	 * to override the default stack trace that is either generated 
	 * by fillInStackTrace()  when a throwable is constructed or 
	 * deserialized when a throwable is read from a serialization 
	 * stream.
	 * @see java.lang.Throwable#setStackTrace(java.lang.StackTraceElement[])
	 */
	public void setStackTrace(StackTraceElement[] stackTrace) {
		throwable.setStackTrace(stackTrace);
	}
}
