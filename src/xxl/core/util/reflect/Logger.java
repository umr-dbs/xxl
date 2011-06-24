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

package xxl.core.util.reflect;

import java.io.PrintStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;

import xxl.core.collections.containers.Container;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.MetaDataCursor;

/**
 * This class provides a general Logger that can be used
 * with every interface. It uses the reflection mechanism that
 * was introduced with jdk 1.3. The logger forwards the calls
 * to a different instance of the same interface (principle of
 * a decorator).
 * <p>
 * For conveniance, some classes of xxl are directly supported.
 */
public class Logger implements InvocationHandler {
	/** Object to log */
	private Object delegate;
	/** Printstream to write on */
	private PrintStream out;

	/** 
	 * Produces dynamically a Logger for the specified interface and object and 
	 * returns it.
	 *
	 * @param delegate Object to which the calls are delegated.
	 * @param interfaceName name of the interface
	 * @param out PrintStream which is used for ouput.
	 * 
	 * @return returns the Logger created
	 */
	public static Object newInstance(Object delegate, String interfaceName, PrintStream out) {
		try {
			Class c = Class.forName(interfaceName);

			return Proxy.newProxyInstance(
				Thread.currentThread().getContextClassLoader(),
				new Class[] { c },
				new Logger(delegate, out)
			);
		}
		catch (ClassNotFoundException e) {
			return null;
		}
	}

	/**
	 * Returns a logger for a MetaDataCursor.
	 * 
	 * @param mdc MetaDataCursor to be logged.
	 * @param out PrintStream which is used for ouput.
	 * @return returns a logger for a MetaDataCursor.
	 */
	public static MetaDataCursor getMetaDataCursorLogger(MetaDataCursor mdc, PrintStream out) {
		return (MetaDataCursor) newInstance(mdc,"xxl.core.cursors.MetaDataCursor",out);
	}

	/**
	 * Returns a logger for a Cursor.
	 * 
	 * @param c Cursor to be logged.
	 * @param out PrintStream which is used for ouput.
	 * @return returns a logger for a Cursor.
	 */
	public static Cursor getCursorLogger(Cursor c, PrintStream out) {
		return (Cursor) newInstance(c,"xxl.core.cursors.Cursor",out);
	}

	/**
	 * Returns a logger for an Iterator.
	 * 
	 * @param it Iterator to be logged.
	 * @param out PrintStream which is used for ouput.
	 * @return returns a logger for a Iterator.
	 */
	public static Iterator getIteratorLogger(Iterator it, PrintStream out) {
		return (Iterator) newInstance(it,"java.util.Iterator",out);
	}

	/**
	 * Returns a logger for a Container.
	 * 
	 * @param container Container to be logged.
	 * @param out PrintStream which is used for ouput.
	 * @return returns a logger for a Container.
	 */
	public static Container getContainerLogger(Container container, PrintStream out) {
		return (Container) newInstance(container,"xxl.core.collections.containers.Container",out);
	}

	/**
	 * Creates a logger (private!).
	 * 
	 * @param delegate Object to which the calls are delegated.
	 * @param out PrintStream which is used for ouput.
	 */
	private Logger(Object delegate, PrintStream out) {
		this.delegate = delegate;
		this.out = out;
	}

	/**
	 * This method is invoked automatically by the proxy. It is 
	 * unusual to call this method directly.
	 *
	 * @param proxy the proxy instance that the method was invoked on
	 * @param meth the Method instance corresponding to the interface method 
	 * 	invoked on the proxy instance. The declaring class of the Method 
	 *	object will be the interface that the method was declared in, which 
	 *	may be a superinterface of the proxy interface that the proxy class 
	 *	inherits the method through.
	 * @param args an array of objects containing the values of the arguments passed 
	 *	in the method invocation on the proxy instance, or null if interface 
	 *	method takes no arguments. Arguments of primitive types are wrapped in 
	 *	instances of the appropriate primitive wrapper class, such as 
	 *	java.lang.Integer or java.lang.Boolean.
	 * @return the value to return from the method invocation on the proxy instance. 
	 *	If the declared return type of the interface method is a primitive type, 
	 *	then the value returned by this method must be an instance of the 
	 *	corresponding primitive wrapper class; otherwise, it must be a type 
	 *	assignable to the declared return type. If the value returned by this 
	 *	method is null and the interface method's return type is primitive, then 
	 *	a NullPointerException will be thrown by the method invocation on the 
	 *	proxy instance. If the value returned by this method is otherwise not 
	 *	compatible with the interface method's declared return type as described 
	 *	above, a ClassCastException will be thrown by the method invocation on 
	 *	the proxy instance.	
	 * @throws Throwable
	 */
	public Object invoke(Object proxy, Method meth, Object[] args) throws Throwable {
		if (args==null)
			out.println("Logger\tbefore\t"+meth.getName()+"\t"+delegate.getClass().getName()+"\tParam:\t0");
		else {
			out.print("Logger\tbefore\t"+meth.getName()+"\t"+delegate.getClass().getName()+"\tParam:\t0\t"+args.length);
			for (int i=0 ; i<args.length ; i++)
				out.print("\t"+args[i]);
			out.println();
		}
		
		// Get stack trace
		try {
			throw new RuntimeException();
		}
		catch (Exception e) {
			out.print("Logger\ttrace\t");
			StackTraceElement st[] = e.getStackTrace();
			for (int i=2; i<st.length; i++) {
				if (i!=1)
					out.print(" \t");
				out.print(st[i].getClassName()+"."+st[i].getMethodName()+":"+st[i].getLineNumber());
			}
			out.println();
		}
		
		try {	
			Object o = meth.invoke(delegate, args);
			out.println("Logger\tafter\t"+meth.getName()+"\t"+delegate.getClass().getName()+"\tOutput\t"+o);
			return o;
		}
		catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
	}
}
