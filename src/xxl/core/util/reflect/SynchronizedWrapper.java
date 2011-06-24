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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * This class wraps a given Object that implements a known interface,
 * so that all Methods become synchronized. It uses the reflection mechanism that
 * was introduced with jdk 1.3.
 */
public class SynchronizedWrapper implements InvocationHandler {
	/** Object that should be synchronized */
	private Object delegate;

	/** 
	 * Produces dynamically a SynchronizedWrapper for the specified interface and object and 
	 * returns it.
	 *
	 * @param delegate Object to which the calls are delegated.
	 * @param interfaceName name of the interface
	 * @return returns a SynchronizedWrapper produced
	 * 
	 */
	public static Object newInstance(Object delegate, String interfaceName) {
		try {
			Class c = Class.forName(interfaceName);

			return Proxy.newProxyInstance(
				Thread.currentThread().getContextClassLoader(),
				new Class[] { c },
				new SynchronizedWrapper(delegate)
			);
		}
		catch (ClassNotFoundException e) {
			return null;
		}
	}

	/**
	 * Creates a SynchronizedWrapper (private!).
	 * 
	 * @param delegate Object to which the calls are delegated.
	 */
	private SynchronizedWrapper(Object delegate) {
		this.delegate = delegate;
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
	public synchronized Object invoke(Object proxy, Method meth, Object[] args) throws Throwable {
		try {	
			return meth.invoke(delegate, args);
		}
		catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
	}

}
