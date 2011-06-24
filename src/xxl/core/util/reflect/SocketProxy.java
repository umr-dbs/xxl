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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import xxl.core.io.CounterInputStream;
import xxl.core.io.CounterOutputStream;
import xxl.core.util.WrappingRuntimeException;

/**
 * This class provides a general Proxy that can be used
 * with every interface. It uses the reflection mechanism that
 * was introduced with jdk 1.3.
 * <p>
 * The client proxy sends all requests to a server via a 
 * socket connection. The server returns results in the same
 * way. Every Object that is transferred has to support the
 * serialisable Interface.
 * <p>
 * This is a very useful class in a distributed environment.
 * <p>
 * The example with no parameters makes some communications
 * on the local host. If you pass 2 or more parameters, the
 * example works in client/server mode and makes the connections
 * over the network. The possible parameters are:
 * <ol>
 * <li>port number</li>
 * <li>'client' or 'server'</li>
 * <li>only in 'client'-mode: name of the server (e.g. localhost or ip-address</li>
 * </ol>
 */
public class SocketProxy implements InvocationHandler {
	/** Socket to use */
	private Socket socket;
	/** InputStream of the socket */
	private ObjectInputStream in;
	/** OutputStream of the socket */
	private ObjectOutputStream out;
	
	/**
	 * Produces dynamically a SocketProxy for the specified interface and object and 
	 * returns it.
	 *
	 * @param address Address where to find the original object
	 * @param port Port to reach the original object
	 * @param interfaceName name of the interface
	 * @return returns the dynamic proxy (in [0] and an access object 
	 *	of class SocketProxy (for shuting down the server).
	 */
	public static Object[] newClientInstance(InetAddress address, int port, String interfaceName) {
		try {
			Class c = Class.forName(interfaceName);
			
			SocketProxy sp = new SocketProxy(address,port);
			return new Object[] {
				Proxy.newProxyInstance(
					Thread.currentThread().getContextClassLoader(),
					new Class[] { c },
					sp
				),
				sp
			};	
		}
		catch (ClassNotFoundException e) {
			return null;
		}
	}

	/**
	 * Starts a server object and starts listening for client access.
	 *
	 * @param port Port to reach the original object
	 * @param origObject object that can be called from a client.
	 * @param newThread running in a new thread or in the same (blocking until termination
	 *	of the server).
	 * @param ps PrintStream for output of statistical information (e.g. System.out). If out==null, then
	 *	no statistical data is collected and printed.
	 */
	public static void newServerInstance(final int port, final Object origObject, boolean newThread, final PrintStream ps) {
		Thread thread = new Thread() {
			public void run() {
				try {
					ServerSocket s = new ServerSocket(port);
					Socket socket = s.accept();
					// The following line is very important if you have
					// only one sending task and small packets.
					socket.setTcpNoDelay(true);
					InputStream inRaw = socket.getInputStream();
					OutputStream outRaw = socket.getOutputStream();
					
					if (ps!=null) {
						inRaw = new CounterInputStream(inRaw);
						outRaw = new CounterOutputStream(outRaw);
					}
					
					ObjectInputStream inServer = new ObjectInputStream(inRaw);
					ObjectOutputStream outServer = new ObjectOutputStream(outRaw);
					
					System.out.println("Connect successful");
					
					Object args[],o;
					Class parTypes[];
					String methodName;
					
					Class cl;
					Method meth;

					while (inServer.read()!=0) {
						methodName = (String) inServer.readObject();
						parTypes = (Class[]) inServer.readObject();
						args = (Object[]) inServer.readObject();
						cl = origObject.getClass();
						meth = cl.getMethod(methodName,parTypes);
						
						o = meth.invoke(origObject,args);
						outServer.writeObject(o);
						outServer.flush();
					}
					
					inServer.close();
					outServer.close();
					socket.close();
					s.close();

					if (ps!=null) {
						CounterInputStream iC = (CounterInputStream) inRaw;
						CounterOutputStream oC = (CounterOutputStream) outRaw;

						ps.println(iC.toString());
						ps.println(oC.toString());
					}
				}
				// don't know what to do with exceptions!
				catch (Exception e) {
					e.printStackTrace();
				}
				/* Exceptions that can occur:
				catch (IOException e) {}
				catch (ClassNotFoundException e) {}
				catch (NoSuchMethodException e) {}
				catch (IllegalAccessException e) {}
				catch (InvocationTargetException e) {}
				*/
				// Server is terminating
			}
		};
		if (newThread)
			thread.start();
		else
			thread.run();
	}

	/**
	 * Creates a new socket proxy and connects it to the specified port number at
	 * the specified IP address.
	 * 
	 * @param address the IP address.
	 * @param port the port number.
	 */
	private SocketProxy(InetAddress address, int port) {

		try {
			this.socket = new Socket(address,port);
			// The following line is very important if you have
			// only one sending task and small packets.
			socket.setTcpNoDelay(true);
			this.out = new ObjectOutputStream(socket.getOutputStream());
			this.in = new ObjectInputStream(socket.getInputStream());
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	/**
	 * Frees the resources used and sends a shutdown message to the server 
	 * object.
	 */
	public void shutdown() {
		try {
			out.write(0);
			out.flush();
			out.close();
			in.close();
			socket.close();
		}
		catch (IOException e) {
			throw new WrappingRuntimeException(e);
		}
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
		try {
			out.write(1);
			out.writeObject(meth.getName());
			out.writeObject(meth.getParameterTypes());
			out.writeObject(args);
			out.flush();
			return in.readObject();
		}
		catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}
}
