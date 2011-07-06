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

package xxl.core.functions;

import java.io.PrintStream;
import java.util.List;

/**
 * The Print-Function prints arguments to a specified print stream and returns
 * them. If no print stream is specified, the standard out is taken as default.
 * Print-Functions can be used to log a composition of functions at a specified
 * level. To do so just a Print-Function has to be inserted at the right place
 * like
 * <code><pre>
 *   Function&lt;Number, Double&gt; tan = Functions.compose(div, Functions.compose(new Print&lt;Double&gt;(), sin), cos);
 * </pre></code>
 * instead of 
 * <code><pre>
 *   Function&lt;Number, Double&gt; tan = Functions.compose(div, sin, cos);
 * </pre></code>
 * 
 * @param <T> the parameter type as well as the return type of this function.
 */
@SuppressWarnings("serial")
public class Print<T> extends AbstractFunction<T, T> {

	/**
	 * Default Function for Print using System.out as
	 * {@link java.io.PrintStream PrintStream} and no line break.
	 */
	public static final Print<Object> PRINT_INSTANCE = new Print<Object>(System.out, false);

	/**
	 * Default Function for Print using System.out as
	 * {@link java.io.PrintStream PrintStream} and a line break.
	 */
	public static final Print<Object> PRINTLN_INSTANCE = new Print<Object>(System.out, true);

	/**
	 * The {@link java.io.PrintStream print stream} used for output.
	 */
	protected PrintStream printStream;

	/**
	 * Determines whether every printed element should be followed by a line break.
	 */
	protected boolean linebreak;

	/**
	 * Constructs a new Print-Function.
	 * @param printStream the {@link java.io.PrintStream print stream} using
	 *        for the output.
	 * @param linebreak determines whether every printed element should be
	 *        followed by a line break.
	 */
	public Print(PrintStream printStream, boolean linebreak) {
		this.printStream = printStream;
		this.linebreak = linebreak;
	}

	/**
	 * Constructs a new Print-Function using a space as delimiter.
	 * 
	 * @param printStream the {@link java.io.PrintStream PrintStream} using for
	 *        the output.
	 */
	public Print(PrintStream printStream) {
		this(printStream, true);
	}

	/**
	 * Constructs a new Print-Function using a space as delimiter and
	 * <code>System.out</code> as output.
	 */
	public Print() {
		this(System.out);
	}

	/** Prints the given argument to a {@link java.io.PrintStream PrintStream}
	 * and returns the argument itself.
	 * 
	 * @param argument the arguments to print.
	 * @return the argument given.
	 */
	@Override
	public T invoke(T argument) {
		printStream.print(argument);
		if (linebreak)
			printStream.println();
		return argument;
	}
	
	/**
	 * Checks whether the number of specified arguments is equal to one and
	 * calls {@link #invoke(Object)}. Otherwise an exception is thrown.
	 * 
	 * @param arguments the list of arguments to print.
	 * @return the argument given.
	 * @throws IllegalArgumentException if the function is not called with
	 *         exactly one argument.
	 */
	@Override
	public T invoke(List<? extends T> arguments) {
		if (arguments.size() != 1)
			throw new IllegalArgumentException("function Print can only be called with exactly one argument");
		return invoke(arguments.get(0));
	}
}
