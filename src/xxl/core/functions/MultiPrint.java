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
 * The MultiPrint-Function prints arguments to a specified print stream and returns
 * them. If no print stream is specified, the standard out is taken as default.
 * Multiple arguments will be separated by a given delimiter. Print-Functions can
 * be used to log a composition of functions at a specified level. To do so just a
 * Print-Function has to be inserted at the right place like
 * <code><pre>
 *   Function&lt;Number, Double&gt; sum = Functions.compose(Functions.composeMulti(Functions.div(), print), Functions.sin(), Functions.cos());
 * </pre></code>
 * instead of 
 * <code><pre>
 *   Function&lt;Number, Double&gt; tan = Functions.compose(div, sin, cos);
 * </pre></code>
 * 
 * @param <T> the parameter type as well as the return type of this function.
 */
@SuppressWarnings("serial")
public class MultiPrint<T> extends AbstractFunction<T, List<? extends T>> {

	/**
	 * Default Function for Print using System.out as
	 * {@link java.io.PrintStream PrintStream}, space as delimiter
	 * and no line break.
	 */
	public static final MultiPrint<Object> PRINT_INSTANCE = new MultiPrint<Object>(System.out, " ", false);

	/**
	 * Default Function for Print using System.out as
	 * {@link java.io.PrintStream PrintStream}, space as delimiter
	 * and no line break.
	 */
	public static final MultiPrint<Object> PRINTLN_INSTANCE = new MultiPrint<Object>(System.out, " ", true);

	/**
	 * The {@link java.io.PrintStream print stream} used for output.
	 */
	protected PrintStream printStream;

	/**
	 * The used delimiter to separate the given arguments.
	 */
	protected String delimiter;
	
	/**
	 * Determines whether every printed element should be followed by a line break.
	 */
	protected boolean linebreak;

	/**
	 * Constructs a new Print-Function.
	 * @param printStream the {@link java.io.PrintStream print stream} using
	 *        for the output.
	 * @param delimiter delimiter used for separating array-arguments.
	 * @param linebreak determines whether every printed element should be
	 *        followed by a line break.
	 */
	public MultiPrint(PrintStream printStream, String delimiter, boolean linebreak) {
		this.printStream = printStream;
		this.delimiter = delimiter;
		this.linebreak = linebreak;
	}

	/**
	 * Constructs a new Print-Function using a space as delimiter.
	 * 
	 * @param printStream the {@link java.io.PrintStream PrintStream} using for
	 *        the output.
	 */
	public MultiPrint(PrintStream printStream) {
		this(printStream, " ", true);
	}

	/**
	 * Constructs a new Print-Function using a space as delimiter and
	 * <code>System.out</code> as output.
	 */
	public MultiPrint() {
		this(System.out);
	}

	/**
	 * Prints the given arguments to a {@link java.io.PrintStream PrintStream}
	 * and returns the arguments.
	 * 
	 * @param arguments the arguments to print.
	 * @return the arguments given.
	 */
	@Override
	public List<? extends T> invoke(List<? extends T> arguments) {
		// arrays of length 0? -> just do nothing
		if (arguments != null && arguments.size() > 0) {
			for (int i = 0; i < arguments.size()-1; i++) {
				printStream.print(arguments.get(i));
				printStream.print(delimiter);
			}
			printStream.print(arguments.get(arguments.size()-1));
			if (linebreak)
				printStream.println();
		}
		return arguments;
	}
}
