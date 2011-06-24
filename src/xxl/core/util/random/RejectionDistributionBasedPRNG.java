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

package xxl.core.util.random;

import xxl.core.math.functions.RealFunction;

/** The class provides a random number generator delivering random numbers distributed
 * like a user-defined function F. <BR>
 * 
 * If f(x) is the pdf of the distribution F, a function g(x) with f(x) <= c * g(x) for some constant c
 * must be given with g(x) the pdf of a distribution G.
 * Random numbers with distribution F could be obtained by the following algorithm:
 * (1) Generrate a random number x with distribution G <BR>
 * (2) calculate r = c * g(x) / f(x) <BR>
 * (3) Generrate an unifomly distribution random number u ( ~ U[0,1]) <BR>
 * (4) IF u*r < 1 return x, otherwise start again <BR>
 * To obtain random numbers x with x ~ G the {@link InversionDistributionBasedPRNG} could be used. <BR>
 * For producing random numbers use an instance of this class with the 
 * {@link xxl.core.cursors.sources.ContinuousRandomNumber ContinuousRandomNumber cursor}.
 */

public class RejectionDistributionBasedPRNG implements ContinuousRandomWrapper{

	/** pdf of the distribution F */
	RealFunction f;

	/** pdf of the distribution G */
	RealFunction g;

	/** some constant with f(x) <= c * g(x) f.a. x */
	double c;

	/** random wrapper delivering random numbers x with x ~ G */
	ContinuousRandomWrapper crwG;

	/** random wrapper delivering random numbers u with u ~ U[0,1] */
	ContinuousRandomWrapper crw;

	/** Constructs a new object of the class.
	 *
	 *
	 * @param crw random wrapper delivering random numbers u with u ~ U[0,1]
	 * @param f pdf of the distribution F the produced random numbers should belong to
	 * @param g pdf of the distribution G with f(x) <= c * g(x) f.a. x
	 * @param c the constant c in f(x) <= c * g(x) f.a. x
	 * @param crwG random wrapper delivering random numbers x with x ~ G
	 *
	 * @see InversionDistributionBasedPRNG
	 * @see ContinuousRandomWrapper
	 * @see xxl.core.math.functions.RealFunction
	 */	 
	public RejectionDistributionBasedPRNG( ContinuousRandomWrapper crw, RealFunction f, RealFunction g, double c, ContinuousRandomWrapper crwG){
		this.crw = crw;
		this.f = f;
		this.g = g;
		this.c = c;
		this.crwG = crwG;
	}

	/** Return the next computed pseudo random number x with x ~ F.
	 * @return the next computed pseudo random number x with x ~ F.
	 */
	public double nextDouble(){
		double x = -1.0;
		boolean found = false;
		do{
			x = crwG.nextDouble();
			double r = c * g.eval( x) / f.eval( x);
			double u = crw.nextDouble();
			if ( u*r < 1.0)
				found = true;
		}
		while (! found);
		return x;
	}
}
