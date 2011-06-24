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

package xxl.core.spatial.predicates;

import xxl.core.predicates.AbstractPredicate;
import xxl.core.spatial.points.Point;

/**
 *	The UnitCubeConstraint-predicate checks if the argument
 *	(which is assumed to be a Point) is inside the unit-cube [0;1)^dim.
 *
 */
public class UnitCubeConstraint extends AbstractPredicate {
    
    /** A default instance of this class.
     */
    public static final UnitCubeConstraint DEFAULT_INSTANCE = new UnitCubeConstraint();
    
    /** Returns the result of the predicate as a primitive boolean.
     <br>
     <pre>
     implementation:
     <code><pre>
     *               return invoke(new Object [] {argument});
     </code></pre>
     @param argument the argument to the predicate
     @return the predicate value is returned
     */
    public boolean invoke (Object argument) {
        Point p = (Point) argument;
        for(int i=0; i< p.dimensions(); i++)
            if(p.getValue(i) < 0 || p.getValue(i) >= 1)
                return false;
        return true;
    }
}
