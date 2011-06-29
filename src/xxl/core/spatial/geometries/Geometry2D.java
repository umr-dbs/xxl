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
package xxl.core.spatial.geometries;

import java.io.Serializable;

import xxl.core.spatial.rectangles.Rectangle;

/** This is the base interface of the geometry model in XXL. The model conforms to the geometry model of the OGC 
 *  <i>Simple Feature Specification for SQL (SFS)</i> <a href="http://www.opengis.org/">http://www.opengis.org</a> 
 *  which specifies spatial datatypes, as well as spatial predicates and operations on them.
 *  <br><br>
 *  The following figures give an overview of the SFS type hierarchy and its realisation in XXL.
 *  <p>
 *  <img src="doc-files/Geometry2D-1.png" alt="Figure 1: OGC Simple Features- Geometry Model">
 *  <br>The geometry model specified in the <i>OGC Simple Features Specification</i>.
 *  </p>
 *  <p>
 *  <img src="doc-files/Geometry2D-2.png" alt="Figure 2: XXL abstract Geometry Model">
 *  <br>The abstract geometry model in XXL</i>.
 *  </p>
 *  The model provides exactly the functionality specified in the SFS, except for the method- names which
 *  have been adapted to Java conventions (e.g. <code>dimensions()</code> became <code>getDimensions()</code>).
 *  <br><br> 
 *  For a concrete implementation of this geometry model have a look at the package {@link xxl.connectivity.jts}
 *  which provides the required functionality based on the Java Topology Suite API.
 *  <br><br>See <a href="./doc-files/ogc_sfs.pdf">Simple Feature Specification (pdf)</a>.
 *  @see xxl.connectivity.jts
 */
public interface Geometry2D extends Comparable<Geometry2D>,  Serializable{	
	
		/** The inherent dimension of this Geometry2D object, which must be less than or equal
		 *  to the coordinate dimension. This specification is restricted to geometries in 
		 *  two-dimensional coordinate space.
		 * @return the dimension of this Geometry2D object
		 */
		public int getDimensions();

		/** Returns the name of the instantiable subtype of Geometry2D of which this
		 *  Geometry2D instance is a member. The name of the instantiable subtype of 
		 *  Geometry2D is returned as a string.
		 *  @return the name of the instantiable subtype of Geometry2D of which this
		 *  		Geometry2D instance is a member
		 */
		public String getGeometryType();
		
		/** Returns the Spatial Reference System ID for this Geometry2D.
		 * @return the Spatial Reference System ID for this Geometry2D
		 */
		public int getSRID();
		
		/** The minimum bounding box for this Geometry2D, returned as a Geometry2D. 
		 * @return the minimum bounding box for this Geometry2D
		 */
		public Geometry2D getEnvelope();
		
		/** The minimum bounding box for this Geometry2D, returned as a Geometry2D. 
		 * @return the minimum bounding box for this Geometry2D
		 */
		public Rectangle getMBR(); 
		
		/** Exports this Geometry2D to a specific well-known text representation of Geometry2D.
		 * @return the well-known text representation of this Geometry2D object
		 */
		public String toWKT();
		
		/** Exports this Geometry2D to a specific well-known binary representation of Geometry2D.
		 * @return the well-known binary representation of this Geometry2D object
		 */
		public byte[] toWKB();	
		
		/** Returns <tt>true</tt> if this Geometry2D is the empty geometry . If true, then this
		 *  Geometry2D represents the empty point set for the coordinate space.
		 * @return <tt>true</tt> if this Geometry2D is the empty geometry, otherwise <tt>false</tt>
		 */
		public boolean isEmpty();

		/** Returns <tt>true</tt> if this Geometry2D has no anomalous geometric points, such as self
		 *  intersection or self tangency. The description of each instantiable geometric class will include the specific
		 *  conditions that cause an instance of that class to be classified as not simple.
		 * @return <tt>true</tt> if this Geometry2D has no anomalous geometric points, <tt>false</tt> otherwise
		 */
		public boolean isSimple();
		
		/** Returns the closure of the combinatorial boundary of this Geometry2D. 
		 * @return the closure of the combinatorial boundary of this Geometry2D
		 */
		public Geometry2D getBoundary(); 		
					
		/** Returns <tt>true</tt> if this Geometry2D is spatially equal to another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D is spatially equal to another Geometry2D
		 */
		public boolean equals(Geometry2D other); 
		
		/** Returns <tt>true</tt> if this Geometry2D is spatially disjoint from another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D is spatially disjoint from another Geometry2D.
		 */
		public boolean isDisjoint(Geometry2D other);
		
		/** Returns <tt>true</tt> if this Geometry2D spatially intersects another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D spatially intersects another Geometry2D.
		 */
		public boolean intersects(Geometry2D other);
		
		/** Returns <tt>true</tt> if this Geometry2D spatially touches another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D spatially touches another Geometry2D.
		 */
		public boolean touches(Geometry2D other);
		
		/** Returns <tt>true</tt> if this Geometry2D spatially crosses another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D spatially crosses another Geometry2D.
		 */
		public boolean crosses(Geometry2D other);
		
		/** Returns <tt>true</tt> if this Geometry2D is spatially within another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D is spatially within another Geometry2D.
		 */
		public boolean isWithin(Geometry2D other);
		
		/** Returns <tt>true</tt> if this Geometry2D spatially contains another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D spatially contains another Geometry2D.
		 */
		public boolean contains(Geometry2D other); 
		
		/** Returns <tt>true</tt> if this Geometry2D spatially covers another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D spatially covers another Geometry2D.
		 */
		public boolean covers(Geometry2D other);

		/** Returns <tt>true</tt> if this Geometry2D is spatially covered by another Geometry2D.
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D is spatially covered by another Geometry2D.
		 */
		public boolean isCoveredBy(Geometry2D other);
		
		/** Returns <tt>true</tt> if this Geometry2D spatially overlaps another Geometry2D. 
		 * @param other the other Geometry2D
		 * @return <tt>true</tt> if this Geometry2D spatially overlaps another Geometry2D.
		 */
		public boolean overlaps(Geometry2D other); 
		
		/** Checks if this Geometry2D is spatially related to another Geometry2D, 
		 *  by testing for intersections between the Interior, Boundary and Exterior of
		 *  the two geometries as specified by the values in the intersectionPattern
		 *  <br> 
		 * Returns <code>true</code> if the elements in the DE-9IM
		 * IntersectionMatrix for the two <code>Geometry</code>s match the elements in <code>intersectionPattern</code>.
		 * The pattern is a 9-character string, with symbols drawn from the following set:
		 *  <UL>
		 *    <LI> 0 (dimension 0)
		 *    <LI> 1 (dimension 1)
	     *    <LI> 2 (dimension 2)
	   	 *    <LI> T ( matches 0, 1 or 2)
	     *    <LI> F ( matches FALSE)
	     *    <LI> * ( matches any value)
	     *  </UL>
	     *  For more information on the DE-9IM, see the <i>OpenGIS Simple Features
	     *  Specification</i>.	   
	     *  
		 * @param other the other Geometry2D
		 * @param intersectionPattern a Stringpattern
		 * @return <tt>true</tt> if this Geometry2D is spatially related to another Geometry2D as
		 *		   specified by the values in the intersectionPattern
		 */
		public boolean relate(Geometry2D other, String intersectionPattern);
		
		
		/** Returns the shortest distance between any two points in the two geometries as calculated 
		 * in the spatial reference system of this Geometry2D. 
		 * @param other the other Geometry2D
		 * @return the shortest distance between any two points in the two geometries
		 */
		public double distance(Geometry2D other);					
				
		/** Returns a geometry that represents all points whose distance from
		 *  this Geometry2D is less than or equal to distance. Calculations are in the Spatial Reference System of this
		 *	Geometry2D. 
		 * @param width the maximum distance between a point inside the buffer and the geometry 
		 * @return a geometry that represents all points whose distance from this Geometry2D is less than or equal to distance
		 */
		public Geometry2D buffer(double width);
		
		/** Returns a geometry that represents the convex hull of this Geometry2D
		 * @return a geometry that represents the convex hull of this Geometry2D
		 */
		public Geometry2D convexHull();
		
		/** Returns a geometry that represents the point set intersection of this 
		 *  Geometry2D and another Geometry2D.
		 * @param other the other Geometry2D
		 * @return a geometry that represents the point set intersection of the two Geometry2D objects
		 */
		public Geometry2D intersection(Geometry2D other); 
		
		/** Returns a geometry that represents the point set union of this Geometry2D 
		 *  and another Geometry2D.
		 * @param other the other Geometry2D
		 * @return a geometry that represents the point set union of the two Geometry2D objects
		 */
		public Geometry2D union(Geometry2D other); 
		
		/** Returns a geometry that represents the point set difference of this Geometry2D 
		 *  and another Geometry2D.
		 * @param other the other Geometry2D
		 * @return a geometry that represents the point set difference of the two Geometry2D objects
		 */
		public Geometry2D difference(Geometry2D other); 
				
		/** Returns a geometry that represents the point set symmetric difference of this Geometry2D 
		 *  and another Geometry2D.
		 * @param other the other Geometry2D other
		 * @return a geometry that represents the point set symmetric difference of the two Geometry2D objects
		 */
		public Geometry2D symDifference(Geometry2D other);
			
}
