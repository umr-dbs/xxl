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

/**
 * Objects implementing this interface can be classified. <br>
 * That means each object is assigned to exactly one cluster.
 * The ID of this cluster can be enquired and changed.
 * Furthermore each object has to provide an internal status
 * that shows if the object has already been classified. <p>
 *
 * However, clustering algrithms make extremely use of objects
 * implementing this interface.
 *
 * @see xxl.core.cursors.groupers.DBScan
 *
 */
public interface Classifiable {

	/**
	 * Returns <tt>true</tt>, if the object has already been classified.
	 *
	 * @return <tt>true</tt> if the object has already been classified, <tt>false</tt> otherwise.
	 */
	public abstract boolean isClassified ();

	/**
	 * Returns the cluster ID of this object.
	 *
	 * @return the cluster ID of this object.
	 */
	public abstract long getClusterID ();

	/**
	 *	Sets the cluster ID of this object.
	 *
	 * @param CLUSTER_ID the new cluster ID of this object.
	 */
	public abstract void setClusterID (long CLUSTER_ID);

}
