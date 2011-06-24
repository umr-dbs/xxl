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

package xxl.core.indexStructures;

import java.util.Stack;

import xxl.core.cursors.AbstractCursor;

/** This class represents lazy <tt>Cursors</tt> which can be used to perform queries on a <tt>Tree</tt>. For example 
 * see {@link BPlusTree.QueryCursor}. 
 */
public abstract class QueryCursor extends AbstractCursor {
	
	/** The <tt>Tree</tt> in which the query has to be executed.
	 */
	protected Tree tree;
	
	/** The <tt>IndexEntry</tt> refering to the current <tt>Node</tt>.
	 */
	protected Tree.IndexEntry indexEntry;
	
	/** A <tt>Descriptor</tt> which specifies the query.
	 */
	protected Descriptor queryRegion;
	
	/** The target level of the query.
	 */
	protected int targetLevel;
	
	/** A <tt>Stack</tt> to hold the path from the root to the current <tt>Node</tt>. 
	 */
	protected Stack path;
	
	/** The current <tt>Node</tt> in which the query is at the moment executed.
	 */
	protected Tree.Node currentNode;
	
	/** Creates a new <tt>QueryCursor</tt>.
	 * 
	 * @param tree the <tt>Tree</tt> in which the query has to be executed
	 * @param subRootEntry the <tt>IndexEntry</tt> which is the root of the subtree in which the query has to be 
	 * exectuted
	 * @param queryRegion a <tt>Descriptor</tt> which specifies the query
	 * @param targetLevel the target level of the query
	 */
	public QueryCursor(Tree tree, Tree.IndexEntry subRootEntry, Descriptor queryRegion, int targetLevel) {		
		this.tree= tree;
		this.indexEntry= subRootEntry;
		this.queryRegion= queryRegion;
		this.targetLevel= targetLevel;
		path= null;
		currentNode=null;
	}
	
	/** In the default implementation it returns <tt>true</tt>.
	 * 
	 * @return <tt>true</tt>
	 * 
	 * @see AbstractCursor#supportsRemove()
	 */
	public boolean supportsRemove() {
		return true;
	}

	/** In the default implementation it returns <tt>true</tt>.
	 * 
	 * @return <tt>true</tt>
	 * @see AbstractCursor#supportsUpdate()
	 */	
	public boolean supportsUpdate() {
		return true;
	}
	
	/** Removes from the underlying <tt>Tree</tt> the last element returned by
	 * the method next(). The default implementation is equivalent to:
	 * <pre><code>
	  				super.remove();
	  				removeObject();
	   </code></pre>
	 */ 	
	public void remove() {
		super.remove();
		removeObject();
	}

	/** Replaces the last element returned by the method next() by the given data object. The default implementation 
	 * is equivalent to:
	 * <pre><code>
	  				super.update(data);
	  				updateObject(data);
	   </code></pre>
	 *  
	 * @param data the object replacing the last object returned by next()  
	 */ 			
	public void update(Object data) {
		super.update(data);
		updateObject(data);
	}
				
	/** It is used to empty the path.
	 */ 
	protected void abolishPath() {
		if(hasPath()) {
			while(!path.isEmpty()) tree.up(path);
			path=null;
		}
	}
	
	/** Gives the current path.
	 * 
	 * @return the current path
	 */			
	public Stack path() {
		return path;
	}

	/** Checks whether the path to the current <tt>Node</tt> exists.
	 * 
	 * @return <tt>false</tt> if path is <tt>null</tt> and <tt>true</tt> otherwise.
	 */
	protected boolean hasPath(){
		return this.path()!=null;
	}
	
	/** This method is called from the method {@link QueryCursor#remove()} and takes over the real work to 
	 * remove an element from the <tt>QueryCursor</tt>. The user has to implement it so that it makes sense in the 
	 * respective <tt>Tree</tt>.
	 */			
	protected abstract void removeObject();

	/** This method is called from the method {@link QueryCursor#update(Object)} and takes over the real work to 
	 * update an element from the <tt>QueryCursor</tt>. The user has to implement it so that it makes sense in the 
	 * respective <tt>Tree</tt>.
	 * 
	 * @param data the new object by which the old object has to be replaced
	 */				
	protected abstract void updateObject(Object data);
}
