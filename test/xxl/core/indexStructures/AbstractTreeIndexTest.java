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

import java.io.IOException;

/**
 * 
 * Skeleton class for developing test for tree class
 *
 * @param <T>
 */
public abstract class AbstractTreeIndexTest<T extends Tree> {

	

	public abstract void prepairIndex();
	
	
	public abstract Object[][] createTestParameter(); 
	
	
	
	public void testTree(Object... args){
		//1.
		loadDataAndSaveTree(args);
		//2.
		reloadRemoveAndThenInsertData(args);
		//3. 
		reloadAndUpdateData(args);
		//4.
		reloadAndQueryTree(args);
		//5. 
		reloadAndAddData(args);
		//6.
		reloadAndQueryAgain(args);
	}
	
	
	protected abstract void saveTree(T tree, String path) throws IOException;
	
	protected abstract T reloadTree(T tree, String path, Object... args) throws IOException;
	
	protected abstract void loadDataAndSaveTree(Object...args);
	
	protected abstract void reloadRemoveAndThenInsertData(Object...args);
	
	protected abstract void reloadAndUpdateData(Object...args);
	
	protected abstract void reloadAndQueryTree(Object...args);
	
	protected abstract void reloadAndAddData(Object...args);
	
	protected abstract void reloadAndQueryAgain(Object...args);
	
}
