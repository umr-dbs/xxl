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

package xxl.core.io.fat;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * This iterator returns filtered entries of the directory. The user can specify by
 * flags which entries should be filtered. The allowed flags are the constant LIST_XXX
 * values of the DIR class.
 */
public class EntriesFilterIterator implements Iterator
{
	/**
	 * Iterator which iterates over directory entries
	 */
	protected Iterator entriesIterator;
	
	/**
	 * Indicates which directory entries should be filtered.
	 */
	protected int listEntryAttribute;
	
	/**
	 * Indicates if there is one more entry.
	 */
	protected boolean hasNext;
	
	/**
	 * Used to increase performance.
	 */
	private boolean isReady;
	
	/**
	 * The actual directory entry.
	 */
	protected DirectoryEntryInformation dirEntInf;
	
	
	/**
	 * Creates an instance of this object.
	 * @param entriesIterator iterator that iterates of directory entries.
	 * @param listEntryAttribute indicates the entries that should be filtered.
	 * The allowed attributes are the LIST_XXX values of the DIR class.
	 */
	public EntriesFilterIterator(Iterator entriesIterator, int listEntryAttribute)
	{
		this.entriesIterator = entriesIterator;
		this.listEntryAttribute = listEntryAttribute;
		hasNext = false;
		isReady = false;
	}	//end constructor
	
	
	/**
	 * Returns true if the iteration has more elements. (In other words,
	 * returns true if next would return an element rather than throwing an exception.)
	 * @return true if the iterator has more elements.
	 */
	public boolean hasNext()
	{			
		if (hasNext)
		{
			return hasNext;
		}
		else if (isReady)
			return false;
		else	//calculate the next entry
		{
			while (entriesIterator.hasNext())
			{
				dirEntInf = (DirectoryEntryInformation)entriesIterator.next();
				
				//in case the free entries should not be returned stop the iteration at the last entry.
				if (!((listEntryAttribute & DIR.LIST_FREE_ENTRY) > 0) &&
					DIR.isLastEntry(dirEntInf.directoryEntry))
				{
					isReady = true;
					hasNext = false;
					return hasNext;
				}
				
				if ((listEntryAttribute & DIR.LIST_DOT_AND_DOTDOT_ENTRY) > 0 &&
					DIR.isDirectory(dirEntInf.directoryEntry))
				{
					hasNext = true;
					return hasNext;
				}
				else if (	(listEntryAttribute & DIR.LIST_DIRECTORY_ENTRY) > 0 &&
							DIR.isDirectory(dirEntInf.directoryEntry) &&
							!DIR.isDotEntry(dirEntInf.directoryEntry) &&
							!DIR.isDotDotEntry(dirEntInf.directoryEntry))
				{
					hasNext = true;
					return hasNext;
				}
				else if (	(listEntryAttribute & DIR.LIST_FILE_ENTRY) > 0 &&
							(DIR.isFile(dirEntInf.directoryEntry) || DIR.isLongEntry(dirEntInf.directoryEntry)))
				{
					//file
					hasNext = true;
					return hasNext;
				}
				else if (	(listEntryAttribute & DIR.LIST_ROOT_ENTRY) > 0 &&
							DIR.isRootDirectory(dirEntInf.directoryEntry))
				{
					//root
					hasNext = true;
					return hasNext;
				}
				else if (	(listEntryAttribute & DIR.LIST_FREE_ENTRY) > 0 &&
							DIR.isFree(dirEntInf.directoryEntry))
				{
					//free
					hasNext = true;
					return hasNext;
				}
			}	//end while
			hasNext = false;
			return hasNext;	
		}
	}	//end hasNext()
	
	
	/**
	 * Returns the next element in the iteration.
	 * @return the next element in the iteration.
	 * @throws NoSuchElementException iteration has no more elements.
	 */
	public Object next() throws NoSuchElementException
	{
		if (hasNext || hasNext())
		{
			hasNext = false;
			return dirEntInf;
		}
		throw new NoSuchElementException();
	}	//end next()
	
	
	/**
	 * Removes from the underlying collection the last element that was returned
	 * by the iterator (optional operation). This method can be called
	 * only once for each next method call. The behavior of an iterator is
	 * unspecified if the underlying collection has been modified during the
	 * iteration progress in any other way than this method.
	 */
	public void remove()
	{
		entriesIterator.remove();
	}	//end remove()
}	//end class EntriesFilterIterator
