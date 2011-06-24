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

package xxl.core.collections.containers.recordManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;

import xxl.core.collections.containers.recordManager.RecordManager.PageInformation;

public class HybridStrategy extends AbstractStrategy {
	protected AbstractStrategy s1;
	protected AbstractStrategy s2;

	public HybridStrategy(AbstractStrategy s1, AbstractStrategy s2) {
		this.s1 = s1;
		this.s2 = s2;
	}
	public Strategy getFirstStrategy(){
	    return s1;
	}
	public Strategy getSecondStrategy(){
	    return s2;
	}
	public Object getPageForRecord(int bytesRequired) {
		Object page1 = s1.getPageForRecord(bytesRequired);
		if (page1!=null)
			return page1;
		else
			return s2.getPageForRecord(bytesRequired);
	}
	public void init(SortedMap pages, int pageSize, int maxObjectSize) {
		s1.init(pages, pageSize, maxObjectSize);
		s2.init(pages, pageSize, maxObjectSize);
	}
	public void pageRemoved(Object pageId, PageInformation pi) {
		s1.pageRemoved(pageId, pi);
		s2.pageRemoved(pageId, pi);
	}
	public void recordUpdated(Object pageId, PageInformation pi, short recordNumber, int recordsAdded, int bytesAdded, int linkRecordsAdded) {
		s1.recordUpdated(pageId, pi, recordNumber, recordsAdded, bytesAdded, linkRecordsAdded);
		s2.recordUpdated(pageId, pi, recordNumber, recordsAdded, bytesAdded, linkRecordsAdded);
	}
	public void close() {
		s1.close();
		s2.close();
	}
	public void pageInserted(Object pageId, PageInformation pi) {
		s1.pageInserted(pageId, pi);
		s2.pageInserted(pageId, pi);
	}
	public void read(DataInput dataInput) throws IOException {
		s1.read(dataInput);
		s2.read(dataInput);
	}
	public void write(DataOutput dataOutput) throws IOException {
		s1.write(dataOutput);
		s2.write(dataOutput);
	}
	public String toString() {
		return "Hybrid "+s1+" with "+s2;
	}
}
