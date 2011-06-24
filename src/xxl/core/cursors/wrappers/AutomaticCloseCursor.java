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

package xxl.core.cursors.wrappers;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.DecoratorCursor;

public class AutomaticCloseCursor extends DecoratorCursor {
	boolean closeWasCalled = false;
	public AutomaticCloseCursor(Cursor cursor) {
		super(cursor);
	}
	public void close() {
		// do not call close again if it is called below.
		if (closeWasCalled) {
			cursor.close();
			closeWasCalled = true;
		}
	}
	public boolean hasNext() {
		boolean hn = cursor.hasNext();
		if (!hn) {
			cursor.close();
			closeWasCalled = true;
		}
		return hn;
	}
}
