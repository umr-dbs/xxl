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

package xxl.core.relational.metaData;

import java.sql.SQLException;

/**
 * Makes a renaming of a result set's column metadata.
 */
public class RenamedColumnMetaData extends DecoratorColumnMetaData {
	
	/**
	 * The new name of the renamed column.
	 */
	protected String renaming;
	
	/**
	 * Creates a new renamed result set's column metadata that renames the
	 * given column using the specified renaming.
	 * 
	 * @param metaData the result set's column metadata to be renamed.
	 * @param renaming the new name of the specified column.
	 */
	public RenamedColumnMetaData(ColumnMetaData metaData, String renaming) {
		super(metaData);
		this.renaming = renaming;
	}

	/**
	 * Gets this column's suggested title for use in printouts and displays.
	 *
	 * @return the suggested column title.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public String getColumnLabel() throws SQLException {
		return renaming;
	}

	/**
	 * Get this column's name.
	 *
	 * @return column name.
	 * @throws SQLException if a database access error occurs.
	 */
	@Override
	public String getColumnName() throws SQLException {
		return renaming;
	}

}
