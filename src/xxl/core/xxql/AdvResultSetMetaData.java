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

package xxl.core.xxql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import xxl.core.relational.metaData.ColumnMetaData;
import xxl.core.relational.metaData.ColumnMetaDataResultSetMetaData;

// TODO: nen interface hiervon, falls mal jemand eigene advresultsetmetadatas bauen will (z.b. aus echten sql-metadaten)?

public class AdvResultSetMetaData extends ColumnMetaDataResultSetMetaData {
	
	private String myAlias=null;
	
	// mapping name -> index if name is unique
	private Map<String, Integer> name2index; 
	
	// mapping (alias, name)->index of object 
	// Spezialfall: Cursorn ueber pseudo-primitive Datentypen ist der name standardmaessig "value"
	// gibt logischen index im zugehoerigen tupel zurueck (wie man ihn mit getObject() nutzen wuerde)
	private Map<AliasName, Integer> aliasName2index; 
	
	public String getAlias(){
		return myAlias;
	}
	
	/**
	 * This constructor is meant to be used with "fresh" tuples, i.e. not concatenated ones or the
	 * like that may already have mappings with multiple aliases on the same column. 
	 * @param myAlias alias of the Cursor belonging to this metadata, null if none
	 * @param columnMetaDatas the metadatas of each column
	 */
	public AdvResultSetMetaData(String myAlias, ColumnMetaData... columnMetaDatas) {
		this(myAlias, true, columnMetaDatas);
	}
	
	/**
	 * This constructor may be used if the mappings can't be trivially generated, but will be done <b>manually
	 * after</b> creating the Object with this constructor (e.g. when concatenating metadata etc). <br>
	 * However if fillMappings is set to true, the mappings will be generated.
	 * @param myAlias alias of the Cursor belonging to this metadata, null if none
	 * @param fillMappings if true, mappings (alias.name&rarr;index) will be generated. If false, you 
	 * 			have to do that yourself afterwards
	 * @param columnMetaDatas the metadatas of each column
	 * 
	 * @see AdvResultSetMetaData#concat(AdvResultSetMetaData, AdvResultSetMetaData) concat(AdvResultSetMetaData, AdvResultSetMetaData)
	 */
	public AdvResultSetMetaData(String myAlias, boolean fillMappings, ColumnMetaData... columnMetaDatas) {
		super(columnMetaDatas);
		this.myAlias = myAlias;
		name2index = new HashMap<String, Integer>();
		aliasName2index = new HashMap<AliasName, Integer>();
		if(fillMappings){ // the simple case: no contained tuples, mappings are straight forward
			try {
				for(int i=0;i<columnMetaDatas.length;i++){
					String name = columnMetaDatas[i].getColumnName();
					// name2index
					name2index.put(name, i+1);
					// aliasName2index
					if(myAlias != null && !myAlias.equals(""))
						addMappingAliasNameIndex(myAlias, name, i+1);
					// if the column has another alias (e.g. tablename from jdbc or the like)
					String altAlias = columnMetaDatas[i].getTableName();
					if(!altAlias.equals(myAlias) && !altAlias.equals(""))
						addMappingAliasNameIndex(altAlias, name, i+1);
				}
			} catch(SQLException e){
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 *  private helping function creating an array of ColumnMetaDatas from a simple ResultSetMetaData
	 * @param rsm ResultSetMetaData, e.g. from a JDBC-ResultSet
	 * @return an array of ColumnMetaDatas suitable for AdvResultSetMetaData
	 */
	private static ColumnMetaData[] cmdsFromRSM(ResultSetMetaData rsm){
		ColumnMetaData[] cmds=null;
		try {
			cmds = new ColumnMetaData[rsm.getColumnCount()];
			for(int i=1;i<=cmds.length;i++){
				Class<?> javaclass = Class.forName(rsm.getColumnClassName(i));
				String name =  rsm.getColumnName(i);
				String altalias = rsm.getTableName(i);
				cmds[i-1] = createColumnMetaData(javaclass, name, altalias);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return cmds;
	}
	
	/**
	 * This constructor creates an AdvResultSetMetaData object from a simple ResultSetMetaData.
	 * Should be used when wrapping JDBC-ResultSets in AdvTupleCursors
	 * @param rsm a ResultSetMetaData, e.g. from a JDBC-ResultSet
	 * @param alias the alias of the new AdvTupleCursor wrapping the ResultSet
	 */
	public AdvResultSetMetaData(ResultSetMetaData rsm, String alias){
		this(alias, cmdsFromRSM(rsm));
	}
	
	// klasse, die 2 strings enthaelt: fuer alias und name, soll als schluessel der aliasName2index-hashmap dienen
	static class AliasName { // FIXME: hierfuer gibts irgendne fertige xxl-klasse
		String alias=null;
		String name=null;
		public AliasName(String alias, String name) {
			this.alias=alias;
			this.name=name;
		}
		
		// sinnvolle vergleichsfunktion, macht sich sicher gut fuers mapping...
		@Override
		public boolean equals(Object obj) {
			AliasName an = (AliasName)obj; // obj really shouldn't be of any other class.
			return an.alias.equals(alias) && an.name.equals(name);
		}
		
		@Override
		public int hashCode() {
			// die codes bitweise verodern, damit alias und name nicht austauschbar sind
			return alias.hashCode() ^ name.hashCode();
		}

		@Override
		public String toString() {
			return alias + "." + name;
		}
	}
	
	
	/**
	 * Returns a concatenatenation of two AdvResultSetMetaDatas
	 * 
	 * @param first 1. AdvResultSetMetaData
	 * @param second 2. AdvResultSetMetaData
	 * @return (first AdvResultSetMetaData)++(second AdvResultSetMetaData)
	 */
	public static AdvResultSetMetaData concat(AdvResultSetMetaData first, AdvResultSetMetaData second){
		return concat(first, second, null);
	}
	
	/**
	 * Returns a concatenatenation of two AdvResultSetMetaDatas with a new Alias for the resulting metadata.<br>
	 * Please note that only columns with unique names (within this metadata) can be accessed by "newAlias.name", 
	 * so you might want to rename some column's names (or use the old alias to access them).
	 * 
	 * @param first 1. AdvResultSetMetaData
	 * @param second 2. AdvResultSetMetaData
	 * @param newAlias the alias of the newly created metadata. null if you don't want an additional alias.
	 * @return (first AdvResultSetMetaData)++(second AdvResultSetMetaData)
	 */
	public static AdvResultSetMetaData concat(AdvResultSetMetaData first, AdvResultSetMetaData second, String newAlias){
		// FIXME: ich *glaube*, dass die mappings bei doppelten namen mit gleichem alias (nach join) spinnen, d.h.
		// vorhanden sind. sollte nicht sein, da join.bla dann uneindeutig ist. - sollte gefixt sein
		int cmdlength;
		ColumnMetaData[] columnMetaDatas;
		try {
			cmdlength = first.getColumnCount()+second.getColumnCount();
		
			columnMetaDatas = new ColumnMetaData[cmdlength];
			
			for(int i=0;i<first.getColumnCount();i++){
				columnMetaDatas[i] = first.columnMetaData[i];
			}
			
			for(int i=0;i<second.getColumnCount();i++){
				columnMetaDatas[i+first.getColumnCount()] = second.columnMetaData[i];
			}
		
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		String tmpAlias = (newAlias == null || newAlias.equals("")) ? first.getAlias() : newAlias;
		// just use the old alias of left father if a new one wasn't given
		AdvResultSetMetaData ret = new AdvResultSetMetaData(tmpAlias, false, columnMetaDatas);
		
		// create and adjust mappings
		try {
			
			// ## name2index ## the elements of the *second* tuple lie behind *first* tuple
			ret.name2index.putAll(first.name2index);
			int elemOffset = first.getColumnCount(); // logical position of the last element from *first* 
			for(Entry<String, Integer> ent : second.name2index.entrySet()){
				if(ret.name2index.containsKey(ent.getKey())) // if the mapping is not unambiguous, don't map at all.
					ret.name2index.remove(ent.getKey());
				else
					ret.name2index.put(ent.getKey(), ent.getValue()+elemOffset);
			}
			
			// ## aliasName2index ## - again, the mappings for *first* tuple don't change..
			ret.aliasName2index.putAll(first.aliasName2index);
			for(Entry<AliasName, Integer> ent : second.aliasName2index.entrySet()){
				// .. but the indexes of the elements from the *second* tuple get that offset
				ret.addMappingAliasNameIndex(ent.getKey().alias, ent.getKey().name, ent.getValue()+elemOffset);
			}
			
			// if there is a new alias, add mappings in aliasName2index
			if(newAlias != null && !newAlias.equals("")) {
				// add a mapping for each unique name and the new alias to aliasName2index
				for(Entry<String, Integer> ent : ret.name2index.entrySet()){
					AliasName tmp = new AliasName(newAlias, ent.getKey());
					ret.aliasName2index.put(tmp, ent.getValue());
				}
			}
			
		} catch(SQLException e){
			throw new RuntimeException(e);
		}
		
		return ret;
	}
	
	/**
	 * Returns a "clone" of this MetaData, but there are new mappings for the new alias.
	 * (newAlias.nameOfSomeColumn &rarr; index)<br>
	 * Please note that there will (and can) be no mapping for duplicate names (You may still acess
	 * them with their original Alias, though).
	 * @param newAlias the new alias
	 * @return an AdvResultSetMetaData with the given alias
	 */
	public AdvResultSetMetaData clone(String newAlias){
		ColumnMetaData[] cmds = new ColumnMetaData[this.columnMetaData.length];
		for(int i=0;i<cmds.length;i++){
			ColumnMetaData tmp = this.columnMetaData[i];
			try {
				// if the column has the old alias as tablename replace it with the new alias
				if(tmp.getTableName().equals(this.getAlias())){
					Class<?> cl = Class.forName(tmp.getColumnClassName());
					tmp = createColumnMetaData(cl, tmp.getColumnName(), newAlias);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			cmds[i]=tmp;
		}
		
		AdvResultSetMetaData ret = new AdvResultSetMetaData(newAlias, false, cmds);
		
		ret.myAlias = newAlias;
		
		ret.name2index = this.name2index; // the names itself won't change
		
		ret.aliasName2index = new HashMap<AliasName, Integer>();
		// the old aliases may still be used, but not the alias of the cloned cursor/metadata
		// mappings for it need to be replaced by mappings with the new alias or selfjoins won't work
		for(Entry<AliasName, Integer> ent : this.aliasName2index.entrySet()){
			// if it's the alias of the cloned metadata
			if(ent.getKey().alias.equals(this.getAlias())) {
				// add a mapping for the new alias
				ret.addMappingAliasNameIndex(newAlias, ent.getKey().name, ent.getValue());
			} else { // else add the old mapping
				ret.aliasName2index.put(ent.getKey(), ent.getValue());
			}
		}
		
		// but we have to add new aliases for newAlias, so we take the non-ambiguous names from
		// name2index and add a mapping for them and the new alias to aliasName2index
		for(Entry<String, Integer> ent : name2index.entrySet()){
			AliasName tmp = new AliasName(newAlias, ent.getKey());
			ret.aliasName2index.put(tmp, ent.getValue());
		}
		
		return ret;
	}
	
	/**
	 * Returns the index of the element that was originally in the tuple/cursor with given alias
	 * and has the given name. <br/>
	 * <i>Note:</i> The default name of an attribute in a Tuple wrapping a primitive Type (or it's wrappers
	 * like java.lang Integer), e.g. from a Cursor<Float>, is "value". 
	 * 
	 * @param alias the tuples/cursors alias
	 * @param name the attribute's/element's name
	 * @return logical index in this tuple (resp. tuples of this cursor) or -1 if not found
	 */
	public int getIndexByAliasAndName(String alias, String name){
		Integer index = aliasName2index.get(new AliasName(alias, name));
		return (index == null) ? -1 : index.intValue();
	}
	
	/**
	 * Returns the index of the element that was originally in the tuple/cursor with given alias
	 * and name. (Format: alias.name or just name, but then the name has to be unique within this Cursor/Metadata) <br/>
	 * <i>Note:</i> The default name of an attribute in a Tuple wrapping a primitive Type (or it's wrappers
	 * like java.lang Integer), e.g. from a Cursor<Float>, is "value". 
	 * 
	 * @param aliasname the tuples/cursors alias and name (alias.name)
	 * @return logical index in this tuple (resp. tuples of this cursor) or -1 if not found
	 */
	public int getIndexByAliasAndName(String aliasname){
		String[] tmp = aliasname.split("\\.");
		if(tmp.length == 2){
			return getIndexByAliasAndName(tmp[0], tmp[1]);
		}
			
		else if(tmp.length == 1)
			return getIndexByName(tmp[0]);
		else 
			return -1;
	}
	
	/**
	 * Returns index for given name or -1 if the name is not found or not unique 
	 * (i.e. the name exists several times with different aliases)
	 * @param name the attributes name
	 * @return index of the column for the given name or -1 if not found
	 */
	public int getIndexByName(String name) {
		Integer index = name2index.get(name);
		
		return (index == null) ? -1 : index.intValue();
	}
	
	/**
	 * Returns the ColumnMetaData of the given column
	 * @param column the index of the column which's metadata is to retrieved
	 * @return ColumnMetaData for column with given index
	 */
	public ColumnMetaData getColumnMetaData(int column){
		return columnMetaData[column-1];
	}
	
	/**
	 * Adds a mapping from alias and name to logical index within a tuple to this metadata.<br>
	 * Should probably not be used much outsite this class.. 
	 * @param alias the alias of the tuple/table containing the element at index
	 * @param name the name of the element at index
	 * @param index the logical index of the element within tuples containing this metadata
	 */
	protected void addMappingAliasNameIndex(String alias, String name, int index){
		AliasName an = new AliasName(alias, name);
		aliasName2index.put(an, index);
		// name2index.put(name, index); wird "von hand" gemacht an den (wenigen) entsprechenden stellen
		// damit werden auch duplikate verhindert.
	}

	/**
	 * Returns an appropriate ColumnMetaData-object for the AdvResultSetMetaData. All relevant information can
	 * (hopefully) be extracted from the class of the Object in this column and the columns name
	 * @param cl a column represented by this metadata will contain an Object of this class 
	 * @param name the Columns name
	 * @return an appropriate ColumnMetaData
	 */
	public static ColumnMetaData createColumnMetaData(final Class<?> cl, final String name, final String alias){
		if(name == null || name.equals(""))
			throw new RuntimeException("You need to specify a name for the new Column");
		
		return new ColumnMetaData() {
			
			@Override
			public boolean isWritable() throws SQLException {
				return false; // schreiben wollnwa nicht - oder?
			}
			
			@Override
			public boolean isSigned() throws SQLException {
				// was ist, wenn das garkeine zahlen sind?
				return true; // java kann ja nix anders..
			}
			
			@Override
			public boolean isSearchable() throws SQLException {
				return true; // grundsaetzlich sollte erstmal alles in ner where-clause gehen
				// TODO: was ist mit arrays?
			}
			
			@Override
			public boolean isReadOnly() throws SQLException {
				return true; // wir wollen nix ueberschreiben.
			}
			
			@Override
			public int isNullable() throws SQLException {
				return ResultSetMetaData.columnNullable; // null kann wohl mal vorkommen, glaub ich
			}
			
			@Override
			public boolean isDefinitelyWritable() throws SQLException {
				return false;
			}
			
			@Override
			public boolean isCurrency() throws SQLException {
				// TODO is this ever needed?
				return false;
			}
			
			@Override
			public boolean isCaseSensitive() throws SQLException {
				return true;
			}
			
			@Override
			public boolean isAutoIncrement() throws SQLException {
				return false;
			}
			
			@Override
			public String getTableName() throws SQLException {
				return alias != null ? alias : "";
			}
			
			@Override
			public String getSchemaName() throws SQLException {
				return "";
			}
			
			@Override
			public int getScale() throws SQLException {
				// TODO is this ever needed?
				return 0;
			}
			
			@Override
			public int getPrecision() throws SQLException {
				// TODO is this ever needed? don't know how to calculate this properly..
				return 0;
			}
			
			@Override
			public String getColumnTypeName() throws SQLException {
				// TODO does this matter?
				return cl.getSimpleName();
			}
			
			@Override
			public int getColumnType() throws SQLException {
				// TODO is this ever needed? if so, we need an appropriate mapping to an sql-type
				// (have fun with that :-P)
				// we *will not* use this. do *not* use the mappings from Types.java, because you'll
				// get an exception if you have any "fancy" type in this Column 
				// (only ~20 basic types are supported)
				// return 0;
				throw new UnsupportedOperationException("Don't use getColumnType(), it's impossible to"+
						"support for non-basic types..");
			}
			
			@Override
			public String getColumnName() throws SQLException {
				return name;
			}
			
			@Override
			public String getColumnLabel() throws SQLException {
				return getColumnName();
			}
			
			@Override
			public int getColumnDisplaySize() throws SQLException {
				// TODO is this ever needed?
				return 128;
			}
			
			@Override
			public String getColumnClassName() throws SQLException {
				return cl.getName();
			}
			
			@Override
			public String getCatalogName() throws SQLException {
				// TODO what is this?
				return "";
			}
		};
	}

}
