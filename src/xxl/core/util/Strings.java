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

import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;

/**
 * This class provides some useful methods for dealing with Strings and parsing Strings.
 */

public class Strings{

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Strings(){}

	/**
	 * Returns a Predicate which returns true iff the object matches the
	 * String which is given.
	 * @param matcher Regular expression.
	 * @see java.lang.String#matches(String)
	 * @return The Predicate.
	 */
	public static Predicate<String> getStringMatchingPredicate(final String matcher) {
		return new AbstractPredicate<String>() {
			@Override
			public boolean invoke(String o) {
				return o.matches(matcher);
			}
		};
	}

	/**
	 * Returns a Predicate which returns true iff the object is inside
	 * the array. The internal implementation puts the array into a
	 * HashSet for better performance.
	 * @param array The Objects.
	 * @return The Predicate.
	 */
	public static Predicate<Object> getIsInArrayPredicate(Object[] array) {
		final Set<Object> set = new HashSet<Object>();
		for (int i=0; i<array.length; i++)
			set.add(array[i]);
		return new AbstractPredicate<Object>() {
			@Override
			public boolean invoke(Object o) {
				return set.contains(o);
			}
		};
	}

	/**
	 * Returns a Function which expects a String parameter s and returns 
	 * prefix+s+suffix as a String.
	 * @param prefix Prefix String.
	 * @param suffix Suffix String.
	 * @return The Function.
	 */
	public static Function<String, String> getPrefixSuffixMapStringFunction(final String prefix, final String suffix) {
		return new AbstractFunction<String, String>() {
			@Override
			public String invoke(String o) {
				return prefix+o+suffix;
			}
		};
	}

	/**
	 * Function which gets an object and returns the String 
	 * representation of the object (uses toString() inside).
	 */
	public static final Function<Object, String> TO_STRING_FUNCTION = new AbstractFunction<Object, String>() {
		@Override
		public String invoke(Object o) {
			return o.toString();
		}
	};

	/** This method is parsing a String using given delimeters and is
	 * returning the parsed String as <tt>String[]</tt>
	 * separated by the delimeters. The delimeters are not returned.
	 * @param s String to parse
	 * @param delims <tt>String[]</tt> used as delimeters
	 * @return <tt>String[]</tt> containing the parsed String
	 */
	public static String[] parse( String s, String[] delims){ 
		List<String> l = new ArrayList<String>();
		if ( s.equals ("") ) return new String[]{""};
		if ( delims.length == 0) return new String[]{s};
		String t = s.trim();
		while ( t.length() > 0){
			int delim = 0;
			int index = t.length() + 1;
			for ( int i= 0; i < delims.length ; i++){
				if ( ( index > t.indexOf(delims[i])) && (t.indexOf(delims[i]) != -1) ){
					index = t.indexOf(delims[i]) ;
					delim = i; // welcher trenner war es?
				}
			}
			try{
				String n = t.substring (0, index);
				l.add ( n);
				t = t.substring (index - 1 + delims[delim].length() , t.length()).trim();
			}
			catch (Exception e){
				l.add ( t);
				t = "";
			}
		}
		String[] r = new String[l.size()];
		for ( int i= 0; i < r.length; i++){
			r[i] = l.get(i);
		}
		return r;
	}

	/** This method is parsing a String using the given delimeter and is
	 * returning the parsed String as String[]
	 * separated by the delimeter. The delimeter is not returned.
	 * @param s String to parse
	 * @param delim String used as delimeter
	 * @return String[] containing the parsed String
	 */
	public static String [] parse( String s, String delim){
		if ( s.trim().equals ("") && !delim.equals(" ")) return new String[]{""};
		if ( s.equals ("") ) return new String[]{""};
		StringTokenizer st = new StringTokenizer( s, delim);
		String[] r = new String[ st.countTokens() ];
		int i=0;
		while( st.hasMoreElements() ) {
	        	r[i] = (String) st.nextElement();
        		i++;
	     	}
		return r;
	}

	/** This method is converting a <tt>String []</tt> to a <tt>double []</tt> of same length.
	 * @param s <tt>String []</tt> to convert
	 * @return <tt>double []</tt> containing a double representation of each String from the
	 * input array.
	 */
	public static double [] toDoubleArray ( String [] s){
		double [] r = new double[ s.length];
		for ( int i = 0; i < r.length ; i++)
			r[i] = (new Double( s[i])).doubleValue();
		return r;
	}

	/** This method is converting a <tt>String []</tt> to a <tt>float []</tt> of same length.
	 * @param s <tt>String []</tt> to convert
	 * @return <tt>float []</tt> containing a float representation of each String from the
	 * input array.
	 */
	public static float[] toFloatArray ( String[] s){
		float [] r = new float[ s.length];
		for ( int i = 0; i < r.length ; i++)
			r[i] = (new Float( s[i])).floatValue();
		return r;
	}

	/** This method is converting a <tt>String []</tt> to a <tt>int []</tt> of same length.
	 * @param s <tt>String []</tt> to convert
	 * @return <tt>int []</tt> containing a float representation of each String from the
	 * input array.
	 */
	public static int [] toIntArray ( String[] s){
		int [] r = new int[ s.length];
		for ( int i = 0; i < r.length ; i++)
			r[i] = (new Integer( s[i])).intValue();
		return r;
	}

	/** This method is converting a <tt>String []</tt> to a <tt>long []</tt> of same length.
	 * @param s <tt>String []</tt> to convert
	 * @return <tt>long []</tt> containing a float representation of each String from the
	 * input array.
	 */
	public static long [] toLongArray ( String[] s){
		long [] r = new long[ s.length];
		for ( int i = 0; i < r.length ; i++)
			r[i] = (new Long( s[i])).longValue();
		return r;
	}

	/** Returns a character reader based reader upon the source given by the name.
	 * Different sources such as web-servers or files will be choosen automatically
	 * determind by the given name, i.e., a source starting with <tt>http</tt> will be treated as
	 * a web-based text-file. At this time only <tt>http</tt> and files are supported.
	 *
	 * @param source name of the data source provided as {@link java.io.Reader reader}
	 * @return the given source as {@link java.io.Reader reader}
	 * @throws WrappingRuntimeException if an error of any type occures
	 */
	public static Reader getReader( String source) throws WrappingRuntimeException{
		Reader r = null;
		boolean init = false;
		try{
			// -- url ---
			if ( source.trim().toLowerCase().startsWith("http")){
				URL url = new URL (source);
				r = new InputStreamReader ( url.openStream());
				init = true;
			}
			// -- default file ---
			if ( ! init){
				r = new FileReader ( new File ( source));
			}
		}
		catch ( Exception e){
			throw new WrappingRuntimeException (e);
		}
		return r;
	}

	/** Assumes that the given String indicates a file and replaces the
	 * suffix by a new one. The suffix if given by "*.suffix" meaning the last found
	 * '.' and all following character will be replaced. If there's no '.' in the given String
	 * the given suffix will be concatenated.
	 * @param filename String treated as file name
	 * @param newSuffix new suffix for the file name
	 * @return a String representing a file name 
	 */
	public static String changeSuffix( String filename, String newSuffix){
		int k = filename.lastIndexOf(".");
		return (k == -1) ?
				filename
			:
				filename.substring(0,k) + newSuffix;
	}

	/** Assumes that the given String represents a fully qualified 
	 * file name (i.e., with path information) and returns the file name without any
	 * folder information by pruning all parts before the last 
	 * separator character 
	 * return the file name itself
	 * @param filename fully qualified file name
	 * @return file name withiout path information
	 * @see File#separator
	 */
	public static String prunePath( String filename){
		String r = filename;
		int i1 = r.lastIndexOf( File.separator);
		if ( i1 != -1)
			r = r.substring( i1+1, r.length());
		return r;
	}
}
