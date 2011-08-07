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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.Descriptor;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.util.Interval1D;

/** This class provides several static methods which are useful for 
 * for preparing tests of indexstructures.
 */
public class Tests {
	
	/** The name of the working directory.
	 */
	public static final String WORK_DIR_NAME="d:\\Benutzer\\tests.old";
	
	/** It makes the program wait for an input from the user.
	 */
	public static void waitForUser() {
		try { 
			System.out.print("Please, press a key to continue!");
			//while(System.in.read()!=13);
			System.in.read();
			System.in.skip(System.in.available()); 
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}		

	/** Creates random <tt>nr</tt> keys from <tt>[low, high[</tt>
	 * and stores thme into the file with the given file 
	 * name. In the file each key lies in a separate line.
	 *  
	 * @param fileName the name of the file into which the created keys have to be stored
	 * @param nr the number of the keys which have to be created
	 * @param low the minimal bound of key range
	 * @param high the maximal bound of the key range
	 * @throws IOException
	 */
	public static void createKeys(String fileName, int nr, int low, int high) throws IOException {
		List keys= new LinkedList();
		for(int i=low; i<high; i++) keys.add(new Integer(i));
		Random r= new Random();
		while(keys.size()>nr) {
			int i=r.nextInt(keys.size());
			keys.remove(i);
		}
		for(int i=0; i<2*nr; i++) {
			int k= r.nextInt(keys.size());
			int l= r.nextInt(keys.size()-1);
			Integer Kk= (Integer)keys.remove(k);
			Integer Kl= (Integer)keys.remove(l);
			keys.add(l, Kk);
			keys.add(k, Kl);		
		}
		OutputStream output= new BufferedOutputStream(new FileOutputStream(fileName));
		PrintStream out=new PrintStream(output);
		for(int i=0; i<keys.size(); i++) out.println(keys.get(i));
		output.flush();
		output.close();
	}

	/** For each interval [k, l[ of <tt>[low, low+jump[, [low+jump, low+2jump[, ..., [low+(n-1)jump, low+n*jump[,</tt> where n 
	 * is the greatest integer so that <tt>n*jump</tt> is smaller than high, creates randomly keys by calling the 
	 * the method {@link #createKeys(String fileName, int nr, int k, int l)}. The <tt>fileName</tt> is 
	 * <tt>WORK_DIR_NAME+"/Keys"+k+"_"+l+".test"</tt> and <tt>nr</tt> equals <tt>2*(l-k)/3</tt>.
	 * 
	 * @param low the minimal bound of key range
	 * @param high the maximal bound of the key range
	 * @param jump the length of the intervals
	 * @throws IOException
	 */	
	public static void createKeys(int low, int high, int jump) throws IOException {
		int nr=2*jump/3;
		for(int i=low; i<high; i+=jump) {
			String fileName= WORK_DIR_NAME+"/Keys"+i+"_"+(i+jump)+".test";
			createKeys(fileName, nr, i, i+jump);	
		}
	}
	
	/** It is equivalent to: 
	 * <pre><code>
	  			createKeys(0, high, 1000);
	   </code></pre>
	 *	   
	 * @param high the maximal bound of the key range
	 * @throws IOException
	 */
	public static void createKeys(int high) throws IOException {
		createKeys(0, high, 1000);
	}
	
	/** Merges the contents of all given files randomly and add simultaneously 
	 * an "incom" component for each key (personal number) (see {@link Tests.Employee}). 
	 * The result is written into the file with the given file name 
	 * <tt>fileName</tt>.
	 * 
	 * @param keysFiles files containig the keys 
	 * @param fileName the name of the result file
	 * @throws IOException
	 */
	public static void makeInsertTestFile(String[] keysFiles, String fileName) throws IOException {
		Random r1= new Random();
		Random r2= new Random();
		BufferedReader[] br=new BufferedReader[keysFiles.length];
		for(int i=0; i<br.length; i++) br[i]=new BufferedReader(new FileReader(keysFiles[i]));
		OutputStream output= new BufferedOutputStream(new FileOutputStream(fileName));
		PrintStream out=new PrintStream(output);		
		while(true) {
			int f=r1.nextInt(br.length);
			int f1=f;
			boolean begin=true;
			String s=null;
			while(s==null && (begin || f1!=f)) {
				s=br[f1].readLine();
				f1=(f1+1)%br.length;
				begin=false;
			}
			if(s==null) break;
			int persNr= Integer.parseInt(s);
			double incom= r2.nextDouble();
			incom=(int)(1000000*incom)/100;
			out.println(persNr+" "+incom);
		}
		output.flush();
		output.close();
		for(int i=0; i<br.length; i++) br[i].close();
	}
	
	/** Gives the names of the files which were created at last by calling the method 
	 * {@link Tests#createKeys(int low, int high, int jump)}.
	 * 
	 * @param low the minimal bound of key range
	 * @param high the maximal bound of the key range
	 * @param jump the length of the intervals
	 * @return an <tt>Array</tt> containig the file names
	 */
	public static String[] getKeysFiles(int low, int high, int jump) {
		String[] keysFiles= new String[(high-low)/jump];
		int i=0;
		for(int l=low; l<high; l+=jump) keysFiles[i++]= WORK_DIR_NAME+"/Keys"+l+"_"+(l+jump)+".test";
		return keysFiles;
	}
	
	/** Is equivalent to:
	 * <pre><code>
	  		getKeysFiles(0, high, 1000);
	   </code></pre>
	 * @param high the maximal bound of the key range
	 * @return an <tt>Array</tt> containig the file names
	 */
	public static String[] getKeysFiles(int high) {
		return getKeysFiles(0, high, 1000);
	}
	
	public static void main(String[] args) throws IOException {
		if(args.length!=1) {
			System.out.println("A method to create test data for index structure tests.");
			System.out.println("Usage: Tests high");
			System.out.println("high= the upper bound of the keys, which are to create.");
		}
		createKeys(Integer.parseInt(args[0]));
		System.out.println("End");
	}
	
	/**
	 * A class to represent the data objects which are stored in an index structure.
	 * An <tt>Employee</tt> has a unique personal number and incom. 
	 */

	public static class Employee implements Comparable {
		private static byte[] data = new byte[0];
		
		public static void setDataSize(int size) {
			data = new byte[size];			
		}
		
		public static int getDataSize() {
			return data.length;
		}

		/** This <tt>Function</tt> is used to extract the key component (persNr) of the data object.
		 */
		public static final Function getKey = new AbstractFunction() {
			public Object invoke(Object data) {
				return new Integer(((Employee)data).getPersNr());	
			}
		};

		/** A default <tt>Converter</tt> to serialize the data objects.
		 */
		public static final MeasuredConverter DEFAULT_CONVERTER = new MeasuredConverter() {
			public Object read(DataInput input, Object object) throws IOException {
				int nr = IntegerConverter.DEFAULT_INSTANCE.readInt(input);
				double incom = DoubleConverter.DEFAULT_INSTANCE.readDouble(input);
				input.readFully(Employee.data);
				return new Employee(nr,incom);
			}

			public void write(DataOutput output, Object object) throws IOException {
				Employee data = (Employee) object;
				IntegerConverter.DEFAULT_INSTANCE.writeInt(output, data.persNr);
				DoubleConverter.DEFAULT_INSTANCE.writeDouble(output,data.incom);
				output.write(Employee.data);
			}
			
			public int getMaxObjectSize() {
				return IntegerConverter.SIZE + DoubleConverter.SIZE + Employee.data.length;
			}
		};
		
		public static Function<Employee,Descriptor> getDescriptor = new AbstractFunction<Employee,Descriptor> () {
			public Descriptor invoke (Employee employee) {
				return new Interval1D(employee.getPersNr());
			}

			public Descriptor invoke (Employee from, Employee to) {
				return new Interval1D(from.getPersNr(), to.getPersNr());
			}
		};
/*
		public static int getMaxObjectSize() {
			return IntegerConverter.SIZE + DoubleConverter.SIZE;
		}

		public static int getMaxKeySize() {
			return IntegerConverter.SIZE;
		}
*/
		/**
		 * A <tt>Converter</tt> to serialize the keys of the data objects.
		 */
		public static final MeasuredConverter KEY_CONVERTER= new MeasuredConverter() {
			public Object read(DataInput input, Object object) throws IOException {
				return IntegerConverter.DEFAULT_INSTANCE.read(input, null);
			}	
			public void write(DataOutput output, Object object) throws IOException {
				IntegerConverter.DEFAULT_INSTANCE.write(output, (Integer)object);
			}
			public int getMaxObjectSize() {
				return IntegerConverter.SIZE;
			}
		};
		
		/** A unique personal number (key).
		 */
		private int persNr;
		
		/** The income of the employee. 
		 */
		private double incom;
		
		/** Creates a new Employee.
		 * 
		 * @param nr the peronal number of the employee 
		 * @param incom the incom of the employee
		 */
		protected Employee(int nr, double incom) {
			this.incom=incom;
			persNr=nr;
		}
		
		/** Gives the personal number of this <tt>Employee</tt>.
		 * 
		 * @return the personal number of this <tt>Employee</tt>
		 */
		public int getPersNr() {
			return persNr;
		}
		
		
		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		public int hashCode() {
			return persNr;
		}
		
		/** Checks whether two <tt>Employees</tt> are the same, 
		 * i.e. they have the same personal number.
		 * 
		 * @param data the second <tt>Employee</tt>
		 * @return <tt>true</tt> if the employees have the same personal number, 
		 * <tt>false</tt> otherwise
		 */ 
		public boolean equals(Object data) {
			return persNr==((Employee)data).persNr;
		}
		
		public int compareTo(Object obj) {
			return ((Integer)this.persNr).compareTo((Integer)((Employee)obj).persNr);
		}

		/** Converts this <tt>Employee</tt> to a String to display it.
		 *  
		 * @return the String display of this <tt>Employee</tt>
		 */
		public String toString() {
			return "\""+persNr+", "+incom+"\"";
		}
		
	}	
}
