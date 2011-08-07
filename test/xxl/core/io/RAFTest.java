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

package xxl.core.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.BufferedRandomAccessFile;
import xxl.core.io.LRUBuffer;
import xxl.core.io.NullOutputStream;
import xxl.core.io.fat.FAT;
import xxl.core.io.fat.FATDevice;
import xxl.core.io.raw.RAFRawAccess;
import xxl.core.io.raw.RawAccessRAF;
import xxl.core.io.raw.RawAccessUtils;
import xxl.core.util.WrappingRuntimeException;

/**
 * Tests different kinds of RandomAccessFiles in xxl (raw-I/O, fat) 
 * against the implementation of java.
 */
public class RAFTest {
	/** number of testoperations */
	public static final int TEST_MAX_OPS = 48;
	/** Output directory */
	public static String outDir = null;

	/**
	 * Performs ops steps of the test of a RandomAccessFile.
	 * @param ra RandomAccessFile which is tested.
	 * @param ops Number of operations which are performed.
	 * @return Values which are generated from the test as String 
	 * 	(can be compared with a test of a different RandomAccessFile).
	 */
	public static String makeRAFTest(RandomAccessFile ra, int ops) {
		StringBuffer s = new StringBuffer();
		byte b[] = new byte[] {1,3,5,7,9,2,4,6,8,15};
		byte b2[] = xxl.core.util.Arrays.newByteArray(1059,(byte)42);
		
		for (int i=1; i<=ops; i++) {
			long ret=0;
			
			try {					
				int opnr=1;
				if (i==opnr++) ret = ra.getFilePointer();
				if (i==opnr++) ra.setLength(712);
				if (i==opnr++) ret = ra.read();			// Reading at the start of the file
				if (i==opnr++) ra.seek(766);
				if (i==opnr++) ra.writeLong(42);
				
				if (i==opnr++) ra.writeDouble(777.3);
				if (i==opnr++) ra.writeFloat(4711.0f);
				if (i==opnr++) ra.writeInt(-1);
				if (i==opnr++) ra.writeShort(16222);
				if (i==opnr++) ra.writeBoolean(true);
				//10
				
				if (i==opnr++) ra.seek(100000000);
				if (i==opnr++) ra.seek(100000);
				if (i==opnr++) ra.write(10);
				if (i==opnr++) ra.setLength(2000);
				if (i==opnr++) ret = ra.length();
				
				if (i==opnr++) ret = ra.getFilePointer();
				if (i==opnr++) ra.write(11);
				if (i==opnr++) ret = ra.length();
				if (i==opnr++) ra.setLength(4000);
				if (i==opnr++) ret = ra.getFilePointer();
				//20
				
				if (i==opnr++) ra.write(12);
				if (i==opnr++) ret = ra.skipBytes(600);
				if (i==opnr++) ret = ra.length();
				if (i==opnr++) ra.write(b);
				if (i==opnr++) ret = ra.length();
				
				if (i==opnr++) ret = ra.skipBytes(3000);		// File length is still 4000, filepos before 2000+1+1+600+10= 2612 --> skip goes beyond end of file
				if (i==opnr++) ret = ra.read(b);			// -1 (Reading at the end of the file)
				if (i==opnr++) ret = ra.getFilePointer();
				if (i==opnr++) ra.seek(4500);				// Jump beyond the end of file
				if (i==opnr++) ret = ra.read(b);			// exception!
				//30
				
				if (i==opnr++) ra.seek(2602);
				if (i==opnr++) { ret = ra.read(b); s.append("value (should be 7):"+b[3]+" ret:"); }	// write a 7 into the file
				if (i==opnr++) ra.seek(10000);
				if (i==opnr++) ra.setLength(8000);
				if (i==opnr++) ret = ra.getFilePointer();
				
				if (i==opnr++) ret = ra.length();
				if (i==opnr++) ra.seek(10000);
				if (i==opnr++) ra.setLength(12000);
				if (i==opnr++) ret = ra.getFilePointer();
				if (i==opnr++) ret = ra.length();
				//40
				
				if (i==opnr++) ra.seek(766);
				if (i==opnr++) { s.append("value (should be 42):"+ra.readLong()+" ret:"); }
				if (i==opnr++) { s.append("value (should be 777.3):"+ra.readDouble()+" ret:"); }
				if (i==opnr++) { s.append("value (should be 4711.0):"+ra.readFloat()+" ret:"); }
				if (i==opnr++) { s.append("value (should be -1):"+ra.readInt()+" ret:"); }
				
				if (i==opnr++) { s.append("value (should be 16222):"+ra.readShort()+" ret:"); }
				if (i==opnr++) { s.append("value (should be true):"+ra.readBoolean()+" ret:"); }
				if (i==opnr++) ra.write(b2);
				
				s.append(ret+"\t");
				// if (i%5==0)
				// 	s.append("\n");
			}
			catch (IOException e) {
				s.append(e);
			}
		}
		// Protokolliere alle Rueckgabewerte und Exceptions und gebe diese fuer Vergleichszwecke zurueck.
		return s.toString();
	}
	
	/**
	 * Tests if the length operation on a RandomAccessFile throws an exception.
	 * @param ra RandomAccessFile
	 * @return true iff an exception was thrown by length.
	 */
	static boolean testLengthForEx(RandomAccessFile ra) {
		try {
			ra.length();
			return false;
		}
		catch (IOException e) {
			return true;
		}
	}
	
	/**
	 * Compares the content of two RandomAccessFiles. The files have to be open.
	 * @param ra1 RandomAccessFile #1
	 * @param ra2 RandomAccessFile #2
	 * @return true iff the files are equal.
	 */
	public static boolean compareRAFs (RandomAccessFile ra1, RandomAccessFile ra2) {
		long pos = 0;
		int v1,v2;
		long l1=0,l2=0;
		boolean ex1=false, ex2=false;
		boolean equal = true;
		
		try {
			ex1 = testLengthForEx(ra1);
			ex2 = testLengthForEx(ra2);
			
			if (l1!=l2) {
				System.out.println("Files do not have the same length: l1="+l1+", l2="+l2);
				return false;
			}

			if (ex1!=ex2) {
				System.out.println("Exceptions have not been the same");
				return false;
			}
			
			ra1.seek(0);
			ra2.seek(0);
			while (pos<ra1.length()) {
				ra2.seek(pos);
				v1 = ra1.read();
				v2 = ra2.read();
				if (v1!=v2) {
					System.out.println("Error at position: "+pos+", v1="+v1+", v2="+v2);
					return false;
				}
				pos++;
			}
			return equal;
		}
		catch (IOException e) {
			System.out.println("Error at position: "+pos);
			e.printStackTrace();
			return false;
		}
	}

	/** Name of the dummyFile */
	private static String dummyFileName = "testRAWdummyFile";
	/** Fat device */
	private static FATDevice fatdevice;
	
	/** Factory for RawAccessRAF */
	private static Function GET_RAW_RAF = new AbstractFunction() {
		public Object invoke(Object o, Object create) {
			try {
				return 
					new RawAccessRAF(
						new RAFRawAccess((String) o),
						((Boolean)create).booleanValue(),
						new File(outDir+dummyFileName),
						true
					);
			}
			catch (Exception e) {
				throw new WrappingRuntimeException(e);
			}
		}
	};
	
	/** Factory for BufferedRandomAccessFile */
	private static Function GET_BUFFERED_RAF = new AbstractFunction() {
		public Object invoke(Object o, Object create) {
			try {
				return new BufferedRandomAccessFile((String) o,"rw",new LRUBuffer(10),512);
			}
			catch (Exception e) {
				throw new WrappingRuntimeException(e);
			}
		}
	};

	/** Factory for java-RandomAccessFile */
	private static Function GET_RAF = new AbstractFunction() {
		public Object invoke(Object o, Object create) {
			try {
				return new RandomAccessFile((String) o,"rw");
			}
			catch (Exception e) {
				throw new WrappingRuntimeException(e);
			}
		}
	};
	
	/** Factory for fat.ExtendedRandomAccessFile */
	private static Function GET_FAT_RAF = new AbstractFunction() {
		public Object invoke(Object o, Object create) {
			try {
				return fatdevice.getRandomAccessFile("testFileNumberOne.bin","rw");
			}
			catch (Exception e) {
				throw new WrappingRuntimeException(e);
			}
		}
	};
	
	/** 
	 * Tests a special RandomAccessFile against java.io.RandomAccessFile.
	 * @param args To get a list of the parameters call the class with no parameters.
	 */
	public static void main(String args[]) {
		int testNr = 0;
		String testRAF1 = "testRAF";
		String testRAF2 = "testRAW";
		
		if (args.length==0) {
			System.out.println("Test RandomAccessFiles of xxl");
			System.out.println("=============================\n");
			System.out.println("Parameters: 1 or 4");
			System.out.println("1. Number of the test (Default: 0)");
			System.out.println("   0: Test java against java");
			System.out.println("   1: Test java against RawAccessRAF");
			System.out.println("   2: Test java against BufferedRandomAccessFile");
			System.out.println("   3: Test java against fat.ExtendedRandomAccessFile");
			System.out.println("2. Filename used for java.io.RandomAccessFile");
			System.out.println("3. Filename used for the comparison RandomAccessFile");
			System.out.println("4. Filename used for the dummy file for xxl.core.io.raw.RawAccessRAF\n");
			System.out.println("The standard filenames for 2-4 are testRAF, testRAW and testRAWdummyFile. Filenames");
			System.out.println("are relative to the output directory.");
			return;
		}
		else {
			testNr = Integer.parseInt(args[0]);

			if ((testNr<0) || (testNr>3)) {
				System.out.println("Value of first parameter out of range");
				return;
			}
				
			if (args.length==4) {
				testRAF1 = args[1];
				testRAF2 = args[2];
				dummyFileName = args[3];
			}
			else if (args.length!=1) {
				System.out.println("Number of parameters was wrong!");
				return;
			}
		}

		System.out.println("Test number: "+testNr);
		outDir = Common.getOutPath();
		System.out.println("Output path: "+outDir);
		
		boolean testExceptionHandling = false;
		Function referenceRAF=GET_RAF;
		Function testRAF=null;
		
		switch (testNr) {
			case 0: testRAF=GET_RAF; break;
			case 1: testRAF=GET_RAW_RAF; break;
			case 2: testRAF=GET_BUFFERED_RAF; break;
			case 3: testRAF=GET_FAT_RAF; break;
		}
		
		try {
			// construct dummy file for raw and fat
			new RandomAccessFile(outDir+dummyFileName,"rw").close();

			RandomAccessFile ra1, ra2;
			boolean equal,succeeded=false;
			String s1=null,s2=null;

			if (testNr==3) {
				RawAccessUtils.createFileForRaw(outDir+testRAF2,1000);
				try {
					fatdevice = new FATDevice(
						"FAT",
						FAT.FAT12, 
						new RAFRawAccess(outDir+testRAF2),
						new PrintStream(NullOutputStream.NULL),
						new File(outDir+dummyFileName)
					);
				}
				catch (Throwable t) {
					t.printStackTrace();
				}
			}
			
			for (int ops=1; ops<=TEST_MAX_OPS; ops++) {
				succeeded = false;
				
				boolean ex1=false, ex2=false;
				
				System.out.println("ops="+ops);
				
				// delete files
				new File(outDir+testRAF1).delete();
				new File(outDir+testRAF2).delete();

				if (testNr==1)
					// construct raw access file
					RawAccessUtils.createFileForRaw(outDir+testRAF2,1000);
				
				ra1 = (RandomAccessFile) referenceRAF.invoke(outDir+testRAF1, Boolean.TRUE);
				s1 = makeRAFTest(ra1,ops);

				ra2 = (RandomAccessFile) testRAF.invoke(outDir+testRAF2, Boolean.TRUE);
				s2 = makeRAFTest(ra2,ops);

				if (!s1.equals(s2)) {
					System.out.println("Tests were not the same!");
					System.out.println("Original:");
					System.out.println(s1);
					System.out.println("Test RandomAccessFile:");
					System.out.println(s2);
					break;
				}
				
				equal = compareRAFs (ra1,ra2);
				if (!equal) {
					System.out.println("Before close: the RAFs produced are the same: "+equal);
					break;
				}

				ra1.close();
				ra2.close();
				
				if (testExceptionHandling) {
					ex1 = testLengthForEx(ra1);
					ex2 = testLengthForEx(ra2);
	
					if (ex1!=ex2) {
						System.out.println("Exception handing after close is different");
						System.out.println(ex1+"-"+ex2);
						return;
					}
				}

				// Reopen
				ra1 = (RandomAccessFile) referenceRAF.invoke(outDir+testRAF1, Boolean.FALSE);
				ra2 = (RandomAccessFile) testRAF.invoke(outDir+testRAF2, Boolean.FALSE);

				equal = compareRAFs (ra1,ra2);
				if (!equal) {
					System.out.println("Reopen: the RAFs produced are the same: "+equal);
					break;
				}
				
				ra1.close();
				ra2.close();
				
				if (testNr==3)
					fatdevice.delete("testFileNumberOne.bin");
				
				succeeded = true;
			}
			if (succeeded) {
				System.out.println("\nLast Test output");
				System.out.println(s1);
				System.out.println("\nTest succeeded");
			}

			// delete files
			new File(outDir+testRAF1).delete();
			new File(outDir+testRAF2).delete();			
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		if (fatdevice!=null) {
			// fatdevice.unmount();
			fatdevice = null;
		}
	}
}
