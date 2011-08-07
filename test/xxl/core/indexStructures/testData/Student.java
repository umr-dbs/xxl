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

package xxl.core.indexStructures.testData;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.StringConverter;
/**
 * 
 * Test data which is used for testing one dimensional indexes
 *
 */
public class Student {
	/**
	 * Converter for student objects 
	 */
	public static Converter<Student> DEFAULT_CONVERTER = new Converter<Student>(){
		@Override
		public Student read(DataInput dataInput, Student object)
				throws IOException {
			String name = StringConverter.DEFAULT_INSTANCE.read(dataInput);
			int mnr = IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
			String info = StringConverter.DEFAULT_INSTANCE.read(dataInput);
			return new Student(name, mnr, info);
		}

		@Override
		public void write(DataOutput dataOutput, Student object)
				throws IOException {
			StringConverter.DEFAULT_INSTANCE.write(dataOutput, object.getName());
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, object.getMatrikelNr());
			StringConverter.DEFAULT_INSTANCE.write(dataOutput, object.getInfo());
			
		}
	};
	
	/**
	 * default key is matrikelNr integer
	 */
	public static final Function<Student, Integer> DEFAULT_GET_DESCRIPTOR_FUNCTION = 
		new AbstractFunction<Student, Integer>() {
			
			public Integer invoke(Student arg ){
				return arg.getMatrikelNr();
			}
	};
	
	
	String name;
	String info;
	int matrikelNr;
	
	
	public Student(String name, int matrikelNr, String info) {
		super();
		this.name = name;
		this.matrikelNr = matrikelNr;
		this.info = info; 
	}
	
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public int getMatrikelNr() {
		return matrikelNr;
	}
	
	public void setMatrikelNr(int matrikelNr) {
		this.matrikelNr = matrikelNr;
	}
	

	public String getInfo() {
		return info;
	}


	public void setInfo(String info) {
		this.info = info;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((info == null) ? 0 : info.hashCode());
		result = prime * result + matrikelNr;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Student other = (Student) obj;
		if (info == null) {
			if (other.info != null)
				return false;
		} else if (!info.equals(other.info))
			return false;
		if (matrikelNr != other.matrikelNr)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "Student [name=" + name + ", info=" + info + ", matrikelNr="
				+ matrikelNr + "]";
	}
	
	
	
	
}
