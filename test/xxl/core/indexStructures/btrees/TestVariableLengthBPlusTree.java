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

package xxl.core.indexStructures.btrees;


import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.functions.AbstractFunction;
import xxl.core.functions.Function;
import xxl.core.indexStructures.AbstractTreeIndexTest;
import xxl.core.indexStructures.keyRanges.StringKeyRange;
import xxl.core.indexStructures.separators.StringSeparator;
import xxl.core.indexStructures.testData.Student;
import xxl.core.indexStructures.vLengthBPlusTree.SplitStrategy;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree;
import xxl.core.indexStructures.vLengthBPlusTree.VariableLengthBPlusTree.IndexEntry;
import xxl.core.indexStructures.vLengthBPlusTree.splitStrategy.ShortestKeyStrategy;
import xxl.core.indexStructures.vLengthBPlusTree.splitStrategy.SimplePrefixBPlusTreeSplit;
import xxl.core.indexStructures.vLengthBPlusTree.underflowHandlers.StandardUnderflowHandler;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.Converters;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.io.converters.StringConverter;


/**
 * 
 * Test with a student data. No duplicate modus of {@link VariableLengthBPlusTree}. 
 */
public class TestVariableLengthBPlusTree extends AbstractTreeIndexTest<VariableLengthBPlusTree>{
 
	public static final int TEST_DATA_NUMBER = 100000;
	public static final int SEED = 42;
	
	public static final MeasuredConverter<Student> dataMeasuredConverter = new MeasuredConverter<Student>(){

		@Override
		public int getMaxObjectSize() {
			// 50 bytes for string name
			// 50 bytes for info
			return 50 + 50 +4;
		}

		@Override
		public Student read(DataInput dataInput, Student object)
				throws IOException {
			return Student.DEFAULT_CONVERTER.read(dataInput, object);
		}

		@Override
		public void write(DataOutput dataOutput, Student object)
				throws IOException {
			Student.DEFAULT_CONVERTER.write(dataOutput, object);
		}
	};
	

	public static final MeasuredConverter<String> keyConverter = new MeasuredConverter<String>(){

		@Override
		public int getMaxObjectSize() {
			return  50;
		}

		@Override
		public String read(DataInput dataInput, String object)
				throws IOException {
			return StringConverter.DEFAULT_INSTANCE.read(dataInput, object);
		}

		@Override
		public void write(DataOutput dataOutput, String object)
				throws IOException {
			StringConverter.DEFAULT_INSTANCE.write(dataOutput, object);
		}
	};
	
	public static final Function<Object, Integer> getDataSize = new AbstractFunction<Object , Integer>() {
		
		public Integer invoke(Object arg){
			//cast to student
			Student std = (Student)arg;
			int nameSize =  Converters.sizeOf(StringConverter.DEFAULT_INSTANCE, std.getName());
			int infoSize =  Converters.sizeOf(StringConverter.DEFAULT_INSTANCE, std.getInfo());
			return nameSize +infoSize + 4; 
		}
	};
	
	public static final Function<Object, Integer> getKeySize = new AbstractFunction<Object , Integer>() {
		
		public Integer invoke(Object arg){
			//cast to student
			String std = (String)arg;
			int nameSize =  Converters.sizeOf(StringConverter.DEFAULT_INSTANCE, std);
			return nameSize; 
		}
	};
	
	
	public static final Function<Student, String> getKeyFunction = new AbstractFunction<Student, String>() {
		
		public String invoke(Student st){
			return st.getName();
		}
	};
	
	private List<Student> oddStudents; // TEST_DATA_NUMBER/2
	private List<Student> evenStudents;
	private String path;
	private int blockSize;
	private SplitStrategy strategy;
	private double capacity; 
	
	
	@BeforeClass
	@Override
	public void prepairIndex() {
		List<Integer> stds = new ArrayList<Integer>(TEST_DATA_NUMBER);
		for(int i = 0; i < TEST_DATA_NUMBER; i++){
			stds.add(i);
		}
		Random r = new Random(42);
		Collections.shuffle(stds, r);
		oddStudents = new ArrayList<Student>(TEST_DATA_NUMBER /2);
		evenStudents = new ArrayList<Student>(TEST_DATA_NUMBER /2);
		for(int i : stds){
			if(i %2 == 0){
				evenStudents.add(new Student("name_"+i, i, "info" + i) );
			}else{
				oddStudents.add(new Student("name_"+i, i, "info" + i));
			}
		}
		// set path
		path = "testVLBTree_dupl_false";
		
	}

	/**
	 * method to save meta info about the tree 
	 * @param btree
	 * @param path
	 * @throws IOException
	 */
	@Override
	protected void saveTree(VariableLengthBPlusTree btree, String path) throws IOException{
		DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(path)));
		IndexEntry entry = (IndexEntry) btree.rootEntry();
		StringKeyRange range = (StringKeyRange) btree.rootDescriptor();
		// store root entry
		// 1. id -> longs
		LongConverter.DEFAULT_INSTANCE.write(out, (Long)entry.id());
		// 2. level 
		IntegerConverter.DEFAULT_INSTANCE.write(out, entry.parentLevel());
		// 3. key of the root
		StringConverter.DEFAULT_INSTANCE.write(out, (String)entry.separator().sepValue());
		// store 
		StringConverter.DEFAULT_INSTANCE.write(out, (String)range.minBound());
		StringConverter.DEFAULT_INSTANCE.write(out, (String)range.maxBound());
		out.close();
	}
	
	@Override
	protected VariableLengthBPlusTree reloadTree(
			VariableLengthBPlusTree tree, String path, Object... args) throws IOException{
		Container container = (Container) args[0];
		SplitStrategy strategy = (SplitStrategy)args[1];
		Double capacity = (Double)args[2];
		DataInputStream in = new DataInputStream( new FileInputStream(new File(path+"_metadata.dat")));
		Long id = LongConverter.DEFAULT_INSTANCE.read(in);
		int level = IntegerConverter.DEFAULT_INSTANCE.readInt(in);
		String key = StringConverter.DEFAULT_INSTANCE.read(in);
		String minKey = StringConverter.DEFAULT_INSTANCE.read(in);
		String maxKey = StringConverter.DEFAULT_INSTANCE.read(in);
		IndexEntry rootEntry = ((IndexEntry)tree.createIndexEntry(level)).initialize(id, new StringSeparator(key));
		StringKeyRange rootDescriptor = new StringKeyRange(minKey, maxKey);		
		in.close();
		tree.initialize(rootEntry, 
				rootDescriptor, 
				getKeyFunction, 
				container,
				keyConverter, 
				dataMeasuredConverter,
				StringSeparator.FACTORY_FUNCTION,
				StringKeyRange.FACTORY_FUNCTION,
				getKeySize,
				getDataSize, 
				strategy,
				new StandardUnderflowHandler()
		);
		
		return tree;
		
	}
	
	
	@DataProvider
	public Object[][] createTestParameter(){
		return new Object[][]{ 
				{new Integer(4096), new ShortestKeyStrategy(), new Double(0.33)}, 
			    {new Integer(4096), new ShortestKeyStrategy(), new Double(0.4)}, 
			    {new Integer(1024), new ShortestKeyStrategy(), new Double(0.33)},
			    {new Integer(2048), new ShortestKeyStrategy(), new Double(0.33)}, 
			    {new Integer(8192), new ShortestKeyStrategy(), new Double(0.4)}, 
			    {new Integer(4096), new SimplePrefixBPlusTreeSplit(), new Double(0.33)}, 
			    {new Integer(1024), new SimplePrefixBPlusTreeSplit(), new Double(0.33)},
			    {new Integer(2048), new SimplePrefixBPlusTreeSplit(), new Double(0.33)}, 
			    {new Integer(8192), new SimplePrefixBPlusTreeSplit(), new Double(0.4)},
			    {new Integer(4096), new SimplePrefixBPlusTreeSplit(), new Double(0.4)}
				};
	}
	
	@Test(dataProvider="createTestParameter")
	public void test(int blockSize, SplitStrategy strategy, double capacity){
		this.blockSize = blockSize;
		this.strategy = strategy;
		this.capacity = capacity;
		super.testTree(null);
	}
	
	/**
	 * 
	 */
	public void loadDataAndSaveTree(Object...args ){
		VariableLengthBPlusTree tree = new VariableLengthBPlusTree(blockSize, capacity, false);
		Container fileContainer = new BlockFileContainer(path, blockSize);
		Container bufferContainer = new BufferedContainer(fileContainer, new LRUBuffer(100));
		Container converterContainer = new ConverterContainer(bufferContainer, tree.nodeConverter());
		tree.initialize(null, 
				null, 
				getKeyFunction, 
				converterContainer,
				keyConverter, 
				dataMeasuredConverter,
				StringSeparator.FACTORY_FUNCTION,
				StringKeyRange.FACTORY_FUNCTION,
				getKeySize,
				getDataSize, 
				strategy,
				new StandardUnderflowHandler()
		);
		// load data even 
		for(Student st : evenStudents){
			tree.insert(st);
		}
		// flush conatiners 
		converterContainer.flush();
		converterContainer.close();
		try{
			saveTree(tree, path+"_metadata.dat");
		}
		catch(IOException ex){
			Assert.fail();
		}
		
	}
	
	
	
	
	/**
	 * 
	 */
	public void reloadRemoveAndThenInsertData(Object...args){
		// take parameters set in previous method
		VariableLengthBPlusTree tree = new VariableLengthBPlusTree(blockSize, capacity, false);
		Container fileContainer = new BlockFileContainer(path);
		Container bufferContainer = new BufferedContainer(fileContainer, new LRUBuffer(200));
		Container converterContainer = new ConverterContainer(bufferContainer, tree.nodeConverter());
		
		try {
			reloadTree(tree,path, converterContainer, strategy, capacity);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Problem with reload");
		}
		// load data even 
		for(Student st : evenStudents){
			Object obj = tree.remove(st);
			if (obj == null){
				Assert.fail("Problem with query");
				break;
			}
			
		}
		for(Student st : evenStudents){
			tree.insert(st);
		}
		// flush containers 
		converterContainer.flush();
		converterContainer.close();
		try{
			saveTree(tree, path+"_metadata.dat");
		}
		catch(IOException ex){
			Assert.fail();
		}
	}
	
	/**
	 * 
	 */
	public void reloadAndUpdateData(Object...args){
		// take parameters set in previous method
		VariableLengthBPlusTree tree = new VariableLengthBPlusTree(blockSize, capacity, false);
		Container fileContainer = new BlockFileContainer(path);
		Container bufferContainer = new BufferedContainer(fileContainer, new LRUBuffer(200));
		Container converterContainer = new ConverterContainer(bufferContainer, tree.nodeConverter());
		
		try {
			reloadTree(tree,path, converterContainer, strategy, capacity);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Problem with reload");
		}
		// load data even 
		int i = 0;
		for(Student st : evenStudents){
			tree.update(st, new Student(st.getName(), st.getMatrikelNr(), st.getInfo() + "_123456789"));
		}
		// flush containers 
		converterContainer.flush();
		converterContainer.close();
		try{
			saveTree(tree, path+"_metadata.dat");
		}
		catch(IOException ex){
			Assert.fail();
		}
	}
	
	
	/**
	 * 
	 */
	public void reloadAndQueryTree(Object...args){
		// take parameters set in previous method
		VariableLengthBPlusTree tree = new VariableLengthBPlusTree(blockSize, capacity, false);
		Container fileContainer = new BlockFileContainer(path);
		Container bufferContainer = new BufferedContainer(fileContainer, new LRUBuffer(200));
		Container converterContainer = new ConverterContainer(bufferContainer, tree.nodeConverter());
		
		try {
			reloadTree(tree,path, converterContainer, strategy, capacity);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("Problem with reload");
		}
		// load data even 
		int i = 0;
		for(Student st : evenStudents){
			Object obj = tree.exactMatchQuery(st.getName());
			if (obj == null){
				Assert.fail("Problem with query");
				break;
			}
		}
		// range query
		Cursor c = tree.rangeQuery("name_10000", "name_19999");
		int count = Cursors.count(c);
		int countLinear = rangeQueryTest(tree.query() , "name_10000", "name_19999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_20000", "name_29999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_20000", "name_29999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_30000", "name_39999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_30000", "name_39999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_40000", "name_59999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_40000", "name_59999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_50000", "name_99999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_50000", "name_99999");
		if (count != countLinear){
			Assert.fail();
		}
	}
	
	/*
	 * c is sorted
	 */
	private int rangeQueryTest(Cursor c , String minKey, String maxKey){
		int count = 0;
		while(c.hasNext()){
			String key = ((Student)c.next()).getName();
			if(key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0 ){
				count++;
			}
			if (key.compareTo(maxKey)>0)
				break;
		}
		return count;
	}
	/**
	 * 
	 */
	public void reloadAndAddData(Object...args){
		// take parameters set in previous method
		VariableLengthBPlusTree tree = new VariableLengthBPlusTree(blockSize, capacity, false);
		Container fileContainer = new BlockFileContainer(path);
		Container bufferContainer = new BufferedContainer(fileContainer, new LRUBuffer(200));
		Container converterContainer = new ConverterContainer(bufferContainer, tree.nodeConverter());
		try {
			reloadTree(tree,path , converterContainer, strategy, capacity);
		} catch (IOException e) {
			Assert.fail("Problem with reload");
		}
		// load data even 
		for(Student st : oddStudents){
			tree.insert(st);
		}
		// flush conatiners 
		converterContainer.flush();
		converterContainer.close();
		try{
			saveTree(tree, path+"_metadata.dat");
		}
		catch(IOException ex){
			Assert.fail();
		}
	}
	/**
	 * 
	 */
	public void reloadAndQueryAgain(Object...args){
		// take parameters set in previous method
		VariableLengthBPlusTree tree = new VariableLengthBPlusTree(blockSize, capacity, false);
		Container fileContainer = new BlockFileContainer(path);
		Container bufferContainer = new BufferedContainer(fileContainer, new LRUBuffer(200));
		Container converterContainer = new ConverterContainer(bufferContainer, tree.nodeConverter());
		try {
			reloadTree(tree,path , converterContainer, strategy, capacity);
		} catch (IOException e) {
			Assert.fail("Problem with reload");
		}
		// check point queries 
		// load data even 
		for(Student st : evenStudents){
			Object obj = tree.exactMatchQuery(st.getName());
			if (obj == null){
				Assert.fail("Problem with query");
				break;
			}
		}
		for(Student st : oddStudents){
			Object obj = tree.exactMatchQuery(st.getName());
			if (obj == null){
				Assert.fail("Problem with query");
				break;
			}
		}
		// range query
		Cursor c = tree.rangeQuery("name_10000", "name_19999");
		int count = Cursors.count(c);
		int countLinear = rangeQueryTest(tree.query() , "name_10000", "name_19999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_20000", "name_29999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_20000", "name_29999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_30000", "name_39999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_30000", "name_39999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_40000", "name_59999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_40000", "name_59999");
		if (count != countLinear){
			Assert.fail();
		}
		c = tree.rangeQuery("name_50000", "name_99999");
		count = Cursors.count(c);
		countLinear = rangeQueryTest(tree.query() , "name_50000", "name_99999");
		if (count != countLinear){
			Assert.fail();
		}
	}
}
