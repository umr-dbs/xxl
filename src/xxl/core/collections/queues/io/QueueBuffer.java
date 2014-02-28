package xxl.core.collections.queues.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.collections.queues.AbstractQueue;
import xxl.core.io.converters.BooleanConverter;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.io.converters.LongConverter;
/**
 * This class implements a queue interface.
 * The aim of this class is to use a queue with a convereterContainer ({@link ConverterContainer}) (the usage is not restricted to this container class, other container hierarchy  could be used).     
 * The queue is implemented as a double linked list of buckets (pages, blocks). Buckets are persisted in container that manages block (or byte arrays). 
 * Queue buckets are mapped to byte arrays using internal converter and user provided {@link ConverterContainer}. 
 * For initializing a queue three parameter needed: Container, serialized size of an entry and block size of lowest container:
 * 
 * The following initialization pattern should be implemented, assume we want to manage a queue of Integer values:  
 *  1. we define a container, for example, {@link BlockFileContainer}
 *    Container blockFile = new {@link BlockFileContainer} (fileName, blockSize); 
 *  2. we need to provide a {@link Converter} for a objects stored in a queue
 *    Converter<Integer> dataConverter = {@link IntegerConverter#DEFAULT_INSTANCE};
 *  3. we create a ConverterContainer 
 *     Container queueConatainer = new {@link ConverterContainer}(blockFile, @link #getPageConverter(dataConverter)});
 *  4. we crete queue 
 *    {@link QueueBuffer}( queueConatainer, {@link IntegerConverter#SIZE}, blockSize );
 *  
 * @author achakeye
 *
 * @param <E>
 */
public class QueueBuffer<E>  extends AbstractQueue<E> {
	
	/**
	 * pointer size 
	 */
	public static final int POINTER_SIZE = 9; 
	/**
	 * number of bytes needed to store the number of elements per page
	 */
	public static final int LENGTH_SIZE = 4; 
	
	
	/**
	 * 
	 * @param entryConverter
	 * @return
	 */
	public static <E> Converter<SimplePage<E>> getPageConverter(final Converter<E> entryConverter){
		return new PageConverter<>(entryConverter); 
	}
	
	/**
	 * 
	 */
	protected Converter<Long> idConverter;
	/**
	 * 
	 */
	protected Container entryContainer;
	/**
	 * 
	 */
	protected int entrySerializedSize;
	/**
	 * 
	 */
	protected Long headId; 
	/**
	 * 
	 */
	protected Long tailId;
	/**
	 * 
	 */
	protected Long next;
	/**
	 * 
	 */
	protected Long lastNotInitPageNumber; 
	/**
	 * 
	 */
	protected boolean lastPageNotInit; 
	/**
	 * 
	 */
	protected int index; 
	/**
	 * 
	 */
	protected int tailOffSet; 
	/**
	 * 
	 */
	private int maxEntriesPerPage;
	
	/**
	 * 
	 *  Creates a new queue
	 * 
	 * @param entryContainer 
	 * @param entrySerializedSize
	 * @param blockSize
	 */
	public QueueBuffer(Container entryContainer,
			int entrySerializedSize, int blockSize) {
		super();
		this.entryContainer = entryContainer;
		this.entrySerializedSize = entrySerializedSize;
		// init first page
		this.headId =   null;
		this.tailId = (Long) this.entryContainer.reserve(null);
		this.index = 0;
		this.tailOffSet = 0; 
		this.size = 0;
		this.next = null;
		lastPageNotInit = true; 
		this.maxEntriesPerPage = (blockSize-2*POINTER_SIZE - LENGTH_SIZE)/entrySerializedSize; 
	}

	
	
	
	/**
	 * 
	 * @return
	 */
	private SimplePage<E> createPage(){
		return new SimplePage<E>(new ArrayList<E>()); 
	}
	
	/*
	 * (non-Javadoc)
	 * @see xxl.core.collections.queues.AbstractQueue#enqueueObject(java.lang.Object)
	 */
	@Override
	public void enqueueObject(E object) throws IllegalStateException {
		SimplePage<E> page = null; 
		if(tailOffSet == 0){
			page = createPage();
			page.entries.add(object); // add to page
			page.next = next; // set link to next
			entryContainer.update(tailId, page); // update entry
			lastPageNotInit = false; 
			tailOffSet++;
		}else{
			page = (SimplePage<E>) entryContainer.get(tailId, false);
			page.entries.add(object); 
			tailOffSet++;
			if (page.entries.size() >= maxEntriesPerPage){	
				next = tailId;
				tailId = (Long) entryContainer.reserve(null); // allocate new page link 
				tailOffSet=0; // reset offset
				lastNotInitPageNumber = tailId; // store as last not init page number
				page.prev = tailId;
				lastPageNotInit = true;
				entryContainer.update(next, page);
			}else{
				entryContainer.update(tailId, page);
			}
			
		}
		if(headId == null){ // not init head
			if(lastPageNotInit){
				headId = next;
				index = page.entries.size()-1; 
			}
			else{
				headId = tailId;
				index =  tailOffSet-1;
			}
		}
		//size++; is managed by super class
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.collections.queues.AbstractQueue#dequeueObject()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public E dequeueObject() throws IllegalStateException, NoSuchElementException {
		if(isEmpty()){
			throw new NoSuchElementException("Queue is empty"); 
		} 
		E entry = null; 
		SimplePage<E> head = (SimplePage<E>) entryContainer.get(headId, true); 
		try{
			entry = head.entries.get(index);
		}catch(Exception ex){
			//
			throw new RuntimeException(ex);
		}
		index++; 
		if(index >= head.entries.size() ){
			// init head
			index = 0; 
			if(headId.compareTo(tailId) != 0 ){
				entryContainer.remove(headId);
			}
			headId = head.prev;
			
		}
//		size--;
		return entry;
	}

	

	
	/*
	 * (non-Javadoc)
	 * @see xxl.core.collections.queues.Queue#clear()
	 */
	@Override
	public void clear() {
		 // simple strategy
		// delete all dequeue all objects
		while(!this.isEmpty()){
			this.dequeue();
		}
		
	}
	/*
	 * (non-Javadoc)
	 * @see xxl.core.collections.queues.AbstractQueue#peekObject()
	 */
	@Override
	protected E peekObject() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Internal bucket/page. 
	 *
	 *
	 */
	public static class SimplePage<E>{
		
		public List<E> entries;
		public Long prev; 
		public Long next;
		
		public SimplePage(List<E> triples) {
			super();
			this.entries = triples;
		}

		public List<E> getTriples() {
			return entries;
		}

		public void setTriples(List<E> triples) {
			this.entries = triples;
		}

		@Override
		public String toString() {
			return "SimplePage [entries=" + entries + ", prev=" + prev
					+ ", next=" + next + "]";
		}
		
	}
	/**
	 * Page converter for a internal page representation;
	 * 
	 * see {@link ConverterContainer}. 
	 * 
	 * 
	 * @author achakeye
	 *
	 * @param <E>
	 */
	public static class PageConverter<E> extends Converter<SimplePage<E>>{

		Converter<E> entryConverter;
		
		protected PageConverter(Converter<E> entryConverter){
			this.entryConverter = entryConverter;
		}
		
		@Override
		public SimplePage<E> read(DataInput dataInput, SimplePage<E> object)
				throws IOException {
			
			int listSize = 	IntegerConverter.DEFAULT_INSTANCE.readInt(dataInput);
			List<E> entries = new ArrayList<>(listSize);
			for(int i = 0; i < listSize; i++){
				entries.add(entryConverter.read(dataInput));
			}
			SimplePage<E> page = new SimplePage<>(entries);
			boolean haslink = BooleanConverter.DEFAULT_INSTANCE.readBoolean(dataInput);
			if(!haslink){
				page.next = new Long(LongConverter.DEFAULT_INSTANCE.readLong(dataInput));
			}
			haslink =  BooleanConverter.DEFAULT_INSTANCE.readBoolean(dataInput);
			if(!haslink){
				page.prev = new Long(LongConverter.DEFAULT_INSTANCE.readLong(dataInput));
			}
			return page;
		}

		@Override
		public void write(DataOutput dataOutput, SimplePage<E> object)
				throws IOException {
			IntegerConverter.DEFAULT_INSTANCE.writeInt(dataOutput, object.entries.size());
			for(E entry: object.entries){
				entryConverter.write(dataOutput, entry);
			}
			BooleanConverter.DEFAULT_INSTANCE.writeBoolean(dataOutput, object.next==null);
			if( object.next!=null){
				LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput,  object.next); 
			}
			BooleanConverter.DEFAULT_INSTANCE.writeBoolean(dataOutput,  object.prev==null);
			if( object.prev!=null){
				LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput,  object.prev); 
			}
		}
		
	}
}
