package xxl.core.collections.containers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import xxl.core.functions.Function;
import xxl.core.io.converters.FixedSizeConverter;
import xxl.core.io.converters.LongConverter;

/**
 * Strores addresses as indexes of array
 * 
 *
 */
public class ArrayContainer extends AbstractContainer{
	
	
	private ArrayList<Object> array;
	/**
	 * A unique object used to identify mappings where no object has been
	 * assigned to so far.
	 */
	protected static final Object empty = new Object();
	
	/**
	 * A counter that is used to create unique ids. Everytime an object is
	 * inserted into the container the counter is increased and a
	 * <tt>Long</tt> object with the actual value of the counter is
	 * returned as id.
	 */
	protected long counter = 0;
	
	public ArrayContainer(int initialSize){
		array = new ArrayList<Object>(initialSize); 
	}
	
	public ArrayContainer(){
		this(42000);
	}
	
	
	@Override
	public Object get(Object id, boolean unfix) throws NoSuchElementException {
		long idLong = (Long)id;
		if (idLong >= array.size() )
			throw new NoSuchElementException();
		return array.get((int)idLong);// fixme down cast
	}
	
	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * This container uses a counter to generate an unique id. Everytime
	 * an object is inserted into the container the counter is increased
	 * and a <tt>Long</tt> object with the actual value of the counter is
	 * returned as id. So the identifier will not be reused again when the
	 * object is deleted from the container. The parameter <tt>unfix</tt> 
	 * has no function because this container is unbuffered.
	 *
	 * @param object is the new object.
	 * @param unfix signals a buffered container whether the object can
	 *        be removed from the underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		Object id = new Long(counter++);
		array.add(object);
		return id;
	}
	
	@Override
	public Iterator ids() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isUsed(Object id) {
		long idLong = (Long)id;
		return (idLong >= array.size() );
	}

	@Override
	public FixedSizeConverter objectIdConverter() {
		return LongConverter.DEFAULT_INSTANCE;
	}

	@Override
	public void remove(Object id) throws NoSuchElementException {
		long idLong = (Long)id;
		if (idLong >= array.size() )
			throw new NoSuchElementException();
		array.remove((int)idLong);
	}

	@Override
	public Object reserve(Function getObject) {
		Object id = new Long(counter++);
		// do not use insert because cloning of empty must not be done.
		array.add(empty);
		return id;
	}

	@Override
	public int size() {
		return array.size();
	}

	@Override
	public void update(Object id, Object object, boolean unfix)
			throws NoSuchElementException {
		long idLong = (Long)id;
		if (idLong >= array.size() )
			throw new NoSuchElementException();
		array.set((int)idLong, object);
	}

}
