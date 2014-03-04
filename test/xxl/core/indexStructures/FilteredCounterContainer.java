package xxl.core.indexStructures;

import java.util.NoSuchElementException;

import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.CounterContainer;
import xxl.core.functions.Functional.UnaryFunction;
/**
 * This counter container can be used for counting leaf access to a index structure. 
 * 
 * This container counts gets and insert only if predicate is true. 
 *  
 * @author achakeye
 *
 */
public class FilteredCounterContainer extends CounterContainer {
	
	
	/**
	 * 
	 */
	public static  UnaryFunction<Object, Boolean> RTREE_LEAF_NODE_COUNTER_FUNCTION  = new UnaryFunction<Object, Boolean>() {
		
		@Override
		public Boolean invoke(Object arg) {
			if(arg instanceof ORTree.Node && ((ORTree.Node)arg).level() == 0){
				return true;
			}
			return false;
		}
	};
	
	
	/**
	 * 
	 */
	protected UnaryFunction<Object, Boolean> filter; 
	
	/**
	 * 
	 */
	public int getsPredicates; 
	
	/**
	 * 
	 */
	public int insertPredicates;
	
	/**
	 * 
	 * @param container
	 */
	public FilteredCounterContainer(Container container, UnaryFunction<Object, Boolean> function) {
		super(container);
		this.filter = function;
	}

	/**
	 * Resets the counters for insert, get, update and remove methods.
	 * A call of this method sets every counter to 0.
	 */
	public void reset () {
		inserts = gets = updates = removes = reserves = insertPredicates = getsPredicates = 0;
	}

	/**
	 * Returns the object associated to the identifier <tt>id</tt>. An
	 * exception is thrown if there is not object stored with this
	 * <tt>id</tt>. If unfix is set to true, the object can be removed
	 * from the underlying buffer. Otherwise (!unfix), the object has
	 * to be kept in the buffer.
	 *
	 * @param id identifier of the object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the object associated to the specified identifier.
	 * @throws NoSuchElementException if the desired object is not found.
	 */
	public Object get (Object id, boolean unfix) throws NoSuchElementException {
		Object object = super.get(id, unfix);
		if(this.filter.invoke(object)){
			getsPredicates++;
		}
		return object;
	}

	/**
	 * Inserts a new object into the container and returns the unique
	 * identifier that the container has been associated to the object.
	 * The identifier can be reused again when the object is deleted from
	 * the buffer. If unfixed, the object can be removed from the buffer.
	 * Otherwise, it has to be kept in the buffer until an
	 * <tt>unfix()</tt> is called.<br>
	 * After an insertion all the iterators operating on the container can
	 * be in an invalid state.<br>
	 * This method also allows an insertion of a null object. In the
	 * application would really like to have such objects in the
	 * container, some methods have to be modified.
	 *
	 * @param object is the new object.
	 * @param unfix signals whether the object can be removed from the
	 *        underlying buffer.
	 * @return the identifier of the object.
	 */
	public Object insert (Object object, boolean unfix) {
		Object id = super.insert(object, unfix);
		if(this.filter.invoke(object)){
			insertPredicates++;
		}
		return id;
	}

	/**
	 * Outputs the collected statistic to a String.
	 * @return a String representation of the collected statistics. 
	 */
	public String toString() {
		return "inserts = "+inserts + ", " +
		       "gets = "+gets + ", " +
		       "updates = "+updates + ", " +
		       "removes = "+removes + ", " +
		       "reserves = "+reserves  + ", " +
		       "insertPredicates =  " + insertPredicates  + ", " +
		       "getPredicates = " + getsPredicates +  ";";
	}
	
}
