package xxl.core.indexStructures.rtrees;

import java.io.IOException;
import java.util.Iterator;

public interface IterativeBulkLoaderInterface<T> {
	/**
	 * 
	 * @param rectangles
	 * @throws IOException
	 */
	public abstract void buildRTree(Iterator<T> data) throws IOException;

}