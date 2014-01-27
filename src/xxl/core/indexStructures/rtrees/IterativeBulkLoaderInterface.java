package xxl.core.indexStructures.rtrees;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.indexStructures.RTree;
import xxl.core.spatial.rectangles.DoublePointRectangle;

public interface IterativeBulkLoaderInterface<T> {
	/**
	 * 
	 * @param rectangles
	 * @throws IOException
	 */
	public abstract void buildRTree(Iterator rectangles) throws IOException;

}