package xxl.core.spatial.histograms;

import java.io.IOException;
import java.util.List;
import java.util.Properties;



import xxl.core.cursors.Cursor;
import xxl.core.spatial.rectangles.DoublePointRectangle;

public interface MHistogram {
	
	/**
	 * computes spatial selectivity
	 */
	public double getSelectivity(DoublePointRectangle queryRec);
	/**
	 * builds histogram
	 * @param rectangles
	 * @param numberOfBuckets
	 * @param props
	 */
	public void buildHistogram(Cursor<DoublePointRectangle> rectangles, int numberOfBuckets, Properties props) throws IOException;
	/**
	 * 
	 * @return
	 */
	public int numberOfBuckets();
	
	/**
	 * 
	 * @return
	 */
	public List<WeightedDoublePointRectangle> getBuckets();
}
