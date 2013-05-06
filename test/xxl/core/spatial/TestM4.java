package xxl.core.spatial;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.io.converters.ConvertableConverter;
import xxl.core.spatial.histograms.RGOhist;
import xxl.core.spatial.rectangles.DoublePointRectangle;


public class TestM4 {
	
	
	
	public static PrintStream getPrintStream(String output) throws IOException{
		return new PrintStream(new File(output)); 
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String pathStore = "f:/hist/";
		String dataPath = "f:/rtree/data";
		String queryPath ="f:/rtree/query_100";
		String path = "f:/hist/";
		String[] prefix  = {
			"rea",
		};
		int[] buckets = {500, 1000, 2000, 3000, 4000, 5000};
		buckets = new int[] {  1000};
		for(String p :prefix){
			System.out.println("++++++++++++++++++++++++++++++++++++\n");
			System.out.println("Data: " + p);
		
			FileInputCursor<DoublePointRectangle> data = new FileInputCursor<DoublePointRectangle>(
					new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), 
					new File(dataPath + "/" + p +"02.rec"));
			HistogramEval eval = new HistogramEval(data, path); 
			for(int numberOfBuckets : buckets){
				System.out.println("Buckets " + numberOfBuckets);
				
				// create 
//				PrintStream stream = getPrintStream(pathStore +  p + new Integer(numberOfBuckets).toString() + ".txt");
				
//				eval.stream = stream;
				eval.buildRTreeHist(numberOfBuckets, true);
				eval.buildRKHist(numberOfBuckets, 0.1, 32*100+6, true);
				eval.buildRHistogramV(numberOfBuckets, 0.4, 0.4, 0.8, true);
//				FileInputCursor<DoublePointRectangle> queryC = (FileInputCursor<DoublePointRectangle>) 
//						Cursors.resetableFileInputCursor(	new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), 
//						new File(queryPath + "/" + p +"02.rec"));
//				eval.buildRHistogramQA(numberOfBuckets, queryC, 0.5, 0.5, 0.8, true);
//				eval.buildRHistogramRK(numberOfBuckets, 0.5, 0.5, 0.5, true);
//				eval.buildRHistogramSKEW(numberOfBuckets, 0.5, 0.5, 0.8, true);
				eval.buildMinSkewHist(numberOfBuckets*2, 8, true);
				eval.buildMinSkewProgressiveHist(numberOfBuckets*2, 8, 3, true);
				//
				//
//				RGOhist.writeHistogram(eval.getRhistogram_V().histogram,pathStore + "ro_"+numberOfBuckets+"_"+p+".rec" );
//				RGOhist.writeHistogram(eval.getRTreeHist().histogram,pathStore + "rs_"+numberOfBuckets+"_"+p+".rec" );
				//
				String query = queryPath + "/" + p  + "02.rec"; 
				// s
				// test rtree
				Cursor<DoublePointRectangle> queryCursor = null;
				System.out.println("RTree");
				queryCursor = new FileInputCursor<DoublePointRectangle>(
						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
				eval.testHistogram(queryCursor, eval.getRTreeHist());
				System.out.println("RK-Hist");
				queryCursor = new FileInputCursor<DoublePointRectangle>(
						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
				eval.testHistogram(queryCursor, eval.getRkHist());
				// test sopttree
				System.out.println("RTree Volume");
				queryCursor = new FileInputCursor<DoublePointRectangle>(
						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
				eval.testHistogram(queryCursor, eval.getRhistogram_V());
//				System.out.println("RTree Volume Query");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getRhistogram_QA());
//				System.out.println("RTree RKHIST Metric");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getRhistogram_RK());
//				System.out.println("RTree SKEW Metric");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getRhistogram_SKEW());
				// test min skew 
//				System.out.println("MinSkew");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getMinSkewHist());
//				System.out.println("MinSkew Progressive");
//				queryCursor = new FileInputCursor<DoublePointRectangle>(
//						new ConvertableConverter<DoublePointRectangle>(RGOhist.factoryFunction(2)), new File(query));
//				eval.testHistogram(queryCursor, eval.getMinSkewProgressiveRefinementHistogram());
				System.out.println("******" +
						"****************\n*******************\n**************");
			}
		}
	}

}