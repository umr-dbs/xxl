/* XXL: The eXtensible and fleXible Library for data processing

Copyright (C) 2000-2013 Prof. Dr. Bernhard Seeger
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
package xxl.core.spatial;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import xxl.core.cursors.Cursor;
import xxl.core.cursors.filters.Filter;
import xxl.core.cursors.identities.TeeCursor;
import xxl.core.cursors.sources.io.FileInputCursor;
import xxl.core.indexStructures.Descriptors;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.DoubleConverter;
import xxl.core.io.converters.MeasuredConverter;
import xxl.core.predicates.AbstractPredicate;
import xxl.core.predicates.Predicate;
import xxl.core.spatial.converters.AsciiKPEConverter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;
//import org.apache.batik.dom.GenericDOMImplementation;
//import org.apache.batik.svggen.SVGGraphics2D;
//import org.apache.batik.dom.GenericDOMImplementation;
//import org.apache.batik.svggen.SVGGraphics2D;

public class TestPlot extends JFrame{
	
	public static final String SVG_PATH = "figure.svg"; 
	
	
	public static final Color D_COLOR_B = Color.RED;
	public static final Color D_COLOR_F = null;
	/**
	 * 
	 * @param dim
	 * @return
	 */
	public static Converter	getACIIRecConverter(final int dim){
		final AsciiKPEConverter kpeConverter = new AsciiKPEConverter( dim); 
		return new Converter(){
			@Override
			public Object read(DataInput dataInput, Object object) throws IOException {
				KPE kpe = (KPE)kpeConverter.read(dataInput, object);
				DoublePointRectangle rec = (DoublePointRectangle)kpe.getData();
				return rec;
			}
			@Override
			public void write(DataOutput dataOutput, Object object) throws IOException {
				throw new UnsupportedOperationException();
				
			}
			
		};
	}
	
	public static DoublePointRectangle computeUniverse(Iterator<DoublePointRectangle> elements, int dim){
		DoublePointRectangle universe = new DoublePointRectangle(dim);
		if(elements.hasNext()){
			universe = (DoublePointRectangle) elements.next().clone();
		}
		while(elements.hasNext()){
			universe.union(elements.next());
		}
		return universe;
	}
	
	
	/**
	 * 
	 * @param dim
	 * @return
	 */
	public static MeasuredConverter getRecsConverter(final int dim){
		return new MeasuredConverter(){
			@Override
			public int getMaxObjectSize() {
				return DoubleConverter.SIZE*2*dim;
			}
			@Override
			public Object read(DataInput dataInput, Object object) throws IOException {
				DoublePointRectangle rec = new DoublePointRectangle(dim);
				rec.read(dataInput);
				return rec;
			}
			@Override
			public void write(DataOutput dataOutput, Object object) throws IOException {
				DoublePointRectangle rec = (DoublePointRectangle)object;
				rec.write(dataOutput);
			}
		};
	}
	
	/**
	 * 
	 * @param data
	 * @param n
	 * @return
	 */
	public static Iterator filterJumpNElements(final Iterator data, final int n){
		final Predicate filterN = new AbstractPredicate(){
			int index = 1;
			public boolean invoke(Object obj){
				return ((index++) % n == 0);  
			};
		}; 
		return new Filter(data, filterN);
	}
	
	JPanel paintPanel;
	Dimension prefferedDim;
	String fileName;
	
	/**
	 * 
	 * @param data
	 * @param raum
	 * @param transform
	 */
	public TestPlot(Iterator data, int raum, DoublePointRectangle r){
		
		paintPanel = new TestPanel(data, r, raum);
		int size = ( ((TestPanel)paintPanel).maxR > 800) ? 800 : ( ((TestPanel)paintPanel).maxR) + 50;
		prefferedDim = new Dimension(size, size);
		init();
	}
	/**
	 * 
	 * @param data
	 * @param raum
	 * @param transform
	 */
	public TestPlot(String name,Iterator data, int raum, DoublePointRectangle r){
		super(name);
		paintPanel = new TestPanel(data, r, raum, name+".svg");
		int size = ( ((TestPanel)paintPanel).maxR > 800) ? 800 : ( ((TestPanel)paintPanel).maxR) + 50;
		prefferedDim = new Dimension(size, size);
		init();
	}
	/**
	 * 
	 *
	 */
	private void init(){
		JScrollPane scrollPane = new JScrollPane(paintPanel); 
		this.getContentPane().add(scrollPane);
		this.setPreferredSize(prefferedDim);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.pack();
		this.setVisible(true);
	}
	/**
	 * 
	 * @param data
	 * @param fill
	 * @param bound
	 */
	public void drawMoreElements(Iterator data, Color fill, Color bound){
		((TestPanel)this.paintPanel).setNewRectanglesToDraw(data, fill, bound);
		((TestPanel)this.paintPanel).repaint();
	}
	/**
	 * 
	 * @param rec
	 * @param fill
	 * @param bound
	 */
	public void drawOneElement(DoublePointRectangle rec, Color fill, Color bound){
		 List list = new ArrayList();
		 list.add(rec);
		((TestPanel)this.paintPanel).setNewRectanglesToDraw(list.iterator(), fill, bound);
		((TestPanel)this.paintPanel).repaint();
	}
	/**
	 * 
	 * @author achakeye
	 *
	 */
	public class TestPanel extends JPanel{
		int rand = 20; 
		int achsen;
		int maxR;
		List<MetaInfoObject> iteratorList; 
		boolean coordinates;
		double x;
		double y;
		double scaleX;
		double scaleY;
		
		/**
		 * generator
		 */
//		private SVGGraphics2D generator; 
		private boolean written = false; 
		private String path;
		
		/**
		 * 
		 * @param data
		 * @param minMaxData
		 * @param raum
		 * @param c
		 */
		public TestPanel(Iterator data, DoublePointRectangle minMaxData,  int raum){
			iteratorList = new ArrayList<MetaInfoObject>();
			iteratorList.add(new MetaInfoObject(data, D_COLOR_F, D_COLOR_B));
			setNewData( data,  minMaxData,  raum,  false);
			/**
			 * svg generator init
			 */
//			DOMImplementation dom = GenericDOMImplementation.getDOMImplementation();
//			Document doc = dom.createDocument(null, "svg", null);
//			generator = new SVGGraphics2D(doc);
//			path = SVG_PATH;
		}
		
		/**
		 * 
		 * @param data
		 * @param minMaxData
		 * @param raum
		 * @param c
		 */
		public TestPanel(Iterator data, DoublePointRectangle minMaxData,  int raum, String path){
			iteratorList = new ArrayList<MetaInfoObject>();
			iteratorList.add(new MetaInfoObject(data, D_COLOR_F, D_COLOR_B));
			setNewData( data,  minMaxData,  raum,  false);
			/**
			 * svg generator init
			 */
//			DOMImplementation dom = GenericDOMImplementation.getDOMImplementation();
//			Document doc = dom.createDocument(null, "svg", null);
//			generator = new SVGGraphics2D(doc);
//			this.path = path;
		}
		
		/**
		 * 
		 */
		public void paintComponent(Graphics g){
			super.paintComponent(g);
			Graphics2D g2D = (Graphics2D)g;
			
			for (int i = 0; i < iteratorList.size(); i++){
				MetaInfoObject o = iteratorList.get(i);
				Iterator elem = o.dataCursor.cursor();
				while(elem.hasNext()){
					DoublePointRectangle rec = (DoublePointRectangle )elem.next();
				//	System.out.println("Rec to draw " + rec);
					paintRec( g2D,rec, x,  y,scaleX, scaleY,
							o.fill,  o.bound);
					// write into 
					if(!written){
						
//						paintRec( generator,rec, x,  y,scaleX, scaleY,
//								o.fill,  Color.black); // 
					}
				}
				
			}
			if (!written){
				written = true; 
//				try {
////					FileWriter file = new FileWriter(this.path);
////					PrintWriter writer = new PrintWriter(file);
////					generator.stream(writer);
//					writer.close();
//				} catch (IOException ioe) {
//					ioe.printStackTrace();
//					throw new RuntimeException("SVG write problem", ioe);
//				}
			}
		}
		
		
		/**
		 * 
		 * @param g2D
		 * @param minx
		 * @param miny
		 * @param maxx
		 * @param maxy
		 * @param fill
		 */
		public void paintRec(Graphics2D g2D, DoublePointRectangle rec, double x, double y, double sX, double sY,
				Color fill1, Color bound1){
			DoublePoint pointLeft = (DoublePoint)(rec.getCorner(false));
			DoublePoint pointRight = (DoublePoint)(rec.getCorner(true));
			double minx =  (((double[])pointLeft.getPoint())[0]) ;
			double miny =  (((double[])pointLeft.getPoint())[1]);
			double maxx =  (((double[])pointRight.getPoint())[0]);
			double maxy =  (((double[])pointRight.getPoint())[1]);
			double d1 = maxx-minx;
			double d2 = maxy-miny;
		
			minx =  (minx + x) * sX; // verschiebe
			miny =  (miny + y) * sY;
			maxx =  (d1*sX) + minx ;
			maxy = (d2*sY) + miny; 
			if(maxx == minx && maxy == miny){
				maxx = 1 + minx;
				maxy = 1 + miny;
				
			}
			if (fill1 != null){
				g2D.setColor(fill1);
				g2D.fillRect((int)minx + rand, -(int)maxy +(rand+achsen), (int)maxx - (int)minx, (int)maxy-(int)miny);
			}
			g2D.setColor(bound1);
			g2D.drawRect((int)minx + rand, -(int)maxy +(rand+achsen), (int)maxx - (int)minx, (int)maxy-(int)miny);
		}
		/**
		 * 
		 * @param c
		 */
		public void setCoord(boolean c){
			this.coordinates = c;
		} 
		/**
		 * 
		 * @param g2D
		 */
		public void paintCoordinates(Graphics2D g2D){
			if (coordinates){
			g2D.setColor(Color.BLUE);
			g2D.drawLine(rand , rand,  rand , achsen + rand);
			g2D.drawLine(rand , rand + achsen,  rand + achsen , rand+achsen);
			g2D.drawString(new Integer(achsen).toString(), rand-5, rand-5);
			g2D.drawString(new Integer(achsen).toString(), rand+ achsen+ 5, rand+5 + achsen);
			for (int i = 10; i <  achsen  ; i += 10 ){
				g2D.drawLine(rand-5, rand + i,  rand, rand + i);
				g2D.drawLine(rand+i , rand+ achsen + 5,  rand + i, rand + achsen );
			}	
			}
		}
		/**
		 * 
		 *
		 */
		public void setNewData(Iterator data, DoublePointRectangle minMaxData,  int raum,  boolean c){
			achsen = raum;
			maxR = raum + rand *2;
			double deltaX = minMaxData.deltas()[0];
			double deltaY = minMaxData.deltas()[1];
			this.setPreferredSize(new Dimension(raum + rand+rand , raum + rand+rand)); 
			this.setDoubleBuffered(true);
			this.setOpaque(true);
			coordinates = c;
			this.x = -minMaxData.getCorner(false).getValue(0) ;
			this.y = -minMaxData.getCorner(false).getValue(1);
			this.scaleX = raum /deltaX;
			this.scaleY = raum /deltaY;
			
			this.setBackground(Color.BLACK);
		}
		/**
		 * 
		 * @param data
		 * @param fill
		 * @param bound
		 */
		public void setNewRectanglesToDraw(Iterator data, Color fill, Color bound){
			MetaInfoObject o = new MetaInfoObject(data, fill, bound);
			iteratorList.add(o);
		}
		
		
		/**
		 * 
		 * 
		 *
		 */
		public class MetaInfoObject{
			Color fill;
			Color bound;
			TeeCursor dataCursor;
			public MetaInfoObject(Iterator data, Color fill, Color bound){
				this.fill = fill;
				this.bound = bound;
				dataCursor = new TeeCursor(data);
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Cursor fileCursor = new FileInputCursor(getACIIRecConverter(2),new File("D:/tests/data2DimGeoTiger/data/stu.asc"));
		DoublePointRectangle mbr1 = (fileCursor.hasNext()) ? (DoublePointRectangle)fileCursor.next(): null;
		while(fileCursor.hasNext() ){
			mbr1 =(DoublePointRectangle) Descriptors.union(mbr1, (DoublePointRectangle)fileCursor.next() );
		}
		System.out.println(mbr1);
		fileCursor = new FileInputCursor(getACIIRecConverter(2),new File("D:/tests/data2DimGeoTiger/data/stu.asc"));
		final DoublePointRectangle mbr = mbr1;
//		final DoublePointRectangle mbr = new DoublePointRectangle( new double[]{-124.406566, 32.540257},  new double[]{-114.133647,42.00924});
		final Iterator dataIterator = filterJumpNElements(fileCursor, 1);
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
              TestPlot frame = new   TestPlot(dataIterator, 800, mbr );
            }
        });

	}

}
