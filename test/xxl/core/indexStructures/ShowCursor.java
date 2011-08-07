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

package xxl.core.indexStructures;

import java.awt.Color;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.image.BufferedImage;
import java.util.Iterator;

import xxl.core.cursors.SecureDecoratorCursor;
import xxl.core.spatial.KPE;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.points.Point;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.core.spatial.rectangles.Rectangle;

/**
 * This cursor draws the objects of the used iterator.
 */
public class ShowCursor<T> extends SecureDecoratorCursor<T> {
	
	/**
	 * The frame showing the Ractangles.
	 */
	protected Frame f = null;

	/**
	 * The <tt>Graphics</tt> Object of {link #f}.
	 */
	Graphics gr_frame = null;

	/**
	 * An image used for drawing.
	 */
	protected BufferedImage image;
	
	/**
	 * The <tt>Graphics</tt> Object of {link #image}.
	 */
	Graphics gr_im;
	
	/**
	 * The width of {@link #f}.
	 */
	int sizeX;
	
	/**
	 * The height of {@link #f}.
	 */
	int sizeY;
	
	/**
	 * The delay between drawing the rectangles.
	 */
	int delay;
	
	/**
	 * A counter for the rectangles.
	 */
	int rCount;
	
	/**
	 * The universe all rectangle lie within. 
	 */
	Rectangle universe;
	
	/**
	 * Translation in x direction.
	 */
	double xt = 0.0;
	
	/**
	 * Translation in y direction.
	 */
	double yt = 0.0;
	
	/**
	 * Dilation in x direction.
	 */
	double wt = 1.0;
	
	/**
	 * Dilation in y direction.
	 */
	double ht = 1.0;
	
	public static int DOUBLE_POINT_MODE = 0;
	public static int KPE_DOUBLE_POINT_RECTANGLE_MODE = 1;
	
	protected int mode;
	
	/**
	 * Creates a new <tt>ShowKPERectanglesCursor</tt>.
	 * 
	 * @param it an iterator providing the rectangles to display
	 * @param universe a rectangle containing all rectangles in <tt>it</tt>
	 * @param delay the delay between drawing the rectangles
	 */
	public ShowCursor(int mode, Iterator<T> it, Rectangle universe, int delay) {
		super(it);
		this.mode = mode;
		this.universe = universe;
		this.delay = delay;
		rCount=0;
	}
	
	/** 
	 * Creates the frame for displaying.
	 * 
	 * @param sizeX width of the frame
	 * @param sizeY height of the frame
	 * @return the creates frame
	 */
	public Frame createFrame(int sizeX, int sizeY) {
		f = new Frame() {
			public void paint(Graphics gr) {
				super.paint(gr);
				if (gr_frame!=null)
					gr_frame.drawImage(image,0,0,f);
			}
		};
		f.setSize(sizeX, sizeY);
		this.sizeX = f.getWidth();
		this.sizeY = f.getHeight();
		f.setVisible(true);
		f.addWindowListener(
			new WindowAdapter() {
				public void windowClosing(WindowEvent e) {
					f.dispose();
				}
			}
		);
		image = new BufferedImage(sizeX,sizeY,BufferedImage.TYPE_INT_RGB);
		gr_im = image.getGraphics();
		gr_im.setColor(Color.white);
		gr_im.fillRect(0, 0, sizeX, sizeY);
		gr_im.setColor(Color.black);
		gr_frame = f.getGraphics();		
		return f;
	}
	
	/** Returns the next element in the iteration.
	 *
	 * @return the next element in the iteration.
	 */
	public T next() {
		T k = super.next();
		if (mode==DOUBLE_POINT_MODE) 			
			draw((DoublePoint)k, Color.black, false);
		else if (mode==KPE_DOUBLE_POINT_RECTANGLE_MODE)
			draw((DoublePointRectangle)((KPE)k).getData(), Color.black);
		try { 
			Thread.sleep(delay); 
		} 
		catch (InterruptedException e) {};	
		return k;
	}
	
	//TODO: Do the scaling in universe
	private int xRealToDraw(double x) {
		return ((int) (x*sizeX-1));
	}
	//TODO: Do the scaling in universe
	private int yRealToDraw(double y) {
		return (sizeY-1-(int) (y*sizeY-1));
	}

	public void drawVoronoiLine(DoublePoint a, DoublePoint b) {
		double ax = (a.getValue(0)-universe.getCorner(false).getValue(0))/universe.deltas()[0];
		double ay = (a.getValue(1)-universe.getCorner(false).getValue(1))/universe.deltas()[1];
		
		double bx = (b.getValue(0)-universe.getCorner(false).getValue(0))/universe.deltas()[0];
		double by = (b.getValue(1)-universe.getCorner(false).getValue(1))/universe.deltas()[1];
		
		//compute slope of AB
		//TODO: catch possible ArithmeticException 
		double slopeAB = (by-ay)/(bx-ax);
		double slope_pb_AB = (-1)/slopeAB;
		
		
		double delta_pb_AB_x = 1;
		double slope_pb_AB_y = slope_pb_AB;
		
		double centerABx = Math.min(ax, bx)+(Math.abs(ax-bx)/2);
		double centerABy = Math.min(ay, by)+(Math.abs(ay-by)/2);
		
		
		
		//TODO: calculate intersection, draw only visible lines
		gr_im.drawLine(xRealToDraw(centerABx - 1), yRealToDraw(centerABy - slope_pb_AB_y),
				xRealToDraw(centerABx + 1), yRealToDraw(centerABy + slope_pb_AB_y)
		);
	}
	/**
	 * Draws a rectangle
	 * @param rect The rectangle to draw
	 */
	public void drawRect(DoublePointRectangle rect) {
		DoublePoint[] corners = (DoublePoint[]) rect.getCorners();
		
		DoublePoint[] scaledCornes = new DoublePoint[corners.length];
		for (int i=0;i<scaledCornes.length;i++) {
			scaledCornes[i] = new DoublePoint(new double[]{
					(corners[i].getValue(0)-universe.getCorner(false).getValue(0))/universe.deltas()[0],
					(corners[i].getValue(1)-universe.getCorner(false).getValue(1))/universe.deltas()[1]
					
			});
		}
		DoublePoint temp = scaledCornes[2];
		scaledCornes[2] = scaledCornes[3];
		scaledCornes[3] = temp;
		
		
		gr_im.setColor(Color.GRAY);
		for (int i=0;i<scaledCornes.length;i++) {
			gr_im.drawLine(
					xRealToDraw(scaledCornes[i].getValue(0)), yRealToDraw(scaledCornes[i].getValue(1)),
					xRealToDraw(scaledCornes[(i+1)%4].getValue(0)), yRealToDraw(scaledCornes[(i+1)%4].getValue(1))
			);
		}
		
	}
	
	/**
	 * Draws the given point in the given color.
	 * 
	 * @param p the point to draw 
	 * @param color the color to use
	 */
	public void mark(DoublePoint p, Color color) {
		draw(p, color, true);
	}
	
	/**
	 * Draws the given point and a horizontal and vertical line through it. 
	 * 
	 * @param p the point to draw
	 */
	public void drawQueryPoint(DoublePoint p) {
		double x = (p.getValue(0)-universe.getCorner(false).getValue(0))/universe.deltas()[0];
		double y = (p.getValue(1)-universe.getCorner(false).getValue(1))/universe.deltas()[1];
		gr_im.setColor(Color.red);
		gr_im.drawLine(0, (sizeY-1-(int) (y*sizeY-1)), sizeX-1, (sizeY-1-(int) (y*sizeY-1)));
		gr_im.drawLine(((int) (x*sizeX-1)), 0, ((int) (x*sizeX-1)), sizeY-1);
	}
	
	/**
	 * Draws the given point in the given color.
	 * 
	 * @param p the point to draw 
	 * @param color the color to use
	 * @param mark if this flag is set, the point in drawn bigger 
	 */
	protected void draw(DoublePoint p, Color color, boolean mark) {
		gr_im.setColor(color);
		double x = (p.getValue(0)-universe.getCorner(false).getValue(0))/universe.deltas()[0];
		double y = (p.getValue(1)-universe.getCorner(false).getValue(1))/universe.deltas()[1];
		
		x = (x-xt) / wt;
		y = (y-yt) / ht;
		
		if ((x>=0) && (x<1.0) && (y>=0) && (y<1.0)) {
			if (mark)
				gr_im.fillRect(
						(int) (x*sizeX-1) - 1,
						sizeY-1 - (int)(y*sizeY-1) - 1,
						3,
						3
					);
			else
				gr_im.drawRect(
						(int) (x*sizeX-1),
						sizeY-1 - (int)(y*sizeY-1),
						0,
						0);
			if (mark || (rCount++)>100*(delay+1)) {
				gr_frame.drawImage(image, 0, 0, f);
				rCount=0;
			}
		}			
	}

	/**
	 * Draws the given rectangle in the given color.
	 * 
	 * @param r the rectangle to draw 
	 * @param color the color to use
	 */
	protected void draw(DoublePointRectangle r, Color color) {
		gr_im.setColor(color);
		Point left = r.getCorner(false);
		double deltas[] = r.deltas();
		
		double x = (left.getValue(0)-universe.getCorner(false).getValue(0))/universe.deltas()[0];
		double y = (left.getValue(1)-universe.getCorner(false).getValue(1))/universe.deltas()[1];
		double w = deltas[0]/universe.deltas()[0];
		double h = deltas[1]/universe.deltas()[1];
		
		x = (x-xt) / wt;
		y = (y-yt) / ht;
		w = w/wt;
		h = h/ht;
		
		if ((x>=0) && (x<1.0) && (y>=0) && (y<1.0)) {
			gr_im.drawRect(
					(int) (x*sizeX),
					sizeY-1 - (int)(y*sizeY),
					(int) (w*sizeX),
					(int) (h*sizeY)
				);
			if ((rCount++)>100*(delay+1)) {
				gr_frame.drawImage(image,0,0,f);
				rCount=0;
			}
		}					
	}
	
	
	/* (non-Javadoc)
	 * @see xxl.core.cursors.SecureDecoratorCursor#close()
	 */
	@Override
	public void close() {
		super.close();
		flush();
	}
	
	/**
	 * Flushes the image.
	 */
	public void flush() {
		paint(f.getGraphics());
	}
	
	/** 
	 * Paints the image to the frame.
	 * 
	 * @param gr a Graphics object for painting to the frame 
	 */
	public void paint(Graphics gr) {
		gr.drawImage(image, 0, 0, f);
	}

	/**
	 * Return the frame.
	 * 
	 * @return {@link #f}
	 */
	public Frame getFrame() {
		return f;
	}
}
