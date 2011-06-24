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

package xxl.core.cursors.visual;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/**
 * This class provides a visual component that can be used for the remote control
 * of an arbitrary object. The class depends on a
 * {@link xxl.core.cursors.visual.Controllable controllable object} and provides
 * a visual controller {@link javax.swing.JPanel panel} for remotely controlling
 * it.
 *
 * @see xxl.core.cursors.visual.IteratorControllable
 * @see javax.swing.JPanel
 */
public class ControllerJPanel extends JPanel {

	/**
	 * The controllable object that should be remotely controlled by this visual
	 * controller panel.
	 */
	protected Controllable controllable;

	/**
	 * A visual button calling the <tt>go</tt> method of the controllable object.
	 */
	protected JButton go;

	/**
	 * A visual button calling the <tt>pause</tt> method of the controllable
	 * object.
	 */
	protected JButton pause;

	/**
	 * A visual button calling the <tt>go(int)</tt> method of the controllable
	 * object.
	 */
	protected JButton next;

	/**
	 * A visual button calling the <tt>reset</tt> method of the controllable
	 * object.
	 */
	protected JButton reset;
	
	/**
	 * A visual button calling the <tt>close</tt> method of the controllable
	 * object.
	 */
	protected JButton close;

	/**
	 * The number of steps the controllable object will be allowed to resume its
	 * life cycle, i.e., the parameter passed to the controllable object's
	 * <tt>go(int)</tt> method.
	 */
	protected int steps = 10; // anzahl schritte in next( int)

	/**
	 * The minimum number of steps the controllable object will be allowed to
	 * resume its life cycle, i.e., the minimum parameter passed to the
	 * controllable object's <tt>go(int)</tt> method.
	 */
	protected int stepsMin;

	/**
	 * The maximum number of steps the controllable object will be allowed to
	 * resume its life cycle, i.e., the maximum parameter passed to the
	 * controllable object's <tt>go(int)</tt> method.
	 */
	protected int stepsMax;
	
	/**
	 * A visual label showing the actual number of steps the controllable object
	 * will be allowed to resume its life cycle, i.e., the parameter passed to
	 * the controllable object's <tt>go(int)</tt> method.
	 */
	protected JLabel stepInfoLabel;
	
	/**
	 * A visual label showing a title for the slider that is used to adjust the
	 * number of steps the controllable object will be allowed to resume its life
	 * cycle, i.e., the parameter passed to the controllable object's
	 * <tt>go(int)</tt> method.
	 */
	protected JLabel stepsLabel;
	
	/**
	 * The visual slider that is used to adjust the number of steps the
	 * controllable object will be allowed to resume its life cycle, i.e., the
	 * parameter passed to the controllable object's <tt>go(int)</tt> method.
	 */
	protected JSlider stepsSlider;

	/**
	 * Creates a new visual controller panel controlling the specified
	 * controllable object using the given layout manager. The minimum and
	 * maximum number of steps the controllable object will be allowed to resume
	 * its life cycle, i.e., the minimum and maximum parameter passed to the
	 * controllable object's <tt>go(int)</tt> method must also be specified.
	 * 
	 * @param controllable the controllable object to be controlled by the visual
	 *        controller panel.
	 * @param layout the layout manager used to manage the layout of this panel.
	 * @param stepsMin the minimum number of steps the controllable object will
	 *        be allowed to resume its life cycle, i.e., the minimum parameter
	 *        passed to the controllable object's <tt>go(int)</tt> method.
	 * @param stepsMax the maximum number of steps the controllable object will
	 *        be allowed to resume its life cycle, i.e., the maximum parameter
	 *        passed to the controllable object's <tt>go(int)</tt> method.
	 */
	public ControllerJPanel(Controllable controllable, LayoutManager layout, int stepsMin, int stepsMax) {
		this.controllable = controllable;
		controllable.init();
		setLayout(layout);
		this.stepsMin = stepsMin;
		this.stepsMax = stepsMax;
		init();
	}

	/**
	 * Creates a new visual controller panel controlling the specified
	 * controllable object using the given layout manager. The minimum and
	 * maximum number of steps the controllable object will be allowed to resume
	 * its life cycle, i.e., the minimum and maximum parameter passed to the
	 * controllable object's <tt>go(int)</tt> method, is set to <tt>1</tt> resp.
	 * <tt>100</tt>.
	 * 
	 * @param controllable the controllable object to be controlled by the visual
	 *        controller panel.
	 */
	public ControllerJPanel(Controllable controllable) {
		this(controllable, new BorderLayout(), 1, 100);
	}

	/**
	 * Initializes the visual controller panel, i.e, the buttons and the slider
	 * contained by it.
	 */
	protected void init() {
		setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
		setLayout(new GridBagLayout());

			GridBagConstraints c = new GridBagConstraints();
			c.fill = GridBagConstraints.BOTH;
			c.weightx = 1.0;
			c.weighty = 3.0;
			c.gridwidth = GridBagConstraints.REMAINDER;
			c.gridheight = GridBagConstraints.RELATIVE;

			JPanel buttons = new JPanel();
			buttons.setLayout(new GridLayout(3, 2, 10, 5));

				// --- button: go
				go = new JButton();
				go.setActionCommand("go");
				go.setText("go");
				go.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						go_pressed();
					} // end of actionPerformed
				});

			buttons.add(go);

				// --- button: next
				next = new JButton();
				next.setActionCommand("next");
				next.setText("go (n)");
				next.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						next_pressed(steps);
					} // end of actionPerformed
				});

			buttons.add(next);

				// --- button: pause
				pause = new JButton();
		        	pause.setActionCommand("pause");
				pause.setText("pause");
				pause.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						pause_pressed();
					} // end of actionPerformed
				});

			buttons.add(pause);

				// --- button: close
				close = new JButton();
				close.setEnabled(true);
				close.setActionCommand("close");
				close.setText("close");
				close.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						close_pressed();
					} // end of actionPerformed
				});

			buttons.add(close);

				// --- empty placeholder

			buttons.add(new JPanel());

				// --- button: reset
				reset = new JButton();
				reset.setActionCommand("reset");
				reset.setText("reset");
				reset.setEnabled(false);
				reset.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						reset_pressed();
					} // end of actionPerformed
				});

			buttons.add(reset);

		add(buttons, c);

		// ---

			c.weighty = 2.0;
			c.gridheight = GridBagConstraints.REMAINDER;
			JPanel slider = new JPanel();
			slider.setLayout(new BorderLayout());

				JPanel labels = new JPanel();
				labels.setLayout(new GridLayout(2, 1));

					stepInfoLabel = new JLabel("steps = " + Integer.toString(steps));

				labels.add(stepInfoLabel);

					stepsLabel = new JLabel();

				labels.add(stepsLabel);

			slider.add(labels, BorderLayout.NORTH);

				stepsSlider = new JSlider();
				stepsSlider.setMajorTickSpacing (1);
				stepsSlider.setMaximum(stepsMax);
				stepsSlider.setMinimum(stepsMin);
				stepsSlider.addChangeListener(new ChangeListener() {
					public void stateChanged(ChangeEvent e) {
						JSlider source = (JSlider)e.getSource();
						if (!source.getValueIsAdjusting()) {
							steps = source.getValue();
							stepInfoLabel.setText("go steps = " + Integer.toString(steps));
						}
						else {
							int t = source.getValue();
							stepInfoLabel.setText("go steps = " + Integer.toString(t));
						}
						stepInfoLabel.invalidate();
					}
				});
				stepsSlider.setValue(steps);

			slider.add(stepsSlider, BorderLayout.CENTER);

		add(slider, c);
		resetButtons();
	}

	/**
	 * Resets the buttons contained by the visual controller panel to their
	 * initial state, which are as follows:
	 * <dl>
	 *     <dt>
	 *         <tt>go</tt>
	 *     </dt>
	 *     <dd>
	 *         enabled
	 *     </dd>
	 *     <dt>
	 *         <tt>pause</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>next</tt>
	 *     </dt>
	 *     <dd>
	 *         enabled if the <tt>supportsGoSteps</tt> returns <tt>true</tt>,
	 *         otherwise disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>close</tt>
	 *     </dt>
	 *     <dd>
	 *         enabled if the <tt>supportsClose</tt> returns <tt>true</tt>,
	 *         otherwise disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>reset</tt>
	 *     </dt>
	 *     <dd>
	 *         enabled if the <tt>supportsReset</tt> returns <tt>true</tt>,
	 *         otherwise disabled
	 *     </dd>
	 *  </dl>
	 */
	protected void resetButtons() {
		// init
		go.setEnabled(true);
		pause.setEnabled(false);
		next.setEnabled(controllable.supportsGoSteps());
		close.setEnabled(controllable.supportsClose());
		reset.setEnabled(controllable.supportsReset());
	}

	/**
	 * Sets the buttons contained by the visual controller panel after the button
	 * <tt>go</tt> has been pressed and calls the concerning operation of the
	 * controllable object. The buttons' state is as follows:
	 * <dl>
	 *     <dt>
	 *         <tt>go</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>pause</tt>
	 *     </dt>
	 *     <dd>
	 *         enabled
	 *     </dd>
	 *     <dt>
	 *         <tt>next</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>close</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>reset</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *  </dl>
	 */
	protected void go_pressed() {
		//System.out.println("go pressed!");
		go.setEnabled(false);
		pause.setEnabled(true);
		next.setEnabled(false);
		close.setEnabled(false);
		reset.setEnabled(false);
		controllable.go();
	}

	/**
	 * Sets the buttons contained by the visual controller panel after the button
	 * <tt>pause</tt> has been pressed and calls the concerning operation of the
	 * controllable object. The buttons' state is set by a call to
	 * <tt>resetButtons</tt>.
	 */
	protected void pause_pressed() {
		//System.out.println("pause pressed!");
		controllable.pause();
		resetButtons();
	}

	/**
	 * Sets the buttons contained by the visual controller panel after the button
	 * <tt>next</tt> has been pressed and calls the concerning operation of the
	 * controllable object. The buttons' state is set by a call to
	 * <tt>resetButtons</tt>.
	 * 
	 * @param n the number of steps the underlying controllable object will be
	 *        allowed to resume its life cycle.
	 */
	protected void next_pressed(int n) {
		//System.out.println("next pressed n= " + n + " !");
		controllable.go(n);
		resetButtons();
	}

	/**
	 * Sets the buttons contained by the visual controller panel after the button
	 * <tt>reset</tt> has been pressed and calls the concerning operation of the
	 * controllable object. The buttons' state is set by a call to
	 * <tt>resetButtons</tt>.
	 */
	protected void reset_pressed() {
		//System.out.println("reset pressed!");
		controllable.reset();
		resetButtons();
	}

	/**
	 * Sets the buttons contained by the visual controller panel after the button
	 * <tt>close</tt> has been pressed and calls the concerning operation of the
	 * controllable object. The buttons' state is as follows:
	 * <dl>
	 *     <dt>
	 *         <tt>go</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>pause</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>next</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>close</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *     <dt>
	 *         <tt>reset</tt>
	 *     </dt>
	 *     <dd>
	 *         disabled
	 *     </dd>
	 *  </dl>
	 */
	protected void close_pressed() {
		//System.out.println("close pressed!");
		controllable.pause();
		go.setEnabled(false);
		pause.setEnabled(false);
		next.setEnabled(false);
		close.setEnabled(false);
		reset.setEnabled(false);
		controllable.close();
	}
}
