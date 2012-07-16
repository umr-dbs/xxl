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

package xxl.core.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import xxl.core.functions.Functional.UnaryFunction;

/**
 * This is a unit test for Pair, Triple and Quadruple, testing the basic
 * functionalities of this class.
 */
public class QuadrupleTest {

	@Test
	public void testValuesFirstWay() {
		Quadruple<Integer, Float, Double, Boolean> q = generateFirstWay();
		check(q.getFirst(), q.getSecond(), q.getThird(), q.getFourth());
	}

	@Test
	public void testValuesSecondWay() {
		Quadruple<Integer, Float, Double, Boolean> q = generateSecondWay();
		check(q.getElement1(), q.getElement2(), q.getElement3(), q.getElement4());
	}

	@Test
	public void testProjections() {
		Quadruple<Integer, Float, Double, Boolean> q = generateFirstWay();

		UnaryFunction f1 = Quadruple.getFirstElementProjection();
		UnaryFunction f2 = Quadruple.getSecondElementProjection();
		UnaryFunction f3 = Quadruple.getThirdElementProjection();
		UnaryFunction f4 = Quadruple.getFourthElementProjection();

		check((Integer) f1.invoke(q), (Float) f2.invoke(q), (Double) f3.invoke(q), (Boolean) f4.invoke(q));
	}

	@Test
	public void testToString() {
		Quadruple<Integer, Float, Double, Boolean> q = generateSecondWay();
		String s = q.toString();
		Assert.assertEquals(s, "<1, 2.0, 3.0, true>");
	}

	@Test
	public void testEquals() {
		Quadruple<Integer, Float, Double, Boolean> q1 = generateFirstWay();
		Quadruple<Integer, Float, Double, Boolean> q2 = generateSecondWay();
		Assert.assertEquals(q1, q2);
	}

	private Quadruple<Integer, Float, Double, Boolean> generateFirstWay() {
		Quadruple<Integer, Float, Double, Boolean> q = new Quadruple<Integer, Float, Double, Boolean>();
		q.setFirst(1);
		q.setSecond(2f);
		q.setThird(3.0);
		q.setFourth(true);
		return q;
	}

	private Quadruple<Integer, Float, Double, Boolean> generateSecondWay() {
		return new Quadruple<Integer, Float, Double, Boolean>(1, 2f, 3.0, true);
	}

	private void check(Integer e1, Float e2, Double e3, Boolean e4) {
		Assert.assertEquals(e1, (Integer) 1);
		Assert.assertEquals(e2, (Float) 2f);
		Assert.assertEquals(e3, (Double) 3.0);
		Assert.assertEquals(e4, (Boolean) true);
	}
}
