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

package xxl.core.relational.cursors;

import java.sql.ResultSet;

import xxl.core.cursors.MetaDataCursor;
import xxl.core.cursors.sources.ContinuousRandomNumber;
import xxl.core.functions.Function;
import xxl.core.relational.metaData.ResultSetMetaDatas;
import xxl.core.relational.tuples.Tuple;
import xxl.core.util.metaData.CompositeMetaData;

/**
 * The sampler is based on {@link xxl.core.cursors.filters.Sampler}. A sampler
 * is a {@link xxl.core.cursors.filters.Filter filter} that generates a sample
 * of the elements contained in a given input iterator. To generate the sample,
 * a pseudo random number generator and some parameters for the generation of
 * random numbers can be passed to a constructor call. For a detailed
 * description see {@link xxl.core.cursors.filters.Sampler}.
 * 
 * <p>The example in the main method wraps an enumeration (integers 0 to 99) to
 * a metadata cursor using
 * {@link xxl.core.cursors.Cursors#wrapToMetaDataCursor(java.util.Iterator, Object)}. 
 * Then, a sample of approximately 10% of size is generated. The interesting
 * call is: 
 * <code><pre>
 *   cursor = new Sampler(cursor, 0.1);
 * </pre></code></p>
 */
public class Sampler extends xxl.core.cursors.filters.Sampler<Tuple> implements MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> {

	/**
	 * The metadata provided by the sampler.
	 */
	protected CompositeMetaData<Object, Object> globalMetaData;

	/**
	 * Creates a new sampler based on a {@link ContinuousRandomNumber} cursor
	 * using {@link java.util.Random} as PRNG and a given Bernoulli
	 * probability.
	 *
	 * @param cursor the metadata cursor containing the elements the sample is
	 *        to be created from.
	 * @param p the Bernoulli probability, i.e. the probability the elements
	 *        will be contained in the delivered sample. <code>p</code> must be
	 *        in range (0,1].
	 * @param seed the seed to the pseudo random number generator.
	 * @throws IllegalArgumentException if <code>p</code> is not in range
	 *         (0, 1].
	 */
	public Sampler(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, double p, long seed) {
		super(cursor, p, seed);
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}
	
	/**
	 * Creates a new sampler based on a {@link ContinuousRandomNumber} cursor
	 * and a given Bernoulli probability.
	 *
	 * @param cursor the metadata cursor containing the elements the sample is
	 *        to be created from.
	 * @param crn a ContinuousRandomNumber cursor wrapping a PRNG.
	 * @param p the Bernoulli probability, i.e. the probability the elements
	 *        will be contained in the delivered sample. <code>p</code> must be
	 *        in range (0,1].
	 * @throws IllegalArgumentException if <code>p</code> is not in range
	 *         (0, 1].
	 */
	public Sampler(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, ContinuousRandomNumber crn, double p) {
		super(cursor, crn, p);
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}
	
	/**
	 * Creates a new sampler based on a {@link ContinuousRandomNumber} cursor
	 * using {@link java.util.Random} as PRNG and a given Bernoulli
	 * probability.
	 *
	 * @param cursor the metadata cursor containing the elements the sample is
	 *        to be created from.
	 * @param p the Bernoulli probability, i.e. the probability the elements
	 *        will be contained in the delivered sample. <code>p</code> must be
	 *        in range (0,1].
	 * @throws IllegalArgumentException if <code>p</code> is not in range
	 *         (0, 1].
	 */
	public Sampler(MetaDataCursor<Tuple, CompositeMetaData<Object, Object>> cursor, double p) {
		super(cursor, p);
		globalMetaData = new CompositeMetaData<Object, Object>();
		globalMetaData.add(ResultSetMetaDatas.RESULTSET_METADATA_TYPE, ResultSetMetaDatas.getResultSetMetaData(cursor));
	}	
	
	/**
	 * Creates a new sampler based on a {@link ContinuousRandomNumber} cursor
	 * using {@link java.util.Random} as PRNG and a given Bernoulli
	 * probability.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createInputTuple a function that maps a (row of the) result set
	 *        to a tuple. The function gets a result set and maps the current
	 *        row to a tuple. If <code>null</code> is passed, the factory
	 *        method of {@link xxl.core.relational.tuples.ArrayTuple} is used. It is
	 *        forbidden to call the <code>next</code>, <code>update</code> and
	 *        similar methods of the result set from inside the function!
	 * @param p the Bernoulli probability, i.e. the probability the elements
	 *        will be contained in the delivered sample. <code>p</code> must be
	 *        in range (0,1].
	 * @param seed the seed to the pseudo random number generator.
	 * @throws IllegalArgumentException if <code>p</code> is not in range
	 *         (0, 1].
	 */
	public Sampler(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createInputTuple , double p, long seed) {
		this(new ResultSetMetaDataCursor(resultSet, createInputTuple), p, seed);
	}

	/**
	 * Creates a new sampler based on a {@link ContinuousRandomNumber} cursor
	 * and a given Bernoulli probability.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createInputTuple a function that maps a (row of the) result set
	 *        to a tuple. The function gets a result set and maps the current
	 *        row to a tuple. If <code>null</code> is passed, the factory
	 *        method of {@link xxl.core.relational.tuples.ArrayTuple} is used. It is
	 *        forbidden to call the <code>next</code>, <code>update</code> and
	 *        similar methods of the result set from inside the function!
	 * @param crn a ContinuousRandomNumber cursor wrapping a PRNG.
	 * @param p the Bernoulli probability, i.e. the probability the elements
	 *        will be contained in the delivered sample. <code>p</code> must be
	 *        in range (0,1].
	 * @throws IllegalArgumentException if <code>p</code> is not in range
	 *         (0, 1].
	 */
	public Sampler(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createInputTuple , ContinuousRandomNumber crn, double p) {
		this(new ResultSetMetaDataCursor(resultSet, createInputTuple), crn, p);
	}
	
	/**
	 * Creates a new sampler based on a {@link ContinuousRandomNumber} cursor
	 * using {@link java.util.Random} as PRNG and a given Bernoulli
	 * probability.
	 *
	 * @param resultSet the input result set delivering the elements. The
	 *        result set is wrapped internally to a metadata cursor using
	 *        {@link ResultSetMetaDataCursor}.
	 * @param createInputTuple a function that maps a (row of the) result set
	 *        to a tuple. The function gets a result set and maps the current
	 *        row to a tuple. If <code>null</code> is passed, the factory
	 *        method of {@link xxl.core.relational.tuples.ArrayTuple} is used. It is
	 *        forbidden to call the <code>next</code>, <code>update</code> and
	 *        similar methods of the result set from inside the function!
	 * @param p the Bernoulli probability, i.e. the probability the elements
	 *        will be contained in the delivered sample. <code>p</code> must be
	 *        in range (0,1].
	 * @throws IllegalArgumentException if <code>p</code> is not in range
	 *         (0, 1].
	 */
	public Sampler(ResultSet resultSet, Function<? super ResultSet, ? extends Tuple> createInputTuple , double p) {
		this(new ResultSetMetaDataCursor(resultSet, createInputTuple), p);
	}

	/**
	 * Returns the metadata information for this metadata-cursor as a composite
	 * metadata ({@link CompositeMetaData}).
	 *
	 * @return the metadata information for this metadata-cursor as a composite
	 *         metadata ({@link CompositeMetaData}).
	 */
	public CompositeMetaData<Object, Object> getMetaData() {
		return globalMetaData;
	}
}
