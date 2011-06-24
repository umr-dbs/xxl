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

package xxl.core.math.queries;

import java.util.ArrayList;
import java.util.List;

/** 
 * This class provides a skeleton implementation for selectivity
 * estimators using query feedback as an improvement. The general idea
 * relies on [CR93]: Chen, Roussopolous, Adaptive Selectivity Estimation using
 * Query Feedback, 1993.
 */

public abstract class QueryFeedbackSelectivityEstimator {

	/** 
	 * Stores the number of estimates already done. 
	 */
	protected int numberOfEstimates;

	/** Stores the already processed queries with the corresponding true selectivity.
	 * The information about these queries is necessary to do a proper adjustment 
	 * of the estimator. 
	 */
	protected List<Object[]> alreadyProcessedQueries;

	/** Constructs a new Object of this type.
	 */
	public QueryFeedbackSelectivityEstimator() {
		alreadyProcessedQueries = new ArrayList<Object[]>();
		numberOfEstimates = 0;
	}

	/**
	 * Returns an estimation of the selectivity of a given query. Before the estimation
	 * is returned, the query and the true selectivity are stored in the list of already
	 * processed queries.
	 * 
	 * @param query query to estimate
	 * @param trueSelectivity true selectivity of the given query
	 * @return an estimation of the selectivity of the given query
	 */
	public double estimate(Object query, double trueSelectivity) {
		// the ordering of first estimate and second store the query is important
		// because otherwise the known selectivity of the given query will be
		// used for adjusting the estimator *before* the given query will be estimated.
		double r = getSelectivity(query);
		// first, estimate the selectivity of the given query
		addQuery(query, trueSelectivity);
		// second, store the last estimated query with its true selectivity
		numberOfEstimates++; // 
		return r; // return the estimated selectivity
	}

	/** Stores an already processed query and its true selectivity 
	 * to do an adjustment in order to improve following estimations.
	 * 
	 * @param query query to store
	 * @param sel true selectivity to store
	 */
	protected void addQuery(Object query, double sel) {
		alreadyProcessedQueries.add(new Object[] { query, new Double(sel)});
	}

	/** Computes an estimation of the selectivity of the given query.
	 * This method must be implemented by classes inherited from this class.
	 * 
	 * @param query query to process
	 * @return an estimation of the selectivity of the given query
	 */
	protected abstract double getSelectivity(Object query);
}
