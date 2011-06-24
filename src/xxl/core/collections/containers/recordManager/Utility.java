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

package xxl.core.collections.containers.recordManager;

import xxl.core.io.converters.FixedSizeConverter;

/**
 * Some static methods which make usage of the RecordManager easier.
 */
public class Utility {
	/**
	 * No instances allowed of this class.
	 */
	private Utility() {
	}

	/** 
	 * Returns a strategy for the RecordManager:
	 * 0: OneRecordPerPage, 1: FirstFit, 2: NextFit, 3: AppendOnly, 4: AppendOnly(n), 5: BestFit(bestFitPercentage), 6: BestFitOnNEmptiestPages(n), 7: NextFitWithH(n), 8: NextFitWithHW(n), 9: HybridAONF(n,goalPercentage), 10: HybridBFOENFHStrategy(n), 11: HybridBFOENFHWStrategy(n), 12: LastToFirstFitStrategy, 13: LRUStrategy(n), 14: HybridLRULFStrategy(n)
	 * @param recordManagerStrategy Number of the strategy.
	 * @param n Parameter n for some strategies
	 * @param bestFitPercentage Parameter for best fit.
	 * @param goalPercentage Percentage of the memory utilization
	 * 	which is wanted (for hybrid strategy).
	 * @return The strategy.
	 */
	public static Strategy getStrategy(int recordManagerStrategy, int n, double bestFitPercentage, double goalPercentage) {
		switch (recordManagerStrategy) {
		case 0: return new OneRecordPerPageStrategy();
		case 1: return new FirstFitStrategy();
		case 2: return new NextFitStrategy();
		case 3: return new AppendOnlyStrategy();
		case 4: return new AppendOnlyNStrategy(n);
		case 5: return new BestFitStrategy(bestFitPercentage);
		case 6: return new BestFitOnNEmptiestPagesStrategy(n);
		case 7: return new NextFitWithHStrategy(n);
		case 8: return new NextFitWithHWStrategy(n);
		case 9: return new HybridAONFStrategy(n, goalPercentage);
		case 10: return new HybridBFOEStrategy(n, new NextFitWithHStrategy(n));
		case 11: return new HybridBFOEStrategy(n, new NextFitWithHWStrategy(n));
		case 12: return new LastToFirstFitStrategy();
		case 13: return new LRUStrategy(n);
		case 14: return new HybridStrategy(new LRUStrategy(n), new LastToFirstFitStrategy());
		default: return null;
		}
	}

	/** 
	 * Returns a strategy for the RecordManager:
	 * 0: OneRecordPerPage, 1: FirstFit, 2: NextFit, 3: AppendOnly, 4: AppendOnly(10), 5: BestFit(0.05), 6: BestFitOnNEmptiestPages(10), 7: NextFitWithH(10), 8: NextFitWithHW(10), 9: HybridAONF(10,0.85), 10: HybridBFOENFHStrategy(10), 11: HybridBFOENFHWStrategy(10), 12: LastToFirstFitStrategy, 13: LRUStrategy(10), 14: HybridLRULFStrategy(10)
	 * @param recordManagerStrategy Number of the strategy.
	 * @return The strategy.
	 */
	public static Strategy getStrategy(int recordManagerStrategy) {
		return getStrategy(recordManagerStrategy, 10, 0.05, 0.85);
	}

	/**
	 * Returns a new TIdManager with the given number and converter.
	 * @param tidManagerNumber A number identifying the TId manager (0: identity TId, 1: map TId).
	 * @param converter Converter for the identifyer.
	 * @return	The TIdManager.
	 */
	public static TIdManager getTIdManager(int tidManagerNumber, FixedSizeConverter converter) {
		switch (tidManagerNumber) {
		case 0: return new IdentityTIdManager(converter);
		case 1: return new MapTIdManager(converter);
		default: return null;
		}
	}
}
