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

package xxl.core.collections;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import xxl.core.comparators.ComparableComparator;

/**
 * This class contains various methods for manipulating lists.
 * 
 * <p>The documentation of the searching methods contained in this class
 * includes a brief description of the implementation. Such descriptions
 * should be regarded as implementation notes, rather than parts of the
 * specification. Implementors should feel free to substitute other
 * algorithms, as long as the specification itself is adhered to.</p>
 *
 * @see ComparableComparator
 * @see Comparator
 * @see List
 */
public class Lists {

	/**
	 * The default constructor has private access in order to ensure
	 * non-instantiability.
	 */
	private Lists() {
		// private constructor avoids instantiation
	}

	/**
	 * Searches the specified list for the first or last appearance of the
	 * specified object using the binary search algorithm. The list
	 * <b>must</b> be sorted into ascending order according to the
	 * specified comparator before calling this method. If it is not
	 * sorted, the results are undefined. If the list contains multiple
	 * elements equal to the specified object, the boolean value
	 * <code>last</code> decides whether the first or last appearance will be
	 * found.
	 *
	 * @param <S> the type of the elements the given lsit is able to store.
	 * @param <T> the type of the object to be searched for (must be a subtype
	 *        of S).
	 * @param list the list to be searched.
	 * @param object the object to be searched for.
	 * @param comparator the comparator by which the list is ordered.
	 * @param last if false, the first appearance will be found; if true,
	 *        the last appearance will be found.
	 * @return index of the search object, if it is contained in the list;
	 *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>.<br>
	 *         The <i>insertion point</i> is defined as the point at which
	 *         the object would be inserted into the list: the index of
	 *         the first element greater than the object, or
	 *         <code>list.size()</code>, if all elements in the list are less
	 *         than the specified object. Note that this guarantees that
	 *         the return value will be &ge; 0 only if the key is
	 *         found.
	 * @throws ClassCastException if the list contains
	 *         elements that are not <i>mutually comparable</i> using the
	 *         specified comparator, or the search object is not
	 *         <i>mutually comparable</i> with the elements of the list
	 *         using this comparator.
	 */
	public static <S, T extends S> int indexOf(List<S> list, T object, Comparator<? super S> comparator, boolean last) {
		return last ? lastIndexOf(list, object, comparator) : firstIndexOf(list, object, comparator);
	}

	/**
	 * Searches the specified list for the first or last appearance of the
	 * specified object using the binary search algorithm. The list
	 * <b>must</b> be sorted into ascending order according to the
	 * <i>natural ordering</i> of its elements before calling this method.
	 * If it is not sorted, the results are undefined. If the list
	 * contains multiple elements equal to the specified object, the
	 * boolean value <code>last</code> decides whether the first or last
	 * appearance will be found.
	 *
	 * @param <S> the type of the elements the given list is able to store.
	 * @param <T> the type of the object to be searched for (must be a subtype
	 *        of S).
	 * @param list the list to be searched.
	 * @param object the object to be searched for.
	 * @param last if false, the first appearance will be found; if true,
	 *        the last appearance will be found.
	 * @return index of the search object, if it is contained in the list;
	 *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>.<br>
	 *         The <i>insertion point</i> is defined as the point at which
	 *         the object would be inserted into the list: the index of
	 *         the first element greater than the object, or
	 *         <code>list.size()</code>, if all elements in the list are less
	 *         than the specified object. Note that this guarantees that
	 *         the return value will be &ge; 0 only if the key is
	 *         found.
	 * @throws ClassCastException if the list contains
	 *         elements that are not <i>mutually comparable</i>, or the
	 *         search object is not <i>mutually comparable</i> with the
	 *         elements of the list.
	 */
	public static <S extends Comparable<S>, T extends S> int indexOf(List<S> list, T object, boolean last) {
		return indexOf(list, object, new ComparableComparator<S>(), last);
	}

	/**
	 * Searches the specified list for the first appearance of the
	 * specified object using the binary search algorithm. The list
	 * <b>must</b> be sorted into ascending order according to the
	 * specified comparator before calling this method. If it is not
	 * sorted, the results are undefined. If the list contains multiple
	 * elements equal to the specified object, the first appearance will
	 * be found.
	 *
	 * @param <S> the type of the elements the given list is able to store.
	 * @param <T> the type of the object to be searched for (must be a subtype
	 *        of S).
	 * @param list the list to be searched.
	 * @param object the object to be searched for.
	 * @param comparator the comparator by which the list is ordered.
	 * @return index of the search object, if it is contained in the list;
	 *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>.<br>
	 *         The <i>insertion point</i> is defined as the point at which
	 *         the object would be inserted into the list: the index of
	 *         the first element greater than the object, or
	 *         <code>list.size()</code>, if all elements in the list are less
	 *         than the specified object. Note that this guarantees that
	 *         the return value will be &ge; 0 only if the key is
	 *         found.
	 * @throws ClassCastException if the list contains
	 *         elements that are not <i>mutually comparable</i> using the
	 *         specified comparator, or the search object is not
	 *         <i>mutually comparable</i> with the elements of the list
	 *         using this comparator.
	 */
	public static <S, T extends S> int firstIndexOf(List<S> list, T object, Comparator<? super S> comparator) {
		int left = 0, right = list.size()-1;

		while (left <= right) {
			int median = (left+right)/2, comparison = comparator.compare(list.get(median), object);

			if (comparison < 0)
				left = median+1;
			else
				if (comparison > 0 || left < median && comparator.compare(list.get(median-1), object) == 0)
					right = median-1;
				else
					return median;
		}
		return -left-1;
	}

	/**
	 * Searches the specified list for the first appearance of the
	 * specified object using the binary search algorithm. The list
	 * <b>must</b> be sorted into ascending order according to the
	 * <i>natural ordering</i> of its elements before calling this method.
	 * If it is not sorted, the results are undefined. If the list
	 * contains multiple elements equal to the specified object, the first
	 * appearance will be found.
	 *
	 * @param <S> the type of the elements the given list is able to store.
	 * @param <T> the type of the object to be searched for (must be a subtype
	 *        of S).
	 * @param list the list to be seached.
	 * @param object the object to be searched for.
	 * @return index of the search object, if it is contained in the list;
	 *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>.<br>
	 *         The <i>insertion point</i> is defined as the point at which
	 *         the object would be inserted into the list: the index of
	 *         the first element greater than the object, or
	 *         <code>list.size()</code>, if all elements in the list are less
	 *         than the specified object. Note that this guarantees that
	 *         the return value will be &ge; 0 only if the key is
	 *         found.
	 * @throws ClassCastException if the list contains
	 *         elements that are not <i>mutually comparable</i>, or the
	 *         search object is not <i>mutually comparable</i> with the
	 *         elements of the list.
	 */
	public static <S extends Comparable<S>, T extends S> int firstIndexOf(List<S> list, T object) {
		return firstIndexOf(list, object, new ComparableComparator<S>());
	}

	/**
	 * Searches the specified list for the last appearance of the
	 * specified object using the binary search algorithm. The list
	 * <b>must</b> be sorted into ascending order according to the
	 * specified comparator before calling this method. If it is not
	 * sorted, the results are undefined. If the list contains multiple
	 * elements equal to the specified object, the last appearance will be
	 * found.
	 *
	 * @param <S> the type of the elements the given lsit is able to store.
	 * @param <T> the type of the object to be searched for (must be a subtype
	 *        of S).
	 * @param list the list to be searched.
	 * @param object the object to be searched for.
	 * @param comparator the comparator by which the list is ordered.
	 * @return index of the search object, if it is contained in the list;
	 *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>.<br>
	 *         The <i>insertion point</i> is defined as the point at which
	 *         the object would be inserted into the list: the index of
	 *         the first element greater than the object, or
	 *         <code>list.size()</code>, if all elements in the list are less
	 *         than the specified object. Note that this guarantees that
	 *         the return value will be &ge; 0 only if the key is
	 *         found.
	 * @throws ClassCastException if the list contains
	 *         elements that are not <i>mutually comparable</i> using the
	 *         specified comparator, or the search object in not
	 *         <i>mutually comparable</i> with the elements of the list
	 *         using this comparator.
	 */
	public static <S, T extends S> int lastIndexOf(List<S> list, T object, Comparator<? super S> comparator) {
		int left = 0, right = list.size()-1;

		while (left <= right) {
			int median = (left+right)/2, comparison = comparator.compare(list.get(median), object);

			if (comparison > 0)
				right = median-1;
			else
				if (comparison < 0 || median < right && comparator.compare(list.get(median+1), object) == 0)
					left = median+1;
				else
					return median;
		}
		return -left-1;
	}

	/**
	 * Searches the specified list for the last appearance of the
	 * specified object using the binary search algorithm. The list
	 * <b>must</b> be sorted into ascending order according to the
	 * <i>natural ordering</i> of its elements before calling this method.
	 * If it is not sorted, the results are undefined. If the list
	 * contains multiple elements equal to the specified object, the last
	 * appearance will be found.
	 *
	 * @param <S> the type of the elements the given list is able to store.
	 * @param <T> the type of the object to be searched for (must be a subtype
	 *        of S).
	 * @param list the list to be searched.
	 * @param object the object to be searched for.
	 * @return index of the search object, if it is contained in the list;
	 *         otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>.<br>
	 *         The <i>insertion point</i> is defined as the point at which
	 *         the object would be inserted into the list: the index of
	 *         the first element greater than the object, or
	 *         <tt>list.size()</tt>, if all elements in the list are less
	 *         than the specified object. Note that this guarantees that
	 *         the return value will be &ge; 0 only if the key is
	 *         found.
	 * @throws ClassCastException if the list contains
	 *         elements that are not <i>mutually comparable</i>, or the
	 *         search object is not <i>mutually comparable</i> with the
	 *         elements of the list.
	 */
	public static <S extends Comparable<S>, T extends S> int lastIndexOf(List<S> list, T object) {
		return lastIndexOf(list, object, new ComparableComparator<S>());
	}
	
	/**
	 * Returns the index of the minimum stored in the list.
	 * 
	 * @param <E> the type of the elements stored in the list.
	 * @param list the list to be searched.
	 * @return the index of the minimum stored in the list or <tt>-1</tt> if
	 *         the list is empty.
	 */
	public static <E extends Comparable<E>> int indexOfMinimum(List<E> list) {
		if (list.size() > 0) {
			int index = 0;
			E minimum = list.get(index);
			for (int i = 1; i < list.size(); i++)
				if (list.get(i).compareTo(minimum) < 0)
					minimum = list.get(index = i);
			return index;
		}
		return -1;
	}
	
	/**
	 * Returns the index of the maximum stored in the list.
	 * 
	 * @param <E> the type of the elements stored in the list.
	 * @param list the list to be searched.
	 * @return the index of the maximum stored in the list or <tt>-1</tt> if
	 *         the list is empty.
	 */
	public static <E extends Comparable<E>> int indexOfMaximum(List<E> list) {
		if (list.size() > 0) {
			int index = 0;
			E maximum = list.get(index);
			for (int i = 1; i < list.size(); i++)
				if (list.get(i).compareTo(maximum) > 0)
					maximum = list.get(index = i);
			return index;
		}
		return -1;
	}
	
	/**
	 * Returns a new list containing the specified object <code>times</code>
	 * times.
	 * 
	 * @param <T> the type of the objects the list is able to store.
	 * @param init the object that should be inserted <code>times</times> times
	 *        for initializing the list.
	 * @param times the number of times the given object should be inserted.
	 * @return the initialized list containing the specified object
	 *         <code>times</code> times.
	 */
	public static <T> List<T> initializedList(T init, int times) {
		ArrayList<T> list = new ArrayList<T>(times);
		for (int i = 0; i < times; i++)
			list.add(init);
		return list;
	}

	/**
	 * Sort the given list by using the quick sort algorithm for lists. The
	 * specified comparator is used for comparing the elements of the list.
	 *
	 * @param <E> the type of the elements stored by the given list.
	 * @param data the list to be sorted.
	 * @param comparator the comparator that is used for comparing the elements
	 *        of the list.
	 */
	public static <E> void quickSort(List<E> data, Comparator<? super E> comparator) {
		quickSort(data, 0, data.size()-1, comparator);
	}

	/**
	 * This method performs the quick sort algorithm for lists. The given list
	 * is sorted using the specified comparator. The specified indices
	 * determine the range of the list that should be sorted by the algorithm.
	 * For (ranges of) lists with no more than ten elements the bubble sort
	 * algorithm is used, other (ranges of) lists are sorted by a quick sort
	 * algorithm using the median of three as pivot element.
	 *
	 * @param <E> the type of the elements stored by the given list.
	 * @param data the list to be sorted.
	 * @param l the index of the list where the range to be sorted starts.
	 * @param r the index of the list where the range to be sorted ends.
	 * @param comparator the comparator that is used for comparing the elements
	 *        of the list.
	 */
	public static <E> void quickSort(List<E> data, int l, int r, Comparator<? super E> comparator) {
		int re, li, mid;
		E tmp;
	
		//rekursiv, direktes Tauschen der Elemente
		if (r-l < 10)
			for (li = l+1; li <= r; li++) {
				tmp = data.get(li);
				for (re = li; re > l && comparator.compare(tmp, data.get(re-1)) < 0; re--)
					data.set(re, data.get(re-1));
				data.set(re, tmp);
			}
		else
			while (l < r) {
				li = l;
				re = r;
				mid = (li + re ) / 2; //>>1; // ???
				if (comparator.compare(data.get(li), data.get(mid)) > 0)
					swap(data, li, mid);
				if (comparator.compare(data.get(mid), data.get(re)) > 0)
					swap(data, mid, re);
				if (comparator.compare(data.get(li), data.get(mid)) > 0)
					swap(data, li, mid);
				tmp = data.get(mid);
				while (li <= re) {
					while (comparator.compare(data.get(li), tmp) < 0)
						li++;
					while (comparator.compare(tmp, data.get(re)) < 0)
						re--;
					if (li <= re) {
						swap(data, li, re);
						li++;
						re--;
					} // end of if
				} // end of while
				if (l < re)
					quickSort(data, l, re, comparator);
				l = li;
			} // end of while
	} //end of quickSort

	/**
	 * This metod swaps the elements of the given list at the specified
	 * indices.
	 *
	 * @param <E> the type of the elements stored by the given list.
	 * @param data the list containing the elements to be swapped.
	 * @param l the index of the first element to be swapped.
	 * @param r the index of the second element to be swapped.
	 */
	private static <E> void swap(List<E> data, int l, int r) {
		E tmp = data.get(l);
		data.set(l, data.get(r));
		data.set(r, tmp);
	}
}
