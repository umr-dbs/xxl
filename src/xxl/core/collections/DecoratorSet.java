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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import xxl.core.util.Decorator;

public class DecoratorSet<T> implements Set<T>, Decorator<Set<T>> {

	protected Set<T> set;
	
	public DecoratorSet(Set<T> set) {
		this.set = set;
	}
	
	public boolean add(T o) {
		return set.add(o);
	}

	public boolean addAll(Collection<? extends T> c) {
		return set.addAll(c);
	}

	public void clear() {
		set.clear();
	}

	public boolean contains(Object o) {
		return set.contains(o);
	}

	public boolean containsAll(Collection<?> c) {
		return set.containsAll(c);
	}

	public boolean isEmpty() {
		return set.isEmpty();
	}

	public Iterator<T> iterator() {
		return set.iterator();
	}

	public boolean remove(Object o) {
		return set.remove(o);
	}

	public boolean removeAll(Collection<?> c) {
		return set.removeAll(c);
	}

	public boolean retainAll(Collection<?> c) {
		return set.retainAll(c);
	}

	public int size() {
		return set.size();
	}

	public Object[] toArray() {
		return set.toArray();
	}

	public <T> T[] toArray(T[] a) {
		return set.toArray(a);
	}
	
	@Override
	public Set<T> getDecoree() {
		return set;
	}

}
