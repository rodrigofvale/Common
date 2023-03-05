package com.gigasynapse.arraydisk;

import java.io.IOException;
import java.util.Iterator;
	
public class DiskSetIterator<E> implements Iterator<E> {

	private DiskSet<E> diskSet;
	NodeDisk<E> current;

	// initialize pointer to head of the list for iteration
	public DiskSetIterator(DiskSet<E> diskSet) {
		this.diskSet = diskSet;
		try {			
			current = diskSet.getFirst();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// returns false if next element does not exist
	public boolean hasNext() {
		return current != null;
	}

	// return current data and update pointer
	public E next()	{
		E value = current.value;
		try {
			current = diskSet.getNext(current);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return value;
	}

	// implement if needed
	public void remove() {
		throw new UnsupportedOperationException();
	}	
}    


