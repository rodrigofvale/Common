package com.gigasynapse.arraydisk;

import java.io.IOException;
import java.util.Iterator;

public class DiskArrayIterator<E> implements Iterator<E> {
	private DiskArray<E> diskArray;
	NodeDisk<E> current;

	// initialize pointer to head of the list for iteration
	public DiskArrayIterator(DiskArray<E> diskArray) {
		this.diskArray = diskArray;
		try {			
			current = diskArray.getFirst();
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
			current = diskArray.getNext(current);
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

