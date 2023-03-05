package com.gigasynapse.arraydisk;

import java.io.RandomAccessFile;

public class NodeDisk<E> {
	final public int key;
	final public E value;
	
	public NodeDisk(int key, E value) {
		this.key = key;
		this.value = value;
	}
	
	public String toString() {
		return "Key: " + key + ", Value: " + value;
	}
}
