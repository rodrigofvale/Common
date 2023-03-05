package com.gigasynapse.arraydisk;

import java.io.RandomAccessFile;

public class NodeDiskSet<E> {
	final public int key;
	final public E value;
	final public long blockAddress;
	final public int index;
	final public int blockCounter;
	final RandomAccessFile rafBin;
	
	public NodeDiskSet(RandomAccessFile rafBin, int key, E value, 
			long blockAddress, int blockCounter, int index) {
		this.rafBin = rafBin;
		this.key = key;
		this.value = value;
		this.blockCounter = blockCounter;
		this.blockAddress = blockAddress; 
		this.index = index;
	}
	
	public String toString() {
		return "Key: " + key + ", Value: " + value + ", blockAddress: " 
					+ blockAddress + ", blockCounter: " + blockCounter;
	}
}
