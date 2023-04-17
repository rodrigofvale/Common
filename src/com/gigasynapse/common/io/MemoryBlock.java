package com.gigasynapse.common.io;

public class MemoryBlock {
	public byte[] data;
	
	public MemoryBlock(int size) {
		data = new byte[size];
	}
}
