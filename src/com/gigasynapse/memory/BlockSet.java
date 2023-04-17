package com.gigasynapse.memory;

public class BlockSet<E> {
	public E value;
	public BlockSet[] data;
	Class<E> clazz;
	
	public BlockSet(Class<E> clazz, int size) {
		this.clazz = clazz;
	}
}
