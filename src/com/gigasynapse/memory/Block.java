package com.gigasynapse.memory;

import java.lang.reflect.Array;

public class Block<E> {
	public E[] data;
	
	public Block(Class<E> clazz, int size) {
		data = (E[]) Array.newInstance(clazz, size);
		for(int i = 0; i < size; i++) {
			data[i] = null;
		}
	}
}
