package com.gigasynapse.memory;

public class DynamicArray<E> {	
	int totalBlocks = 10240;
	int blockSize = 10240;
	int totalElements = 0;
	Block[] blockMap;
	Class<E> clazz;
	
	
	public DynamicArray(Class<E> clazz) {
		this.clazz = clazz;
		blockMap = new Block[totalBlocks];
		for(int i = 0; i < totalBlocks; i++) {
			blockMap[i] = null;
		}
	}
	
	public synchronized int add(E e) {
		return add(totalElements, e);
	}

	public synchronized int add(int i, E e) {
		int blockId = i / blockSize;
		int index = i % blockSize;
		
		if (blockMap[blockId] == null) {
			blockMap[blockId] = new Block<E>(clazz, blockSize);
		}
		blockMap[blockId].data[index] = e;
		
		if (i > totalElements) {
			totalElements = i;
		} else if (i == totalElements) {
			totalElements++;
		}
		
		return i;
	}
	
	public synchronized void del(int i) {
		int blockId = i / blockSize;
		int index = i % blockSize;
		
		if (blockMap[blockId] == null) {
			return;
		}
		blockMap[blockId].data[index] = null;
	}

	public synchronized E get(int i) {
		int blockId = i / blockSize;
		int index = i % blockSize;
		
		if (blockMap[blockId] == null) {
			return null;
		}
		return (E) blockMap[blockId].data[index];
	}
	
	public static void main(String[] args) throws Exception {
		DynamicArray<String> a = new DynamicArray<String>(String.class);
		a.add("rodrigo");
		a.add("vania");
		a.add(10000, "rafael");
		
		System.out.println(a.get(10000));
		a.del(10000);
		System.out.println(a.get(10000));
		
		DynamicArray<Integer> b = new DynamicArray<Integer>(Integer.class);
		b.add(100);
		b.add(200);
		System.out.println(b.get(1));
	}	
	
	
}
