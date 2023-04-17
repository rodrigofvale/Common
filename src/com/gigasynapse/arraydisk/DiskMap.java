package com.gigasynapse.arraydisk;

import java.io.File;
import java.io.IOException;

/*
 * The set uses the array file as a base and it stores the set index in blocks 
 * controlled by a set map, as follows:
 *  
 * blockSetCounter:  [0, 0, ...]
 * blockSetHashId:   [0, 0, ...]
 * blockSetPointers: [blockSet1Pointer, blockSet2Pointer, ...]
 * blockSet1: [(collision, pointer), (collision, pointer), ...]
 *   
 *    
 * +---------+--------+-------------------------------------------------------+
 * | Address | offset | description                                           |
 * +---------+--------+-------------------------------------------------------+
 * |    0x20 |      8 | blockSetPointersAddress: a pointer to the block in    |
 * |         |        | disk with the address of each blockSetPointers        |
 * +---------+--------+-------------------------------------------------------+
 * |    0x28 |      8 | blockSetHashIdAddress: a pointer to the block in      |
 * |         |        | disk with the id of each Hash assigned to a block     |
 * +---------+--------+-------------------------------------------------------+
 * |    0x30 |      8 | blockSetCounterAddress: a pointer to the block in     |
 * |         |        | disk with the items counter for each blockSet         |
 * +---------+--------+-------------------------------------------------------+
 * |    0x38 |      4 | bockSetEmptyIndex: an index to the first empty        |
 * |         |        | blockSetPointers                                      |
 * +---------+--------+-------------------------------------------------------+
 */

public class DiskMap<E, V> {
	DiskSet<E> keys;
	DiskArray<V> values;
	Class<V> typeArgumentClass2;
	
	public DiskMap(Class<E> typeArgumentClass, Class<V> typeArgumentClass2)
			throws IOException {
		this(typeArgumentClass, typeArgumentClass2, 1024 * 1024, 1024, 
				10240, 0);
	}
	
	public DiskMap(Class<E> typeArgumentClass, Class<V> typeArgumentClass2,
			int mapSize, int tableBlockSize) throws IOException {
		this(typeArgumentClass, typeArgumentClass2, mapSize, 
				tableBlockSize, 10240, 0);
	}

	public DiskMap(Class<E> typeArgumentClass, Class<V> typeArgumentClass2, 
			int blockMapSize, int blockContentSize, 
			int emptyDiskNodesSize, int offset) throws IOException {
		keys = new DiskSet(typeArgumentClass, blockMapSize, blockContentSize, 
				emptyDiskNodesSize, 0);
		values = new DiskArray(typeArgumentClass2, blockMapSize, 
					blockContentSize, emptyDiskNodesSize, 0x3C);			
	}
	
	public void open(File file) throws IOException {
		boolean toClear = !file.exists();
		keys.open(file);
		values.open(file, toClear);
	}
	
	public synchronized int add(E e, V v) throws IOException {
		Integer id = keys.add(e);
		values.add(id, v);		
		return id;
	}	
	
	public V get(E e) throws IOException {
	    byte data[] = keys.toBytes(e);
	    int id = keys.getId(e);
	    if (id != -1) {
	    	return values.get(id);
	    }
	    return null;
	}
	
	public void close() throws IOException {
		keys.close();
	}
	

	public static void unitTest() throws IOException {
		File file = new File("/tmp/diskset.bin");
		file.delete();
		DiskMap diskMap = new DiskMap(String.class, String.class, 10, 10);
		diskMap.open(file);
		
		diskMap.add("rodrigo", "vale");
		diskMap.add("rodrigo", "de freitas vale");
		diskMap.add("milena", "mila");
		diskMap.add("gustavo", "vale");
		diskMap.add("rafael", "evangelista");
		System.out.println(diskMap.get("rodrigo"));
		System.out.println(diskMap.get("milena"));
		System.out.println(diskMap.get("gustavo"));
		System.out.println(diskMap.get("rafael"));
		//diskSet.add("gustavo", "vale");
		diskMap.close();
	}
	
	public static void main(String[] args) throws Exception {
		unitTest();
	}	
	
}
