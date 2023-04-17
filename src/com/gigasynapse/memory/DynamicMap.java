package com.gigasynapse.memory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.gigasynapse.arraydisk.GenericDiskElement;
import com.gigasynapse.common.Hash;

public class DynamicMap<K, V> {
	private BlockSetItem[] root;
	private int rootBlockSize = 1024 * 1024;
	private int subBlockSize = 127;
	private DynamicArray<K> keys;
	private DynamicArray<V> values;
	public int maxDeepth = 0;
	public int deepth = 0;
	
	public DynamicMap(Class<K> clazzKey, Class<V> clazzValue) {
		root = createBlock(rootBlockSize);
		keys = new DynamicArray<K>(clazzKey);
		values = new DynamicArray<V>(clazzValue);
	}
	
	public BlockSetItem[] createBlock(int size) {
		BlockSetItem[] block = new BlockSetItem[size];
		for(int i = 0; i < size; i++) {
			block[i] = new BlockSetItem(-1, null, 0);
		}
		return block;
	}
	
	public K getKey(int id) {
		return keys.get(id);
	}
	
	public V getValue(int id) {
		return values.get(id);
	}
	
	public V get(K key) {
		int id = getId(key);
		if (id == -1) {
			return null;
		}
		return values.get(id);
	}
	
	public int getId(K e) {
	    byte data[] = toBytes(e);
	    return get(root, data, 0);
	}	

	private synchronized int get(BlockSetItem[] pointer, byte data[], 
			int hashId) {
	    int hash = Math.abs((int) (Hash.compute(data, hashId) % pointer.length));
	    BlockSetItem item = pointer[hash];
	    
	    if ((item.id == -1) && (item.data == null)) {
	    	return -1;
	    }
	    
	    if (item.data == null) {
	    	K element = keys.get(item.id);
	    	byte[] existingData = toBytes(element);
	    	if (Arrays.equals(data, existingData)) {
	    		return item.id;
	    	}
	    	return -1;
	    }
	    
	    return get(item.data, data, item.hashId);
	}
	
	public synchronized int add(K key, V value) {
		int id = getId(key);
		if (id != -1) {
			return id;
		}
		
		id = keys.add(key);
		values.add(value);
		
		byte data[] = toBytes(key);		
		deepth = 0;
		return add(root, id, data, 0);
	}	
	
	private synchronized int add(BlockSetItem[] pointer, int id, byte data[], 
			int hashId) {
		deepth++;
	    int hash = Math.abs((int) (Hash.compute(data, hashId) % pointer.length));
	    BlockSetItem item = pointer[hash];
	    
	    if ((item.id == -1) && (item.data == null)) {
	    	item.id = id;
	    	if (maxDeepth < deepth) {
	    		maxDeepth = deepth;
	    	}
	    	return id;
	    }
	    
	    if (item.data != null) {
	    	return add(item.data, id, data, item.hashId);
	    }
	    
	    K element = keys.get(item.id);
	    byte[] existingData = toBytes(element);
	    if (Arrays.equals(data, existingData)) {
	    	return item.id;
	    }
	    
	    int existingId = item.id;
	    
	    item.id = -1;
	    item.data = createBlock(subBlockSize);
	    item.hashId = Hash.getHashId(data, existingData, subBlockSize);
	    if (item.hashId == -1) {
	    	System.out.println("Couldn't find a hashId");
	    }
	    
	    add(item.data, existingId, existingData, item.hashId);
	    return add(item.data, id, data, item.hashId);
	}
	
	protected byte[] toBytes(Object element) {
		byte data[] = null;
		ByteBuffer byteBuffer = null;
		switch (element.getClass().getSimpleName()) {
		case "String":
			return ((String) element).getBytes();
		case "Date":
			return ByteBuffer.allocate(Long.BYTES).putLong(((Date) element).getTime()).array();
		case "Integer":
			return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) element).array();
		case "Float":
			return ByteBuffer.allocate(Float.BYTES).putFloat((Float) element).array();
		case "Short":
			return ByteBuffer.allocate(Short.BYTES).putShort((Short) element).array();
		case "Long":
			return ByteBuffer.allocate(Long.BYTES).putLong((Long) element).array();
		case "Double":
			return ByteBuffer.allocate(Double.BYTES).putDouble((Double) element).array();
		case "Character":
			return ByteBuffer.allocate(Character.BYTES).putChar((Character) element).array();
		case "int[]":
			int values[] = (int[]) element;			
			byteBuffer = ByteBuffer.allocate(Integer.BYTES * values.length);
			for(int i = 0; i < values.length; i++) {
				byteBuffer.putInt(values[i]);
			}
			return byteBuffer.array();
		case "float[]":
			float floatValues[] = (float[]) element;			
			byteBuffer = ByteBuffer.allocate(Float.BYTES * floatValues.length);
			for(int i = 0; i < floatValues.length; i++) {
				byteBuffer.putFloat(floatValues[i]);
			}
			return byteBuffer.array();
		case "double[]":
			double doubleValues[] = (double[]) element;			
			byteBuffer = ByteBuffer.allocate(Double.BYTES * doubleValues.length);
			for(int i = 0; i < doubleValues.length; i++) {
				byteBuffer.putDouble(doubleValues[i]);
			}
			return byteBuffer.array();
		case "short[]":
			short shortValues[] = (short[]) element;			
			byteBuffer = ByteBuffer.allocate(Short.BYTES * shortValues.length);
			for(int i = 0; i < shortValues.length; i++) {
				byteBuffer.putShort(shortValues[i]);
			}
			return byteBuffer.array();
		case "long[]":
			long longValues[] = (long[]) element;			
			byteBuffer = ByteBuffer.allocate(Long.BYTES * longValues.length);
			for(int i = 0; i < longValues.length; i++) {
				byteBuffer.putLong(longValues[i]);
			}
			return byteBuffer.array();
		case "byte[]":
			return (byte[]) element;			
		default:
			return ((GenericDiskElement) element).toBytes();
		}		
	}
	
	public static void main(String[] args) throws Exception {
		DynamicMap<String, String> dm = new DynamicMap<String, String>(String.class, String.class);
		dm.add("rodrigo", "vale");
		dm.add("vania", "araujo");
		int id = dm.getId("rodrigo");
		System.out.println(dm.getKey(id));
		System.out.println(dm.getValue(id));
		System.out.println(dm.get("rodrigo"));
		System.out.println(dm.get("vania"));
	}
}