package com.gigasynapse.common.io;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.gigasynapse.common.Hash;

public class DiskMap<K, V> {
	RandomAccessFileAbstract file;
	private DiskArray<K> keys;
	private DiskArray<V> values;
	private long rootBlockAddress;
	private int rootBlockSize = 1024 * 1024;
	private int subBlockSize = 127;

	public int maxDeepth = 0;
	public int deepth = 0;
	
	public DiskMap(Class<K> clazzK, Class<V> clazzV, RandomAccessFileAbstract file) throws IOException {
		this.file = file;
		
		boolean toInit = (file.length() == 0);
		
		// DiskArray will init the file in case it does not exists or size == 0
		keys = new DiskArray<K>(clazzK, file);
		values = new DiskArray<V>(clazzV, file, Integer.BYTES * 6 + Long.BYTES, toInit);
		
		if (toInit) {
			setRootBlockSize(rootBlockSize);
			setSubBlockSize(subBlockSize);
			rootBlockAddress = file.length();
			setRootBlockAddress(rootBlockAddress);
			createNewBlock(rootBlockSize);
		} else {
			rootBlockSize = getRootBlockSize();
			subBlockSize = getSubBlockSize();
			rootBlockAddress = getRootBlockAddress();
		}
	}
	
	private void setRootBlockSize(int rootBlockSize) throws IOException {
		file.seek(Integer.BYTES * 4);
		file.writeInt(rootBlockSize);
	}
	
	private void setSubBlockSize(int subBlockSize) throws IOException {
		file.seek(Integer.BYTES * 5);
		file.writeInt(subBlockSize);
	}
	
	private void setRootBlockAddress(long rootBlockAddress) throws IOException {
		file.seek(Integer.BYTES * 6);
		file.writeLong(rootBlockAddress);
	}
		
	private int getRootBlockSize() throws IOException {
		file.seek(Integer.BYTES * 4);
		return file.readInt();
	}
	
	private int getSubBlockSize() throws IOException {
		file.seek(Integer.BYTES * 5);
		return file.readInt();
	}
	
	private long getRootBlockAddress() throws IOException {
		file.seek(Integer.BYTES * 6);
		return file.readLong();
	}
		
	private void createNewBlock(int size) throws IOException {
		file.seek(file.length());
		BlockSetItem item = new BlockSetItem(-1, -1, -1);
		byte data[] = item.toBytes();
		for(int i = 0; i < size ; i++) {
			file.write(data);
		}		
	}
	
	public K getKeyById(int id) throws IOException {
		return keys.get(id);
	}
	
	public V getValue(K e) throws IOException {
		int id = getId(e);
		if (id == -1) {
			return null;
		}		
		return values.get(id);
	}
	
	public DiskArray<K> getKeysArray() {
		return keys;
	}
	
	public DiskArray<V> getValuesArray() {
		return values;
	}
	
	public V getValueById(int id) throws IOException {
		return values.get(id);
	}
	
	public void setValue(int id, V value) throws IOException {
		values.add(id, value);
	}
	
	public int getId(K e) throws IOException {
	    byte data[] = IOUtils.toBytes(e);
	    return get(rootBlockAddress, rootBlockSize, data, 0);
	}	
	
	private void set(long fileAddress, int index, BlockSetItem item) throws IOException {
		file.seek(fileAddress + index * BlockSetItem.size());
		byte data[] = new byte[BlockSetItem.size()];
		file.write(item.toBytes());
	}

	private BlockSetItem get(long fileAddress, int index) throws IOException {
		file.seek(fileAddress + index * BlockSetItem.size());
		byte data[] = new byte[BlockSetItem.size()];
		file.read(data);
		return new BlockSetItem(data);
	}

	private synchronized int get(long fileAddress, int size, byte data[], 
			int hashId) throws IOException {
	    int hash = Math.abs((int) (Hash.compute(data, hashId) % size));
	    BlockSetItem item = get(fileAddress, hash);
	    
	    if ((item.id == -1) && (item.filePos == -1)) {
	    	return -1;
	    }
	    
	    if (item.filePos == -1) {
	    	K element = keys.get(item.id);
	    	byte[] existingData = IOUtils.toBytes(element);
	    	if (Arrays.equals(data, existingData)) {
	    		return item.id;
	    	}
	    	return -1;
	    }
	    
	    return get(item.filePos, subBlockSize, data, item.hashId);
	}
	
	public synchronized int add(K e, V v) throws IOException {
		int id = getId(e);
		if (id != -1) {
			return id;
		}
		
		id = keys.add(e);
		values.add(v);
		
		byte data[] = IOUtils.toBytes(e);
		deepth = 0;
		return add(rootBlockAddress, rootBlockSize, id, data, 0);
	}	
	
	private synchronized int add(long fileAddress, int size, int id, byte data[], 
			int hashId) throws IOException {
		deepth++;
	    int hash = Math.abs((int) (Hash.compute(data, hashId) % size));
	    BlockSetItem item = get(fileAddress, hash);
	    
	    if ((item.id == -1) && (item.filePos == -1)) {
	    	item.id = id;
	    	if (maxDeepth < deepth) {
	    		maxDeepth = deepth;
	    	}
	    	set(fileAddress, hash, item);
	    	return id;
	    }
	    
	    if (item.filePos != -1) {
	    	return add(item.filePos, subBlockSize, id, data, item.hashId);
	    }
	    
	    K element = keys.get(item.id);
	    byte[] existingData = IOUtils.toBytes(element);
	    if (Arrays.equals(data, existingData)) {
	    	return item.id;
	    }
	    
	    int existingId = item.id;
	    
	    item.id = -1;
	    item.filePos = file.length();
	    item.hashId = Hash.getHashId(data, existingData, subBlockSize);
	    set(fileAddress, hash, item);
	    if (item.hashId == -1) {
	    	System.out.println("Couldn't find a hashId");
	    }
	    
	    createNewBlock(subBlockSize);	    
	    add(item.filePos, subBlockSize, existingId, existingData, item.hashId);
	    return add(item.filePos, subBlockSize, id, data, item.hashId);
	}
	
	public static void load(DiskMap<String, MyDiskElement> ds)  throws IOException {
		List<String> lines = FileUtils.readLines(new File("/home/rodrigo/verbos.txt/Verbos_202304121758.txt"), StandardCharsets.UTF_8);
		lines.forEach(item -> {
			try {
				MyDiskElement mde = new MyDiskElement(item.length(), item);
				ds.add(item, mde);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});		
		System.gc();
	}
	
	public static void main(String[] args) throws Exception {
		RandomAccessFileInDisk file = new RandomAccessFileInDisk(new File("/tmp/diskMap.bin"), "rw");
		DiskMap<String, MyDiskElement> ds = new DiskMap<String, MyDiskElement>(String.class, MyDiskElement.class, file);

//		load(ds);
		
		System.out.println(ds.getId("leiautarmos"));
		System.out.println(ds.getValue("leiautarmos").id);
		System.out.println(ds.getValue("leiautarmos").value);
		System.out.println(ds.getId("leiautem"));	
		System.out.println(ds.getValue("leiautem").id);	
		System.out.println(ds.getValue("leiautem").value);	
		file.close();
	}
}
