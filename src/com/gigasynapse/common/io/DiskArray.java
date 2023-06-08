package com.gigasynapse.common.io;

import java.io.File;
import java.io.IOException;

public class DiskArray<E> {	
	int totalBlocks = 10240;
	int blockSize = 10240;
	int blockMapDiskAddress = 0;
	Class<E> clazz;
	RandomAccessFileAbstract file;
	int offset;
	
	
	public DiskArray(Class<E> clazz, RandomAccessFileAbstract file) throws IOException {
		this(clazz, file, 0, file.length() == 0);
	}
	
	public DiskArray(Class<E> clazz, RandomAccessFileAbstract file, int offset, boolean toInit) throws IOException {
		this.clazz = clazz;
		this.file = file;
		this.offset = offset;
		
		if (toInit) {
			file.seek(0);
			// 1k of area to be used as intended by any class including this one
			if (file.length() == 0) {
				for(int i = 0; i < 1024; i++) {
					file.writeInt(0);
				}
			}
			setTotalBlocks(totalBlocks);
			setBlockSize(blockSize);
			
			blockMapDiskAddress = (int) file.length();
			setBlockMapDiskAddress(blockMapDiskAddress);			
			createNewBlock(totalBlocks);
		} else {
			totalBlocks = getTotalBlocks();
			blockSize = getBlockSize();
			blockMapDiskAddress = getBlockMapDiskAddress();
		}
	}
	
	public void setTotalBlocks(int totalBlocks) throws IOException {
		file.seek(offset + 0);
		file.writeInt(totalBlocks);
	}
	
	public int getTotalBlocks() throws IOException {
		file.seek(offset + 0);
		return file.readInt();
	}
	
	public void setBlockSize(int blockSize) throws IOException {
		file.seek(offset + Integer.BYTES);
		file.writeInt(blockSize);
	}
	
	public int getBlockSize() throws IOException {
		file.seek(offset + Integer.BYTES);
		return file.readInt();
	}
	
	public void setBlockMapDiskAddress(int blockMapDiskAddress) throws IOException {
		file.seek(offset + Integer.BYTES * 2);
		file.writeInt(blockMapDiskAddress);
	}
	
	public int getBlockMapDiskAddress() throws IOException {
		file.seek(offset + Integer.BYTES * 2);
		return file.readInt();
	}
	
	public void setTotalElements(int totalElements) throws IOException {
		file.seek(offset + Integer.BYTES * 3);
		file.writeInt(totalElements);
	}
	
	public int getTotalElements() throws IOException {
		file.seek(offset + Integer.BYTES * 3);
		return file.readInt();
	}
	
	public synchronized int add(E e) throws IOException {
		return add(getTotalElements(), e);
	}
	
	private void createNewBlock(int size) throws IOException {
		file.seek(file.length());
		for(int i = 0; i < size; i++) {
			file.writeInt(-1);
		}		
	}

	public synchronized int add(int i, E e) throws IOException {
		int blockId = i / blockSize;
		int index = i % blockSize;
		
		file.seek(blockMapDiskAddress + blockId * Integer.BYTES);
		int blockDiskAddress = file.readInt();
		if (blockDiskAddress == -1) {
			file.seek(blockMapDiskAddress + blockId * Integer.BYTES);
			file.writeInt((int) file.length());
			
			blockDiskAddress = (int) file.length();
			file.seek(file.length());
			createNewBlock(blockSize);
		}
		
		file.seek(blockDiskAddress + index * Integer.BYTES);
		int filePos = file.readInt();
		
		byte data[] = IOUtils.toBytes(e);
		if (filePos == -1) {
			filePos = (int) file.length();
			file.seek(blockDiskAddress + index * Integer.BYTES);
			file.writeInt(filePos);
			file.seek(filePos);
			file.writeInt(data.length);
			file.write(data);
		} else {
			file.seek(filePos);
			int datasize = file.readInt();
			if (datasize >= data.length) {
				file.write(data);
			} else {
				filePos = (int) file.length();
				file.seek(blockDiskAddress + index * Integer.BYTES);
				file.writeInt(filePos);
				file.seek(filePos);
				file.writeInt(data.length);
				file.write(data);
			}
		}
		
		int totalElements = getTotalElements();
		if (i > totalElements) {
			setTotalElements(i);
		} else if (i == totalElements) {
			totalElements++;
			setTotalElements(totalElements);
		}
		
		return i;
	}
	
	public synchronized E get(int i) throws IOException {
		int blockId = i / blockSize;
		int index = i % blockSize;
		
		file.seek(blockMapDiskAddress + blockId * Integer.BYTES);
		int blockDiskAddress = file.readInt();
		if (blockDiskAddress == -1) {
			return null;
		}
		
		file.seek(blockDiskAddress + index * Integer.BYTES);
		int filePos = file.readInt();
		
		if (filePos == -1) {
			return null;
		}
		
		file.seek(filePos);
		int datasize = file.readInt();
		byte data[] = new byte[datasize];
		file.read(data);
		
		return (E) IOUtils.toObject(clazz, data);		
	}
	
	public synchronized void del(int i) throws IOException {
		int blockId = i / blockSize;
		int index = i % blockSize;
		
		file.seek(blockMapDiskAddress + blockId * Integer.BYTES);
		int blockDiskAddress = file.readInt();
		if (blockDiskAddress == -1) {
			return;
		}
		
		file.seek(blockDiskAddress + index * Integer.BYTES);
		file.writeInt(-1);
	}
	
	public RandomAccessFileAbstract getFile() {
		return file;
	}
	
	public static void main(String[] args) throws Exception {		
		RandomAccessFileInMemory file = new RandomAccessFileInMemory();
		
		DiskArray<String> a = new DiskArray<String>(String.class, file);
		
		a.add("rodrigo");
		a.add(0, "rodrigo");
		a.add(0, "helena");
		a.add(1, "vania");
		
		System.out.println(a.get(0));
		System.out.println(a.get(1));
		System.out.println(a.get(2));
		file.close();
	}	
	
	
}
