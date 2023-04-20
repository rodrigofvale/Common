package com.gigasynapse.common.io;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.Stack;

public class RandomAccessFileInCache extends RandomAccessFileAbstract {
	RandomAccessFile raf;
	
	int cacheSize = 1024 * 1024 * 100;
	int blockSize = 1024 * 1024;
	int totalBlocks = cacheSize / blockSize;
	long filePos = 0;
	long fileSize = 0;
	HashMap<Integer, byte[]> cache = new HashMap<Integer, byte[]>();
	LinkedList<Integer> cacheFifo = new LinkedList<Integer>();
	
	public RandomAccessFileInCache(String file, String mode) throws IOException {
		super(file, mode);
		raf = new RandomAccessFile(file, mode);
		filePos = 0;
		fileSize = raf.length();
	}
	
	public RandomAccessFileInCache(File file, String mode) throws IOException {
		super(file, mode);
		raf = new RandomAccessFile(file, mode);
		filePos = 0;
		fileSize = raf.length();
	}

	
	@Override
	public File getFile() {
		return file;
	}
	
	private void loadCacheBlock(int blockId) throws IOException {
		long blockFilePos = blockId * blockSize;
		
		if (blockFilePos >= raf.length()) {
			cache.put(blockId, new byte[blockSize]);
			return;
		}
		
		byte data[] = new byte[blockSize];
		raf.seek(blockFilePos);
		raf.read(data);
		cache.put(blockId, data);
	}
	
	private void dumpCacheBlock(int blockId) throws IOException {
		int bytes2Write = blockSize;
		long fileSizeForThisBlock = blockId * blockSize + blockSize;
		if (fileSize < fileSizeForThisBlock) {
			bytes2Write = (int) (fileSize - (blockId * blockSize));
		}
		
		if (raf.length() < fileSize) {
			raf.setLength(fileSize);
		}
		
		raf.seek(blockId * blockSize);
		if (bytes2Write != blockSize) {
			raf.write(cache.get(blockId), 0, bytes2Write);
		} else {
			raf.write(cache.get(blockId));
		}
	}

	@Override
	public void write(byte b) throws IOException {
		int blockId = (int) (filePos / blockSize);
		int index = (int) (filePos % blockSize);
		
		if (cache.containsKey(blockId)) {
			cache.get(blockId)[index] = b;
		} else {
			if ((cache.size() > 0) && (cache.size() >= totalBlocks)) {
				int id = cacheFifo.removeFirst();
				dumpCacheBlock(id);
				cache.remove(id);
			}
			loadCacheBlock(blockId);
			cacheFifo.add(blockId);
			cache.get(blockId)[index] = b;
		}
		filePos++;
		if (filePos > fileSize) {
			fileSize = filePos;
		}
	}
	
	public void flush() throws IOException {
		Iterator<Integer> i = cache.keySet().iterator();
		while (i.hasNext()) {
			int blockId = i.next();
			dumpCacheBlock(blockId);
			i.remove();
		}
		cacheFifo.clear();
	}

	@Override
	public void close() throws IOException {
		flush();
		raf.close();
	}

	@Override
	public void seek(long pos) throws IOException {
		filePos = pos;
	}

	@Override
	public FileChannel getChannel() {
		return raf.getChannel();
	}

	@Override
	public FileDescriptor getFD() throws IOException {
		return raf.getFD();
	}

	@Override
	public int skipBytes(int n) throws IOException {
		if (raf.length() - filePos > n) {
			filePos += n;
			return n;			
		}
		int skipped = (int) (raf.length() - filePos);
		filePos = raf.length();
		return skipped;
	}

	@Override
	public int read() throws IOException {
		int blockId = (int) (filePos / blockSize);
		int index = (int) (filePos % blockSize);
		
		if (!cache.containsKey(blockId)) {
			if (cache.size() >= totalBlocks) {
				int id = cacheFifo.removeFirst();
				dumpCacheBlock(id);
				cache.remove(id);				
			}
			loadCacheBlock(blockId);
			cacheFifo.add(blockId);
		}
		filePos++;
		return cache.get(blockId)[index] & 0xFF;
	}

	@Override
	public long length() throws IOException {
		return fileSize;
	}

	@Override
	public void setLength(long newLength) throws IOException {
		fileSize = newLength;
	}

	@Override
	public long getFilePointer() throws IOException {
		return raf.getFilePointer();
	}	
}
