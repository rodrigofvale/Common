package com.gigasynapse.common.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class RandomAccessFileInMemory extends RandomAccessFileAbstract {
	// the default value allows you to have up to 32GB of in memory file 
	private int blockSize = 1024 * 1024;
	private int totalBlocks = 1024 * 32;
	private MemoryBlock blocks[];
	private long fileSize;
	private File file;

	public RandomAccessFileInMemory() 
			throws FileNotFoundException {
		super((File) null, null);
		init();
	}
	
	public void init() {
		blocks = new MemoryBlock[totalBlocks];
		for(int i = 0; i < totalBlocks; i++) {
			blocks[i] = null;
		}
		filePos = 0;
		fileSize = 0;
		this.file = file;		
	}
	
	public void load(File file) throws FileNotFoundException {
		if (file == null) {
			return;
		}
		
		init();
		
    	FileInputStream fis = null;
    	BufferedInputStream bis = null;
		try {
			fis = new FileInputStream(file);
	    	bis = new BufferedInputStream(fis, 1024 * 1024);
		} catch (FileNotFoundException e1) {
			if (mode.equals("r")) {
				throw new FileNotFoundException(e1.getMessage());
			}
			return;
		}
    	
		System.out.println("Loading...");
        try {
        	int b = bis.read();
			while(b != -1){     
				write(b);
				b = bis.read();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		System.out.println("Loaded");
	}

	@Override
	public File getFile() {
		// TODO Auto-generated method stub
		return file;
	}

	@Override
	public void write(byte b) throws IOException {
		int blockId = (int) (filePos / blockSize);
		int index = (int) (filePos % blockSize);
		
		if (blocks[blockId] == null) {
			blocks[blockId] = new MemoryBlock(blockSize);
		}
		blocks[blockId].data[index] = b;
		filePos++;
		
		if (filePos > fileSize) {
			fileSize = filePos;
		}
	}

	public void save(File file) throws IOException {
		if (file == null) {
			return;
		}
		
		FileOutputStream fos = new FileOutputStream(file);
    	BufferedOutputStream bos = new BufferedOutputStream(fos);
    	this.seek(0);
    	for(int i = 0; i < fileSize; i++) {
    		bos.write(this.read());
    	}
    	bos.close();
		for(int i = 0; i < totalBlocks; i++) {
			blocks[i] = null;
		}
		blocks = null;
	}
	
	@Override
	public void seek(long pos) throws IOException {
		filePos = pos;
	}

	@Override
	public FileChannel getChannel() {
		return null;
	}

	@Override
	public FileDescriptor getFD() throws IOException {
		return null;
	}

	@Override
	public int skipBytes(int n) throws IOException {
		filePos += n;
		return n;
	}

	@Override
	public int read() throws IOException {
		int blockId = (int) (filePos / blockSize);
		int index = (int) (filePos % blockSize);
		
		if (blocks[blockId] == null) {
			throw new EOFException();
		}
		
		if (filePos >= fileSize) {
			return -1;
		}
		
		filePos++;
		return blocks[blockId].data[index] & 0xFF;
	}

	@Override
	public long length() throws IOException {
		return fileSize;
	}

	@Override
	public void setLength(long newLength) throws IOException {
		if (newLength > fileSize) {
			seek(fileSize);
			for(long i = fileSize; i < newLength; i++) {
				write(0);
			}
		}
		fileSize = newLength;
	}

	@Override
	public long getFilePointer() throws IOException {
		return -1;
	}

	public static void main(String[] args) throws Exception {
		RandomAccessFileInMemory rafm = 
				new RandomAccessFileInMemory();
		
		System.out.println(rafm.length());
		rafm.writeInt(5);
		rafm.writeFloat(5.5f);
		rafm.seek(0);
		System.out.println(rafm.readInt());
		System.out.println(rafm.readFloat());
		System.out.println(rafm.length());
		rafm.save(new File("/tmp/tmp.db"));
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
