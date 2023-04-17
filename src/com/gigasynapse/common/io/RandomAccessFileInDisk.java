package com.gigasynapse.common.io;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class RandomAccessFileInDisk extends RandomAccessFileAbstract {
	RandomAccessFile raf;
	
	public RandomAccessFileInDisk(String file, String mode) throws FileNotFoundException {
		super(file, mode);
		raf = new RandomAccessFile(file, mode);
	}
	
	public RandomAccessFileInDisk(File file, String mode) throws FileNotFoundException {
		super(file, mode);
		raf = new RandomAccessFile(file, mode);
	}

	
	@Override
	public File getFile() {
		return file;
	}

	@Override
	public void write(byte b) throws IOException {
		raf.write(b);
	}

	@Override
	public void close() throws IOException {
		raf.close();
	}

	@Override
	public void seek(long pos) throws IOException {
		raf.seek(pos);
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
		return raf.skipBytes(n);
	}

	@Override
	public int read() throws IOException {
		return raf.read();
	}

	@Override
	public long length() throws IOException {
		return raf.length();
	}

	@Override
	public void setLength(long newLength) throws IOException {
		raf.setLength(newLength);
	}

	@Override
	public long getFilePointer() throws IOException {
		return raf.getFilePointer();
	}
}
