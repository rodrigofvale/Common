package com.gigasynapse.common.io;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

public abstract class RandomAccessFile {
	public RandomAccessFile(File file, String mode, int cacheBlockSize, 
			int cacheSize) throws FileNotFoundException {
	}
	
	public RandomAccessFile(File file, String mode, int cacheSize) 
			throws FileNotFoundException {
		
	}
	
	public abstract File getFile();
	public abstract void write(byte b) throws IOException;
	public abstract void write(byte[] b) throws IOException;
	public abstract void write(byte[] b, int off, int len) throws IOException;
	public abstract void write(int b) throws IOException;
    public abstract void writeShort(int v) throws IOException;
    public abstract void writeChar(int v) throws IOException;
    public abstract void writeInt(int v) throws IOException;
    public abstract void writeLong(long v) throws IOException;
    public abstract void writeFloat(float v) throws IOException;
	public abstract void writeBoolean(boolean v) throws IOException;
	public abstract void writeByte(int v) throws IOException;
	public abstract void writeBytes(String s) throws IOException;	
	public abstract void writeBytes(byte b[], int off, int len) throws IOException;
    public abstract void writeChars(String s) throws IOException;
    public abstract void writeDouble(double v) throws IOException;
    public abstract void writeUTF(String str) throws IOException;
    public abstract String readUTF() throws IOException;
    public abstract void close() throws IOException;
	public abstract void seek(long pos) throws IOException;
    public abstract FileChannel getChannel();
    public abstract FileDescriptor getFD() throws IOException;
	public abstract int read(byte[] b) throws IOException;
	public abstract byte readByte() throws IOException;
	public abstract int skipBytes(int n);
    public abstract char readChar() throws IOException;
	public abstract int read() throws IOException;
	public abstract int read(byte[] b, int off, int len) throws IOException;
	public abstract int readBytes(byte b[], int off, int len) throws IOException;
	public abstract void readFully(byte[] b) throws IOException;
	public abstract void readFully(byte[] b, int off, int len) throws IOException;
    public abstract boolean readBoolean() throws IOException;
    public abstract float readFloat() throws IOException;
    public abstract int readInt() throws IOException;
    public abstract long readLong() throws IOException;
    public abstract short readShort() throws IOException;
	public abstract long length() throws IOException;
    public abstract double readDouble() throws IOException;
    public abstract void setLength(long newLength) throws IOException;
	public abstract long getFilePointer();
    public abstract String readLine() throws IOException;
    public abstract int readUnsignedByte() throws IOException;
    public abstract int readUnsignedShort() throws IOException;
}
