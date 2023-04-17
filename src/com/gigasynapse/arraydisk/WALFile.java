package com.gigasynapse.arraydisk;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class WALFile {
	protected RandomAccessFile walFile;

	public WALFile(File file) throws FileNotFoundException {
		walFile = new RandomAccessFile(file, "rw");
	}

	public void set(int id, byte data[]) throws IOException {
		walFile.setLength(0);
		walFile.writeInt(id);
		walFile.writeInt(data.length);
		walFile.write(data);
	}

	public void set(int id) throws IOException {
		walFile.setLength(0);
		walFile.writeInt(id);
		walFile.writeInt(0);
	}

	public void add(byte value) throws IOException {
		walFile.writeByte(value);
	}

	public void add(int value) throws IOException {
		walFile.writeInt(value);
	}

	public void add(long value) throws IOException {
		walFile.writeLong(value);
	}

	public void add(byte value[]) throws IOException {
		walFile.write(value.length);
		walFile.write(value);
	}

	public void flush() throws IOException {
		int size = (int) walFile.length() - Integer.BYTES * 2;
		walFile.seek(Integer.BYTES);
		walFile.writeInt(size);
/*
		System.out.print("{");
		walFile.seek(0);
		byte b = walFile.readByte();
		try {
			do {
				System.out.printf("%d,", b);
				b = walFile.readByte();
			} while (true);
		} catch (EOFException e) {
			System.out.println("}");
		}
*/		
	}

	public void clean() throws IOException {
		walFile.setLength(0);
	}

	public WALContent get() throws IOException {
		if (walFile.length() == 0) {
			return new WALContent(-1, null);
		}
		walFile.seek(0);
		int id = walFile.readInt();
		int size = walFile.readInt();
		
		if (size == 0) {
			return new WALContent(-1, null);
		}
		
		byte data[] = new byte[size];
		int bytes = walFile.read(data);
		if (bytes == size) {
			return new WALContent(id, data);
		}
		return null;
	}

	public void close() throws IOException {
		walFile.setLength(0);
		walFile.close();
	}
}
