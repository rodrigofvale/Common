package com.gigasynapse.arraydisk;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

class DiskArrayTest {

	@Test
	void testAdd() throws IOException {
		File file = new File("/tmp/diskarray-add.bin");
		file.delete();
		DiskArray<String> hashSetDisk = new DiskArray(String.class, 10, 10, 10, 10);		
		hashSetDisk.open(file);
		
		hashSetDisk.add("content1");
		String value = hashSetDisk.get(0);		
		Assert.assertEquals(value, "content1");
		hashSetDisk.add("content2");
		value = hashSetDisk.get(1);
		Assert.assertEquals(value, "content2");
		long length1 = hashSetDisk.length();
		hashSetDisk.add(1, "content3");
		long length2 = hashSetDisk.length();
		value = hashSetDisk.get(1);		
		Assert.assertEquals(length1, length2);
		value = hashSetDisk.get(1);
		Assert.assertEquals(value, "content3");
	}

	@Test
	void testDel() throws IOException {
		File file = new File("/tmp/diskarray-del.bin");
		file.delete();
		DiskArray<String> hashSetDisk = new DiskArray(String.class, 10, 10, 10, 10);		
		hashSetDisk.open(file);
		
		hashSetDisk.add("content1");
		hashSetDisk.add("content2");
		String value = hashSetDisk.get(0);		
		Assert.assertEquals(value, "content1");
		hashSetDisk.del(0);
		value = hashSetDisk.get(0);		
		Assert.assertEquals(value, null);
		long length1 = hashSetDisk.length();
		hashSetDisk.add(0, "content1");
		long length2 = hashSetDisk.length();
		Assert.assertEquals(length1, length2);
	}

	@Test
	void testGet() throws IOException {
		File file = new File("/tmp/diskarray-get.bin");
		file.delete();
		DiskArray<String> hashSetDisk = new DiskArray(String.class, 10, 10, 10, 10);		
		hashSetDisk.open(file);

		hashSetDisk.add("content1");
		String value = hashSetDisk.get(0);		
		Assert.assertEquals(value, "content1");		
	}
	
	@Test
	void testInvalidWAL() throws IOException {
		File file = new File("/tmp/diskarray-get.bin");
		file.delete();
		DiskArray<String> hashSetDisk = new DiskArray(String.class, 10, 10, 10, 10);		
		hashSetDisk.open(file);
		hashSetDisk.add("content1");
		hashSetDisk.close();
		
		// writes an invalid WAL file
		RandomAccessFile indexFile = new RandomAccessFile(
				new File("/tmp/diskarray-get.bin.wal"), "rw");
		indexFile.write(1);
		indexFile.writeInt(4);
		indexFile.close();
		
		hashSetDisk.open(file);
		String value = hashSetDisk.get(0);
		Assert.assertEquals(value, "content1");		
	}

	@Test
	void testWAL() throws IOException {
		File file = new File("/tmp/diskarray-get.bin");
		file.delete();
		
		File walFile = new File("/tmp/diskarray-get.bin.wal");
		walFile.delete();
		
		DiskArray<String> hashSetDisk = new DiskArray(String.class, 10, 10, 10, 10);		
		hashSetDisk.open(file);
		hashSetDisk.add("content1");
		hashSetDisk.add("content2");
		hashSetDisk.add("content3");
		hashSetDisk.close();
		
		Path walPath = Paths.get("/tmp/diskarray-get.bin.wal");
		byte data0[] = {0,0,0,1,0,0,0,12,0,0,0,0,0,0,0,0,0,0,0,-14};
		byte data1[] = {0,0,0,3,0,0,0,21,0,0,0,0,0,0,1,66,0,0,0,8,8,99,111,110,116,101,110,116,49};
		byte data2[] = {0,0,0,4,0,0,0,20,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,66};
		byte data3[] = {0,0,0,3,0,0,0,21,0,0,0,0,0,0,1,78,0,0,0,8,8,99,111,110,116,101,110,116,50};
		byte data4[] = {0,0,0,4,0,0,0,20,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,1,78};
		byte data5[] = {0,0,0,3,0,0,0,21,0,0,0,0,0,0,1,90,0,0,0,8,8,99,111,110,116,101,110,116,51};
		byte data6[] = {0,0,0,4,0,0,0,20,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,1,90};

		Files.write(walPath, data0);
		hashSetDisk.open(file);
		Assert.assertEquals(hashSetDisk.get(0), "content1");		
		Assert.assertEquals(hashSetDisk.get(1), "content2");		
		Assert.assertEquals(hashSetDisk.get(2), "content3");		
		hashSetDisk.close();
		
		Files.write(walPath, data1);
		hashSetDisk.open(file);
		Assert.assertEquals(hashSetDisk.get(0), "content1");		
		Assert.assertEquals(hashSetDisk.get(1), "content2");		
		Assert.assertEquals(hashSetDisk.get(2), "content3");		
		hashSetDisk.close();

		Files.write(walPath, data2);
		hashSetDisk.open(file);
		Assert.assertEquals(hashSetDisk.get(0), "content1");		
		Assert.assertEquals(hashSetDisk.get(1), "content2");		
		Assert.assertEquals(hashSetDisk.get(2), "content3");		
		hashSetDisk.close();

		Files.write(walPath, data3);
		hashSetDisk.open(file);
		Assert.assertEquals(hashSetDisk.get(0), "content1");		
		Assert.assertEquals(hashSetDisk.get(1), "content2");		
		Assert.assertEquals(hashSetDisk.get(2), "content3");		
		hashSetDisk.close();

		Files.write(walPath, data4);
		hashSetDisk.open(file);
		Assert.assertEquals(hashSetDisk.get(0), "content1");		
		Assert.assertEquals(hashSetDisk.get(1), "content2");		
		Assert.assertEquals(hashSetDisk.get(2), "content3");		
		hashSetDisk.close();

	}

}
