package com.gigasynapse.arraydisk;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

class DiskSetTest {

	@Test
	void testWAL() throws IOException {
		File file = new File("/tmp/diskset-get.bin");
		file.delete();
		
		File walFile = new File("/tmp/diskset-get.bin.wal");
		walFile.delete();
		
		DiskSet<String> hashSetDisk = new DiskSet(String.class, 10, 10, 10, 10);		
		hashSetDisk.open(file);
		hashSetDisk.add("content1");
		hashSetDisk.add("content2");
		hashSetDisk.add("content3");
		hashSetDisk.close();
		
		Path walPath = Paths.get("/tmp/diskset-get.bin.wal");
		byte data0[] = {0,0,0,10,0,0,0,8,0,0,0,0,0,0,0,-14};
		byte data1[] = {0,0,0,11,0,0,0,8,0,0,0,0,0,0,1,66};
		byte data2[] = {0,0,0,12,0,0,0,8,0,0,0,0,0,0,1,76};
		byte data3[] = {0,0,0,13,0,0,0,8,0,0,0,0,0,0,0,0};
		byte data4[] = {0,0,0,8,0,0,0,5,0,0,0,0,0};
		byte data5[] = {0,0,0,7,0,0,0,12,0,0,0,0,0,0,0,0,0,0,1,116};
		byte data6[] = {0,0,0,9,0,0,0,4,0,0,0,1};
		byte data7[] = {0,0,0,6,0,0,0,13,0,0,0,0,0,0,1,-105,0,0,0,0,0};
		
		Files.write(walPath, data0);
		hashSetDisk.open(file);
		int id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data1);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data2);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data3);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data4);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data5);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data6);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
		
		Files.write(walPath, data7);
		hashSetDisk.open(file);
		id = hashSetDisk.getId("content1");
		Assert.assertEquals(id, 0);		
		id = hashSetDisk.getId("content2");
		Assert.assertEquals(id, 1);		
		id = hashSetDisk.getId("content3");
		Assert.assertEquals(id, 2);		
		hashSetDisk.close();		
	}
}
