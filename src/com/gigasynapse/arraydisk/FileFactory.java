package com.gigasynapse.arraydisk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashMap;

public class FileFactory {
	private static HashMap<String, RandomAccessFile> fabric = 
			new HashMap<String, RandomAccessFile>();
	
	public static RandomAccessFile get(File file, String mode) 
			throws FileNotFoundException {
		String fileName = file.getAbsolutePath();
		if (fabric.containsKey(fileName)) {
			return fabric.get(fileName);
		}
		
		RandomAccessFile cachedRandomAccessFile = 
				new RandomAccessFile(file, mode);
		
		fabric.put(fileName, cachedRandomAccessFile);
		return cachedRandomAccessFile;		
	}

	public static void closeAll() {
		Collection<RandomAccessFile> collection = fabric.values();
		collection.forEach(item -> {
			try {
				item.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		fabric.clear();
	}
}
