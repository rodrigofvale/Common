package com.gigasynapse.common.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MyDiskElement extends DiskElement {
	int id;
	String value;

	public MyDiskElement() {
	}
	
	public MyDiskElement(int id, String value) {
		this.id = id;
		this.value = value;
	}
	
	@Override
	public byte[] toBytes() {
	    byte[] data = value.getBytes(StandardCharsets.UTF_8);
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length + Integer.BYTES 
				+ Integer.BYTES);
		byteBuffer.putInt(id);
		byteBuffer.putInt(data.length);
		byteBuffer.put(data);
		return byteBuffer.array();
	}

	@Override
	public void fromBytes(byte[] data) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(data);
		id = byteBuffer.getInt();
		int length = byteBuffer.getInt();
		byte stringInBytes[] = new byte[length];
		byteBuffer.get(stringInBytes);
		value = new String(stringInBytes, StandardCharsets.UTF_8);
	}
}
