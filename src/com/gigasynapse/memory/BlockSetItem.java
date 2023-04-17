package com.gigasynapse.memory;

import java.nio.ByteBuffer;

import com.gigasynapse.common.io.DiskElement;

public class BlockSetItem extends DiskElement {
	public int id;
	public long filePos;
	public int hashId;
	
	public BlockSetItem(byte data[]) {
		fromBytes(data);
	}
	
	public BlockSetItem(int id, long filePos, int hashId) {
		this.id = id;
		this.filePos = filePos;
		this.hashId = hashId;
	}

	@Override
	public byte[] toBytes() {
		ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + 
				Long.BYTES);
		bb.putInt(id);
		bb.putInt(hashId);
		bb.putLong(filePos);
		return bb.array();
	}

	@Override
	public void fromBytes(byte[] data) {
		ByteBuffer bb = ByteBuffer.wrap(data);
		id = bb.getInt();
		hashId = bb.getInt();
		filePos = bb.getLong();
	}
	
	public static int size() {
		return Integer.BYTES + Integer.BYTES + Long.BYTES;
	}
}
