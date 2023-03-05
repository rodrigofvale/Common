package com.gigasynapse.arraydisk;

class EmptyDiskNode {
	public long fileIndex;
	public int size;
	
	public EmptyDiskNode(long fileIndex, int size) {
		this.fileIndex = fileIndex;
		this.size = size;
	}
	
	public static int bytes() {
		return Long.BYTES + Integer.BYTES;
	}
	
	public byte[] toBytes() {
		byte data[] = new byte[bytes()];
		
		data[0] = (byte) ((fileIndex >>> 56) & 0xFF);
		data[1] = (byte) ((fileIndex >>> 48) & 0xFF);
		data[2] = (byte) ((fileIndex >>> 40) & 0xFF);
		data[3] = (byte) ((fileIndex >>> 32) & 0xFF);
		data[4] = (byte) ((fileIndex >>> 24) & 0xFF);
		data[5] = (byte) ((fileIndex >>> 16) & 0xFF);
		data[6] = (byte) ((fileIndex >>>  8) & 0xFF);
		data[7] = (byte) ((fileIndex >>>  0) & 0xFF);
		data[8] = (byte) ((size >>> 24) & 0xFF);
		data[9] = (byte) ((size >>> 16) & 0xFF);
		data[10] = (byte) ((size >>>  8) & 0xFF);
		data[11] = (byte) ((size >>>  0) & 0xFF);
				
		return data;
	}
	
	public static EmptyDiskNode fromBytes(byte data[]) {
		long fileIndex = ((data[0] << 56) + (data[1] << 48) + (data[2] << 40) + 
				(data[3] << 32) + (data[4] << 24) + (data[5] << 16) + 
				(data[6] << 8) + (data[7] << 0));
		int size = ((data[8] << 24) + (data[9] << 16) + (data[10] << 8) + 
				(data[11] << 0));
		
		return new EmptyDiskNode(fileIndex, size);
	}
}
