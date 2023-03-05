package com.gigasynapse.arraydisk;

public class BlockSetItem {
	public byte collision;
	public int pointer;
	
	public BlockSetItem(byte collision, int pointer) {
		this.collision = collision;
		this.pointer = pointer;
	}
	
	public static int bytes() {
		return Byte.BYTES + Integer.BYTES;
	}
}
