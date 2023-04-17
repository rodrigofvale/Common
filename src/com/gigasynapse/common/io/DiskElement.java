package com.gigasynapse.common.io;

public abstract class DiskElement {
	public abstract byte[] toBytes();
	public abstract void fromBytes(byte[] data);	
}
