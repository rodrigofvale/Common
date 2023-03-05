package com.gigasynapse.arraydisk;

public abstract class GenericDiskElement {
	public abstract byte[] toBytes();
	public abstract void fromBytes(byte[] data);
}
