package com.gigasynapse.arraydisk;

public class WALContent {
	public int id;
	public byte data[];
	
	public WALContent(int id, byte data[]) {
		this.id = id;
		this.data = data;
	}
}
