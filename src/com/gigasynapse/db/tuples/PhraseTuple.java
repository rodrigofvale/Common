package com.gigasynapse.db.tuples;

import java.util.Date;

public class PhraseTuple {
	public int websiteId;
	public Date date;
	public String md5;
	public int counter;
	
	public PhraseTuple(int websiteId, Date date, String md5, int counter) {
		this.websiteId = websiteId;
		this.date = date;
		this.md5 = md5;
		this.counter = counter;
	}
}
