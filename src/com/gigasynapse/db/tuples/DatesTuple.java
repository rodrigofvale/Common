package com.gigasynapse.db.tuples;

import java.util.Date;

public class DatesTuple {
	public Date date;
	public Date lastModified;
	public Date lastProcessed;
	
	public DatesTuple(Date date, Date lastModified, Date lastProcessed) {
		this.date = date;
		this.lastModified = lastModified;
		this.lastProcessed = lastProcessed;
	}
}
