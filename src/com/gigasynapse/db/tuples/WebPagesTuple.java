package com.gigasynapse.db.tuples;

import java.util.Date;

import com.gigasynapse.db.enums.ProcessStatus;

public class WebPagesTuple {
	public int id;
	public int websiteId;
	public String url;
	public boolean isArticle;
	public String md5;
	public Date date;
	public int depth;
	public ProcessStatus status;
	
	public WebPagesTuple(int id, int websiteId, String url, boolean isArticle, 
			int depth, ProcessStatus status) {
		this.id = id;
		this.websiteId = websiteId;
		this.url = url;
		this.isArticle = isArticle;
		this.depth = depth;
		this.status = status;
	}	
}
