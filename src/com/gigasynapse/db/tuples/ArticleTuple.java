package com.gigasynapse.db.tuples;

import java.util.Date;

import com.gigasynapse.db.enums.ArticleStage;

public class ArticleTuple {
	public int docId = -1;
	public int websiteId = -1;
	public String url = "";
	public String mimeType = "";
	public Date firstSeen = new Date();
	public Date lastSeen = new Date();
	public ArticleStage stage;
	public String md5;
	
	public ArticleTuple() {		
	}
	
	public ArticleTuple(int docId, int websiteId, String url, Date firstSeen, 
			Date lastSeen, String mimeType, ArticleStage stage,
			String md5) {
		this.docId = docId;
		this.websiteId = websiteId;
		this.url = url;
		this.firstSeen = firstSeen;
		this.lastSeen = lastSeen;
		this.mimeType = mimeType;
		this.stage = stage;
		this.md5 = md5;
	}
}
