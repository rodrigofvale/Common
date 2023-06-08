package com.gigasynapse.crawler;

import java.util.Date;

public class WebPageMeta {
	public String url;
	public String title;	
	public String imageUrl;
	public Date pubDate;
	public String contentType;
	
	public WebPageMeta(String url, String title, String imageUrl, Date pubDate, 
			String contentType) {
		this.url = url;
		this.title = title;
		this.imageUrl = imageUrl;
		this.pubDate = pubDate;
		this.contentType = contentType;
	}
}
