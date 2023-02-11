package com.gigasynapse.db.tuples;

import java.io.Serializable;
import java.util.Date;

public class WebsiteTuple implements Serializable {
	public int id = 0;
	public String name = "";
	public String url = "";
	public String domains = "";
	public String regexp = "";
	public String articlesIdRegExp = "";
	public int depth = 10;
	public int region = 0;
	public String rejectRegExp = "";
	public String feed = "";
	public boolean active = true;
	public long lastVisit = 0;
	public boolean toBeReviewed = false;
	public Date created;

	public WebsiteTuple(int id, String name, String url,	String domains, 
			String regexp, String articlesIdRegExp, int depth, int region, 
			String rejectRegExp, String feed, boolean active, long lastVisit, 
			boolean toBeReviewed, Date created) {
		this.id = id;
		this.name = name;
		this.url = url;
		this.domains = domains;
		this.regexp = regexp;
		this.articlesIdRegExp = articlesIdRegExp;
		this.depth = depth;
		this.region = region;
		this.rejectRegExp = rejectRegExp;
		this.feed = feed;
		this.active = active;
		this.lastVisit = lastVisit;
		this.toBeReviewed = toBeReviewed;
		this.created = created;
	}
}
