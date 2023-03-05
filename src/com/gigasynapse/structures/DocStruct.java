package com.gigasynapse.structures;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;

public class DocStruct implements Serializable {
	public Date date;
	public Integer[] entities;
	public Short region;
	public Short websiteId;
	public Integer idDoc;
	
	// field and filesize
	public HashMap<String, Long> fileIndex = new HashMap<String, Long>();
	
	public DocStruct(Date date, Integer[] entities, short region, short websiteId) {
		this.date = date;
		this.entities = entities;
		this.region = region;
		this.websiteId = websiteId;		
	}
}
