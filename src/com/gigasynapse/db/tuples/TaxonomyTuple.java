package com.gigasynapse.db.tuples;

import org.json.JSONObject;

public class TaxonomyTuple {
	public int id;
	public int parentId;
	public String label;
	public String synonyms;
	public int crossWith;
	public String negative;
	public TaxonomyTuple() {		
	}
	
	public TaxonomyTuple(int id, int parentId, String label, 
			String synonyms, int crossWith, String negative) {
		this.id = id;
		this.parentId = parentId;
		this.label = label;
		this.synonyms = synonyms;
		this.crossWith = crossWith;
		this.negative = negative;
	}
	
	public JSONObject toJSON() {
		JSONObject item = new JSONObject();
		item.put("id", this.id);
		item.put("parentId", this.parentId);
		item.put("label", this.label);
		item.put("synonyms", this.synonyms);
		item.put("crossWith", this.crossWith);
		item.put("negative", this.negative);		
		return item;
	}
}
