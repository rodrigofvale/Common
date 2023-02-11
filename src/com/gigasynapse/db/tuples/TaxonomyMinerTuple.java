package com.gigasynapse.db.tuples;

import java.io.Serializable;
import java.util.Date;

public class TaxonomyMinerTuple implements Serializable {
	public int taxonomyId;
	public String term;
	public float ratio;
	public float globalRatio;
	public TaxonomyMinerTuple(int taxonomyId, String term, float ratio, 
			float globalRatio) {
		this.taxonomyId = taxonomyId;
		this.term = term;
		this.ratio = ratio;
		this.globalRatio = globalRatio;
	}
}
