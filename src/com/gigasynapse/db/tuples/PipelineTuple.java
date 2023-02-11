package com.gigasynapse.db.tuples;

import com.gigasynapse.db.enums.ProcessStatus;

public class PipelineTuple {
	public String url;
	public ProcessStatus status;
	public int depth;
	public String obs;
	
	public PipelineTuple(String url, ProcessStatus status, int depth, String obs) {
		this.url = url;
		this.status = status;
		this.depth = depth;
		this.obs = obs;
	}
}
