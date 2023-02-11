package com.gigasynapse.db.tuples;

import java.util.Date;

import org.json.JSONObject;

import com.gigasynapse.db.enums.ProcessStatus;

public class JobsTuple {
	public int id;
	public String name;
	public Date created;
	public Date lastPing;
	public ProcessStatus status;
	public JSONObject json;
	
	public JobsTuple(int id, String name, Date created, Date lastPing, ProcessStatus status, JSONObject json) {
		this.id = id;
		this.name = name;
		this.created = created;
		this.lastPing = lastPing;
		this.status = status;
		this.json = json;
	}
	
	public static void main(String[] args) throws Exception {
		
	}
}
