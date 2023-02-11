package com.gigasynapse.common;

import org.json.JSONArray;
import org.json.JSONObject;

public class URLFetchStatus {
	public int code;
	public String answer;
	
	public URLFetchStatus(int code, String answer) {
		this.code = code;
		this.answer = answer;
		
	}
	
	public JSONArray getAsJSONArray() {
		JSONArray jsonArray = new JSONArray(answer);
		return jsonArray;
	}
	
	public JSONObject getAsJSONObject() {
		JSONObject jsonObject = new JSONObject(answer);
		return jsonObject;
	}
}
