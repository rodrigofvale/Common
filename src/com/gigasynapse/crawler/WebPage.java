package com.gigasynapse.crawler;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import com.gigasynapse.common.ServiceConfig;
import com.gigasynapse.common.Utils;


public class WebPage {
	public int idDoc;
	public String url;
	public String title;
	public String body;
	public String summary;
	public String imageUrl;
	public Date date;
	public String contentType;
	public String html;
	
	public WebPage(int idDoc, String url, String title, String body, 
			String summary, String imageUrl, Date date, String contentType, 
			String html) {
		this.idDoc = idDoc;
		this.url = url;
		this.title = title;
		this.body = body;
		this.imageUrl = imageUrl;
		this.summary = summary;
		this.date = date;
		this.contentType = contentType;
		this.html = html;
	}
	
	public void save() {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("idDoc", idDoc);
		jsonObject.put("url", url);
		jsonObject.put("title", title);
		jsonObject.put("body", body);
		jsonObject.put("imageUrl", imageUrl);
		jsonObject.put("date", Utils.toString(date));		
		jsonObject.put("contentType", contentType);
		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(date),
				chunks[0]
		);
			
		File file = new File(finalFolder);
		file.mkdirs();
		String fileName = String.format("%s/%d.html", finalFolder, idDoc);
		try {
			file = new File(fileName);
			FileUtils.write(file, html, StandardCharsets.UTF_8);
			fileName = String.format("%s/%d.json", finalFolder, idDoc);
			FileUtils.write(new File(fileName), jsonObject.toString(), 
					StandardCharsets.UTF_8);
			fileName = String.format("%s/%d.txt", finalFolder, idDoc);
			FileUtils.write(new File(fileName), summary, 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public void saveSummary() {
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(date),
				chunks[0]
		);
			
		File file = new File(finalFolder);
		file.mkdirs();
		try {
			String fileName = String.format("%s/%d.txt", finalFolder, idDoc);
			FileUtils.write(new File(fileName), summary, 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public static WebPage load(int idDoc, Date date) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(date),
				chunks[0]
		);
				
		try {
			String fileName = String.format("%s/%d.html", finalFolder, idDoc);
			String html = FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
			fileName = String.format("%s/%d.json", finalFolder, idDoc);
			String jsonTxt = FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
			JSONObject jsonObject = new JSONObject(jsonTxt);
			idDoc = jsonObject.getInt("idDoc");
			String url = jsonObject.getString("url");
			String title = jsonObject.getString("title");
			String body = jsonObject.getString("body");
			String imageUrl = jsonObject.getString("imageUrl");
			date = Utils.toDate(jsonObject.getString("date"));	
			String contentType = jsonObject.getString("contentType");
			
			WebPage webpage = new WebPage(idDoc, url, title, body, "", imageUrl, date, contentType, html);
			webpage.loadSummary(idDoc, date);
			return webpage;
		} catch (Exception e) {			
			e.printStackTrace();
		}		
		return null;
	}
	
	public void loadSummary(int idDoc, Date date) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(date),
				chunks[0]
		);
				
		try {
			String fileName = String.format("%s/%d.txt", finalFolder, idDoc);
			summary = FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public void loadMeta(int idDoc, Date date) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(date),
				chunks[0]
		);
				
		try {
			String fileName = String.format("%s/%d.json", finalFolder, idDoc);
			String jsonTxt = FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
			JSONObject jsonObject = new JSONObject(jsonTxt);
			idDoc = jsonObject.getInt("idDoc");
			url = jsonObject.getString("url");
			title = jsonObject.getString("title");
			body = jsonObject.getString("body");
			imageUrl = jsonObject.getString("imageUrl");
			date = Utils.toDate(jsonObject.getString("date"));	
			contentType = jsonObject.getString("contentType");
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
}
