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
	public Date firstSeen;
	public Date date;
	public String contentType;
	public String html;
	
	public WebPage(int idDoc, String url, String title, String body, 
			String summary, String imageUrl, Date date, String contentType, 
			String html, Date firstSeen) {
		this.idDoc = idDoc;
		this.url = url;
		this.title = title;
		this.body = body;
		this.imageUrl = imageUrl;
		this.summary = summary;
		this.date = date;
		this.firstSeen = firstSeen;
		this.contentType = contentType;
		this.html = html;
	}
	
	public void save() {
		saveHtml();
		saveMeta();
		saveBody();
	}
	
	public void saveHtml() {
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
				chunks[0]
		);
			
		File file = new File(finalFolder);
		file.mkdirs();
		String fileName = String.format("%s/%d.html", finalFolder, idDoc);
		try {
			file = new File(fileName);
			FileUtils.write(file, html, StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public void saveMeta() {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("idDoc", idDoc);
		jsonObject.put("url", url);
		jsonObject.put("title", title);
		jsonObject.put("imageUrl", imageUrl);
		jsonObject.put("date", Utils.toString(date));		
		jsonObject.put("contentType", contentType);
		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
				chunks[0]
		);
			
		File file = new File(finalFolder);
		file.mkdirs();
		try {
			String fileName = String.format("%s/%d.json", finalFolder, idDoc);
			FileUtils.write(new File(fileName), jsonObject.toString(), 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public void saveBody() {
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
				chunks[0]
		);
			
		File file = new File(finalFolder);
		file.mkdirs();
		try {
			String fileName = String.format("%s/%d.body", finalFolder, idDoc);
			FileUtils.write(new File(fileName), body, 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public static void saveSummary(int idDoc, Date firstSeen, String summary) {
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
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
	
	public void loadSummary() {
		summary = loadSummary(idDoc, firstSeen);
	}
	
	public static String loadSummary(int idDoc, Date firstSeen) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
				chunks[0]
		);
				
		try {
			String fileName = String.format("%s/%d.txt", finalFolder, idDoc);
			return FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}			
		return null;
	}
	
	
	public static WebPage load(int idDoc, Date firstSeen) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
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
			String imageUrl = jsonObject.getString("imageUrl");
			Date date = Utils.toDate(jsonObject.getString("date"));	
			String contentType = jsonObject.getString("contentType");
			
			WebPage webpage = new WebPage(idDoc, url, title, "", "", imageUrl, date, contentType, html, firstSeen);
			webpage.loadBody();
			return webpage;
		} catch (Exception e) {			
			e.printStackTrace();
		}		
		return null;
	}
	
	public void loadBody() {
		body = loadBody(idDoc, firstSeen);
	}
	
	public static String loadBody(int idDoc, Date firstSeen) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
				chunks[0]
		);
				
		try {
			String fileName = String.format("%s/%d.body", finalFolder, idDoc);
			return FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}			
		return null;
	}
	
	public void loadMeta(int idDoc, Date firstSeen) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{5})");		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				Utils.toString(firstSeen),
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
