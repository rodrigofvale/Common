package com.gigasynapse.crawler;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import com.gigasynapse.common.ServiceConfig;
import com.gigasynapse.common.Utils;
import com.gigasynapse.db.enums.ArticleStage;
import com.gigasynapse.db.tables.ArticlesTable;
import com.gigasynapse.db.tuples.ArticleTuple;


public class WebPage {
	private final static Logger LOGGER = Logger.getLogger("GLOBAL");
	
	public int idDoc;
	public String url;
	public String title;
	public String body;
	public String txt;
	public String imageUrl;
	public Date pubDate;
	public String contentType;
	public String html;
	
	public WebPage(int idDoc, String url, String title, String body, 
			String summary, String imageUrl, Date pubDate, String contentType, 
			String html) {
		this.idDoc = idDoc;
		this.url = url;
		this.title = title;
		this.body = body;
		this.imageUrl = imageUrl;
		this.txt = summary;
		this.pubDate = pubDate;
		this.contentType = contentType;
		this.html = html;
	}
	
	public WebPage(int idDoc) {
		this.idDoc = idDoc;
		loadBody();
		loadMeta();
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
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
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
		jsonObject.put("pubDate", Utils.toString(pubDate));		
		jsonObject.put("contentType", contentType);
		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
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
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
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
	
	public static void saveText(int idDoc, String summary) {
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
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
	
	public void loadText() {
		txt = loadText(idDoc);
	}
	
	public static String loadText(int idDoc) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
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
	
	public void loadHtml() {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
		);
				
		try {
			String fileName = String.format("%s/%d.html", finalFolder, idDoc);
			html = FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}			
	}
	
	
	public static String loadBody(int idDoc) {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
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
	
	public void loadBody() {
		body = loadBody(idDoc);
	}
	
	public void loadMeta() {		
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
		);
				
		String fileName = String.format("%s/%d.json", finalFolder, idDoc);
		try {
			String jsonTxt = FileUtils.readFileToString(new File(fileName), 
					StandardCharsets.UTF_8);
			boolean toSave = false;			
			JSONObject jsonObject = new JSONObject(jsonTxt);
			idDoc = jsonObject.getInt("idDoc");
			title = jsonObject.getString("title");
			imageUrl = jsonObject.getString("imageUrl");
			pubDate = Utils.toDate(jsonObject.getString("pubDate"));	
			contentType = "text/html";
			if (jsonObject.has("contentType")) {
				contentType = jsonObject.getString("contentType");
			} else {
				toSave = true;
			}
			if (jsonObject.has("url")) {
				url = jsonObject.getString("url");
			} else {
				ArticleTuple articleTuple = ArticlesTable.get(idDoc);
				url = articleTuple.url;
				toSave = true;
			}
			
			if (toSave) {
				saveMeta();
			}
		} catch (Exception e) {
			LOGGER.severe(String.format("Failed to load meta information from "
					+ "idDoc %s %s", idDoc, fileName));
			ArticlesTable.setStage(idDoc, ArticleStage.TOBEDELETED);
		}				
	}
}
