package com.gigasynapse.crawler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
	public String body;
	public String txt;
	public String html;
	

	public WebPageMeta webPageMeta;
	
	public WebPage(int idDoc, String url, String title, String body, 
			String summary, String imageUrl, Date pubDate, String contentType, 
			String html) {
		this.idDoc = idDoc;
		this.body = body;
		this.txt = summary;
		this.html = html;
		
		webPageMeta = new WebPageMeta(url, title, imageUrl, pubDate, 
				contentType);
	}
	
	public WebPage(int idDoc) {
		this.idDoc = idDoc;
		loadBody();
		loadMeta();
	}
	
	public WebPage() {
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
		String fileName = String.format("%s/%d.html.compressed", finalFolder, idDoc);
		try {
			file = new File(fileName);
			byte dataUncompressed[] = html.getBytes(StandardCharsets.UTF_8);
			byte dataCompressed[] = Utils.compress(dataUncompressed);
			FileUtils.writeByteArrayToFile(new File(fileName), dataCompressed);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public void saveMeta() {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("idDoc", idDoc);
		jsonObject.put("url", webPageMeta.url);
		jsonObject.put("title", webPageMeta.title);
		jsonObject.put("imageUrl", webPageMeta.imageUrl);
		jsonObject.put("pubDate", Utils.toString(webPageMeta.pubDate));		
		jsonObject.put("contentType", webPageMeta.contentType);
		
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
		if (body == null) {
			return;
		}
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
			String fileName = String.format("%s/%d.body.compressed", finalFolder, idDoc);
			byte dataUncompressed[] = body.getBytes(StandardCharsets.UTF_8);
			byte dataCompressed[] = Utils.compress(dataUncompressed);
			FileUtils.writeByteArrayToFile(new File(fileName), dataCompressed);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
	
	public static void delete(int idDoc) {
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
		);
		
		String fileName = String.format("%d*", idDoc);
		
	    DirectoryStream<Path> newDirectoryStream;
		try {
			newDirectoryStream = Files.newDirectoryStream(Paths.get(finalFolder), fileName);
	        for (final Path newDirectoryStreamItem : newDirectoryStream) {
	        	try {
	        		Files.delete(newDirectoryStreamItem);
	        		LOGGER.info("Deleted : " + newDirectoryStreamItem.toFile().getAbsolutePath());
	        	} catch (IOException e) {
	        		LOGGER.info("File don´t exist : " + newDirectoryStreamItem.toFile().getAbsolutePath());	        		
	        	}
	        }
		} catch (IOException e1) {
			LOGGER.info("Folder don´t exist : " + finalFolder);	        		
		}
		ArticlesTable.setStage(idDoc, ArticleStage.DELETED);
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
		String fileName = String.format("%s/%d.txt.compressed", finalFolder, idDoc);
		try {
			byte dataUncompressed[] = summary.getBytes(StandardCharsets.UTF_8);
			byte dataCompressed[] = Utils.compress(dataUncompressed);
			FileUtils.writeByteArrayToFile(new File(fileName), dataCompressed);
		} catch (Exception e) {
			LOGGER.fine(String.format("Failed to save file %s", fileName));
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
				
		String fileName = String.format("%s/%d.txt.compressed", finalFolder, idDoc);
		try {
			byte dataCompressed[] = FileUtils.readFileToByteArray(new File(fileName));
			byte dataDecompressed[] = Utils.decompress(dataCompressed);
			return new String(dataDecompressed, StandardCharsets.UTF_8);
		} catch (Exception e) {
			LOGGER.fine("File do not exists:" + fileName);
		}			
		return null;
	}
	
	public static WebPageMeta loadMeta(int idDoc) {		
		WebPageMeta webPageMeta = null;
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
			JSONObject jsonObject = new JSONObject(jsonTxt);
			String title = jsonObject.getString("title");
			String imageUrl = jsonObject.getString("imageUrl");
			Date pubDate = Utils.toDate(jsonObject.getString("pubDate"));	
			String contentType = jsonObject.getString("contentType");
			String url = jsonObject.getString("url");
			
			webPageMeta = new WebPageMeta(url, title, imageUrl, pubDate, contentType);
		} catch (Exception e) {
			LOGGER.fine(String.format("Failed to load meta information from "
					+ "idDoc %s %s", idDoc, fileName));
			ArticlesTable.setStage(idDoc, ArticleStage.TOBEDELETED);
		}
		return webPageMeta;
	}
	
	public void loadHtml() {		
		this.html = WebPage.loadHtml(idDoc);
	}
	
	public static String loadHtml(int idDoc) {		
		String html = null;
		String folder = ServiceConfig.getInstance().obj
				.getJSONObject("crawler").getString("htmlFolder");
		
		String idDocAsString = String.format("%010d", idDoc);
		String[] chunks = idDocAsString.split("(?<=\\G.{3})");
		
		String finalFolder = String.format("%s/%s/%s/",
				folder,
				chunks[0],
				chunks[1]
		);
				
		String fileName = String.format("%s/%d.html.compressed", finalFolder, idDoc);			
		try {
			byte dataCompressed[] = FileUtils.readFileToByteArray(new File(fileName));
			byte dataDecompressed[] = Utils.decompress(dataCompressed);
			html = new String(dataDecompressed, StandardCharsets.UTF_8);
		} catch (Exception e) {
			LOGGER.fine(String.format("Failed to load file %s", fileName));
		}
		return html;
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
						
		String fileName = String.format("%s/%d.body.compressed", finalFolder, idDoc);
		try {
			byte dataCompressed[] = FileUtils.readFileToByteArray(new File(fileName));
			byte dataDecompressed[] = Utils.decompress(dataCompressed);
			return new String(dataDecompressed, StandardCharsets.UTF_8);
		} catch (Exception e) {
			LOGGER.fine(String.format("Failed to load file %s", fileName));
		}
		return null;
	}
	
	public void loadBody() {
		body = loadBody(idDoc);
	}
	
	public void loadMeta() {	
		webPageMeta = loadMeta(idDoc); 
	}
}
