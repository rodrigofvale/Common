package com.gigasynapse.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.gigasynapse.common.ServiceConfig;

public class WebCrawlerDB {
	private static WebCrawlerDB globalsInstance = new WebCrawlerDB();
	private Connection conn = null;
	
	public WebCrawlerDB() {
		connect();
	}

	public static WebCrawlerDB getInstance() {
        return globalsInstance;
    }	
	
	
	public Connection getConnection() {
		try {
			if (conn == null) {
				connect();
			}
			
			if (conn.isClosed()) {
				connect();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	
	public void connect() {
		try {
			String url = String.format("jdbc:%s:%d/%s?user=%s&password=%s&allowPublicKeyRetrieval=true",
				ServiceConfig.getInstance().obj.getJSONObject("WebCrawlerDB").getString("host"),
				ServiceConfig.getInstance().obj.getJSONObject("WebCrawlerDB").getInt("port"),
				ServiceConfig.getInstance().obj.getJSONObject("WebCrawlerDB").getString("database"),
				ServiceConfig.getInstance().obj.getJSONObject("WebCrawlerDB").getString("user"),
				ServiceConfig.getInstance().obj.getJSONObject("WebCrawlerDB").getString("password"));

			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
}
