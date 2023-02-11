package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.gigasynapse.common.Utils;
import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.PhraseTuple;
import com.gigasynapse.db.tuples.WebPagesTuple;

public class PhrasesTable {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public static PhraseTuple getTuple(ResultSet rs) {
		try {
			return new PhraseTuple(rs.getInt("websiteId"), rs.getDate("date"),  
					rs.getString("md5"), rs.getInt("count"));
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return null;	
	}
	
	public static void set(PhraseTuple item) {
		String sql = "REPLACE INTO Phrases (websiteId, date, id, counter) "
				+ "VALUES (?, ?, ?, ?)";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, item.websiteId);
			pstmt.setDate(2, new java.sql.Date(item.date.getTime()));
			pstmt.setString(3, item.md5);
			pstmt.setInt(4, item.counter);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static PhraseTuple get(int websiteId, Date date, String md5) {
		PhraseTuple item = null;
		String sql = "SELECT * from Phrases WHERE websiteId = ? and date = ? "
				+ "and md5 = ?";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, websiteId);
			pstmt.setDate(2, new java.sql.Date(date.getTime()));
			pstmt.setString(3, md5);
			ResultSet rs = pstmt.executeQuery();			
			if (rs.next()) {
				item = getTuple(rs);				
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return item;
	}		
	
	public static HashMap<String, Integer> get(int websiteId, Date date) {
		HashMap<String, Integer> md5Map = new HashMap<String, Integer>();
		String sql = "SELECT * from Phrases WHERE websiteId = ? and date = ?";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, websiteId);
			pstmt.setDate(2, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();			
			while (rs.next()) {
				PhraseTuple item = getTuple(rs);
				md5Map.put(item.md5, item.counter);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return md5Map;
	}		
	
	
	
	public static void put_(int websiteId, Date date, 
			HashMap<String, Integer> map) {
		if (map.size() == 0) {
			return;
		}
		
		if (map.size() > 20) {
			List<HashMap<String, Integer>> parts = Utils.splitMap(map, 20);
			parts.forEach(item -> {
				put_(websiteId, date, item);
			});
			return;
		}
		
		String sql = "REPLACE INTO Phrases (websiteId, date, md5, count) "
				+ "VALUES ";
		
		Iterator<String> i = map.keySet().iterator();
		while(i.hasNext()) {
			String md5 = i.next();
			sql += "( ?, ?, ?, ?),";
		}
		sql = sql.substring(0, sql.length() - 1);
		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			int count = 1;
			i = map.keySet().iterator();
			while(i.hasNext()) {
				String md5 = i.next();
				int counter = map.get(md5);
				pstmt.setInt(count++, websiteId);
				pstmt.setDate(count++, new java.sql.Date(date.getTime()));
				pstmt.setString(count++, md5);
				pstmt.setInt(count++, counter);				
			}
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(sql);			
		}
	}

	public static void put(int websiteId, Date date, 
			HashMap<String, Integer> map) {
		if (map.size() == 0) {
			return;
		}
		
		if (map.size() > 20) {
			List<HashMap<String, Integer>> parts = Utils.splitMap(map, 20);
			parts.forEach(item -> {
				put(websiteId, date, item);
			});
			return;
		}
		
		String sql = "INSERT INTO Phrases (websiteId, date, md5, count) "
				+ "VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE count = ?";
		try {			
			WebCrawlerDB.getInstance()
					.getConnection().setAutoCommit(false);
			Iterator<String> i = map.keySet().iterator();
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);				
			while(i.hasNext()) {
				String md5 = i.next();
				int counter = map.get(md5);
				pstmt.setInt(1, websiteId);
				pstmt.setDate(2, new java.sql.Date(date.getTime()));				
				pstmt.setString(3, md5);
				pstmt.setInt(4, counter);
				pstmt.setInt(5, counter);
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			pstmt.close();
			WebCrawlerDB.getInstance()
					.getConnection().setAutoCommit(true);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(sql);			
			try {
				WebCrawlerDB.getInstance().getConnection().rollback();
				WebCrawlerDB.getInstance()
					.getConnection().setAutoCommit(true);				
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
}
