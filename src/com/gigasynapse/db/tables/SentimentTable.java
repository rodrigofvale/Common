package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.logging.Logger;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.DatesTuple;
import com.gigasynapse.db.tuples.VerbTuple;

public class SentimentTable {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public static HashSet<String> getGoodSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(GoodSentiment) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	public static HashSet<String> getBadSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(BadSentiment) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	public static HashSet<String> getIncreaseSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(Increase) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	public static HashSet<String> getAlwaysNegativeSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(AlwaysNegative) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	public static HashSet<String> getAlwaysPositiveSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(AlwaysPositive) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	public static HashSet<String> getInverterSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(Inverter) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	public static HashSet<String> getGlobalInverterSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(GlobalInverter) from Sentiment";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String kwd = rs.getString(1);
				if ((kwd != null) && (kwd.trim().length() > 0)) {
					list.add(kwd);				
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}	
}
