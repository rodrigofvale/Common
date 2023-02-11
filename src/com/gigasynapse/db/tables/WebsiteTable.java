package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.WebsiteTuple;

public class WebsiteTable {
	HashMap<Integer, WebsiteTuple> hash = new HashMap<Integer, WebsiteTuple>(); 
	
	private static WebsiteTuple ResultSet2WebSite(ResultSet rs) {
		try {
			return new WebsiteTuple(rs.getInt("id"), rs.getString("name"), 
					rs.getString("url"), rs.getString("domains"), 
					rs.getString("regularexp"), 
					rs.getString("articlesIdRegExp"), rs.getInt("depth"), 
					rs.getInt("region"), rs.getString("rejectRegExp"), 
					rs.getString("feed"), rs.getBoolean("active"), 
					rs.getLong("lastVisit"), rs.getBoolean("toBeReviewed"),
					new Date(rs.getDate("created").getTime()));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}
	
	public static ArrayList<WebsiteTuple> list() {
		ArrayList<WebsiteTuple> websites = new ArrayList<WebsiteTuple>();
		String sql = "SELECT * FROM Websites WHERE active = true";
		Statement stmt;
		try {
			stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				WebsiteTuple website = ResultSet2WebSite(rs);
				websites.add(website);
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return websites;
	}
	
	public static WebsiteTuple get(int id) {
		String sql = "SELECT * FROM Websites WHERE id = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, id);			
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				WebsiteTuple website = ResultSet2WebSite(rs);
				rs.close();
				pstmt.close();
				return website;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static WebsiteTuple get(String url) {
		String sql = "SELECT * FROM Websites WHERE url = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setString(1, url);			
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				WebsiteTuple website = ResultSet2WebSite(rs);
				rs.close();
				pstmt.close();
				return website;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void set(WebsiteTuple website) {
		PreparedStatement pstmt;
		try {		
			String sql = 
					"REPLACE INTO Websites (id, name, url, domains, "
					+ "regularexp, articlesIdRegExp, depth, region, "
					+ "rejectRegExp, feed, active, lastVisit, toBeReviewed) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			pstmt = WebCrawlerDB.getInstance().getConnection()
					.prepareStatement(sql);
			pstmt.setInt(1, website.id);
			pstmt.setString(2, website.name);
			pstmt.setString(3, website.url);
			pstmt.setString(4, website.domains);
			pstmt.setString(5, website.regexp);
			pstmt.setString(6, website.articlesIdRegExp);
			pstmt.setInt(7, website.depth);
			pstmt.setInt(8, website.region);
			pstmt.setString(9, website.rejectRegExp);
			pstmt.setString(10, website.feed);
			pstmt.setBoolean(11, website.active);
			pstmt.setLong(12, website.lastVisit);
			pstmt.setBoolean(13, website.toBeReviewed);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}		
}
