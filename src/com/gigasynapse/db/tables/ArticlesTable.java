package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;

import com.gigasynapse.common.Utils;
import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.enums.ArticleStage;
import com.gigasynapse.db.tuples.ArticleTuple;

public class ArticlesTable {
	private final static Logger LOGGER = Logger.getLogger("GLOBAL");

	public static ArrayList<Date> getAllDates() {
		ArrayList<Date> dates = new ArrayList<Date>();
		try {
			String sql = "SELECT DISTINCT firstSeen FROM Articles ORDER BY firstSeen ASC";
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Date date = new Date(rs.getDate(1).getTime());
				dates.add(date);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return dates;
	}

	private static ArticleTuple ResultSet2Article(ResultSet rs) {
		try {
			return new ArticleTuple(
					rs.getInt("docId"), 
					rs.getInt("websiteId"), 
					rs.getString("url"), 
					new Date(rs.getDate("firstSeen").getTime()), 
					new Date(rs.getDate("lastSeen").getTime()),
					rs.getString("mimetype"),
					ArticleStage.values()[rs.getInt("stage")],
					rs.getString("md5"));
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}

	public static ArrayList<ArticleTuple> list() {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles";
		Statement stmt;
		try {
			stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				ArticleTuple item = ResultSet2Article(rs);
				list.add(item);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArrayList<Date> getDates(ArticleStage stage) {
		ArrayList<Date> list = new ArrayList<Date>();
		String sql = "SELECT DISTINCT firstSeen FROM Articles WHERE stage = ? "
				+ "ORDER BY firstSeen ASC";
		Statement stmt;
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Date date = new Date(rs.getDate(1).getTime());
				list.add(date);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArrayList<Integer> getWebsiteIds(Date date, 
			ArticleStage stage) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		String sql = "SELECT DISTINCT websiteId FROM Articles WHERE stage = ? "
				+ " AND firstSeen = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			pstmt.setDate(2, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(rs.getInt(1));
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArrayList<Date> getDates(int year, int month, 
			ArticleStage stage) {
		ArrayList<Date> list = new ArrayList<Date>();
		String sql = "SELECT DISTINCT firstSeen FROM Articles WHERE stage = ? "
				+ " AND MONTH(firstSeen) = ? AND YEAR(firstSeen) = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			pstmt.setInt(2, month);
			pstmt.setInt(3, year);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Date date = new Date(rs.getDate(1).getTime());
				list.add(date);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArrayList<Date> getDates(Date date, ArticleStage stage) {
		ArrayList<Date> list = new ArrayList<Date>();
		String sql = "SELECT DISTINCT firstSeen FROM Articles WHERE stage = ? "
				+ " AND WEEK(firstSeen) = WEEK(?)";
		Statement stmt;
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			pstmt.setDate(2, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Date d = new Date(rs.getDate(1).getTime());
				list.add(d);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArrayList<ArticleTuple> listAfter(Date date) {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE `date` > ?";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArticleTuple item = ResultSet2Article(rs);
				list.add(item);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArrayList<ArticleTuple> listDistinct(ArticleStage stage) {		
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "select distinct firstSeen, websiteId from Articles "
				+ "where stage = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArticleTuple item = new ArticleTuple(
						0, 
						rs.getInt("websiteId"), 
						null, 
						new Date(rs.getDate("firstSeen").getTime()), 
						null,
						null,
						stage,
						null);								
				list.add(item);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArrayList<ArticleTuple> list(Date date) {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE `firstSeen` = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArticleTuple item = ResultSet2Article(rs);
				list.add(item);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArrayList<ArticleTuple> list(Date date, int websiteId, 
			ArticleStage stage) {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE firstSeen = ? AND "
				+ "websiteId = ? AND stage = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			pstmt.setInt(2, websiteId);
			pstmt.setInt(3, stage.getInt());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArticleTuple item = ResultSet2Article(rs);
				list.add(item);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArrayList<ArticleTuple> list(int webSiteId) {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE websiteId = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, webSiteId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArticleTuple item = ResultSet2Article(rs);
				list.add(item);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static ArticleTuple ping(String url) {
		String sql = "UPDATE Articles SET lastSeen = now() WHERE url = ?";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setString(1, url);
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}
	
	public static ArticleTuple setToBeDeleted(String url, boolean toBeDeleted) {
		String sql = "UPDATE Articles SET toBeDeleted = ? WHERE url = ?";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setBoolean(1, toBeDeleted);
			pstmt.setString(2, url);
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}
	
	public static ArticleTuple get(String url) {
		String sql = "SELECT * FROM Articles WHERE url = ?";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setString(1, url);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				articleTuple = ResultSet2Article(rs);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}

	public static boolean hasMD5(String url, String md5) {
		boolean answer = false;
		String sql = "SELECT md5 FROM Articles WHERE url = ? and md5 = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setString(1, url);
			pstmt.setString(2, md5);
			ResultSet rs = pstmt.executeQuery();
			answer = rs.next();
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return answer;
	}
	
	public static boolean hasMD5(String md5) {
		boolean answer = false;
		String sql = "SELECT md5 FROM Articles WHERE md5 = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setString(1, md5);
			ResultSet rs = pstmt.executeQuery();
			answer = rs.next();
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return answer;
	}
	
	public static ArrayList<ArticleTuple> get(Date date) {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE firstSeen = ? AND toBeDeleted = false";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				articleTuple = ResultSet2Article(rs);
				list.add(articleTuple);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;
	}

	public static ArrayList<ArticleTuple> getBetween(Date startDate, Date endDate) {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE date >= ? and date < ?";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(startDate.getTime()));
			pstmt.setDate(2, new java.sql.Date(endDate.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				articleTuple = ResultSet2Article(rs);
				list.add(articleTuple);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;
	}

	public static ArrayList<ArticleTuple> getNoMd5() {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Articles WHERE md5 is null and stage = 0 limit 1000";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				articleTuple = ResultSet2Article(rs);
				list.add(articleTuple);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static ArticleTuple get(int docId) {
		String sql = "SELECT * FROM Articles WHERE docId = ?";
		ArticleTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, docId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				articleTuple = ResultSet2Article(rs);
			}
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return articleTuple;
	}

	public static void set(ArticleTuple item) {
		String sql = "";
		int count = 1;
		PreparedStatement pstmt;
		try {		
			sql = "REPLACE INTO Articles (docId, websiteId, url, firstSeen, "
					+ "lastSeen, mimetype, stage, md5) VALUES( "
					+ "?, ?, ?, ?, ?, ?, ?, ?)";
			pstmt = WebCrawlerDB.getInstance().getConnection()
					.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, item.docId);
			pstmt.setInt(2, item.websiteId);
			pstmt.setString(3, item.url);
			pstmt.setDate(4, new java.sql.Date(item.firstSeen.getTime()));
			pstmt.setDate(5, new java.sql.Date(item.firstSeen.getTime()));
			pstmt.setString(6, item.mimeType);
			pstmt.setInt(7, item.stage.getInt());
			pstmt.setString(8, item.md5);
			pstmt.executeUpdate();		
			ResultSet rs = pstmt.getGeneratedKeys();
			rs.next();
			item.docId = rs.getInt(1);
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();			
		}
	}
	
	public static void setStage(int idDoc, ArticleStage stage) {
		String sql = "UPDATE Articles set stage = ? WHERE docId = ?";
		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			pstmt.setInt(2, idDoc);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(sql);			
		}		
	}
	
	public static void setStage(ArrayList<Integer> list, ArticleStage stage) {
		if (list.size() == 0) {
			return;
		}
		
		if (list.size() > 20) {
			ArrayList<ArrayList<Integer>> parts = Utils.split(list, 20);
			parts.forEach(item -> {
				setStage(item, stage);
			});	
			return;
		}
		
		String sql = "UPDATE Articles set stage = ? WHERE docId in (";
		
		for(int i = 0; i < list.size(); i++) {
			sql += " ?,";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")";
		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, stage.getInt());
			for(int i = 0; i < list.size(); i++) {
				pstmt.setInt(i + 2, list.get(i));
			}
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(sql);			
		}		
	}
}
