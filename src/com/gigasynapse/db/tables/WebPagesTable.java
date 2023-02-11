package com.gigasynapse.db.tables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.gigasynapse.common.ServiceConfig;
import com.gigasynapse.common.Utils;
import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.enums.ProcessStatus;
import com.gigasynapse.db.tuples.ArticleTuple;
import com.gigasynapse.db.tuples.PipelineTuple;
import com.gigasynapse.db.tuples.WebPagesTuple;

public class WebPagesTable {
	private final static Logger LOGGER = Logger.getLogger(
			Logger.GLOBAL_LOGGER_NAME);
	
	private static WebPagesTuple ResultSet2Pipeline(ResultSet rs) {
		try {
			return new WebPagesTuple(rs.getInt("id"), rs.getInt("websiteId"),
					rs.getString("url"), rs.getBoolean("isArticle"), 
					rs.getInt("depth"), 
					ProcessStatus.values()[rs.getInt("status")]);
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}
	
	public static void clear(int websiteId) {
		String sql = "DELETE FROM WebPages WHERE websiteId = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, websiteId);
			pstmt.execute();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static WebPagesTuple get(String url) {
		String sql = "SELECT * FROM WebPages WHERE url = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setString(1, url);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				WebPagesTuple webPageTuple = ResultSet2Pipeline(rs);
				pstmt.close();
				return webPageTuple;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;		
	}
	
	public static ArrayList<String> getNewFeeds(int websiteId) {
		ArrayList<String> list = new ArrayList<String>();
		String sql =
				"SELECT wpl.websiteId , wp.url, COUNT(*) AS news\n"
				+ "FROM WebPageLinks wpl\n"
				+ "INNER JOIN WebPages wp ON\n"
				+ "   wpl.source = wp.id \n"
				+ "   AND wpl.websiteId = wp.websiteId \n"
				+ "INNER JOIN WebPages wp2 ON\n"
				+ "   wpl.destination = wp2.id \n"
				+ "   AND wpl.websiteId = wp2.websiteId \n"
				+ "WHERE\n"
				+ "   wp.isArticle  = FALSE\n"
				+ "   AND wp2.isArticle  = TRUE\n"
				+ "   AND wpl.websiteId = ?\n"
				+ "GROUP BY 1, 2\n"
				+ "HAVING COUNT(*) > 10\n"
				+ "ORDER BY 1, 3 DESC";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, websiteId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				WebPagesTuple webpagesTuple = ResultSet2Pipeline(rs);
				list.add(webpagesTuple.url);
			}
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(sql);
		}
		return list;
	}
		
	public static void set(WebPagesTuple item) {
		String sql = "";
		try {
			sql = "INSERT INTO WebPages (id, websiteId, url, isArticle, "
					+ "depth, status) VALUES( ?, ?, ?, ?, ?, ?) "
					+ "ON DUPLICATE KEY UPDATE isArticle = ?, depth = ?, "
					+ "status = ?";
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, item.id);
			pstmt.setInt(2, item.websiteId);
			pstmt.setBytes(3, item.url.getBytes(StandardCharsets.US_ASCII));
			pstmt.setBoolean(4, item.isArticle);			
			pstmt.setInt(5, item.depth);
			pstmt.setInt(6, item.status.getInt());
			pstmt.setBoolean(7, item.isArticle);			
			pstmt.setInt(8, item.depth);
			pstmt.setInt(9, item.status.getInt());
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();			
		}
	}
		
	public static WebPagesTuple next(int websiteId) {
		String sql = "SELECT * FROM WebPages WHERE websiteId = ? AND "
				+ "status = ? LIMIT 1";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, websiteId);
			pstmt.setInt(2, ProcessStatus.QUEUED.getInt());
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				WebPagesTuple webpageTuple = ResultSet2Pipeline(rs);
				pstmt.close();
				webpageTuple.status = ProcessStatus.PROCESSING;
				set(webpageTuple);
				return webpageTuple;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return null;        
	}
	
	public static ArrayList<Integer> getIds(ArrayList<String> urls) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		if (urls.size() == 0) {
			return list;
		}
		
		String sql = "select id from WebPages WHERE url in (";
		for (int i = 0; i < urls.size(); i++) {
			if (i + 1 == urls.size()) {
				sql += "?)";
			} else {
				sql += "?, ";
			}
		}
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			int count = 1;
			for (int i = 0; i < urls.size(); i++) {
				pstmt.setBytes(count++, urls.get(i)
						.getBytes(StandardCharsets.US_ASCII));				
			}
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(rs.getInt("id"));
            }
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static void bulkSet_(ArrayList<WebPagesTuple> list) {
		if (list.size() == 0) {
			return;
		}
		String sql = "REPLACE INTO WebPages (id, websiteId, url, isArticle, "
				+ "depth, status) VALUES ";
		for(int i = 0; i < list.size(); i++) {
			if ((i + 1) == list.size()) {
				sql += "( ?, ?, ?, ?, ?, ?)";
			} else {
				sql += "( ?, ?, ?, ?, ?, ?),";
			}
		}
		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			int count = 1;
			for (int i = 0; i < list.size(); i++) {
				WebPagesTuple item = list.get(i);
				pstmt.setInt(count++, item.id);
				pstmt.setInt(count++, item.websiteId);
				pstmt.setString(count++, item.url);
				pstmt.setBoolean(count++, item.isArticle);				
				pstmt.setInt(count++, item.depth);				
				pstmt.setInt(count++, item.status.getInt());				
			}
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(String.valueOf(sql.length()));
			LOGGER.severe(sql);			
		}
	}

	public static void bulkSet(ArrayList<WebPagesTuple> list) {
		if (list.size() == 0) {
			return;
		}
		if (list.size() > 20) {
			List<ArrayList<WebPagesTuple>> parts = Utils.split(list, 20);
			parts.forEach(item -> {
				bulkSet(item);
			});
			return;
		}

		String sql = "INSERT INTO WebPages (id, websiteId, url, isArticle, "
				+ "depth, status) VALUES (?, ?, ?, ?, ?, ?) "
				+ "ON DUPLICATE KEY UPDATE isArticle = ?, depth = ?, "
				+ "status = ?";
		
		try {			
			WebCrawlerDB.getInstance()
					.getConnection().setAutoCommit(false);
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);				
			
			list.forEach(item -> {
				try {
					pstmt.setInt(1, item.id);
					pstmt.setInt(2, item.websiteId);
					pstmt.setString(3, item.url);
					pstmt.setBoolean(4, item.isArticle);				
					pstmt.setInt(5, item.depth);				
					pstmt.setInt(6, item.status.getInt());				
					pstmt.setBoolean(7, item.isArticle);				
					pstmt.setInt(8, item.depth);				
					pstmt.setInt(9, item.status.getInt());
					pstmt.addBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
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
	
	
	public static ArrayList<String> bulkGet(int websiteId, ArrayList<String> list) {
		ArrayList<String> existing = new ArrayList<String>(); 
		String sql = "";
		
		if (list.size() > 20) {
			ArrayList<ArrayList<String>> blocks = Utils.split(list, 20);
			blocks.forEach(subList -> {
				ArrayList<String> answer = bulkGet(websiteId, subList);
				existing.addAll(answer);
			});
			return existing;
		}
		
		
		if (list.size() > 0) {
			sql = "SELECT DISTINCT url FROM WebPages WHERE websiteId = ? AND url IN ("; 
			for(int i = 0; i < list.size(); i++) {
				if ((i + 1) == list.size()) {
					sql += "?)";
				} else {
					sql += "?, ";
				}
			}

			try {
				PreparedStatement pstmt = WebCrawlerDB.getInstance()
						.getConnection().prepareStatement(sql);
				pstmt.setInt(1, websiteId);

				int count = 2;
				for (int i = 0; i < list.size(); i++) {
					pstmt.setBytes(count++, list.get(i)
							.getBytes(StandardCharsets.US_ASCII));
				}
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					existing.add(rs.getString(1));
				}
				pstmt.close();
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.severe(sql);
			}
		}
		return existing; 
	}
}
