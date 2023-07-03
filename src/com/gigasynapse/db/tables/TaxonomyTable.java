package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Logger;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.TaxonomyTuple;

public class TaxonomyTable {
	private final static Logger LOGGER = Logger.getLogger("GLOBAL");
	
	private static TaxonomyTuple fromResultSet(ResultSet rs) {
		try {
			return new TaxonomyTuple(
					rs.getInt("id"), 				
					rs.getInt("parentId"),
					rs.getString("label"), 
					rs.getString("synonyms"),
					rs.getInt("crossWith"),
					rs.getInt("sentiment")
			);
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}
	
	public static HashSet<String> getGoodSentiment() {
		HashSet<String> list = new HashSet<String>();
		String sql = "SELECT LOWER(synonyms) from Taxonomy WHERE LOWER(TRIM(Sentiment)) = 'p'";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String synonyms = rs.getString(1);
				if ((synonyms != null) && (synonyms.trim().length() > 0)) {
					String kwds[] = synonyms.split(",");
					for(String kwd: kwds) {
						list.add(kwd);				
					}
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
		String sql = "SELECT LOWER(synonyms) from Taxonomy WHERE LOWER(TRIM(Sentiment)) = 'n'";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				String synonyms = rs.getString(1);
				if ((synonyms != null) && (synonyms.trim().length() > 0)) {
					String kwds[] = synonyms.split(",");
					for(String kwd: kwds) {
						list.add(kwd);				
					}
				}
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;		
	}
	
	
	public static ArrayList<TaxonomyTuple>list() {
		ArrayList<TaxonomyTuple> list = new ArrayList<TaxonomyTuple>();
		String sql = "SELECT * FROM Taxonomy";
		Statement stmt;
		try {
			stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				TaxonomyTuple item = fromResultSet(rs);
				list.add(item);
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return list;
	}
		
	public static ArrayList<TaxonomyTuple> getChild(int parentId) {
		ArrayList<TaxonomyTuple> list = new ArrayList<TaxonomyTuple>();
		String sql = "SELECT * FROM Taxonomy WHERE parentId = ?";
		Statement stmt;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, parentId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				TaxonomyTuple item = fromResultSet(rs);
				list.add(item);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return list;
	}
	
	public static TaxonomyTuple get(int id) {
		String sql = "SELECT * FROM Taxonomy WHERE id = ?";
		TaxonomyTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, id);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				articleTuple = fromResultSet(rs);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}
	
	public static TaxonomyTuple get(String label) {
		String sql = "SELECT * FROM Taxonomy WHERE label = ?  collate utf8mb4_bin";
		TaxonomyTuple articleTuple = null;
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setString(1, label);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				articleTuple = fromResultSet(rs);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}
	
	public static TaxonomyTuple get(int parentId, String label) {
		String sql = "SELECT * FROM Taxonomy WHERE parentId = ? AND label = ?";
		TaxonomyTuple articleTuple = null;
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, parentId);
			pstmt.setString(2, label);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				articleTuple = fromResultSet(rs);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}
	
	public static TaxonomyTuple del(int id) {
		String sql = "DELETE FROM Taxonomy WHERE id = ?";
		TaxonomyTuple articleTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return articleTuple;
	}
	
	public static void set(TaxonomyTuple item) {
		int count = 1;
		PreparedStatement pstmt;
		try {		
			String sql = "REPLACE INTO Taxonomy (id, parentId, label, synonyms, crossWith, sentiment) VALUES (?, ?, ?, ?, ?, ?)";
			pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, item.id);
			pstmt.setInt(2, item.parentId);
			pstmt.setString(3, item.label);
			pstmt.setString(4, item.synonyms);
			pstmt.setInt(5, item.crossWith);
			pstmt.setInt(6, item.sentiment);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();			
		}
	}
	
	public static void create(TaxonomyTuple item) {
		int count = 1;
		PreparedStatement pstmt;
		try {		
			String sql = "INSERT INTO Taxonomy (parentId, label, synonyms, crossWith, sentiment) VALUES (?, ?, ?, ?, ?)";
			pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, item.parentId);
			pstmt.setString(2, item.label);
			pstmt.setString(3, item.synonyms);
			pstmt.setInt(4, item.crossWith);
			pstmt.setInt(5, item.sentiment);
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			if (rs.next()) {
				item.id = rs.getInt(1);
			}		
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();			
		}
	}	
}
