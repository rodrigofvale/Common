package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.ArticlesRegExpTuple;


public class ArticlesRegExpTable {
	private final static Logger LOGGER = Logger.getLogger(
			Logger.GLOBAL_LOGGER_NAME);
	
	private static ArticlesRegExpTuple ResultSet2Pipeline(ResultSet rs) {
		try {
			return new ArticlesRegExpTuple(rs.getString("regexp"), rs.getInt("type"));
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}
		
	public static ArrayList<String> list(int type) {
		ArrayList<String> list = 
				new ArrayList<String>(); 
		String sql = "SELECT * FROM ArticlesRegExp WHERE type = ?";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance()
					.getConnection().prepareStatement(sql);
			pstmt.setInt(1, type);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				ArticlesRegExpTuple item = ResultSet2Pipeline(rs);
				list.add(item.regexp);
            }
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;		
	}
}
