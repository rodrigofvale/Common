package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.TaxonomyMinerTuple;
import com.gigasynapse.db.tuples.WebsiteTuple;

public class TaxonomyMinerTable {
	
	private static TaxonomyMinerTuple ResultSet2WebSite(ResultSet rs) {
		try {
			return new TaxonomyMinerTuple(rs.getInt("taxonomyId"), 
					rs.getString("term"), 
					rs.getFloat("ratio"), rs.getFloat("globalRatio"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}
	
	public static ArrayList<TaxonomyMinerTuple> list() {
		ArrayList<TaxonomyMinerTuple> items = new ArrayList<TaxonomyMinerTuple>();
		String sql = "SELECT * FROM TaxonomyMiner";
		Statement stmt;
		try {
			stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				TaxonomyMinerTuple item = ResultSet2WebSite(rs);
				items.add(item);
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return items;
	}
	
	public static TaxonomyMinerTuple get(int taxonomyId, String term) {
		String sql = "SELECT * FROM TaxonomyMiner WHERE taxonomyId = ? "
				+ "and term = ?";
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, taxonomyId);
			pstmt.setString(2, term);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				TaxonomyMinerTuple item = ResultSet2WebSite(rs);
				rs.close();
				pstmt.close();
				return item;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void set(TaxonomyMinerTuple item) {
		PreparedStatement pstmt;
		try {		
			String sql = 
					"REPLACE INTO TaxonomyMiner (taxonomyId, term, ratio, "
					+ "globalRatio) VALUES(?, ?, ?, ?)";
			pstmt = WebCrawlerDB.getInstance().getConnection()
					.prepareStatement(sql);
			pstmt.setInt(1, item.taxonomyId);
			pstmt.setString(2, item.term);
			pstmt.setFloat(3, item.ratio);
			pstmt.setFloat(4, item.globalRatio);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}		
}
