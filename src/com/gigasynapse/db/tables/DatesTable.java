package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.DatesTuple;

public class DatesTable {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public static DatesTuple getTuple(ResultSet rs) {
		try {
			return new DatesTuple(
					new Date(rs.getDate("date").getTime()),
					new Date(rs.getDate("lastModified").getTime()),
					new Date(rs.getDate("lastProcessed").getTime())
			);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return null;	
	}
	
	public static void set(DatesTuple item) {
		String sql = "REPLACE INTO Dates (date, lastModified, lastProcessed) VALUES (?, ?, ?)";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(item.date.getTime()));
			pstmt.setTimestamp(2, new java.sql.Timestamp(item.lastModified.getTime()));
			pstmt.setTimestamp(3, new java.sql.Timestamp(item.lastProcessed.getTime()));
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	public static DatesTuple getPrevious(Date date) {
		String sql = "WITH aux AS (\n"
				+ "	SELECT \n"
				+ "		date as NextDate, \n"
				+ "		LAG(date, 1) OVER (\n"
				+ "			ORDER BY date\n"
				+ "		) date,\n"
				+ "		LAG(lastModified, 1) OVER (\n"
				+ "			ORDER BY date\n"
				+ "		) lastModified,\n"
				+ "		LAG(lastProcessed, 1) OVER (\n"
				+ "			ORDER BY date\n"
				+ "		) lastProcessed\n"
				+ "	FROM Dates\n"
				+ ")\n"
				+ "SELECT * from aux where NextDate = ?";
		DatesTuple datesTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				datesTuple = getTuple(rs);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return datesTuple;
	}
	
	public static DatesTuple get(Date date) {
		String sql = "SELECT * from Dates WHERE date = ?";
		DatesTuple datesTuple = null;
		try {
			PreparedStatement pstmt  = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				datesTuple = getTuple(rs);
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return datesTuple;
	}
	
	public static ArrayList<DatesTuple> getModified() {
		ArrayList<DatesTuple> list = new ArrayList<DatesTuple>();
		String sql = "SELECT * from Dates WHERE lastModified > lastProcessed ORDER BY date ASC";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				list.add(getTuple(rs));				
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;
	}	
	
	public static ArrayList<DatesTuple> list() {
		ArrayList<DatesTuple> list = new ArrayList<DatesTuple>();
		String sql = "SELECT * from Dates ORDER BY date ASC";
		try {
			Statement stmt = WebCrawlerDB.getInstance().getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);			
			while (rs.next()) {
				list.add(getTuple(rs));				
            }
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;
	}	
	
	public static ArrayList<DatesTuple> listUntil(Date date) {
		ArrayList<DatesTuple> list = new ArrayList<DatesTuple>();
		String sql = "SELECT * from Dates where Date <= ? ORDER BY date ASC";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(getTuple(rs));				
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;
	}	
	
	public static ArrayList<DatesTuple> listAfter(Date date) {
		ArrayList<DatesTuple> list = new ArrayList<DatesTuple>();
		String sql = "SELECT * from Dates where Date > ? ORDER BY date ASC";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setDate(1, new java.sql.Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(getTuple(rs));				
            }
			rs.close();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();		
		}
		return list;
	}	
}
