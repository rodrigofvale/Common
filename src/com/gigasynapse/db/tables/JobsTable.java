package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.enums.ProcessStatus;
import com.gigasynapse.db.tuples.JobsTuple;

public class JobsTable {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public static JobsTuple getTuple(ResultSet rs) {
		try {
			return new JobsTuple(
					rs.getInt("id"),
					rs.getString("name"),
					new Date(rs.getTimestamp("created").getTime()),
					new Date(rs.getTimestamp("lastPing").getTime()),
					ProcessStatus.values()[rs.getInt("status")],
					new JSONObject(rs.getString("json"))
			);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return null;	
	}
	
	public static JobsTuple create(String name, ProcessStatus status, JSONObject json) {
		String sql = "INSERT INTO Jobs (name, created, lastPing, status, json) VALUES (?, now(), now(), ?, ?)";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, name);
			pstmt.setInt(2, status.getInt());
			pstmt.setString(3, json.toString());
			pstmt.executeUpdate();
			
			ResultSet rs = pstmt.getGeneratedKeys();
			if (rs.next()) {
				JobsTuple jobsTuple = get(rs.getInt(1));
				rs.close();
				pstmt.close();
				return jobsTuple;
			}		
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void set(JobsTuple item) {
		String sql = "REPLACE INTO Jobs (id, name, created, lastPing, status, json) VALUES (?, ?, ?, now(), ?, ?)";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, item.id);
			pstmt.setString(2, item.name);
			pstmt.setTimestamp(3, new java.sql.Timestamp(item.created.getTime()));
			pstmt.setInt(4, item.status.getInt());
			pstmt.setString(5, item.json.toString());
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static JobsTuple get(int id) {
		String sql = "SELECT * from Jobs WHERE id = ?";
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, id);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				JobsTuple jobsTuple = getTuple(rs);
				rs.close();
				pstmt.close();
				return jobsTuple;				
            }
		} catch (SQLException e) {			
			e.printStackTrace();		
		}
		return null;
	}
	
	public static void ping(int id) {
		String sql = "UPDATE Jobs SET lastPing = now() WHERE id = ?";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);			
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public static void wait(int id) {
		while(true) {
			try {
				Thread.sleep(1000);
				JobsTuple job = JobsTable.get(id);
				if (job.status != ProcessStatus.PROCESSING) {
					return;
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void delAll(String name) {
		String sql = "DELETE FROM Jobs WHERE name = ?";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);			
			pstmt.setString(1, name);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public static void setStatus(int id, ProcessStatus status) {
		String sql = "UPDATE Jobs SET status = ?, lastPing = now() WHERE id = ?";		
		try {
			PreparedStatement pstmt = WebCrawlerDB.getInstance().getConnection().prepareStatement(sql);
			pstmt.setInt(1, status.getInt());
			pstmt.setInt(2, id);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}	
}
