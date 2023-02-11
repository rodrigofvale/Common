package com.gigasynapse.db.tables;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import com.gigasynapse.common.ServiceConfig;
import com.gigasynapse.db.enums.ProcessStatus;
import com.gigasynapse.db.tuples.ArticleTuple;
import com.gigasynapse.db.tuples.PipelineTuple;

public class PipelineTable {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	private Connection conn = null;
	private HashMap<String, PipelineTuple> pipeline = new HashMap<String, PipelineTuple>(); 
	
/*	
	CREATE TABLE "stats" (
			"TimeStamp"	INTEGER,
			"Status"	INTEGER,
			"Count"	INTEGER,
			PRIMARY KEY("TimeStamp")
		);
		
CREATE TABLE "stats" (
	"TimeStamp"	INTEGER,
	"Status"	INTEGER,
	"Count"	INTEGER,
	PRIMARY KEY("TimeStamp","Status")
);

insert into stats
Select strftime('%s'), status, count(*) from Pipeline Group by status		
*/	

	public PipelineTable(int id) {	
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	String dbFolder = ServiceConfig.getInstance().obj.getJSONObject("crawler").getString("dbFolder");    	
    	File aux = new File(dbFolder);
    	aux.mkdirs();
    	
        String url = String.format("jdbc:sqlite:%s/Website-%d.db", dbFolder, id);

        try {
        	conn = DriverManager.getConnection(url);
            if (conn != null) {
            	String sql =
            		"CREATE TABLE IF NOT EXISTS 'Pipeline' ( " +
            		"	'url'	    TEXT, " +
            		"	'status'	INTEGER, " +
            		"	'depth'	    INTEGER, " +
            		"	'obs'	    TEXT, " +
            		"	PRIMARY KEY('url'));";
            	Statement stmt = conn.createStatement();
        		stmt.execute(sql);
        		stmt.close();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }        
	}
	
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public synchronized void clear() {		
		String sql = "DELETE FROM Pipeline";
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
	

	private static PipelineTuple ResultSet2Pipeline(ResultSet rs) {
		try {
			return new PipelineTuple(rs.getString("url"), ProcessStatus.values()[rs.getInt("status")], rs.getInt("depth"), rs.getString("obs"));
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}
	
	public synchronized PipelineTuple get(String url) {
		String sql = "SELECT * FROM Pipeline WHERE url = ?";
		try {
			PreparedStatement pstmt  = conn.prepareStatement(sql);
			pstmt.setString(1, url);			
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				PipelineTuple pipelineTuple = ResultSet2Pipeline(rs);
				pstmt.close();
				return pipelineTuple;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;		
	}	
	
	public synchronized void bulkSet(ArrayList<PipelineTuple> list) {
		if (list.size() == 0) {
			return;
		}
		String sql = "REPLACE INTO Pipeline (url, status, depth, obs) VALUES "; 
		for(int i = 0; i < list.size(); i++) {
			if ((i + 1) == list.size()) {
				sql += "( ?, ?, ?, ?)";
			} else {
				sql += "( ?, ?, ?, ?), ";
			}
		}
		
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			int count = 1;
			for (int i = 0; i < list.size(); i++) {
				PipelineTuple item = list.get(i);
				pstmt.setString(count++, item.url);
				pstmt.setInt(count++, item.status.getInt());
				pstmt.setInt(count++, item.depth);
				pstmt.setString(count++, item.obs);				
			}
			pstmt.executeUpdate();
			pstmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.severe(String.valueOf(sql.length()));
			LOGGER.severe(sql);			
		}
	}

	public synchronized ArrayList<String> bulkGet(ArrayList<String> list) {
		ArrayList<String> existing = new ArrayList<String>(); 
		if (list.size() > 0) {
			String sql = "SELECT * FROM Pipeline WHERE url IN ("; 
			for(int i = 0; i < list.size(); i++) {
				if ((i + 1) == list.size()) {
					sql += "?)";
				} else {
					sql += "?, ";
				}
			}

			try {
				PreparedStatement pstmt = conn.prepareStatement(sql);
				int count = 1;
				for (int i = 0; i < list.size(); i++) {
					pstmt.setString(count++, list.get(i));
				}
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					PipelineTuple pipelineTuple = ResultSet2Pipeline(rs);
					existing.add(pipelineTuple.url);
				}
				pstmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return existing; 
	}
	
	public synchronized void set(PipelineTuple item) {
		String sql = "";
		PreparedStatement pstmt;
		try {		
			sql = "REPLACE INTO Pipeline (url, status, depth, obs) VALUES( ?, ?, ?, ?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, item.url);
			pstmt.setInt(2, item.status.getInt());
			pstmt.setInt(3, item.depth);
			pstmt.setString(4, item.obs);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();			
		}		
	}
	
	public synchronized PipelineTuple next() {
		ArrayList<ArticleTuple> list = new ArrayList<ArticleTuple>();
		String sql = "SELECT * FROM Pipeline WHERE status = ? LIMIT 1";
		Statement stmt;
		try {
			PreparedStatement pstmt  = conn.prepareStatement(sql);
			pstmt.setInt(1, ProcessStatus.QUEUED.getInt());
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				PipelineTuple pipelineTuple = ResultSet2Pipeline(rs);
				pipelineTuple.status = ProcessStatus.PROCESSING;
				set(pipelineTuple);
				pstmt.close();
				return pipelineTuple;				
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return null;        
	}
	
}
