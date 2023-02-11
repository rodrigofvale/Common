package com.gigasynapse.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class DBTable {
	protected Connection conn = null;
	
	protected void execute(String sql) {
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
	
	protected abstract void connect();
	
}
