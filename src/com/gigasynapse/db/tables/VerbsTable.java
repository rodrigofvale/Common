package com.gigasynapse.db.tables;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.logging.Logger;

import com.gigasynapse.db.WebCrawlerDB;
import com.gigasynapse.db.tuples.DatesTuple;
import com.gigasynapse.db.tuples.VerbTuple;

public class VerbsTable {
	private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public static VerbTuple getTuple(ResultSet rs) {
		try {
			return new VerbTuple(rs.getString("verbo"), 
					rs.getString("gerundio"), rs.getString("participioPassado"), 
					rs.getString("infinitido"), rs.getString("modo"), 
					rs.getString("tempo") , rs.getString("pessoa"), 
					rs.getString("conjugacao")
			);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return null;	
	}
	
	public static ArrayList<VerbTuple> list() {
		ArrayList<VerbTuple> list = new ArrayList<VerbTuple>();
		String sql = "SELECT * from Verbos WHERE tempo IN ('Pretérito Imperfeito', 'Pretérito Perfeito', 'Pretérito Imperfeito', 'Futuro', 'Pretérito Perfeito', 'Pretérito Imperfeito') AND pessoa in ('eu','ele','nós','eles')";
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
}
