package com.gigasynapse.db.enums;

public enum ArticleStage {
	QUEUED(0), 
	TOBEIGNORED(1), 
	SAMECONTENT(2), 
	TBD4(3), 
	READYTOINDEX(4), 
	TBD1(5), 
	TBD2(6), 
	INDEXED(7), 
	SUMMARYFAILED(8), 
	TOBEDELETED(9),
	DELETED(10);
	
	private int status = 0;	
	public int getInt(){
		return this.status;
	}
	
	ArticleStage(int status){
	    this.status = status;
	}	
}
// test 2
