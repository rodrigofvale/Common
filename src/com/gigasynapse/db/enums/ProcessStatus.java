package com.gigasynapse.db.enums;

public enum ProcessStatus {
	QUEUED(0), PROCESSING(1), SUCCESS(2), FAILTOLOAD(3), FAILTOSAVE(4), FAILTOCONNECT(5), FAILED(6), TIMEOUT(7), CACHED(8), INVALID (9), LINKS(10), BROWSED(11), OUTOFMEMORY(12), NOCHANGE(13);
	private int status = 0;
	
	public int getInt(){
		return this.status;
	}
	
	ProcessStatus(int status){
	    this.status = status;
	}	
}
// test 2
