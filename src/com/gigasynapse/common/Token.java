package com.gigasynapse.common;

public class Token {
	public String text;
	public boolean isWord;
	public boolean hasSpaceBefore;
	public boolean hasSpaceAfter;
	public int start;
	public int end;
	
	public Token() {
		text = "";
		isWord = false;
		hasSpaceBefore = false;
		hasSpaceAfter = false;				
	}
	
	public void reset() {
		text = "";
		isWord = false;
		hasSpaceBefore = false;
		hasSpaceAfter = false;		
	}
	
	public Token(String text, Boolean isWord) {
		this.text = text;
		this.isWord = isWord;		
		hasSpaceBefore = false;
		hasSpaceAfter = false;
	}
}