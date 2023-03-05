package com.gigasynapse.common;

public class Tokenize {
	public char[] text;
	public int beginIndex = 0;
	public int endIndex = 0;
	public boolean isPunc;
	private Token token = new Token();
	private StringBuilder sb = new StringBuilder();;
	private final String punctuation = "\n~!@#$%^&*()_+`-={}|[]\\:\";'<>?,./¡";
	private String characters;
			
	public Tokenize() {
		characters = 
				"0123456789" +
				"abcdefghijklmnopqrstuvwxyzàáâãäçèéêëìíîïñòóôõöùúûüýÿ" +
				"ABCDEFGHIJKLMNOPQRSTUVWXYZÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝŸ";  
	}
	
	public void set(String text) {
		this.text = text.toCharArray();
		endIndex = 0;
	}
	
	public boolean hasNext() {
		return (text.length > endIndex);
	}
	
	public Token nextWord() {
		while (hasNext()) {
			Token token = next();
			if (token.isWord) {
				return token;
			}
		}
		return null;
	}
	
	public Token next() {
		token.reset();
		boolean isWord = true;
		char c;
		token.hasSpaceBefore = false;
		StringBuilder sb = new StringBuilder();
		sb.setLength(0);
		
		while ((endIndex < text.length) && !isValid(text[endIndex])) {
			token.hasSpaceBefore = true;
			endIndex++;
		}
		beginIndex = endIndex;
		
		while ((endIndex < text.length) && isValid(text[endIndex])) {
			c = text[endIndex];
			endIndex++;
			if (!isWord(c)) {
				if ((beginIndex + 1) == endIndex) {
					isWord = false;
				} else {
					endIndex--;
				}
				break;
			}
		}	
		
		if ((endIndex + 1) < text.length) {
			token.hasSpaceAfter = (!isValid(text[endIndex]));
		}
		token.isWord = isWord;
		if (!token.isWord) {
			if (token.hasSpaceBefore) {
				sb.append(" ");
			}
			sb.append(text, beginIndex, endIndex - beginIndex);
			if (token.hasSpaceAfter) {
				sb.append(" ");
			}
		} else {
			sb.append(text, beginIndex, endIndex - beginIndex);	
		}
		token.start = beginIndex;
		token.end = endIndex;
		token.text = sb.toString();
		return token;
	}
	
	public boolean isValid(char c) {
		return (characters.indexOf(c) >= 0) || (punctuation.indexOf(c) >= 0); 
	}
	
	public boolean isPunctuation() {
		return isPunc;
	}
	
	public boolean isWord(char c) {
		return (characters.indexOf(c) >= 0);		
	}
	
	public static void main(String[] args) throws Exception {
		String characters = 
				"0123456789abcdefghijklmnopqrstuvwxyzàáâãäçèéêëìíîïñòóôõöùúûüýÿ";  

		for(int i = 0; i < 0xFFFF; i++) {
			Character a = (char) i;			
				String t = "" + a;
				// this is a uppercase char
				if (!t.toLowerCase().equals(t)) {
					if (characters.indexOf(t.toLowerCase().charAt(0)) >= 0) { 					
					if (!t.toLowerCase().toUpperCase().equals(t)) {
						System.out.println(t + " " + t.toLowerCase() + " " + t.toLowerCase().toUpperCase());
						System.out.printf("%d %d %d\n", (int) t.charAt(0), (int) t.toLowerCase().charAt(0), (int) t.toLowerCase().toUpperCase().charAt(0));
					}
					}
				}
		}
	}

}