package com.gigasynapse.common;

public class NLP {
	public static String[] getPhrases(String text) {
		if (text == null) {
			return new String[0];
		}
		String aux = text.replaceAll("\\n+", ". ");
//		String aux = text.replaceAll("\\n", ". ");
		aux = aux.replaceAll("\\s+", " ");
		return aux.split("[.?!]\\s");		
	}

}
