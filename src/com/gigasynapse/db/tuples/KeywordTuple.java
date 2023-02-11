package com.gigasynapse.db.tuples;

import java.io.Serializable;

public class KeywordTuple  implements Serializable {
	public int id;
	public boolean isWord;
	public String text;
	
	public KeywordTuple(int id, String text, boolean isWord) {
		this.id = id;
		this.text = text;
		this.isWord = isWord;
	}
}
