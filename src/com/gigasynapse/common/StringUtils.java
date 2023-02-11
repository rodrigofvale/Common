package com.gigasynapse.common;

import java.util.stream.IntStream;

public class StringUtils {
	public static String toLowerCaseNormilized(String text) {
		StringBuilder sb = new StringBuilder();
		StringBuilder sbWord = new StringBuilder();
		int[] codes = text.codePoints().toArray();
		boolean keepOriginal = false;
		boolean newWord = true;
		int countUpperCase = 0;
		for(int i = 0; i < codes.length; i++) {
			if (Character.isLetter(codes[i])) {
				sbWord.appendCodePoint(codes[i]);
				if (Character.isUpperCase(codes[i])) 
					countUpperCase++;
			} else {
				if (countUpperCase > 1) {
					sb.append(sbWord);
				} else {
					sb.append(sbWord.toString().toLowerCase());
				}
				sbWord.setLength(0);
				countUpperCase = 0;
				sb.appendCodePoint(codes[i]);
			}
		}
		if (countUpperCase > 1) {
			sb.append(sbWord);
		} else {
			sb.append(sbWord.toString().toLowerCase());
		}

		return sb.toString();
	}

	public static void main(String[] args) {
		System.out.print(
				StringUtils.toLowerCaseNormilized("Departamento Operações Mercado Aberto Banco Central ,DEMAB, Departamento Operações Mercado Aberto BC"));
	}
}
