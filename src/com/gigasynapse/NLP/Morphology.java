package com.gigasynapse.NLP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

public class Morphology {
	static String regex = "[^abcdefghijklmnopqrstuvwxyzàáâãäçèéêëìíîïñòóôõö"
			+ "ùúûüýÿABCDEFGHIJKLMNOPQRSTUVWXYZÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖ"
			+ "ÙÚÛÜÝŸ0123456789]+";
	
	public static String removeStopWords(String text) {		
		ArrayList<String> newString = new ArrayList<String>(); 
		HashSet<String> stopWords = new HashSet<String>();
		String[] words = {"de","do","da","em","e","ou","a","à","dos","das","in","no","na","nas","nos","ao","por","com"};
		stopWords.addAll(Arrays.asList(words));
		String kwds[] = text.trim().split(regex);
		for (int i = 0; i < kwds.length; i++) {
			if (!stopWords.contains(kwds[i].toLowerCase()))
				newString.add(kwds[i]);
		}
		return String.join(" ", newString);
	}
	
	public static String toSingular(String text) {
		String kwds[] = text.trim().split(regex);
		for (int i = 0; i < kwds.length; i++) {
			kwds[i] = singular(kwds[i]);
		}
		
		return String.join(" ", kwds);		
	}
	
	public static String toPlural(String text) {
		String regex = "[^abcdefghijklmnopqrstuvwxyzàáâãäçèéêëìíîïñòóôõö"
				+ "ùúûüýÿABCDEFGHIJKLMNOPQRSTUVWXYZÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖ"
				+ "ÙÚÛÜÝŸ0123456789]+";

		String kwds[] = text.trim().split(regex);
		for (int i = 0; i < kwds.length; i++) {
			kwds[i] = plural(kwds[i]);
		}
		
		return String.join(" ", kwds);		
	}
	
	public static String singular(String text) {
		// is plural?
		String rule1 = "(ais|uéis|óis)$";
		if (Pattern.compile(rule1).matcher(text).find()) {
			return text.replaceAll("ais$", "al").replaceAll("uéis$", "uel").replaceAll("óis$", "ol");
		}
		
		String rule2 = "res$";
		if (Pattern.compile(rule2).matcher(text).find()) {
			return text.replaceAll(rule2, "r");
		}
		
		String rule3 = "zes$";
		if (Pattern.compile(rule3).matcher(text).find()) {
			return text.replaceAll(rule3, "z");
		}
		
		String rule4 = "ses$";
		if (Pattern.compile(rule4).matcher(text).find()) {
			return text.replaceAll(rule4, "s");
		}
		
		String rule5 = "(ões|ãos|ães)$";
		if (Pattern.compile(rule5).matcher(text).find()) {
			return text.replaceAll(rule5, "ão");
		}
		
		String rule6 = "(is|eis)$";
		if (Pattern.compile(rule6).matcher(text).find()) {
			return text.replaceAll(rule6, "il");
		}
		
		String rule7 = "ins$";
		if (Pattern.compile(rule7).matcher(text).find()) {
			return text.replaceAll(rule7, "im");
		}
		
		String rule8 = "ens$";
		if (Pattern.compile(rule8).matcher(text).find()) {
			return text.replaceAll(rule8, "en");
		}
		
		String rule9 = "ons$";
		if (Pattern.compile(rule9).matcher(text).find()) {
			return text.replaceAll(rule9, "om");
		}
		
		String rule10 = "([aeiou])s$";
		if (Pattern.compile(rule10).matcher(text).find()) {
			return text.replaceAll(rule10, "$1");
		}
		
		return text;
	}
	
	public static String plural(String text) {
		
		if (text.equals("de")) {
			return "de";
		}
		if (text.equals("em")) {
			return "em";
		}
		if (text.equals("e")) {
			return "e";
		}
		if (text.equals("ou")) {
			return "ou";
		}
		if (text.equals("por")) {
			return "por";
		}
		if (text.equals("entre")) {
			return "entre";
		}
		
		if (text.equals("para")) {
			return "para";
		}
		if (text.equals("órfão")) {
			return "órfãos";
		}
		if (text.equals("sótão")) {
			return "sótãos";
		}
		if (text.equals("órgão")) {
			return "órgãos";
		}
		if (text.equals("cidadão")) {
			return "cidadãos";
		}
		if (text.equals("irmão")) {
			return "irmãos";
		}
		if (text.equals("cristão")) {
			return "cristãos";
		}
		if (text.equals("grão")) {
			return "grãos";
		}
		if (text.equals("chão")) {
			return "chãos";
		}
		if (text.equals("vão")) {
			return "vãos";
		}
		if (text.equals("acórdão")) {
			return "acórdãos";
		}		
		
		if (text.equals("alemão")) {
			return "alemães";
		}		
		if (text.equals("cão")) {
			return "cães";
		}		
		if (text.equals("capitão")) {
			return "capitães";
		}		
		if (text.equals("charlatão")) {
			return "charlatães";
		}		
		if (text.equals("escrivão")) {
			return "escrivães";
		}		
		if (text.equals("guardião")) {
			return "guardiães";
		}		
		if (text.equals("pão")) {
			return "pães";
		}		
		if (text.equals("tabelião")) {
			return "tabeliães";
		}		
		
		String rule1 = "(a|e)r$";
		if (Pattern.compile(rule1).matcher(text).find()) {
			return text.replaceAll(rule1, "$1res");
		}
		
		String rule2= "(a|e|i|o|u)z$";
		if (Pattern.compile(rule2).matcher(text).find()) {
			return text.replaceAll(rule2, "$1zes");
		}
		
		String rule3 = "ís$";
		if (Pattern.compile(rule3).matcher(text).find()) {
			return text.replaceAll(rule3, "íses");
		}
		
		String rule4 = "(é|ê)s$";
		if (Pattern.compile(rule4).matcher(text).find()) {
			return text.replaceAll(rule4, "eses");
		}
		
		String rule5 = "ão$";
		if (Pattern.compile(rule5).matcher(text).find()) {
			return text.replaceAll(rule5, "ões");
		}
		
		String rule6 = "al$";
		if (Pattern.compile(rule6).matcher(text).find()) {
			return text.replaceAll(rule6, "ais");
		}
		
		String rule7 = "el$";
		if (Pattern.compile(rule7).matcher(text).find()) {
			return text.replaceAll(rule7, "éis");
		}

		String rule8 = "ol$";
		if (Pattern.compile(rule8).matcher(text).find()) {
			return text.replaceAll(rule8, "óis");
		}

		String rule9 = "ul$";
		if (Pattern.compile(rule9).matcher(text).find()) {
			return text.replaceAll(rule9, "uis");
		}
				
		String rule10 = "il$";
		if (Pattern.compile(rule10).matcher(text).find()) {
			return text.replaceAll(rule10, "is");
		}
		
		String rule11 = "m$";
		if (Pattern.compile(rule11).matcher(text).find()) {
			return text.replaceAll(rule11, "ns");
		}
	
		String rule12 = "n$";
		if (Pattern.compile(rule12).matcher(text).find()) {
			return text.replaceAll(rule12, "nes");
		}
		String rule13 = "([aeiou])$";
		if (Pattern.compile(rule13).matcher(text).find()) {
			return text.replaceAll(rule13, "$1s");
		}
		return text;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(Morphology.singular("varais"));
		System.out.println(Morphology.singular("troféus"));
		System.out.println(Morphology.singular("mulheres"));
		System.out.println(Morphology.singular("avestruzes"));
		System.out.println(Morphology.singular("portugueses"));
		System.out.println(Morphology.singular("opiniões"));
		System.out.println(Morphology.singular("orfãos"));
		System.out.println(Morphology.singular("capitães"));
		System.out.println(Morphology.singular("canis"));
		System.out.println(Morphology.singular("fósseis"));
		System.out.println(Morphology.singular("jardins"));
		System.out.println(Morphology.singular("pólens"));
		System.out.println(Morphology.singular("batons"));
		System.out.println(Morphology.singular("batom"));
		System.out.println(Morphology.singular("varais"));
		System.out.println(Morphology.singular("aluguéis"));
		System.out.println(Morphology.singular("lençóis"));
		System.out.println(Morphology.singular("pauis"));
		System.out.println(Morphology.singular("sexuais"));
		System.out.println(Morphology.singular("anomalias"));
		System.out.println("--");
		System.out.println(Morphology.plural("hamburguer"));
		System.out.println(Morphology.plural("açúcar"));
		System.out.println(Morphology.plural("gravidez"));
		System.out.println(Morphology.plural("rapaz"));
		System.out.println(Morphology.plural("país"));
		System.out.println(Morphology.plural("revés"));
		System.out.println(Morphology.plural("freguês"));
		System.out.println(Morphology.plural("acórdão"));
		System.out.println(Morphology.plural("pão"));
		System.out.println(Morphology.plural("varal"));
		System.out.println(Morphology.plural("aluguel"));
		System.out.println(Morphology.plural("lençol"));
		System.out.println(Morphology.plural("paul"));
		System.out.println(Morphology.plural("míssil"));
		System.out.println(Morphology.plural("jardim"));
		System.out.println(Morphology.plural("bombom"));
		System.out.println(Morphology.plural("pólen"));
		
		System.out.println(Morphology.plural("advocacia"));
		
		System.out.println(Morphology.removeStopWords("rodrigo DE freitas"));
		
	}
}
