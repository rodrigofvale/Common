package com.gigasynapse.db.tuples;

public class VerbTuple {
	public String verbo;
	public String gerundio;
	public String participioPassado;
	public String infinitido;
	public String modo;
	public String tempo;
	public String pessoa;
	public String conjugacao;
	
	public VerbTuple(String verbo, String gerundio, 
			String participioPassado, String infinitido, String modo, 
			String tempo, String pessoa, String conjugacao) {
		this.verbo = verbo;
		this.gerundio = gerundio;
		this.participioPassado = participioPassado;
		this.infinitido = infinitido;
		this.modo = modo;
		this.tempo = tempo;
		this.pessoa = pessoa;
		this.conjugacao	= conjugacao; 
	}
}
