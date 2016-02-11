package main;

public class SharedDouble {

	private double value;
	
	public SharedDouble(){value=0;}
	public SharedDouble(double x){value=x;}
	public void incrementBy(double x){ value+=x; }
	public double get(){ return value; }
	
}
