package main;

public class SharedDouble {

	private volatile double value;
	
	public SharedDouble(){value=0;}
	public SharedDouble(double x){value=x;}
	
	public synchronized void incrementBy(double x){ value+=x; }
	public double get(){ return value; }
}
