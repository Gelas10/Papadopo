package main;

public class MutableDouble {

	private volatile double value;
	
	public MutableDouble(){value=0;}
	public MutableDouble(double x){value=x;}
	
	public synchronized void incrementBy(double x){ value+=x; }
	public double get(){ return value; }
}
